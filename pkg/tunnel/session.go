package tunnel

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	libquic "github.com/quic-go/quic-go"
	"go.uber.org/zap"

	"github.com/sambigeara/pollen/pkg/quic"
	"github.com/sambigeara/pollen/pkg/types"
)

var (
	ErrSessionClosed  = errors.New("session closed")
	ErrSessionExists  = errors.New("session already exists for peer")
	ErrInvalidPortLen = errors.New("invalid port length in stream header")
	ErrNoServicePort  = errors.New("no service port in stream header")
)

// Session wraps a QUIC connection to a peer for multiplexed stream tunneling.
// Each stream carries a 2-byte service port header followed by bidirectional data.
type Session struct {
	conn      *quic.Conn
	closeCh   chan struct{}
	logger    *zap.SugaredLogger
	closeOnce sync.Once
	peerID    types.PeerKey
}

func newSession(conn *quic.Conn, peerID types.PeerKey) *Session {
	return &Session{
		conn:    conn,
		peerID:  peerID,
		closeCh: make(chan struct{}),
		logger:  zap.S().Named("session").With("peer", peerID.String()[:8]),
	}
}

// OpenStream opens a new QUIC stream to the given service port.
// The port is sent as the first 2 bytes (big-endian uint16) on the stream.
func (s *Session) OpenStream(servicePort uint16) (net.Conn, error) {
	select {
	case <-s.closeCh:
		return nil, ErrSessionClosed
	default:
	}

	stream, err := s.conn.OpenStreamSync(context.Background())
	if err != nil {
		return nil, err
	}

	// Write the service port as header.
	var header [2]byte
	binary.BigEndian.PutUint16(header[:], servicePort)
	if _, err := stream.Write(header[:]); err != nil {
		stream.Close()
		return nil, err
	}

	s.logger.Debugw("opened stream", "port", servicePort)
	return &streamConn{Stream: stream}, nil
}

// AcceptStream accepts an incoming QUIC stream and returns the target service port.
func (s *Session) AcceptStream(ctx context.Context) (net.Conn, uint16, error) {
	stream, err := s.conn.AcceptStream(ctx)
	if err != nil {
		return nil, 0, err
	}

	// Read the service port header.
	var header [2]byte
	if _, err := io.ReadFull(stream, header[:]); err != nil {
		stream.Close()
		return nil, 0, err
	}

	port := binary.BigEndian.Uint16(header[:])
	if port == 0 {
		stream.Close()
		return nil, 0, ErrNoServicePort
	}

	s.logger.Debugw("accepted stream", "port", port)
	return &streamConn{Stream: stream}, port, nil
}

// Close tears down the session and underlying QUIC connection.
func (s *Session) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closeCh)
		err = s.conn.Close()
		s.logger.Info("session closed")
	})
	return err
}

// IsClosed returns true if the session has been closed.
func (s *Session) IsClosed() bool {
	select {
	case <-s.closeCh:
		return true
	default:
		return false
	}
}

// PeerID returns the peer's identity key.
func (s *Session) PeerID() types.PeerKey {
	return s.peerID
}

// streamConn wraps a quic.Stream to implement net.Conn for compatibility with Bridge().
type streamConn struct {
	*libquic.Stream
}

func (s *streamConn) LocalAddr() net.Addr  { return stubAddr{} }
func (s *streamConn) RemoteAddr() net.Addr { return stubAddr{} }

func (s *streamConn) CloseWrite() error {
	return s.Close()
}

func (s *streamConn) CloseRead() error {
	s.CancelRead(0)
	return nil
}

type stubAddr struct{}

func (stubAddr) Network() string { return "quic" }
func (stubAddr) String() string  { return "quic-stream" }

// StreamHandler is called for each incoming stream on a session.
type StreamHandler func(stream net.Conn, servicePort uint16)

// SessionManager manages per-peer sessions backed by QUIC connections.
type SessionManager struct {
	sessions     map[types.PeerKey]*Session
	waiters      map[types.PeerKey][]chan struct{}
	handler      StreamHandler
	onDisconnect func(types.PeerKey)
	logger       *zap.SugaredLogger
	mu           sync.RWMutex
}

// NewSessionManager creates a new session manager.
func NewSessionManager(handler StreamHandler) *SessionManager {
	return &SessionManager{
		sessions: make(map[types.PeerKey]*Session),
		waiters:  make(map[types.PeerKey][]chan struct{}),
		handler:  handler,
		logger:   zap.S().Named("sessions"),
	}
}

// SetOnDisconnect sets a callback invoked when a session is removed.
func (sm *SessionManager) SetOnDisconnect(fn func(types.PeerKey)) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.onDisconnect = fn
}

// Register registers a session from a QUIC connection.
func (sm *SessionManager) Register(conn *quic.Conn, peerID types.PeerKey) (*Session, error) {
	sm.mu.Lock()

	session := newSession(conn, peerID)

	if existing, ok := sm.sessions[peerID]; ok {
		sm.logger.Warnw("replacing existing session", "peer", peerID.String()[:8])
		existing.Close()
		delete(sm.sessions, peerID)
	}

	sm.sessions[peerID] = session
	waiters := sm.waiters[peerID]
	delete(sm.waiters, peerID)
	sm.mu.Unlock()

	for _, ch := range waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	sm.logger.Infow("registered session", "peer", peerID.String()[:8])
	go sm.acceptStreams(session)
	return session, nil
}

// RegisterOutbound is an alias for Register (no client/server distinction in QUIC).
func (sm *SessionManager) RegisterOutbound(conn *quic.Conn, peerID types.PeerKey) (*Session, error) {
	return sm.Register(conn, peerID)
}

// GetOrCreate returns an existing session or waits for one to appear.
func (sm *SessionManager) GetOrCreate(ctx context.Context, peerID types.PeerKey) (*Session, error) {
	sm.mu.RLock()
	session, ok := sm.sessions[peerID]
	sm.mu.RUnlock()

	if ok && !session.IsClosed() {
		return session, nil
	}

	return sm.waitForSession(ctx, peerID)
}

func (sm *SessionManager) waitForSession(ctx context.Context, peerID types.PeerKey) (*Session, error) {
	for {
		sm.mu.Lock()
		if session, ok := sm.sessions[peerID]; ok && !session.IsClosed() {
			sm.mu.Unlock()
			return session, nil
		}
		ch := make(chan struct{}, 1)
		sm.waiters[peerID] = append(sm.waiters[peerID], ch)
		sm.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			sm.removeWaiter(peerID, ch)
			return nil, ctx.Err()
		}
	}
}

func (sm *SessionManager) removeWaiter(peerID types.PeerKey, ch chan struct{}) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	waiters := sm.waiters[peerID]
	for i := range waiters {
		if waiters[i] != ch {
			continue
		}
		waiters[i] = waiters[len(waiters)-1]
		waiters = waiters[:len(waiters)-1]
		break
	}

	if len(waiters) == 0 {
		delete(sm.waiters, peerID)
		return
	}
	sm.waiters[peerID] = waiters
}

func (sm *SessionManager) acceptStreams(session *Session) {
	ctx := context.Background()
	for {
		stream, port, err := session.AcceptStream(ctx)
		if err != nil {
			if !session.IsClosed() {
				sm.logger.Warnw("accept stream error", "peer", session.peerID.String()[:8], "err", err)
			}
			sm.removeIfSame(session)
			return
		}

		if sm.handler != nil {
			go sm.handler(stream, port)
		} else {
			stream.Close()
		}
	}
}

func (sm *SessionManager) removeIfSame(session *Session) bool {
	sm.mu.Lock()
	current, ok := sm.sessions[session.peerID]
	if !ok || current != session {
		sm.mu.Unlock()
		return false
	}

	delete(sm.sessions, session.peerID)
	cb := sm.onDisconnect
	sm.mu.Unlock()

	_ = session.Close()
	sm.logger.Infow("removed session", "peer", session.peerID.String()[:8])
	if cb != nil {
		cb(session.peerID)
	}

	return true
}

// Remove closes and removes a session.
func (sm *SessionManager) Remove(peerID types.PeerKey) {
	sm.mu.Lock()
	session, ok := sm.sessions[peerID]
	cb := sm.onDisconnect
	if ok {
		session.Close()
		delete(sm.sessions, peerID)
	}
	sm.mu.Unlock()

	if ok {
		sm.logger.Infow("removed session", "peer", peerID.String()[:8])
		if cb != nil {
			cb(peerID)
		}
	}
}

// Close closes all sessions.
func (sm *SessionManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for peerID, session := range sm.sessions {
		session.Close()
		delete(sm.sessions, peerID)
	}
	sm.logger.Info("all sessions closed")
}
