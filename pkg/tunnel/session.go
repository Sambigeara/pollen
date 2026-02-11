package tunnel

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/hashicorp/yamux"
	"go.uber.org/zap"

	"github.com/sambigeara/pollen/pkg/types"
)

var (
	ErrSessionClosed  = errors.New("session closed")
	ErrSessionExists  = errors.New("session already exists for peer")
	ErrInvalidPortLen = errors.New("invalid port length in stream header")
	ErrNoServicePort  = errors.New("no service port in stream header")
)

// Session wraps a long-lived multiplexed connection to a peer.
// All streams over this session share the same underlying TCP+TLS connection.
type Session struct {
	conn      net.Conn
	mux       *yamux.Session
	closeCh   chan struct{}
	logger    *zap.SugaredLogger
	closeOnce sync.Once
	peerID    types.PeerKey
	isClient  bool
}

// newClientSession creates a session as the initiator (client side of yamux).
func newClientSession(conn net.Conn, peerID types.PeerKey) (*Session, error) {
	cfg := yamux.DefaultConfig()
	cfg.LogOutput = io.Discard // Use zap instead

	mux, err := yamux.Client(conn, cfg)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Session{
		peerID:   peerID,
		conn:     conn,
		mux:      mux,
		isClient: true,
		closeCh:  make(chan struct{}),
		logger:   zap.S().Named("session").With("peer", peerID.String()[:8]),
	}, nil
}

// newServerSession creates a session as the responder (server side of yamux).
func newServerSession(conn net.Conn, peerID types.PeerKey) (*Session, error) {
	cfg := yamux.DefaultConfig()
	cfg.LogOutput = io.Discard

	mux, err := yamux.Server(conn, cfg)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Session{
		peerID:   peerID,
		conn:     conn,
		mux:      mux,
		isClient: false,
		closeCh:  make(chan struct{}),
		logger:   zap.S().Named("session").With("peer", peerID.String()[:8]),
	}, nil
}

// OpenStream opens a new multiplexed stream to the given service port.
// The port is sent as the first 2 bytes (big-endian uint16) on the stream.
func (s *Session) OpenStream(servicePort uint16) (net.Conn, error) {
	select {
	case <-s.closeCh:
		return nil, ErrSessionClosed
	default:
	}

	stream, err := s.mux.OpenStream()
	if err != nil {
		return nil, err
	}

	// Write the service port as header
	var header [2]byte
	binary.BigEndian.PutUint16(header[:], servicePort)
	if _, err := stream.Write(header[:]); err != nil {
		stream.Close()
		return nil, err
	}

	s.logger.Debugw("opened stream", "port", servicePort)
	return stream, nil
}

// AcceptStream accepts an incoming stream and returns the target service port.
func (s *Session) AcceptStream() (net.Conn, uint16, error) {
	stream, err := s.mux.AcceptStream()
	if err != nil {
		return nil, 0, err
	}

	// Read the service port header
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
	return stream, port, nil
}

// Close tears down the session and underlying connection.
func (s *Session) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closeCh)
		err = s.mux.Close()
		s.conn.Close()
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

// StreamHandler is called for each incoming stream on a session.
type StreamHandler func(stream net.Conn, servicePort uint16)

// SessionManager manages per-peer sessions.
type SessionManager struct {
	sessions     map[types.PeerKey]*Session
	waiters      map[types.PeerKey][]chan struct{}
	handler      StreamHandler
	onDisconnect func(types.PeerKey)
	logger       *zap.SugaredLogger
	mu           sync.RWMutex
}

// NewSessionManager creates a new session manager.
// handler is called for each incoming stream on any session.
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

// RegisterOutbound registers a session initiated locally.
func (sm *SessionManager) RegisterOutbound(conn net.Conn, peerID types.PeerKey) (*Session, error) {
	return sm.register(conn, peerID, true)
}

// GetOrCreate returns an existing session or waits for one to appear.
// It blocks until RegisterOutbound/Accept delivers a session, or ctx expires.
func (sm *SessionManager) GetOrCreate(ctx context.Context, peerID types.PeerKey) (*Session, error) {
	// Fast path: check for existing session.
	sm.mu.RLock()
	session, ok := sm.sessions[peerID]
	sm.mu.RUnlock()

	if ok && !session.IsClosed() {
		return session, nil
	}

	return sm.waitForSession(ctx, peerID)
}

// waitForSession blocks until an active session for peerID exists or ctx expires.
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

// Accept registers a session initiated by a remote peer.
// The connection should already be authenticated.
func (sm *SessionManager) Accept(conn net.Conn, peerID types.PeerKey) (*Session, error) {
	return sm.register(conn, peerID, false)
}

func (sm *SessionManager) register(conn net.Conn, peerID types.PeerKey, outbound bool) (*Session, error) {
	sm.mu.Lock()

	if existing, ok := sm.sessions[peerID]; ok {
		sm.logger.Warnw("replacing existing session", "peer", peerID.String()[:8])
		existing.Close()
		delete(sm.sessions, peerID)
	}

	var (
		session *Session
		err     error
	)
	if outbound {
		session, err = newClientSession(conn, peerID)
	} else {
		session, err = newServerSession(conn, peerID)
	}
	if err != nil {
		sm.mu.Unlock()
		return nil, err
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

	if outbound {
		sm.logger.Infow("registered outbound session", "peer", peerID.String()[:8])
	} else {
		sm.logger.Infow("accepted session", "peer", peerID.String()[:8])
	}

	go sm.acceptStreams(session)

	return session, nil
}

// acceptStreams runs the stream accept loop for a session.
func (sm *SessionManager) acceptStreams(session *Session) {
	for {
		stream, port, err := session.AcceptStream()
		if err != nil {
			if !session.IsClosed() {
				sm.logger.Warnw("accept stream error", "peer", session.peerID.String()[:8], "err", err)
			}
			sm.Remove(session.peerID)
			return
		}

		if sm.handler != nil {
			go sm.handler(stream, port)
		} else {
			stream.Close()
		}
	}
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
