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

// DialFunc is called by SessionManager to establish the underlying connection.
type DialFunc func(ctx context.Context, peerID types.PeerKey) (net.Conn, error)

// StreamHandler is called for each incoming stream on a session.
type StreamHandler func(stream net.Conn, servicePort uint16)

type sessionDial struct {
	done    chan struct{}
	session *Session
	err     error
	cancel  context.CancelFunc
	once    sync.Once
}

func newSessionDial(cancel context.CancelFunc) *sessionDial {
	return &sessionDial{done: make(chan struct{}), cancel: cancel}
}

func (d *sessionDial) resolve(session *Session, err error) {
	d.once.Do(func() {
		d.session = session
		d.err = err
		close(d.done)
	})
}

// SessionManager manages per-peer sessions.
type SessionManager struct {
	dial     DialFunc
	sessions map[types.PeerKey]*Session
	inflight map[types.PeerKey]*sessionDial
	handler  StreamHandler
	logger   *zap.SugaredLogger
	mu       sync.RWMutex
}

// NewSessionManager creates a new session manager.
// dial is called when a new session needs to be established.
// handler is called for each incoming stream on any session.
func NewSessionManager(dial DialFunc, handler StreamHandler) *SessionManager {
	return &SessionManager{
		dial:     dial,
		sessions: make(map[types.PeerKey]*Session),
		inflight: make(map[types.PeerKey]*sessionDial),
		handler:  handler,
		logger:   zap.S().Named("sessions"),
	}
}

// GetOrCreate returns an existing session or creates a new one.
// If the session doesn't exist, it uses the dial function to establish one.
func (sm *SessionManager) GetOrCreate(ctx context.Context, peerID types.PeerKey) (*Session, error) {
	// Fast path: check for existing session
	sm.mu.RLock()
	session, ok := sm.sessions[peerID]
	sm.mu.RUnlock()

	if ok && !session.IsClosed() {
		return session, nil
	}

	// Slow path: need to establish new session
	sm.mu.Lock()
	// Double-check after acquiring write lock
	if session, ok := sm.sessions[peerID]; ok && !session.IsClosed() {
		sm.mu.Unlock()
		return session, nil
	}

	if dial, ok := sm.inflight[peerID]; ok {
		sm.mu.Unlock()
		select {
		case <-dial.done:
			if dial.session != nil && !dial.session.IsClosed() {
				return dial.session, nil
			}
			if dial.err != nil {
				return nil, dial.err
			}
			return nil, ErrSessionClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Clean up any closed session
	if session, ok := sm.sessions[peerID]; ok {
		delete(sm.sessions, peerID)
		session.Close() // Ensure cleanup
	}

	dialCtx, cancel := context.WithCancel(ctx)
	dial := newSessionDial(cancel)
	sm.inflight[peerID] = dial
	sm.mu.Unlock()

	sm.logger.Infow("establishing new session", "peer", peerID.String()[:8])

	conn, err := sm.dial(dialCtx, peerID)
	cancel()

	var newSess *Session
	if err == nil {
		newSess, err = newClientSession(conn, peerID)
	}

	sm.mu.Lock()
	if existing, ok := sm.sessions[peerID]; ok && !existing.IsClosed() {
		if newSess != nil {
			newSess.Close()
		}
		dial.resolve(existing, nil)
		if cur, ok := sm.inflight[peerID]; ok && cur == dial {
			delete(sm.inflight, peerID)
		}
		sm.mu.Unlock()
		return existing, nil
	}
	if existing, ok := sm.sessions[peerID]; ok {
		delete(sm.sessions, peerID)
		existing.Close()
	}
	if err == nil && newSess != nil {
		sm.sessions[peerID] = newSess
		sm.logger.Infow("session established", "peer", peerID.String()[:8])
	}
	dial.resolve(newSess, err)
	if cur, ok := sm.inflight[peerID]; ok && cur == dial {
		delete(sm.inflight, peerID)
	}
	sm.mu.Unlock()

	return newSess, err
}

// Accept registers a session initiated by a remote peer.
// The connection should already be authenticated.
func (sm *SessionManager) Accept(conn net.Conn, peerID types.PeerKey) (*Session, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// If we already have a session to this peer, close the old one
	if existing, ok := sm.sessions[peerID]; ok {
		sm.logger.Warnw("replacing existing session", "peer", peerID.String()[:8])
		existing.Close()
		delete(sm.sessions, peerID)
	}

	session, err := newServerSession(conn, peerID)
	if err != nil {
		return nil, err
	}

	sm.sessions[peerID] = session
	sm.logger.Infow("accepted session", "peer", peerID.String()[:8])
	if dial, ok := sm.inflight[peerID]; ok {
		if dial.cancel != nil {
			dial.cancel()
		}
		dial.resolve(session, nil)
		delete(sm.inflight, peerID)
	}

	// Start accepting streams in background
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
	defer sm.mu.Unlock()

	if session, ok := sm.sessions[peerID]; ok {
		session.Close()
		delete(sm.sessions, peerID)
		sm.logger.Infow("removed session", "peer", peerID.String()[:8])
	}
}

// Get returns a session if it exists and is open.
func (sm *SessionManager) Get(peerID types.PeerKey) (*Session, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	session, ok := sm.sessions[peerID]
	if !ok || session.IsClosed() {
		return nil, false
	}
	return session, true
}

// Close closes all sessions.
func (sm *SessionManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for peerID, session := range sm.sessions {
		session.Close()
		delete(sm.sessions, peerID)
	}
	for peerID, dial := range sm.inflight {
		if dial.cancel != nil {
			dial.cancel()
		}
		dial.resolve(nil, ErrSessionClosed)
		delete(sm.inflight, peerID)
	}
	sm.logger.Info("all sessions closed")
}
