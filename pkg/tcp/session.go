package tcp

import (
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type key []byte

type Store struct {
	m           map[string]*Session
	mu          sync.Mutex
	inflightMgr *handshakeManager
}

func NewStore() *Store {
	return &Store{
		m: make(map[string]*Session),
		inflightMgr: &handshakeManager{
			inflightHandshakes: make(map[uint32]chan []byte),
		},
	}
}

func (s *Store) CreateSession(k key) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}

	sess := &Session{
		ln: listener,
	}

	s.m[hex.EncodeToString(k)] = sess

	return sess, nil
}

func (s *Store) Dial(k key, peerAddr string, cfg *tls.Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dialer := &net.Dialer{Timeout: 5 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", peerAddr, cfg)
	if err != nil {
		return err
	}

	sess := &Session{
		Conn: conn,
	}

	s.m[hex.EncodeToString(k)] = sess

	return nil
}

func (s *Store) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sess := range s.m {
		sess.close()
	}
}

func (s *Store) SetInflight(sessID uint32, c chan []byte) error {
	return s.inflightMgr.set(sessID, c)
}

func (s *Store) AckInflight(sessID uint32, msg []byte) error {
	ch, ok := s.inflightMgr.get(sessID)
	if !ok {
		return errors.New("unrecognised sessID")
	}

	go func() {
		ch <- msg
		s.inflightMgr.expire(sessID)
	}()

	return nil
}

func (s *Store) ExpireInflight(sessID uint32) {
	s.inflightMgr.expire(sessID)
}

func (s *Store) GetConn(k key) (net.Conn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.m[hex.EncodeToString(k)]
	if !ok || sess.Conn == nil {
		return nil, false
	}
	return sess.Conn, true
}

func (s *Session) GetAddrPort() string {
	_, port, _ := net.SplitHostPort(s.ln.Addr().String())
	return port
}

type handshakeManager struct {
	inflightHandshakes map[uint32]chan []byte
	mu                 sync.RWMutex
}

func (m *handshakeManager) set(sessID uint32, c chan []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.inflightHandshakes[sessID]; ok {
		// TODO(sam) probably should expire the last handshake and replace
		// but this requires thought
		return errors.New("handshake already exists")
	}

	m.inflightHandshakes[sessID] = c
	return nil
}

func (m *handshakeManager) get(sessID uint32) (chan []byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.inflightHandshakes[sessID]
	return c, ok
}

func (m *handshakeManager) expire(sessID uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.inflightHandshakes, sessID)
}

type Session struct {
	ln   net.Listener
	Conn net.Conn
}

func (s *Session) GetAddr() string {
	return s.ln.Addr().String()
}

func (s *Session) UpgradeToTLS(cfg *tls.Config) error {
	// TODO(saml) does the original listener need to stay open once the tls listener is created?
	tlsListener := tls.NewListener(s.ln, cfg)

	fmt.Println("tcp: waiting for TLS accept on", s.ln.Addr().String())
	conn, err := tlsListener.Accept()
	if err != nil {
		return err
	}
	fmt.Println("tcp: accepted TLS connection from", conn.RemoteAddr().String())

	s.Conn = conn

	return nil
}

func (s *Session) close() {
	// TODO(saml) graceful handling of errors
	if s.ln != nil {
		s.ln.Close()
	}
	if s.Conn != nil {
		s.Conn.Close()
	}
}
