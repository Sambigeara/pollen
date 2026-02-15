package quic

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

// Conn wraps a QUIC connection to a single peer. It provides methods for
// sending/receiving datagrams (for gossip, control) and opening/accepting
// streams (for service tunneling).
type Conn struct {
	qc      *quic.Conn
	log     *zap.SugaredLogger
	peerID  types.PeerKey
	onClose func()
	mu      sync.RWMutex
	closed  bool
}

// NewConn wraps an established QUIC connection.
func NewConn(qc *quic.Conn, peerID types.PeerKey) *Conn {
	return NewConnWithClose(qc, peerID, nil)
}

func NewConnWithClose(qc *quic.Conn, peerID types.PeerKey, onClose func()) *Conn {
	return &Conn{
		qc:      qc,
		peerID:  peerID,
		log:     zap.S().Named("quic.conn").With("peer", peerID.Short()),
		onClose: onClose,
	}
}

// SendDatagram sends an unreliable datagram to the peer.
func (c *Conn) SendDatagram(data []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return ErrConnClosed
	}
	return c.qc.SendDatagram(data)
}

// ReceiveDatagram blocks until a datagram is received or context is cancelled.
func (c *Conn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return c.qc.ReceiveDatagram(ctx)
}

// OpenStream opens a new bidirectional QUIC stream to the peer.
func (c *Conn) OpenStream() (*quic.Stream, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrConnClosed
	}
	return c.qc.OpenStream()
}

// OpenStreamSync opens a new bidirectional stream, blocking until a stream
// slot is available or the context expires.
func (c *Conn) OpenStreamSync(ctx context.Context) (*quic.Stream, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrConnClosed
	}
	return c.qc.OpenStreamSync(ctx)
}

// AcceptStream waits for and returns the next incoming stream.
func (c *Conn) AcceptStream(ctx context.Context) (*quic.Stream, error) {
	return c.qc.AcceptStream(ctx)
}

// PeerID returns the remote peer's identity key.
func (c *Conn) PeerID() types.PeerKey {
	return c.peerID
}

// QUICConn returns the underlying quic.Conn.
func (c *Conn) QUICConn() *quic.Conn {
	return c.qc
}

// Close terminates the connection with an application error.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	err := c.qc.CloseWithError(0, "connection closed")
	c.runCloseHookLocked()
	return err
}

// CloseWithError terminates the connection with a specific error code and message.
func (c *Conn) CloseWithError(code quic.ApplicationErrorCode, msg string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	err := c.qc.CloseWithError(code, msg)
	c.runCloseHookLocked()
	return err
}

// IsClosed returns whether the connection has been closed.
func (c *Conn) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

func (c *Conn) runCloseHookLocked() {
	if c.onClose == nil {
		return
	}
	onClose := c.onClose
	c.onClose = nil
	onClose()
}

// StreamConn adapts a quic.Stream to work with Bridge() by mapping
// QUIC stream errors to io.EOF for clean shutdown.
type StreamConn struct {
	*quic.Stream
}

// Read implements io.Reader, mapping QUIC stream reset to io.EOF.
func (s *StreamConn) Read(p []byte) (int, error) {
	n, err := s.Stream.Read(p)
	if err != nil {
		var streamErr *quic.StreamError
		if errors.As(err, &streamErr) {
			return n, io.EOF
		}
	}
	return n, err
}
