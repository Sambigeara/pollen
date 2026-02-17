package tunnel

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
	"go.uber.org/zap"

	"github.com/sambigeara/pollen/pkg/types"
)

var (
	ErrSessionClosed = errors.New("session closed")
	ErrNoServicePort = errors.New("no service port in stream header")
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
		err = s.conn.CloseWithError(0, "session closed")
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
	*quic.Stream
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
