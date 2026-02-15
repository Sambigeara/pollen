package quic

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/sambigeara/pollen/pkg/types"
)

type Transport struct {
	cert     tls.Certificate
	qt       *quic.Transport
	listener *quic.Listener
	mu       sync.RWMutex
}

func newQUICTransport(port int, signPriv ed25519.PrivateKey) (*Transport, error) {
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		return nil, fmt.Errorf("listen UDP: %w", err)
	}

	cert, err := generateIdentityCert(signPriv)
	if err != nil {
		return nil, fmt.Errorf("generate identity cert: %w", err)
	}

	return &Transport{
		cert: cert,
		qt:   &quic.Transport{Conn: udpConn},
	}, nil
}

func (t *Transport) listen() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	ln, err := t.qt.Listen(newServerTLSConfig(t.cert), &quic.Config{EnableDatagrams: true})
	if err != nil {
		return fmt.Errorf("quic listen: %w", err)
	}

	t.listener = ln
	return nil
}

func (t *Transport) accept(ctx context.Context) (*quic.Conn, error) {
	t.mu.RLock()
	ln := t.listener
	t.mu.RUnlock()

	if ln == nil {
		return nil, fmt.Errorf("transport not listening")
	}

	return ln.Accept(ctx)
}

func (t *Transport) dialExpectedPeer(ctx context.Context, addr net.Addr, expectedPeer types.PeerKey) (*quic.Conn, error) {
	tlsCfg := newExpectedPeerTLSConfig(t.cert, expectedPeer)
	conn, err := t.qt.Dial(ctx, addr, tlsCfg, &quic.Config{EnableDatagrams: true})
	if err != nil {
		return nil, fmt.Errorf("quic dial %s: %w", addr, err)
	}

	return conn, nil
}

func (t *Transport) close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.listener != nil {
		_ = t.listener.Close()
	}

	return t.qt.Close()
}
