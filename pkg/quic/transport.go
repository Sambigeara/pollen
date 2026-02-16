package quic

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	streamTypeInvite byte = 0x02

	inviteTokenMaxLen = 255
	inviteHeaderLen   = 2

	handshakeTimeout = 5 * time.Second
)

type Transport struct {
	cert     tls.Certificate
	invites  admission.Store
	qt       *quic.Transport
	listener *quic.Listener
	mu       sync.RWMutex
}

// NewTransport creates a transport with the given signing key and invite store.
// Call Listen to start accepting connections.
func NewTransport(signPriv ed25519.PrivateKey, invites admission.Store) (*Transport, error) {
	cert, err := generateIdentityCert(signPriv)
	if err != nil {
		return nil, fmt.Errorf("generate identity cert: %w", err)
	}
	return &Transport{cert: cert, invites: invites}, nil
}

// Listen starts accepting QUIC connections on the given PacketConn.
func (t *Transport) Listen(conn net.PacketConn) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.qt = &quic.Transport{Conn: conn}
	ln, err := t.qt.Listen(newServerTLSConfig(t.cert), &quic.Config{EnableDatagrams: true})
	if err != nil {
		return fmt.Errorf("quic listen: %w", err)
	}
	t.listener = ln
	return nil
}

// Accept waits for the next incoming QUIC connection.
func (t *Transport) Accept(ctx context.Context) (*quic.Conn, error) {
	t.mu.RLock()
	ln := t.listener
	t.mu.RUnlock()
	if ln == nil {
		return nil, fmt.Errorf("transport not listening")
	}
	return ln.Accept(ctx)
}

// Dial establishes a QUIC connection to the peer at sock.RemoteAddr().
// For the main socket, uses the shared QUIC transport. For ephemeral sockets,
// creates a dedicated QUIC transport. Returns the connection and an optional
// cleanup function (non-nil for ephemeral sockets) that must be called when
// the connection is closed.
func (t *Transport) Dial(ctx context.Context, sock *Socket, expectedPeer types.PeerKey) (*quic.Conn, func(), error) {
	tlsCfg := newExpectedPeerTLSConfig(t.cert, expectedPeer)
	qCfg := &quic.Config{EnableDatagrams: true}

	if sock.IsMain() {
		t.mu.RLock()
		qt := t.qt
		t.mu.RUnlock()
		qc, err := qt.Dial(ctx, sock.RemoteAddr(), tlsCfg, qCfg)
		if err != nil {
			return nil, nil, fmt.Errorf("quic dial %s: %w", sock.RemoteAddr(), err)
		}
		return qc, nil, nil
	}

	// Ephemeral socket — create a dedicated QUIC transport.
	qt := &quic.Transport{Conn: sock.UDPConn()}
	qc, err := qt.Dial(ctx, sock.RemoteAddr(), tlsCfg, qCfg)
	if err != nil {
		_ = qt.Close()
		return nil, nil, fmt.Errorf("quic dial %s: %w", sock.RemoteAddr(), err)
	}
	return qc, func() { _ = qt.Close() }, nil
}

// JoinWithInvite connects to a peer using an invite token. It dials the
// addresses in the invite, performs the invite handshake, and returns the QUIC
// connection, peer key, and remote address on success.
func (t *Transport) JoinWithInvite(ctx context.Context, inv *peerv1.Invite) (*quic.Conn, types.PeerKey, *net.UDPAddr, error) {
	if len(inv.Fingerprint) != ed25519.PublicKeySize {
		return nil, types.PeerKey{}, nil, fmt.Errorf("invalid fingerprint length in invite")
	}
	if len(inv.Id) == 0 {
		return nil, types.PeerKey{}, nil, fmt.Errorf("missing invite id")
	}
	token := []byte(inv.Id)
	if len(token) > inviteTokenMaxLen {
		return nil, types.PeerKey{}, nil, fmt.Errorf("invite id too long")
	}

	peerKey := types.PeerKeyFromBytes(inv.Fingerprint)

	for _, addrStr := range inv.Addr {
		addr, err := net.ResolveUDPAddr("udp", addrStr)
		if err != nil {
			continue
		}

		qc, err := t.dial(ctx, addr, peerKey)
		if err != nil {
			continue
		}

		stream, err := qc.OpenStreamSync(ctx)
		if err != nil {
			_ = qc.CloseWithError(0, "stream open failed")
			continue
		}

		_ = stream.SetDeadline(time.Now().Add(handshakeTimeout))

		req := make([]byte, 0, inviteHeaderLen+len(token))
		req = append(req, streamTypeInvite, byte(len(token)))
		req = append(req, token...)
		if _, err := stream.Write(req); err != nil {
			_ = stream.Close()
			_ = qc.CloseWithError(0, "invite write failed")
			continue
		}

		var resp [1]byte
		if _, err := io.ReadFull(stream, resp[:]); err != nil {
			_ = stream.Close()
			_ = qc.CloseWithError(0, "invite read failed")
			continue
		}
		_ = stream.Close()

		if resp[0] != 0 {
			_ = qc.CloseWithError(0, "invite rejected")
			continue
		}

		return qc, peerKey, addr, nil
	}

	return nil, types.PeerKey{}, nil, fmt.Errorf("failed to join via invite at any address")
}

// AcceptInviteStream validates an invite token from an incoming QUIC stream.
func (t *Transport) AcceptInviteStream(ctx context.Context, qc *quic.Conn) error {
	acceptCtx, acceptCancel := context.WithTimeout(ctx, handshakeTimeout)
	defer acceptCancel()

	stream, err := qc.AcceptStream(acceptCtx)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	_ = stream.SetDeadline(time.Now().Add(handshakeTimeout))

	var header [2]byte
	if _, err := io.ReadFull(stream, header[:]); err != nil {
		return fmt.Errorf("read invite header: %w", err)
	}
	if header[0] != streamTypeInvite {
		return fmt.Errorf("unexpected stream type: %d", header[0])
	}

	tokenLen := int(header[1])
	if tokenLen == 0 {
		_, _ = stream.Write([]byte{1})
		return fmt.Errorf("empty invite token")
	}

	tokenBuf := make([]byte, tokenLen)
	if _, err := io.ReadFull(stream, tokenBuf); err != nil {
		_, _ = stream.Write([]byte{1})
		return fmt.Errorf("read invite token: %w", err)
	}

	ok, err := t.invites.ConsumeToken(string(tokenBuf))
	if err != nil {
		_, _ = stream.Write([]byte{1})
		return fmt.Errorf("consume invite token: %w", err)
	}
	if !ok {
		_, _ = stream.Write([]byte{1})
		return fmt.Errorf("invalid invite token")
	}

	if _, err := stream.Write([]byte{0}); err != nil {
		return fmt.Errorf("write invite response: %w", err)
	}
	return nil
}

// Close shuts down the QUIC transport and listener.
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.listener != nil {
		_ = t.listener.Close()
	}
	if t.qt != nil {
		return t.qt.Close()
	}
	return nil
}

// dial is an internal helper that dials using the main QUIC transport.
func (t *Transport) dial(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey) (*quic.Conn, error) {
	t.mu.RLock()
	qt := t.qt
	t.mu.RUnlock()
	if qt == nil {
		return nil, fmt.Errorf("transport not initialised")
	}
	tlsCfg := newExpectedPeerTLSConfig(t.cert, expectedPeer)
	return qt.Dial(ctx, addr, tlsCfg, &quic.Config{EnableDatagrams: true})
}
