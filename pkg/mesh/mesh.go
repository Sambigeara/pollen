package mesh

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
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/sock"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	handshakeTimeout = 3 * time.Second
)

type Packet struct {
	Peer    types.PeerKey
	Payload []byte
}

type Mesh interface {
	Start(ctx context.Context) error
	Send(ctx context.Context, peerKey types.PeerKey, payload []byte) error
	Recv(ctx context.Context) (Packet, error)
	Events() <-chan peer.Input
	JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error
	Connect(ctx context.Context, peer types.PeerKey, addrs []*net.UDPAddr) error
	Punch(ctx context.Context, peer types.PeerKey, addr *net.UDPAddr) error
	GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)
	GetConn(peer types.PeerKey) (*quic.Conn, bool)
	BroadcastDisconnect() error
	Close() error
}

type impl struct {
	cert     tls.Certificate
	localKey types.PeerKey
	invites  admission.Store
	socks    sock.SockStore

	mu         sync.RWMutex
	mainQT     *quic.Transport
	transports []*quic.Transport
	listener   *quic.Listener
	peers      map[types.PeerKey]*quic.Conn
	inCh       chan peer.Input
	recvCh     chan Packet
	port       int
}

func NewMesh(defaultPort int, signPriv ed25519.PrivateKey, invites admission.Store) (Mesh, error) {
	cert, err := generateIdentityCert(signPriv)
	if err != nil {
		return nil, fmt.Errorf("generate identity cert: %w", err)
	}

	return &impl{
		cert:     cert,
		localKey: types.PeerKeyFromBytes(signPriv.Public().(ed25519.PublicKey)),
		invites:  invites,
		socks:    sock.NewSockStore(),
		port:     defaultPort,
		peers:    make(map[types.PeerKey]*quic.Conn),
		recvCh:   make(chan Packet, 64),
		inCh:     make(chan peer.Input, 64),
	}, nil
}

func (m *impl) Start(ctx context.Context) error {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: m.port})
	if err != nil {
		return fmt.Errorf("listen udp:%d: %w", m.port, err)
	}
	if err := m.listen(conn); err != nil {
		_ = conn.Close()
		return err
	}

	m.socks.SetMainProbeWriter(func(payload []byte, addr *net.UDPAddr) error {
		_, err := m.mainQT.WriteTo(payload, addr)
		return err
	})
	go m.runMainProbeLoop(ctx, m.mainQT)
	go m.acceptLoop(ctx)
	return nil
}

// listen starts accepting QUIC connections on the given PacketConn.
func (m *impl) listen(conn net.PacketConn) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	qt := &quic.Transport{Conn: conn}
	ln, err := qt.Listen(newServerTLSConfig(m.cert), &quic.Config{EnableDatagrams: true})
	if err != nil {
		return fmt.Errorf("quic listen: %w", err)
	}
	m.mainQT = qt
	m.transports = append(m.transports, qt)
	m.listener = ln
	return nil
}

func (m *impl) Recv(ctx context.Context) (Packet, error) {
	select {
	case p := <-m.recvCh:
		return p, nil
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	}
}

func (m *impl) Events() <-chan peer.Input {
	return m.inCh
}

// dial establishes a QUIC connection to addr. It first tries all existing
// transports in the pool. If none succeed, it requests a new socket from the
// sock store, creates a new QUIC transport, and dials on that.
func (m *impl) dial(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey, withPunch bool) (*quic.Conn, error) {
	tlsCfg := newExpectedPeerTLSConfig(m.cert, expectedPeer)
	qCfg := &quic.Config{EnableDatagrams: true}

	// TODO(saml)
	if !withPunch {
		m.mu.RLock()
		existing := make([]*quic.Transport, len(m.transports))
		copy(existing, m.transports)
		m.mu.RUnlock()

		for _, qt := range existing {
			qc, err := qt.Dial(ctx, addr, tlsCfg, qCfg)
			if err == nil {
				return qc, nil
			}
		}
	}

	conn, err := m.socks.GetOrCreate(ctx, addr, withPunch)
	if err != nil {
		return nil, fmt.Errorf("quic dial %s: sock store: %w", addr, err)
	}

	dialAddr := addr
	if peerAddr := conn.Peer(); peerAddr != nil {
		dialAddr = peerAddr
	}

	if conn.Shared() {
		qc, err := m.mainQT.Dial(ctx, dialAddr, tlsCfg, qCfg)
		if err != nil {
			return nil, fmt.Errorf("quic dial %s: %w", dialAddr, err)
		}
		return qc, nil
	}

	qt := &quic.Transport{Conn: conn.UDPConn}
	m.mu.Lock()
	m.transports = append(m.transports, qt)
	m.mu.Unlock()

	qc, err := qt.Dial(ctx, dialAddr, tlsCfg, qCfg)
	if err != nil {
		_ = qt.Close()
		conn.Close()
		return nil, fmt.Errorf("quic dial %s: %w", dialAddr, err)
	}
	return qc, nil
}

func (m *impl) JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error {
	peerKey := types.PeerKeyFromBytes(inv.Fingerprint)

	// TODO(saml) parallelise requests?
	for _, addr := range inv.Addr {
		addr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			continue
		}

		qc, err := m.dial(ctx, addr, peerKey, false)
		if err != nil {
			continue
		}

		stream, err := qc.OpenStreamSync(ctx)
		if err != nil {
			_ = qc.CloseWithError(0, "stream open failed")
			continue
		}

		_ = stream.SetDeadline(time.Now().Add(handshakeTimeout))

		reqData, err := (&peerv1.InviteRequest{Token: inv.Id}).MarshalVT()
		if err != nil {
			_ = stream.Close()
			_ = qc.CloseWithError(0, "invite marshal failed")
			continue
		}
		if _, err := stream.Write(reqData); err != nil {
			_ = stream.Close()
			_ = qc.CloseWithError(0, "invite write failed")
			continue
		}
		_ = stream.Close() // FIN write side; read side stays open for response

		respBuf, err := io.ReadAll(stream)
		if err != nil {
			_ = qc.CloseWithError(0, "invite read failed")
			continue
		}
		resp := &peerv1.InviteResponse{}
		if err := resp.UnmarshalVT(respBuf); err != nil {
			_ = qc.CloseWithError(0, "invite read failed")
			continue
		}

		if !resp.Accepted {
			_ = qc.CloseWithError(0, "invite rejected")
			continue
		}

		m.addPeer(qc, peerKey)
		return nil
	}

	return fmt.Errorf("failed to join via invite at any address")
}

func (m *impl) Send(ctx context.Context, peerKey types.PeerKey, payload []byte) error {
	m.mu.RLock()
	qc, ok := m.peers[peerKey]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no connection to peer %s", peerKey.Short())
	}
	return qc.SendDatagram(payload)
}

func (m *impl) Connect(ctx context.Context, peerKey types.PeerKey, addrs []*net.UDPAddr) error {
	if len(addrs) == 0 {
		return fmt.Errorf("connect to %s: no addresses", peerKey.Short())
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		qc  *quic.Conn
		err error
	}
	ch := make(chan result, len(addrs))

	for _, addr := range addrs {
		go func(a *net.UDPAddr) {
			qc, err := m.dial(ctx, a, peerKey, false)
			ch <- result{qc, err}
		}(addr)
	}

	var (
		winner  *quic.Conn
		lastErr error
	)
	for range addrs {
		r := <-ch
		if r.err != nil {
			lastErr = r.err
			continue
		}
		if winner == nil {
			winner = r.qc
			cancel() // cancel remaining dials
		} else {
			_ = r.qc.CloseWithError(0, "replaced")
		}
	}
	if winner != nil {
		m.addPeer(winner, peerKey)
		return nil
	}
	return fmt.Errorf("connect to %s: %w", peerKey.Short(), lastErr)
}

func (m *impl) Punch(ctx context.Context, peerKey types.PeerKey, addr *net.UDPAddr) error {
	qc, err := m.dial(ctx, addr, peerKey, true)
	if err != nil {
		return err
	}
	m.addPeer(qc, peerKey)
	return nil
}

func (m *impl) GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool) {
	m.mu.RLock()
	qc, ok := m.peers[peerKey]
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}
	addr, ok := qc.RemoteAddr().(*net.UDPAddr)
	return addr, ok
}

func (m *impl) GetConn(peerKey types.PeerKey) (*quic.Conn, bool) {
	m.mu.RLock()
	qc, ok := m.peers[peerKey]
	m.mu.RUnlock()
	return qc, ok
}

func (m *impl) BroadcastDisconnect() error {
	m.mu.Lock()
	peers := m.peers
	m.peers = make(map[types.PeerKey]*quic.Conn)
	m.mu.Unlock()
	for _, qc := range peers {
		_ = qc.CloseWithError(0, "disconnect")
	}
	return nil
}

func (m *impl) addPeer(qc *quic.Conn, peerKey types.PeerKey) {
	m.mu.Lock()
	if old, ok := m.peers[peerKey]; ok {
		if old.Context().Err() == nil {
			// Both connections are live — deterministic tie-break:
			// the peer with the smaller key keeps its connection.
			// If we're the smaller key, keep ours (close the new one).
			// If they're the smaller key, replace ours with theirs.
			if m.localKey.Less(peerKey) {
				m.mu.Unlock()
				_ = qc.CloseWithError(0, "duplicate")
				return
			}
		}
		_ = old.CloseWithError(0, "replaced")
	}
	m.peers[peerKey] = qc
	m.mu.Unlock()

	// Use the connection's own context so the recv goroutine lives as long as
	// the QUIC connection, not as long as the (potentially short-lived) dial ctx.
	go m.recvDatagrams(qc, peerKey)

	addr := qc.RemoteAddr().(*net.UDPAddr)
	select {
	case m.inCh <- peer.ConnectPeer{
		PeerKey:      peerKey,
		IP:           addr.IP,
		ObservedPort: addr.Port,
	}:
	default:
	}
}

func (m *impl) recvDatagrams(qc *quic.Conn, peerKey types.PeerKey) {
	ctx := qc.Context() // cancelled when the connection closes
	for {
		payload, err := qc.ReceiveDatagram(ctx)
		if err != nil {
			select {
			case m.inCh <- peer.PeerDisconnected{PeerKey: peerKey}:
			default:
			}
			m.mu.Lock()
			if current, ok := m.peers[peerKey]; ok && current == qc {
				delete(m.peers, peerKey)
			}
			m.mu.Unlock()
			return
		}
		select {
		case m.recvCh <- Packet{Peer: peerKey, Payload: payload}:
		case <-ctx.Done():
			return
		}
	}
}

func (m *impl) runMainProbeLoop(ctx context.Context, qt *quic.Transport) {
	buf := make([]byte, 2048)
	for {
		n, sender, err := qt.ReadNonQUICPacket(ctx, buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			return
		}
		udpSender, ok := sender.(*net.UDPAddr)
		if !ok {
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		m.socks.HandleMainProbePacket(data, udpSender)
	}
}

func (m *impl) acceptLoop(ctx context.Context) {
	for {
		qc, err := m.listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		peerKey, err := peerKeyFromConn(qc)
		if err != nil {
			_ = qc.CloseWithError(0, "identity failed")
			continue
		}

		m.addPeer(qc, peerKey)
		go m.acceptInviteStream(ctx, qc)
	}
}

func (m *impl) acceptInviteStream(ctx context.Context, qc *quic.Conn) error {
	acceptCtx, acceptCancel := context.WithTimeout(ctx, handshakeTimeout)
	defer acceptCancel()

	stream, err := qc.AcceptStream(acceptCtx)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	_ = stream.SetDeadline(time.Now().Add(handshakeTimeout))

	reqBuf, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("read invite request: %w", err)
	}
	req := &peerv1.InviteRequest{}
	if err := req.UnmarshalVT(reqBuf); err != nil {
		return fmt.Errorf("read invite request: %w", err)
	}

	sendResponse := func(accepted bool) error {
		data, err := (&peerv1.InviteResponse{Accepted: accepted}).MarshalVT()
		if err != nil {
			return err
		}
		_, err = stream.Write(data)
		return err
	}

	if req.Token == "" {
		_ = sendResponse(false)
		return fmt.Errorf("empty invite token")
	}

	ok, err := m.invites.ConsumeToken(req.Token)
	if err != nil {
		_ = sendResponse(false)
		return fmt.Errorf("consume invite token: %w", err)
	}
	if !ok {
		_ = sendResponse(false)
		return fmt.Errorf("invalid invite token")
	}

	if err := sendResponse(true); err != nil {
		return fmt.Errorf("write invite response: %w", err)
	}
	return nil
}

func (m *impl) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, qc := range m.peers {
		_ = qc.CloseWithError(0, "shutdown")
	}
	m.peers = make(map[types.PeerKey]*quic.Conn)
	close(m.recvCh)
	if m.listener != nil {
		_ = m.listener.Close()
	}
	for _, qt := range m.transports {
		_ = qt.Close()
	}
	m.transports = nil
	_ = m.mainQT.Conn.Close()
	return nil
}
