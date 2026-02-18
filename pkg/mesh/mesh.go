package mesh

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/sock"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	handshakeTimeout = 3 * time.Second
)

type Packet struct {
	Payload []byte
	Peer    types.PeerKey
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
	invites  admission.Store
	socks    sock.SockStore
	mainQT   *quic.Transport
	listener *quic.Listener
	peers    map[types.PeerKey]*peerSession
	inCh     chan peer.Input
	recvCh   chan Packet
	port     int
	mu       sync.RWMutex
	localKey types.PeerKey
}

type peerSession struct {
	conn      *quic.Conn
	transport *quic.Transport
	sockConn  *sock.Conn
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
		peers:    make(map[types.PeerKey]*peerSession),
		recvCh:   make(chan Packet, 64),
		inCh:     make(chan peer.Input, 64),
	}, nil
}

func (m *impl) Start(ctx context.Context) error {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: m.port})
	if err != nil {
		return fmt.Errorf("listen udp:%d: %w", m.port, err)
	}

	qt := &quic.Transport{Conn: conn}
	ln, err := qt.Listen(newServerTLSConfig(m.cert), &quic.Config{EnableDatagrams: true})
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("quic listen: %w", err)
	}

	m.mu.Lock()
	m.mainQT = qt
	m.listener = ln
	m.mu.Unlock()

	m.socks.SetMainProbeWriter(func(payload []byte, addr *net.UDPAddr) error {
		_, err := qt.WriteTo(payload, addr)
		return err
	})
	go m.runMainProbeLoop(ctx, qt)
	go m.acceptLoop(ctx)
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

func (m *impl) dialDirect(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey) (*peerSession, error) {
	tlsCfg := newExpectedPeerTLSConfig(m.cert, expectedPeer)
	qCfg := &quic.Config{EnableDatagrams: true}

	qc, err := m.mainQT.Dial(ctx, addr, tlsCfg, qCfg)
	if err != nil {
		return nil, fmt.Errorf("quic dial %s: %w", addr, err)
	}
	return &peerSession{
		conn:      qc,
		transport: m.mainQT,
	}, nil
}

func (m *impl) dialPunch(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey) (*peerSession, error) {
	tlsCfg := newExpectedPeerTLSConfig(m.cert, expectedPeer)
	qCfg := &quic.Config{EnableDatagrams: true}

	conn, err := m.socks.Punch(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("quic dial %s: sock store: %w", addr, err)
	}

	dialAddr := addr
	if peerAddr := conn.Peer(); peerAddr != nil {
		dialAddr = peerAddr
	}

	// Easy-side punch winners reuse the main transport and don't carry a UDPConn.
	if conn.UDPConn == nil {
		qc, err := m.mainQT.Dial(ctx, dialAddr, tlsCfg, qCfg)
		if err != nil {
			return nil, fmt.Errorf("quic dial %s: %w", dialAddr, err)
		}
		return &peerSession{
			conn:      qc,
			transport: m.mainQT,
		}, nil
	}

	qt := &quic.Transport{Conn: conn.UDPConn}

	qc, err := qt.Dial(ctx, dialAddr, tlsCfg, qCfg)
	if err != nil {
		_ = qt.Close()
		conn.Close()
		return nil, fmt.Errorf("quic dial %s: %w", dialAddr, err)
	}

	return &peerSession{
		conn:      qc,
		transport: qt,
		sockConn:  conn,
	}, nil
}

func (m *impl) JoinWithInvite(ctx context.Context, inv *peerv1.Invite) error {
	peerKey := types.PeerKeyFromBytes(inv.Fingerprint)
	if len(inv.Addr) == 0 {
		return fmt.Errorf("failed to join via invite at any address")
	}

	resolved := make([]*net.UDPAddr, 0, len(inv.Addr))
	for _, addr := range inv.Addr {
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			continue
		}
		resolved = append(resolved, udpAddr)
	}
	if len(resolved) == 0 {
		return fmt.Errorf("failed to join via invite at any address")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		session *peerSession
		err     error
	}
	ch := make(chan result, len(resolved))

	for _, addr := range resolved {
		go func() {
			s, err := m.dialDirect(ctx, addr, peerKey)
			if err != nil {
				ch <- result{err: err}
				return
			}

			accepted, err := m.exchangeInvite(ctx, s.conn, inv.Id)
			if err != nil {
				m.closeSession(s, "invite exchange failed")
				ch <- result{err: err}
				return
			}
			if !accepted {
				m.closeSession(s, "invite rejected")
				ch <- result{err: fmt.Errorf("invite rejected")}
				return
			}

			ch <- result{session: s}
		}()
	}

	var (
		winner  *peerSession
		lastErr error
	)
	for range resolved {
		r := <-ch
		if r.err != nil {
			lastErr = r.err
			continue
		}
		if winner == nil {
			winner = r.session
			cancel()
		} else {
			m.closeSession(r.session, "replaced")
		}
	}
	if winner != nil {
		m.addPeer(winner, peerKey)
		return nil
	}
	if lastErr != nil {
		return fmt.Errorf("failed to join via invite at any address: %w", lastErr)
	}
	return fmt.Errorf("failed to join via invite at any address")
}

func (m *impl) exchangeInvite(ctx context.Context, qc *quic.Conn, token string) (bool, error) {
	reqData, err := (&statev1.DatagramEnvelope{
		Body: &statev1.DatagramEnvelope_InviteRequest{
			InviteRequest: &peerv1.InviteRequest{Token: token},
		},
	}).MarshalVT()
	if err != nil {
		return false, err
	}
	if err := qc.SendDatagram(reqData); err != nil {
		return false, err
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, handshakeTimeout)
	defer waitCancel()

	for {
		payload, err := qc.ReceiveDatagram(waitCtx)
		if err != nil {
			return false, err
		}
		env := &statev1.DatagramEnvelope{}
		if err := env.UnmarshalVT(payload); err != nil {
			continue
		}
		if body, ok := env.GetBody().(*statev1.DatagramEnvelope_InviteResponse); ok {
			return body.InviteResponse.GetAccepted(), nil
		}
	}
}

func (m *impl) Send(ctx context.Context, peerKey types.PeerKey, payload []byte) error {
	m.mu.RLock()
	s, ok := m.peers[peerKey]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no connection to peer %s", peerKey.Short())
	}
	return s.conn.SendDatagram(payload)
}

func (m *impl) Connect(ctx context.Context, peerKey types.PeerKey, addrs []*net.UDPAddr) error {
	if len(addrs) == 0 {
		return fmt.Errorf("connect to %s: no addresses", peerKey.Short())
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		session *peerSession
		err     error
	}
	ch := make(chan result, len(addrs))

	for _, addr := range addrs {
		go func() {
			s, err := m.dialDirect(ctx, addr, peerKey)
			ch <- result{session: s, err: err}
		}()
	}

	var (
		winner  *peerSession
		lastErr error
	)
	for range addrs {
		r := <-ch
		if r.err != nil {
			lastErr = r.err
			continue
		}
		if winner == nil {
			winner = r.session
			cancel() // cancel remaining dials
		} else {
			m.closeSession(r.session, "replaced")
		}
	}
	if winner != nil {
		m.addPeer(winner, peerKey)
		return nil
	}
	return fmt.Errorf("connect to %s: %w", peerKey.Short(), lastErr)
}

func (m *impl) Punch(ctx context.Context, peerKey types.PeerKey, addr *net.UDPAddr) error {
	s, err := m.dialPunch(ctx, addr, peerKey)
	if err != nil {
		return err
	}
	m.addPeer(s, peerKey)
	return nil
}

func (m *impl) GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool) {
	m.mu.RLock()
	s, ok := m.peers[peerKey]
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}
	addr, ok := s.conn.RemoteAddr().(*net.UDPAddr)
	return addr, ok
}

func (m *impl) GetConn(peerKey types.PeerKey) (*quic.Conn, bool) {
	m.mu.RLock()
	s, ok := m.peers[peerKey]
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return s.conn, true
}

func (m *impl) BroadcastDisconnect() error {
	m.mu.Lock()
	peers := m.peers
	m.peers = make(map[types.PeerKey]*peerSession)
	m.mu.Unlock()
	for _, s := range peers {
		m.closeSession(s, "disconnect")
	}
	return nil
}

func (m *impl) addPeer(s *peerSession, peerKey types.PeerKey) {
	var replace *peerSession

	m.mu.Lock()
	if old, ok := m.peers[peerKey]; ok {
		if old.conn.Context().Err() == nil {
			// Both connections are live — deterministic tie-break:
			// the peer with the smaller key keeps its connection.
			// If we're the smaller key, keep ours (close the new one).
			// If they're the smaller key, replace ours with theirs.
			if m.localKey.Less(peerKey) {
				m.mu.Unlock()
				m.closeSession(s, "duplicate")
				return
			}
		}
		replace = old
	}
	m.peers[peerKey] = s
	m.mu.Unlock()

	if replace != nil {
		m.closeSession(replace, "replaced")
	}

	// Use the connection's own context so the recv goroutine lives as long as
	// the QUIC connection, not as long as the (potentially short-lived) dial ctx.
	go m.recvDatagrams(s, peerKey)

	addr := s.conn.RemoteAddr().(*net.UDPAddr)
	select {
	case m.inCh <- peer.ConnectPeer{
		PeerKey:      peerKey,
		IP:           addr.IP,
		ObservedPort: addr.Port,
	}:
	default:
	}
}

func (m *impl) recvDatagrams(s *peerSession, peerKey types.PeerKey) {
	ctx := s.conn.Context() // cancelled when the connection closes
	for {
		payload, err := s.conn.ReceiveDatagram(ctx)
		if err != nil {
			m.mu.Lock()
			isCurrent := false
			if current, ok := m.peers[peerKey]; ok && current == s {
				delete(m.peers, peerKey)
				isCurrent = true
			}
			m.mu.Unlock()

			if isCurrent {
				select {
				case m.inCh <- peer.PeerDisconnected{PeerKey: peerKey}:
				default:
				}
				m.closeSession(s, "disconnected")
			}
			return
		}
		if m.handleInviteControlDatagram(s, payload) {
			continue
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

		m.addPeer(&peerSession{conn: qc, transport: m.mainQT}, peerKey)
	}
}

func (m *impl) handleInviteControlDatagram(s *peerSession, payload []byte) bool {
	env := &statev1.DatagramEnvelope{}
	if err := env.UnmarshalVT(payload); err != nil {
		return false
	}

	switch body := env.GetBody().(type) {
	case *statev1.DatagramEnvelope_InviteRequest:
		accepted := false
		if req := body.InviteRequest; req != nil && req.Token != "" {
			ok, err := m.invites.ConsumeToken(req.Token)
			accepted = err == nil && ok
		}

		respData, err := (&statev1.DatagramEnvelope{
			Body: &statev1.DatagramEnvelope_InviteResponse{
				InviteResponse: &peerv1.InviteResponse{Accepted: accepted},
			},
		}).MarshalVT()
		if err == nil {
			_ = s.conn.SendDatagram(respData)
		}
		return true
	case *statev1.DatagramEnvelope_InviteResponse:
		return true
	default:
		return false
	}
}

func (m *impl) Close() error {
	m.mu.Lock()
	peers := m.peers
	m.peers = make(map[types.PeerKey]*peerSession)
	listener := m.listener
	mainQT := m.mainQT
	m.mu.Unlock()

	for _, s := range peers {
		m.closeSession(s, "shutdown")
	}

	close(m.recvCh)
	if listener != nil {
		_ = listener.Close()
	}
	if mainQT != nil {
		_ = mainQT.Close()
		if mainQT.Conn != nil {
			_ = mainQT.Conn.Close()
		}
	}
	return nil
}

func (m *impl) closeSession(s *peerSession, reason string) {
	if s == nil {
		return
	}
	_ = s.conn.CloseWithError(0, reason)
	if s.transport != nil && s.transport != m.mainQT {
		_ = s.transport.Close()
	}
	if s.sockConn != nil {
		_ = s.sockConn.Close()
	}
}
