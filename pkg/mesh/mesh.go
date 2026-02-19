package mesh

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/sock"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const (
	handshakeTimeout    = 3 * time.Second
	quicIdleTimeout     = 30 * time.Second
	quicKeepAlivePeriod = 10 * time.Second
	eventSendTimeout    = 5 * time.Second
	queueBufSize        = 64
	probeBufSize        = 2048
)

func quicConfig() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:  quicIdleTimeout,
		KeepAlivePeriod: quicKeepAlivePeriod,
		EnableDatagrams: true,
	}
}

type Packet struct {
	Envelope *meshv1.Envelope
	Peer     types.PeerKey
}

type Mesh interface {
	Start(ctx context.Context) error
	Send(ctx context.Context, peerKey types.PeerKey, env *meshv1.Envelope) error
	Recv(ctx context.Context) (Packet, error)
	Events() <-chan peer.Input
	OpenStream(ctx context.Context, peer types.PeerKey) (io.ReadWriteCloser, error)
	AcceptStream(ctx context.Context) (types.PeerKey, io.ReadWriteCloser, error)
	JoinWithInvite(ctx context.Context, inv *admissionv1.Invite) error
	Connect(ctx context.Context, peer types.PeerKey, addrs []*net.UDPAddr) error
	Punch(ctx context.Context, peer types.PeerKey, addr *net.UDPAddr) error
	GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)
	GetConn(peer types.PeerKey) (*quic.Conn, bool)
	BroadcastDisconnect() error
	Close() error
}

type impl struct {
	log      *zap.SugaredLogger
	cert     tls.Certificate
	invites  admission.Store
	socks    sock.SockStore
	inCh     chan peer.Input
	listener *quic.Listener
	sessions *sessionRegistry
	mainQT   *quic.Transport
	recvCh   chan Packet
	streamCh chan incomingStream
	acceptWG sync.WaitGroup
	port     int
	mu       sync.RWMutex
	localKey types.PeerKey
}

type incomingStream struct {
	stream  io.ReadWriteCloser
	peerKey types.PeerKey
}

type peerSession struct {
	conn      *quic.Conn
	transport *quic.Transport
	sockConn  *sock.Conn
}

type directDialResult struct {
	session *peerSession
	err     error
}

func NewMesh(defaultPort int, signPriv ed25519.PrivateKey, invites admission.Store) (Mesh, error) {
	cert, err := generateIdentityCert(signPriv)
	if err != nil {
		return nil, fmt.Errorf("generate identity cert: %w", err)
	}

	pub, ok := signPriv.Public().(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("identity private key is not ed25519")
	}

	return &impl{
		log:      zap.S().Named("mesh"),
		cert:     cert,
		localKey: types.PeerKeyFromBytes(pub),
		invites:  invites,
		socks:    sock.NewSockStore(),
		port:     defaultPort,
		sessions: newSessionRegistry(),
		recvCh:   make(chan Packet, queueBufSize),
		inCh:     make(chan peer.Input, queueBufSize),
		streamCh: make(chan incomingStream, queueBufSize),
	}, nil
}

func (m *impl) Start(ctx context.Context) error {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: m.port})
	if err != nil {
		return fmt.Errorf("listen udp:%d: %w", m.port, err)
	}

	qt := &quic.Transport{Conn: conn}
	ln, err := qt.Listen(newServerTLSConfig(m.cert), quicConfig())
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

func (m *impl) OpenStream(ctx context.Context, peerKey types.PeerKey) (io.ReadWriteCloser, error) {
	s, err := m.sessions.waitFor(ctx, peerKey)
	if err != nil {
		return nil, err
	}

	stream, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}

	return stream, nil
}

func (m *impl) AcceptStream(ctx context.Context) (types.PeerKey, io.ReadWriteCloser, error) {
	select {
	case incoming, ok := <-m.streamCh:
		if !ok {
			return types.PeerKey{}, nil, net.ErrClosed
		}
		return incoming.peerKey, incoming.stream, nil
	case <-ctx.Done():
		return types.PeerKey{}, nil, ctx.Err()
	}
}

func (m *impl) dialDirect(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey) (*peerSession, error) {
	tlsCfg := newExpectedPeerTLSConfig(m.cert, expectedPeer)
	qCfg := quicConfig()

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
	qCfg := quicConfig()

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

func (m *impl) JoinWithInvite(ctx context.Context, inv *admissionv1.Invite) error {
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

	winner, err := m.raceDirectDial(ctx, peerKey, resolved)
	if err != nil {
		return fmt.Errorf("failed to join via invite at any address: %w", err)
	}

	accepted, err := m.exchangeInvite(ctx, winner.conn, inv.Id)
	if err != nil {
		m.closeSession(winner, "invite exchange failed")
		return fmt.Errorf("failed to join via invite at any address: %w", err)
	}
	if !accepted {
		m.closeSession(winner, "invite rejected")
		return fmt.Errorf("failed to join via invite at any address: invite rejected")
	}

	m.addPeer(winner, peerKey)
	return nil
}

func (m *impl) raceDirectDial(ctx context.Context, peerKey types.PeerKey, addrs []*net.UDPAddr) (*peerSession, error) {
	dialCtx, cancelDial := context.WithCancel(ctx)
	defer cancelDial()

	ch := make(chan directDialResult, len(addrs))
	for _, addr := range addrs {
		go func() {
			s, err := m.dialDirect(dialCtx, addr, peerKey)
			ch <- directDialResult{session: s, err: err}
		}()
	}

	var lastErr error
	remaining := len(addrs)
	for remaining > 0 {
		r := <-ch
		remaining--
		if r.err != nil {
			lastErr = r.err
			continue
		}

		cancelDial()
		go func(remaining int) {
			for range remaining {
				r := <-ch
				if r.err == nil {
					m.closeSession(r.session, "replaced")
				}
			}
		}(remaining)
		return r.session, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("failed all dial attempts")
}

func (m *impl) exchangeInvite(ctx context.Context, qc *quic.Conn, token string) (bool, error) {
	reqData, err := (&meshv1.Envelope{
		Body: &meshv1.Envelope_InviteRequest{
			InviteRequest: &meshv1.InviteRequest{Token: token},
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
		env := &meshv1.Envelope{}
		if err := env.UnmarshalVT(payload); err != nil {
			continue
		}
		if body, ok := env.GetBody().(*meshv1.Envelope_InviteResponse); ok {
			return body.InviteResponse.GetAccepted(), nil
		}
	}
}

func (m *impl) Connect(ctx context.Context, peerKey types.PeerKey, addrs []*net.UDPAddr) error {
	if len(addrs) == 0 {
		return fmt.Errorf("connect to %s: no addresses", peerKey.Short())
	}

	winner, err := m.raceDirectDial(ctx, peerKey, addrs)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", peerKey.Short(), err)
	}

	m.addPeer(winner, peerKey)
	return nil
}

func (m *impl) Send(_ context.Context, peerKey types.PeerKey, env *meshv1.Envelope) error {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return fmt.Errorf("no connection to peer %s", peerKey.Short())
	}
	b, err := env.MarshalVT()
	if err != nil {
		return err
	}
	return s.conn.SendDatagram(b)
}

func (m *impl) Punch(ctx context.Context, peerKey types.PeerKey, addr *net.UDPAddr) error {
	s, err := m.dialPunch(ctx, addr, peerKey)
	if err != nil {
		return err
	}
	m.addPeer(s, peerKey)
	return nil
}

func (m *impl) GetConn(peerKey types.PeerKey) (*quic.Conn, bool) {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return nil, false
	}
	return s.conn, true
}

func (m *impl) GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool) {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return nil, false
	}
	addr, err := net.ResolveUDPAddr("udp", s.conn.RemoteAddr().String())
	if err != nil {
		return nil, false
	}
	return addr, true
}

func (m *impl) BroadcastDisconnect() error {
	peers := m.sessions.drainPeers()
	for _, s := range peers {
		m.closeSession(s, "disconnect")
	}
	return nil
}

func (m *impl) addPeer(s *peerSession, peerKey types.PeerKey) {
	replace, ok := m.sessions.add(peerKey, s, func(current *peerSession) bool {
		if current == nil {
			return true
		}

		if current.conn.Context().Err() != nil {
			return true
		}

		// Both connections are live — deterministic tie-break:
		// the peer with the smaller key keeps its connection.
		// If we're the smaller key, keep ours (close the new one).
		// If they're the smaller key, replace ours with theirs.
		return !m.localKey.Less(peerKey)
	})
	if !ok {
		m.closeSession(s, "duplicate")
		return
	}

	if replace != nil {
		m.closeSession(replace, "replaced")
	}

	// Use the connection's own context so the recv goroutine lives as long as
	// the QUIC connection, not as long as the (potentially short-lived) dial ctx.
	go m.recvDatagrams(s, peerKey)
	m.acceptWG.Go(func() {
		m.acceptStreams(s, peerKey)
	})

	addr, err := net.ResolveUDPAddr("udp", s.conn.RemoteAddr().String())
	if err != nil {
		return
	}
	select {
	case m.inCh <- peer.ConnectPeer{
		PeerKey:      peerKey,
		IP:           addr.IP,
		ObservedPort: addr.Port,
	}:
	case <-time.After(eventSendTimeout):
		m.log.Warnw("dropped connect event, consumer lagging",
			"peer", peerKey.Short(),
		)
	case <-s.conn.Context().Done():
	}
}

func (m *impl) acceptStreams(s *peerSession, peerKey types.PeerKey) {
	ctx := s.conn.Context()
	for {
		stream, err := s.conn.AcceptStream(ctx)
		if err != nil {
			return
		}

		select {
		case m.streamCh <- incomingStream{peerKey: peerKey, stream: stream}:
		case <-ctx.Done():
			_ = stream.Close()
			return
		}
	}
}

func (m *impl) recvDatagrams(s *peerSession, peerKey types.PeerKey) {
	ctx := s.conn.Context()
	for {
		payload, err := s.conn.ReceiveDatagram(ctx)
		if err != nil {
			isCurrent := m.sessions.removeIfCurrent(peerKey, s)

			if isCurrent {
				reason := classifyQUICError(err)
				m.log.Debugw("peer session died",
					"peer", peerKey.Short(),
					"reason", reason,
					"err", err,
				)
				select {
				case m.inCh <- peer.PeerDisconnected{
					PeerKey: peerKey,
					Reason:  reason,
				}:
				case <-time.After(eventSendTimeout):
					m.log.Warnw("dropped disconnect event, consumer lagging",
						"peer", peerKey.Short(),
					)
				}
				m.closeSession(s, "disconnected")
			}
			return
		}
		env := &meshv1.Envelope{}
		if err := env.UnmarshalVT(payload); err != nil {
			continue
		}

		switch env.GetBody().(type) {
		case *meshv1.Envelope_InviteRequest, *meshv1.Envelope_InviteResponse:
			m.handleInviteControlDatagram(s, env)
			continue
		default:
		}

		select {
		case m.recvCh <- Packet{Peer: peerKey, Envelope: env}:
		case <-ctx.Done():
			return
		}
	}
}

func classifyQUICError(err error) peer.DisconnectReason {
	var idleErr *quic.IdleTimeoutError
	if errors.As(err, &idleErr) {
		return peer.DisconnectIdleTimeout
	}
	var resetErr *quic.StatelessResetError
	if errors.As(err, &resetErr) {
		return peer.DisconnectReset
	}
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		return peer.DisconnectGraceful
	}
	return peer.DisconnectUnknown
}

func (m *impl) runMainProbeLoop(ctx context.Context, qt *quic.Transport) {
	buf := make([]byte, probeBufSize)
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

func (m *impl) handleInviteControlDatagram(s *peerSession, env *meshv1.Envelope) {
	body, ok := env.GetBody().(*meshv1.Envelope_InviteRequest)
	if !ok {
		return
	}

	accepted := false
	if req := body.InviteRequest; req != nil && req.Token != "" {
		ok, err := m.invites.ConsumeToken(req.Token)
		accepted = err == nil && ok
	}

	respData, err := (&meshv1.Envelope{
		Body: &meshv1.Envelope_InviteResponse{
			InviteResponse: &meshv1.InviteResponse{Accepted: accepted},
		},
	}).MarshalVT()
	if err == nil {
		_ = s.conn.SendDatagram(respData)
	}
}

func (m *impl) Close() error {
	peers := m.sessions.drainPeers()

	m.mu.Lock()
	listener := m.listener
	mainQT := m.mainQT
	m.mu.Unlock()

	for _, s := range peers {
		m.closeSession(s, "shutdown")
	}

	m.acceptWG.Wait()
	close(m.streamCh)

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
