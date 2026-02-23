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
	"github.com/sambigeara/pollen/pkg/auth"
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
	inviteRedeemTTL     = 5 * time.Minute
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
	JoinWithToken(ctx context.Context, token *admissionv1.JoinToken) error
	JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error)
	Connect(ctx context.Context, peer types.PeerKey, addrs []*net.UDPAddr) error
	Punch(ctx context.Context, peer types.PeerKey, addr *net.UDPAddr) error
	GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)
	GetConn(peer types.PeerKey) (*quic.Conn, bool)
	BroadcastDisconnect() error
	Close() error
}

type impl struct {
	meshCert     tls.Certificate
	bareCert     tls.Certificate
	socks        sock.SockStore
	inCh         chan peer.Input
	recvCh       chan Packet
	inviteSigner *auth.AdminSigner
	streamCh     chan incomingStream
	trustBundle  *admissionv1.TrustBundle
	log          *zap.SugaredLogger
	listener     *quic.Listener
	sessions     *sessionRegistry
	mainQT       *quic.Transport
	acceptWG     sync.WaitGroup
	port         int
	mu           sync.RWMutex
	localKey     types.PeerKey
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

func NewMesh(defaultPort int, signPriv ed25519.PrivateKey, creds *auth.NodeCredentials) (Mesh, error) {
	meshCert, err := generateIdentityCert(signPriv, creds.Cert)
	if err != nil {
		return nil, fmt.Errorf("generate mesh cert: %w", err)
	}

	bareCert, err := generateIdentityCert(signPriv, nil)
	if err != nil {
		return nil, fmt.Errorf("generate bare cert: %w", err)
	}

	pub, ok := signPriv.Public().(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("identity private key is not ed25519")
	}

	return &impl{
		log:          zap.S().Named("mesh"),
		meshCert:     meshCert,
		bareCert:     bareCert,
		trustBundle:  creds.Trust,
		inviteSigner: creds.InviteSigner,
		localKey:     types.PeerKeyFromBytes(pub),
		socks:        sock.NewSockStore(),
		port:         defaultPort,
		sessions:     newSessionRegistry(),
		recvCh:       make(chan Packet, queueBufSize),
		inCh:         make(chan peer.Input, queueBufSize),
		streamCh:     make(chan incomingStream, queueBufSize),
	}, nil
}

func (m *impl) Start(ctx context.Context) error {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: m.port})
	if err != nil {
		return fmt.Errorf("listen udp:%d: %w", m.port, err)
	}

	qt := &quic.Transport{Conn: conn}
	ln, err := qt.Listen(newServerTLSConfig(serverTLSParams{
		meshCert:      m.meshCert,
		inviteCert:    m.bareCert,
		trustBundle:   m.trustBundle,
		inviteEnabled: m.inviteSigner != nil,
	}), quicConfig())
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
	tlsCfg := newExpectedPeerTLSConfig(m.meshCert, expectedPeer, m.trustBundle)
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
	tlsCfg := newExpectedPeerTLSConfig(m.meshCert, expectedPeer, m.trustBundle)
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

func (m *impl) JoinWithToken(ctx context.Context, token *admissionv1.JoinToken) error {
	claims := token.GetClaims()
	if claims == nil {
		return fmt.Errorf("join token missing claims")
	}

	bootstraps := claims.GetBootstrap()
	if len(bootstraps) == 0 {
		return fmt.Errorf("join token contains no bootstrap peers")
	}

	var lastErr error
	for _, bootstrap := range bootstraps {
		peerKey := types.PeerKeyFromBytes(bootstrap.GetPeerPub())
		resolved := make([]*net.UDPAddr, 0, len(bootstrap.GetAddrs()))
		for _, addr := range bootstrap.GetAddrs() {
			udpAddr, err := net.ResolveUDPAddr("udp", addr)
			if err != nil {
				continue
			}
			resolved = append(resolved, udpAddr)
		}
		if len(resolved) == 0 {
			continue
		}

		winner, err := m.raceDirectDial(ctx, peerKey, resolved)
		if err != nil {
			lastErr = err
			continue
		}

		m.addPeer(winner, peerKey)
		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("failed to join via token bootstrap peers: %w", lastErr)
	}

	return fmt.Errorf("failed to join via token bootstrap peers")
}

func (m *impl) JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error) {
	joinToken, err := redeemInviteWithDial(ctx, token, ed25519.PublicKey(m.localKey.Bytes()), func(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey) (*quic.Conn, error) {
		return m.mainQT.Dial(ctx, addr, newInviteDialerTLSConfig(m.bareCert, expectedPeer), quicConfig())
	})
	if err != nil {
		return nil, err
	}

	if err := m.JoinWithToken(ctx, joinToken); err != nil {
		return nil, err
	}

	return joinToken, nil
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

		switch qc.ConnectionState().TLS.NegotiatedProtocol {
		case alpnMesh:
			m.addPeer(&peerSession{conn: qc, transport: m.mainQT}, peerKey)
		case alpnInvite:
			go m.handleInviteConnection(ctx, qc, peerKey)
		default:
			_ = qc.CloseWithError(0, "unknown protocol")
		}
	}
}

func (m *impl) handleInviteConnection(ctx context.Context, qc *quic.Conn, peerKey types.PeerKey) {
	waitCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	first, err := recvEnvelope(waitCtx, qc)
	if err != nil {
		_ = qc.CloseWithError(0, "recv failed")
		return
	}

	body, ok := first.GetBody().(*meshv1.Envelope_InviteRedeemRequest)
	if !ok {
		_ = qc.CloseWithError(0, "unexpected message on invite connection")
		return
	}

	if err := m.handleInviteRedeem(qc, peerKey, body.InviteRedeemRequest); err != nil {
		m.log.Debugw("rejected invite", "peer", peerKey.Short(), "err", err)
		_ = qc.CloseWithError(0, "invite failed")
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
	_ = s.conn.CloseWithError(0, reason)
	if s.transport != nil && s.transport != m.mainQT {
		_ = s.transport.Close()
	}
	if s.sockConn != nil {
		_ = s.sockConn.Close()
	}
}

func (m *impl) handleInviteRedeem(qc *quic.Conn, peerKey types.PeerKey, req *meshv1.InviteRedeemRequest) error {
	signer := m.inviteSigner
	now := time.Now()
	verified, err := auth.VerifyInviteToken(req.GetToken(), ed25519.PublicKey(peerKey.Bytes()), now)
	if err != nil {
		_ = sendInviteRedeemResponse(qc, nil, err)
		return err
	}

	ttl := inviteRedeemTTL
	if remaining := time.Unix(verified.Claims.GetExpiresAtUnix(), 0).Sub(now); remaining < ttl {
		ttl = remaining
	}
	if ttl <= 0 {
		err := errors.New("invite token expired")
		_ = sendInviteRedeemResponse(qc, nil, err)
		return err
	}

	consumed, err := signer.Consumed.TryConsume(req.GetToken(), now)
	if err != nil {
		_ = sendInviteRedeemResponse(qc, nil, err)
		return err
	}
	if !consumed {
		err := errors.New("invite token already consumed")
		_ = sendInviteRedeemResponse(qc, nil, err)
		return err
	}

	joinToken, err := auth.IssueJoinTokenWithIssuer(
		signer.Priv,
		signer.Trust,
		signer.Issuer,
		ed25519.PublicKey(peerKey.Bytes()),
		verified.Claims.GetBootstrap(),
		now,
		ttl,
	)
	if err != nil {
		_ = sendInviteRedeemResponse(qc, nil, err)
		return err
	}
	return sendInviteRedeemResponse(qc, joinToken, nil)
}

func sendInviteRedeemResponse(qc *quic.Conn, joinToken *admissionv1.JoinToken, redeemErr error) error {
	reason := ""
	accepted := redeemErr == nil
	if redeemErr != nil {
		reason = redeemErr.Error()
	}
	return sendEnvelope(qc, &meshv1.Envelope{
		Body: &meshv1.Envelope_InviteRedeemResponse{
			InviteRedeemResponse: &meshv1.InviteRedeemResponse{
				Accepted:  accepted,
				Reason:    reason,
				JoinToken: joinToken,
			},
		},
	})
}

func redeemInviteOnConn(
	ctx context.Context,
	qc *quic.Conn,
	token *admissionv1.InviteToken,
	subject ed25519.PublicKey,
) (*admissionv1.JoinToken, error) {
	waitCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	if err := sendEnvelope(qc, &meshv1.Envelope{
		Body: &meshv1.Envelope_InviteRedeemRequest{
			InviteRedeemRequest: &meshv1.InviteRedeemRequest{
				Token:      token,
				SubjectPub: append([]byte(nil), subject...),
			},
		},
	}); err != nil {
		return nil, err
	}

	resp, err := recvInviteRedeemResponse(waitCtx, qc)
	if err != nil {
		return nil, err
	}
	if !resp.GetAccepted() {
		if resp.GetReason() == "" {
			return nil, errors.New("invite token rejected")
		}
		return nil, errors.New(resp.GetReason())
	}

	return resp.GetJoinToken(), nil
}

func recvInviteRedeemResponse(ctx context.Context, qc *quic.Conn) (*meshv1.InviteRedeemResponse, error) {
	for {
		env, err := recvEnvelope(ctx, qc)
		if err != nil {
			return nil, err
		}
		if body, ok := env.GetBody().(*meshv1.Envelope_InviteRedeemResponse); ok {
			return body.InviteRedeemResponse, nil
		}
	}
}

func recvEnvelope(ctx context.Context, qc *quic.Conn) (*meshv1.Envelope, error) {
	for {
		payload, err := qc.ReceiveDatagram(ctx)
		if err != nil {
			return nil, err
		}
		env := &meshv1.Envelope{}
		if err := env.UnmarshalVT(payload); err != nil {
			continue
		}
		return env, nil
	}
}

func sendEnvelope(qc *quic.Conn, env *meshv1.Envelope) error {
	b, err := env.MarshalVT()
	if err != nil {
		return err
	}
	return qc.SendDatagram(b)
}
