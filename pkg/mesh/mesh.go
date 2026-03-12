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
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/observability/traces"
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
	maxBidiStreams      = 256
	queueBufSize        = maxBidiStreams
	probeBufSize        = 2048
	inviteRedeemTTL     = 5 * time.Minute
	sessionReapInterval = 5 * time.Minute
	streamTypeTimeout   = 5 * time.Second

	streamTypeClock  byte = 1
	streamTypeTunnel byte = 2
	streamTypeRouted byte = 3

	routeHeaderSize = 66 // 32 dest + 32 source + 1 TTL + 1 inner stream type (only tunnel=2 accepted at delivery)
	defaultRouteTTL = 16
)

func quicConfig() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:        quicIdleTimeout,
		KeepAlivePeriod:       quicKeepAlivePeriod,
		EnableDatagrams:       true,
		MaxIncomingUniStreams: -1,
		MaxIncomingStreams:    maxBidiStreams,
	}
}

// streamCloser wraps a quic.Stream so that Close cancels reads (STOP_SENDING)
// in addition to closing the write side. Without this, a blocked Read on the
// remote side keeps the stream alive as a zombie.
type streamCloser struct {
	*quic.Stream
}

func (s streamCloser) CloseWrite() error {
	return s.Stream.Close()
}

func (s streamCloser) Close() error {
	s.CancelRead(0)
	return s.Stream.Close()
}

// Router provides next-hop lookups for multi-hop mesh routing.
type Router interface {
	Lookup(dest types.PeerKey) (nextHop types.PeerKey, ok bool)
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
	OpenClockStream(ctx context.Context, peer types.PeerKey) (io.ReadWriteCloser, error)
	AcceptClockStream(ctx context.Context) (types.PeerKey, io.ReadWriteCloser, error)
	JoinWithToken(ctx context.Context, token *admissionv1.JoinToken) error
	JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error)
	Connect(ctx context.Context, peer types.PeerKey, addrs []*net.UDPAddr) error
	Punch(ctx context.Context, peer types.PeerKey, addr *net.UDPAddr, localNAT nat.Type) error
	GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)
	GetConn(peer types.PeerKey) (*quic.Conn, bool)
	PeerCertExpiresAt(peer types.PeerKey) (time.Time, bool)
	PeerDelegationCert(peer types.PeerKey) (*admissionv1.DelegationCert, bool)
	IsOutbound(types.PeerKey) bool
	ConnectedPeers() []types.PeerKey
	ClosePeerSession(peerKey types.PeerKey, reason CloseReason)
	ListenPort() int
	BroadcastDisconnect() error
	UpdateMeshCert(cert tls.Certificate)
	RequestCertRenewal(ctx context.Context, peer types.PeerKey) (*admissionv1.DelegationCert, error)
	SetTracer(t *traces.Tracer)
	SetRouter(r Router)
	Close() error
}

type CloseReason string

const (
	CloseReasonDenied        CloseReason = "denied"
	CloseReasonTopologyPrune CloseReason = "topology_prune"
	CloseReasonCertExpired   CloseReason = "cert_expired"
	CloseReasonCertRotation  CloseReason = "cert_rotation"
	CloseReasonDisconnect    CloseReason = "disconnect"
	CloseReasonDuplicate     CloseReason = "duplicate"
	CloseReasonReplaced      CloseReason = "replaced"
	CloseReasonDisconnected  CloseReason = "disconnected"
	CloseReasonShutdown      CloseReason = "shutdown"
)

type impl struct {
	meshCert         atomic.Pointer[tls.Certificate]
	bareCert         tls.Certificate
	socks            sock.SockStore
	isDenied         func(types.PeerKey) bool
	inCh             chan peer.Input
	recvCh           chan Packet
	renewalCh        chan *meshv1.CertRenewalResponse
	inviteSigner     *auth.DelegationSigner
	streamCh         chan incomingStream
	clockStreamCh    chan incomingStream
	trustBundle      *admissionv1.TrustBundle
	log              *zap.SugaredLogger
	metrics          *metrics.MeshMetrics
	tracer           *traces.Tracer
	router           Router
	listener         *quic.Listener
	sessions         *sessionRegistry
	mainQT           *quic.Transport
	acceptWG         sync.WaitGroup
	port             int
	localKey         types.PeerKey
	membershipTTL    time.Duration
	reconnectWindow  time.Duration
	maxConnectionAge time.Duration
}

type incomingStream struct {
	stream  io.ReadWriteCloser
	peerKey types.PeerKey
}

type peerSession struct {
	conn           *quic.Conn
	transport      *quic.Transport
	sockConn       *sock.Conn
	delegationCert *admissionv1.DelegationCert
	createdAt      time.Time
	certExpiresAt  time.Time
	outbound       bool
}

func newPeerSession(conn *quic.Conn, transport *quic.Transport, outbound bool) *peerSession {
	dc := delegationCertFromConn(conn)
	return &peerSession{
		conn:           conn,
		transport:      transport,
		delegationCert: dc,
		createdAt:      time.Now(),
		certExpiresAt:  auth.CertExpiresAt(dc),
		outbound:       outbound,
	}
}

type directDialResult struct {
	session *peerSession
	err     error
}

func NewMesh(defaultPort int, signPriv ed25519.PrivateKey, creds *auth.NodeCredentials, tlsIdentityTTL, membershipTTL, reconnectWindow, maxConnectionAge time.Duration, isDenied func(types.PeerKey) bool, mm *metrics.MeshMetrics) (Mesh, error) {
	meshCert, err := GenerateIdentityCert(signPriv, creds.Cert, tlsIdentityTTL)
	if err != nil {
		return nil, fmt.Errorf("generate mesh cert: %w", err)
	}

	bareCert, err := GenerateIdentityCert(signPriv, nil, tlsIdentityTTL)
	if err != nil {
		return nil, fmt.Errorf("generate bare cert: %w", err)
	}

	localKey := types.PeerKeyFromBytes(signPriv.Public().(ed25519.PublicKey)) //nolint:forcetypeassert
	m := &impl{
		log:              zap.S().Named("mesh"),
		bareCert:         bareCert,
		trustBundle:      creds.Trust,
		inviteSigner:     creds.DelegationKey,
		isDenied:         isDenied,
		localKey:         localKey,
		socks:            sock.NewSockStore(),
		port:             defaultPort,
		membershipTTL:    membershipTTL,
		reconnectWindow:  reconnectWindow,
		maxConnectionAge: maxConnectionAge,
		sessions:         newSessionRegistry(mm.SessionsActive),
		recvCh:           make(chan Packet, queueBufSize),
		renewalCh:        make(chan *meshv1.CertRenewalResponse, 1),
		inCh:             make(chan peer.Input, queueBufSize),
		streamCh:         make(chan incomingStream, queueBufSize),
		clockStreamCh:    make(chan incomingStream, queueBufSize),
		metrics:          mm,
	}
	m.meshCert.Store(&meshCert)
	return m, nil
}

func (m *impl) Start(ctx context.Context) error {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: m.port})
	if err != nil {
		return fmt.Errorf("listen udp:%d: %w", m.port, err)
	}

	qt := &quic.Transport{Conn: conn}
	ln, err := qt.Listen(newServerTLSConfig(serverTLSParams{
		meshCertPtr:     &m.meshCert,
		inviteCert:      m.bareCert,
		trustBundle:     m.trustBundle,
		reconnectWindow: m.reconnectWindow,
		inviteEnabled:   m.inviteSigner != nil,
	}), quicConfig())
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("quic listen: %w", err)
	}

	m.mainQT = qt
	m.listener = ln

	m.socks.SetMainProbeWriter(func(payload []byte, addr *net.UDPAddr) error {
		_, err := qt.WriteTo(payload, addr)
		return err
	})
	go m.runMainProbeLoop(ctx, qt)
	go m.acceptLoop(ctx)
	if m.maxConnectionAge > 0 {
		go m.sessionReaper(ctx)
	}
	return nil
}

func (m *impl) ListenPort() int {
	return m.mainQT.Conn.LocalAddr().(*net.UDPAddr).Port //nolint:forcetypeassert
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
	for {
		// Snapshot both notification channels BEFORE reading any state.
		sessionCh := m.sessions.onChange()
		routeCh := routeChanged(m.router)

		// Fast path: direct session exists.
		if _, ok := m.sessions.get(peerKey); ok {
			return m.openTypedStream(ctx, peerKey, streamTypeTunnel)
		}

		// No router — only option is waiting for a direct session.
		if m.router == nil {
			return m.openTypedStream(ctx, peerKey, streamTypeTunnel)
		}

		// Check routed path.
		if nextHop, ok := m.router.Lookup(peerKey); ok {
			if _, ok := m.sessions.get(nextHop); ok {
				return m.openRoutedStream(ctx, peerKey, streamTypeTunnel, nextHop)
			}
		}

		// No viable path yet — wait for a session or route change.
		select {
		case <-sessionCh:
		case <-routeCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func routeChanged(r Router) <-chan struct{} {
	type notifier interface{ Changed() <-chan struct{} }
	if n, ok := r.(notifier); ok {
		return n.Changed()
	}
	return nil
}

func (m *impl) AcceptStream(ctx context.Context) (types.PeerKey, io.ReadWriteCloser, error) {
	return m.acceptFromCh(ctx, m.streamCh)
}

func (m *impl) OpenClockStream(ctx context.Context, peerKey types.PeerKey) (io.ReadWriteCloser, error) {
	return m.openTypedStream(ctx, peerKey, streamTypeClock)
}

func (m *impl) AcceptClockStream(ctx context.Context) (types.PeerKey, io.ReadWriteCloser, error) {
	return m.acceptFromCh(ctx, m.clockStreamCh)
}

func (m *impl) openTypedStream(ctx context.Context, peerKey types.PeerKey, streamType byte) (io.ReadWriteCloser, error) {
	s, err := m.sessions.waitFor(ctx, peerKey)
	if err != nil {
		return nil, err
	}
	stream, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := stream.Write([]byte{streamType}); err != nil {
		stream.CancelWrite(0)
		stream.CancelRead(0)
		return nil, err
	}
	if streamType == streamTypeTunnel {
		return streamCloser{stream}, nil
	}
	return stream, nil
}

func (m *impl) acceptFromCh(ctx context.Context, ch <-chan incomingStream) (types.PeerKey, io.ReadWriteCloser, error) {
	select {
	case incoming, ok := <-ch:
		if !ok {
			return types.PeerKey{}, nil, net.ErrClosed
		}
		return incoming.peerKey, incoming.stream, nil
	case <-ctx.Done():
		return types.PeerKey{}, nil, ctx.Err()
	}
}

func (m *impl) dialDirect(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey) (*peerSession, error) {
	tlsCfg := newExpectedPeerTLSConfig(&m.meshCert, expectedPeer, m.trustBundle, m.reconnectWindow)
	qCfg := quicConfig()

	qc, err := m.mainQT.Dial(ctx, addr, tlsCfg, qCfg)
	if err != nil {
		return nil, fmt.Errorf("quic dial %s: %w", addr, err)
	}

	return newPeerSession(qc, m.mainQT, true), nil
}

func (m *impl) dialPunch(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey, localNAT nat.Type) (*peerSession, error) {
	tlsCfg := newExpectedPeerTLSConfig(&m.meshCert, expectedPeer, m.trustBundle, m.reconnectWindow)
	qCfg := quicConfig()

	conn, err := m.socks.Punch(ctx, addr, localNAT)
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
		return newPeerSession(qc, m.mainQT, true), nil
	}

	qt := &quic.Transport{Conn: conn.UDPConn}

	qc, err := qt.Dial(ctx, dialAddr, tlsCfg, qCfg)
	if err != nil {
		_ = qt.Close()
		conn.Close()
		return nil, fmt.Errorf("quic dial %s: %w", dialAddr, err)
	}

	s := newPeerSession(qc, qt, true)
	s.sockConn = conn
	return s, nil
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

	return nil, lastErr
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
	if err := s.conn.SendDatagram(b); err != nil {
		m.handleSendFailure(peerKey, s, err)
		m.metrics.DatagramErrors.Inc()
		return err
	}
	m.metrics.DatagramsSent.Inc()
	m.metrics.DatagramBytesSent.Add(int64(len(b)))
	return nil
}

func (m *impl) handleSendFailure(peerKey types.PeerKey, s *peerSession, err error) {
	reason := classifyQUICError(err)
	if reason == peer.DisconnectUnknown {
		return
	}
	if !m.sessions.removeIfCurrent(peerKey, s) {
		return
	}
	m.metrics.SessionDisconnects.Inc()
	m.closeSession(s, CloseReasonDisconnect)
	select {
	case m.inCh <- peer.PeerDisconnected{PeerKey: peerKey, Reason: reason}:
	case <-time.After(eventSendTimeout):
		m.log.Warnw("dropped peer disconnect event after send failure", "peer", peerKey.Short(), "reason", reason)
	}
}

func (m *impl) Punch(ctx context.Context, peerKey types.PeerKey, addr *net.UDPAddr, localNAT nat.Type) error {
	s, err := m.dialPunch(ctx, addr, peerKey, localNAT)
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
	return s.conn.RemoteAddr().(*net.UDPAddr), true //nolint:forcetypeassert
}

func (m *impl) PeerCertExpiresAt(peerKey types.PeerKey) (time.Time, bool) {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return time.Time{}, false
	}
	return s.certExpiresAt, true
}

func (m *impl) PeerDelegationCert(peerKey types.PeerKey) (*admissionv1.DelegationCert, bool) {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return nil, false
	}
	return s.delegationCert, s.delegationCert != nil
}

func (m *impl) IsOutbound(peerKey types.PeerKey) bool {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return false
	}
	return s.outbound
}

func (m *impl) ConnectedPeers() []types.PeerKey {
	return m.sessions.connectedPeers()
}

func (m *impl) ClosePeerSession(peerKey types.PeerKey, reason CloseReason) {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return
	}
	if m.sessions.removeIfCurrent(peerKey, s) {
		m.metrics.SessionDisconnects.Inc()
		m.closeSession(s, reason)
	}
}

func (m *impl) sessionReaper(ctx context.Context) {
	ticker := time.NewTicker(sessionReapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			for _, peerKey := range m.ConnectedPeers() {
				s, ok := m.sessions.get(peerKey)
				if !ok {
					continue
				}
				if now.Sub(s.createdAt) < m.maxConnectionAge {
					continue
				}
				m.log.Debugw("reconnecting peer to refresh certificates", "peer", peerKey.Short(), "age", now.Sub(s.createdAt))
				if m.sessions.removeIfCurrent(peerKey, s) {
					m.metrics.SessionDisconnects.Inc()
					m.closeSession(s, CloseReasonCertRotation)
					select {
					case m.inCh <- peer.PeerDisconnected{PeerKey: peerKey, Reason: peer.DisconnectCertRotation}:
					case <-time.After(eventSendTimeout):
						m.log.Warnw("dropped cert rotation disconnect event", "peer", peerKey.Short())
					}
				}
			}
		}
	}
}

func (m *impl) BroadcastDisconnect() error {
	peers := m.sessions.drainPeers()
	for _, s := range peers {
		m.closeSession(s, CloseReasonDisconnect)
	}
	return nil
}

func (m *impl) addPeer(s *peerSession, peerKey types.PeerKey) {
	span := m.tracer.Start("mesh.addPeer")
	span.SetAttr("peer", peerKey.Short())

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
		span.SetAttr("result", "duplicate")
		span.End()
		m.closeSession(s, CloseReasonDuplicate)
		return
	}

	if replace != nil {
		m.closeSession(replace, CloseReasonReplaced)
	}

	span.End()
	m.metrics.SessionConnects.Inc()

	// Use the connection's own context so the recv goroutine lives as long as
	// the QUIC connection, not as long as the (potentially short-lived) dial ctx.
	m.acceptWG.Go(func() { m.recvDatagrams(s, peerKey) })
	m.acceptWG.Go(func() { m.acceptBidiStreams(s, peerKey) })

	addr := s.conn.RemoteAddr().(*net.UDPAddr) //nolint:forcetypeassert
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

func (m *impl) acceptBidiStreams(s *peerSession, peerKey types.PeerKey) {
	ctx := s.conn.Context()
	for {
		stream, err := s.conn.AcceptStream(ctx)
		if err != nil {
			return
		}

		_ = stream.SetReadDeadline(time.Now().Add(streamTypeTimeout))
		var typeBuf [1]byte
		if _, err := io.ReadFull(stream, typeBuf[:]); err != nil {
			stream.CancelRead(0)
			stream.CancelWrite(0)
			continue
		}
		_ = stream.SetReadDeadline(time.Time{})

		var ch chan incomingStream
		switch typeBuf[0] {
		case streamTypeClock:
			ch = m.clockStreamCh
		case streamTypeTunnel:
			ch = m.streamCh
		case streamTypeRouted:
			go m.handleRoutedStream(ctx, stream)
			continue
		default:
			stream.CancelRead(0)
			stream.CancelWrite(0)
			continue
		}

		var wrapped io.ReadWriteCloser = stream
		if typeBuf[0] == streamTypeTunnel {
			wrapped = streamCloser{stream}
		}

		select {
		case ch <- incomingStream{peerKey: peerKey, stream: wrapped}:
		case <-ctx.Done():
			stream.CancelRead(0)
			stream.CancelWrite(0)
			return
		default:
			stream.CancelRead(0)
			stream.CancelWrite(0)
		}
	}
}

func (m *impl) recvDatagrams(s *peerSession, peerKey types.PeerKey) {
	ctx := s.conn.Context()
	for {
		payload, err := s.conn.ReceiveDatagram(ctx)
		if err != nil {
			if m.sessions.removeIfCurrent(peerKey, s) {
				m.metrics.SessionDisconnects.Inc()
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
				m.closeSession(s, CloseReasonDisconnected)
			}
			return
		}
		m.metrics.DatagramsRecv.Inc()
		m.metrics.DatagramBytesRecv.Add(int64(len(payload)))
		env := &meshv1.Envelope{}
		if err := env.UnmarshalVT(payload); err != nil {
			continue
		}

		if resp, ok := env.GetBody().(*meshv1.Envelope_CertRenewalResponse); ok {
			select {
			case m.renewalCh <- resp.CertRenewalResponse:
			default:
			}
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
		switch appErr.ErrorMessage {
		case string(CloseReasonTopologyPrune):
			return peer.DisconnectTopologyPrune
		case string(CloseReasonDenied):
			return peer.DisconnectDenied
		case string(CloseReasonCertExpired):
			return peer.DisconnectCertExpired
		case string(CloseReasonCertRotation):
			return peer.DisconnectCertRotation
		default:
			return peer.DisconnectGraceful
		}
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
			if m.isDenied != nil && m.isDenied(peerKey) {
				_ = qc.CloseWithError(0, string(CloseReasonDenied))
				continue
			}
			m.addPeer(newPeerSession(qc, m.mainQT, false), peerKey)
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

func (m *impl) SetTracer(t *traces.Tracer) {
	m.tracer = t
}

func (m *impl) SetRouter(r Router) {
	m.router = r
}

func (m *impl) openRoutedStream(ctx context.Context, dest types.PeerKey, innerType byte, nextHop types.PeerKey) (io.ReadWriteCloser, error) {
	s, ok := m.sessions.get(nextHop)
	if !ok {
		return nil, fmt.Errorf("no session to next hop %s", nextHop.Short())
	}
	stream, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}

	var header [1 + routeHeaderSize]byte
	header[0] = streamTypeRouted
	copy(header[1:33], dest[:])
	copy(header[33:65], m.localKey[:])
	header[65] = defaultRouteTTL
	header[66] = innerType
	if _, err := stream.Write(header[:]); err != nil {
		stream.CancelWrite(0)
		stream.CancelRead(0)
		return nil, err
	}
	return streamCloser{stream}, nil
}

func (m *impl) handleRoutedStream(ctx context.Context, stream *quic.Stream) {
	var header [routeHeaderSize]byte
	if _, err := io.ReadFull(stream, header[:]); err != nil {
		stream.CancelRead(0)
		stream.CancelWrite(0)
		return
	}

	var dest, source types.PeerKey
	copy(dest[:], header[0:32])
	copy(source[:], header[32:64])
	ttl := header[64]
	innerType := header[65]

	if dest == m.localKey {
		m.deliverRoutedStream(ctx, stream, source, innerType)
		return
	}

	if ttl <= 1 {
		m.log.Debugw("routed stream TTL exhausted", "dest", dest.Short(), "source", source.Short())
		stream.CancelRead(0)
		stream.CancelWrite(0)
		return
	}

	m.forwardRoutedStream(ctx, stream, dest, source, ttl-1, innerType)
}

// deliverRoutedStream dispatches a routed stream that has arrived at its final
// destination. Only tunnel streams are accepted; clock/gossip streams must not
// travel routed paths (gossip already propagates transitively via direct peers).
//
// NOTE: The source peerKey is asserted by the routing header, NOT authenticated
// by the QUIC session. Today this is acceptable because the tunnel manager uses
// peerKey only for logging. If future code makes authorization decisions based
// on AcceptStream's peerKey, routed streams must carry end-to-end proof of
// origin (e.g., a signature over the stream header).
func (m *impl) deliverRoutedStream(ctx context.Context, stream *quic.Stream, source types.PeerKey, innerType byte) {
	if innerType != streamTypeTunnel {
		stream.CancelRead(0)
		stream.CancelWrite(0)
		return
	}

	select {
	case m.streamCh <- incomingStream{peerKey: source, stream: streamCloser{stream}}:
	case <-ctx.Done():
		stream.CancelRead(0)
		stream.CancelWrite(0)
	default:
		stream.CancelRead(0)
		stream.CancelWrite(0)
	}
}

func (m *impl) forwardRoutedStream(ctx context.Context, inbound *quic.Stream, dest, source types.PeerKey, ttl, innerType byte) {
	// Find next hop: try direct session first, then router.
	nextHop := dest
	s, ok := m.sessions.get(dest)
	if !ok {
		if m.router == nil {
			m.log.Debugw("no route to forward", "dest", dest.Short())
			inbound.CancelRead(0)
			inbound.CancelWrite(0)
			return
		}
		nextHop, ok = m.router.Lookup(dest)
		if !ok {
			m.log.Debugw("no route to forward", "dest", dest.Short())
			inbound.CancelRead(0)
			inbound.CancelWrite(0)
			return
		}
		if nextHop == source {
			m.log.Debugw("routing loop detected", "dest", dest.Short(), "source", source.Short())
			inbound.CancelRead(0)
			inbound.CancelWrite(0)
			return
		}
		s, ok = m.sessions.get(nextHop)
		if !ok {
			m.log.Debugw("no session to next hop", "nextHop", nextHop.Short())
			inbound.CancelRead(0)
			inbound.CancelWrite(0)
			return
		}
	}

	outbound, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		m.log.Debugw("open outbound routed stream failed", "nextHop", nextHop.Short(), "err", err)
		inbound.CancelRead(0)
		inbound.CancelWrite(0)
		return
	}

	// Write routing header on outbound stream.
	var header [1 + routeHeaderSize]byte
	header[0] = streamTypeRouted
	copy(header[1:33], dest[:])
	copy(header[33:65], source[:])
	header[65] = ttl
	header[66] = innerType
	if _, err := outbound.Write(header[:]); err != nil {
		outbound.CancelWrite(0)
		outbound.CancelRead(0)
		inbound.CancelRead(0)
		inbound.CancelWrite(0)
		return
	}

	bridgeStreams(streamCloser{inbound}, streamCloser{outbound})
}

// --- Routed-stream bridge helpers ---

const routeBufSize = 64 * 1024

var routeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, routeBufSize)
		return &b
	},
}

func bridgeStreams(c1, c2 io.ReadWriteCloser) {
	var wg sync.WaitGroup
	transfer := func(dst, src io.ReadWriteCloser) {
		bufPtr := routeBufPool.Get().(*[]byte) //nolint:forcetypeassert
		defer routeBufPool.Put(bufPtr)
		_, _ = io.CopyBuffer(dst, src, *bufPtr)
		meshCloseWrite(dst)
	}
	wg.Go(func() { transfer(c1, c2) })
	wg.Go(func() { transfer(c2, c1) })
	wg.Wait()
	_ = c1.Close()
	_ = c2.Close()
}

func meshCloseWrite(conn io.Closer) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
		return
	}
	_ = conn.Close()
}

func (m *impl) UpdateMeshCert(cert tls.Certificate) {
	m.meshCert.Store(&cert)
}

func (m *impl) RequestCertRenewal(ctx context.Context, peerKey types.PeerKey) (*admissionv1.DelegationCert, error) {
	s, ok := m.sessions.get(peerKey)
	if !ok {
		return nil, fmt.Errorf("no connection to peer %s", peerKey.Short())
	}

	currentCert := m.meshCert.Load()
	var currentCertRaw []byte
	if len(currentCert.Certificate) > 0 {
		currentCertRaw = currentCert.Certificate[0]
	}

	if err := sendEnvelope(s.conn, &meshv1.Envelope{
		Body: &meshv1.Envelope_CertRenewalRequest{
			CertRenewalRequest: &meshv1.CertRenewalRequest{
				SubjectPub:  m.localKey.Bytes(),
				CurrentCert: currentCertRaw,
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("send renewal request: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	select {
	case resp := <-m.renewalCh:
		if !resp.GetAccepted() {
			reason := resp.GetReason()
			if reason == "" {
				reason = "renewal rejected"
			}
			return nil, errors.New(reason)
		}
		return resp.GetCert(), nil
	case <-waitCtx.Done():
		return nil, fmt.Errorf("recv renewal response: %w", waitCtx.Err())
	}
}

func (m *impl) Close() error {
	for _, s := range m.sessions.drainPeers() {
		m.closeSession(s, CloseReasonShutdown)
	}

	m.acceptWG.Wait()
	close(m.streamCh)
	close(m.clockStreamCh)
	close(m.recvCh)

	if m.listener != nil {
		_ = m.listener.Close()
	}
	if m.mainQT != nil {
		_ = m.mainQT.Close()
		if m.mainQT.Conn != nil {
			_ = m.mainQT.Conn.Close()
		}
	}
	return nil
}

func (m *impl) closeSession(s *peerSession, reason CloseReason) {
	if s.conn != nil {
		_ = s.conn.CloseWithError(0, string(reason))
	}
	if s.transport != nil && s.transport != m.mainQT {
		_ = s.transport.Close()
	}
	if s.sockConn != nil {
		_ = s.sockConn.Close()
	}
}

func (m *impl) handleInviteRedeem(qc *quic.Conn, peerKey types.PeerKey, req *meshv1.InviteRedeemRequest) (retErr error) {
	defer func() {
		if retErr != nil {
			_ = sendInviteRedeemResponse(qc, nil, retErr)
		}
	}()

	signer := m.inviteSigner
	now := time.Now()
	verified, err := auth.VerifyInviteToken(req.GetToken(), ed25519.PublicKey(peerKey.Bytes()), now)
	if err != nil {
		return err
	}

	ttl := inviteRedeemTTL
	if remaining := time.Unix(verified.Claims.GetExpiresAtUnix(), 0).Sub(now); remaining < ttl {
		ttl = remaining
	}
	if ttl <= 0 {
		return errors.New("invite token expired")
	}

	consumed, err := signer.Consumed.TryConsume(req.GetToken(), now)
	if err != nil {
		return err
	}
	if !consumed {
		return errors.New("invite token already consumed")
	}

	membershipTTL := m.membershipTTL
	var accessDeadline time.Time
	if s := verified.Claims.GetMembershipTtlSeconds(); s > 0 {
		accessDeadline = now.Add(time.Duration(s) * time.Second)
	}

	joinToken, err := auth.IssueJoinTokenWithIssuer(
		signer.Priv,
		signer.Trust,
		signer.Issuer,
		ed25519.PublicKey(peerKey.Bytes()),
		verified.Claims.GetBootstrap(),
		now,
		ttl,
		membershipTTL,
		accessDeadline,
	)
	if err != nil {
		return err
	}
	return sendInviteRedeemResponse(qc, joinToken, nil)
}

func sendInviteRedeemResponse(qc *quic.Conn, joinToken *admissionv1.JoinToken, redeemErr error) error {
	resp := &meshv1.InviteRedeemResponse{
		Accepted:  redeemErr == nil,
		JoinToken: joinToken,
	}
	if redeemErr != nil {
		resp.Reason = redeemErr.Error()
	}
	return sendEnvelope(qc, &meshv1.Envelope{
		Body: &meshv1.Envelope_InviteRedeemResponse{InviteRedeemResponse: resp},
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

	for {
		env, err := recvEnvelope(waitCtx, qc)
		if err != nil {
			return nil, err
		}
		resp, ok := env.GetBody().(*meshv1.Envelope_InviteRedeemResponse)
		if !ok {
			continue
		}
		if !resp.InviteRedeemResponse.GetAccepted() {
			if reason := resp.InviteRedeemResponse.GetReason(); reason != "" {
				return nil, errors.New(reason)
			}
			return nil, errors.New("invite token rejected")
		}
		return resp.InviteRedeemResponse.GetJoinToken(), nil
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
