package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/types"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

type Transport interface {
	Start(ctx context.Context) error
	Stop() error
	Connect(ctx context.Context, key types.PeerKey, addrs []netip.AddrPort) error
	Punch(ctx context.Context, peer types.PeerKey, addr *net.UDPAddr, localNAT nat.Type) error
	Send(ctx context.Context, peer types.PeerKey, data []byte) error
	Recv(ctx context.Context) (Packet, error)
	SendTunnelDatagram(ctx context.Context, peer types.PeerKey, data []byte) error
	SendMembershipDatagram(ctx context.Context, peer types.PeerKey, data []byte) error
	RecvTunnelDatagram(ctx context.Context) (Packet, error)
	OpenStream(ctx context.Context, peer types.PeerKey, st StreamType) (Stream, error)
	AcceptStream(ctx context.Context) (Stream, StreamType, types.PeerKey, error)

	DiscoverPeer(pk types.PeerKey, ips []net.IP, port int, lastAddr *net.UDPAddr, privatelyRoutable, publiclyAccessible bool)
	ForgetPeer(pk types.PeerKey)
	ConnectFailed(pk types.PeerKey)
	IsPeerConnected(pk types.PeerKey) bool
	IsPeerConnecting(pk types.PeerKey) bool
	ClosePeerSession(peer types.PeerKey, reason DisconnectReason)

	PeerEvents() <-chan PeerEvent
	SupervisorEvents() <-chan PeerEvent
	ConnectedPeers() []types.PeerKey
	GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)
	IsOutbound(peer types.PeerKey) bool
	ListenPort() int
	GetConn(peer types.PeerKey) (*quic.Conn, bool)
	PeerStateCounts() PeerStateCounts

	UpdateMeshCert(cert tls.Certificate)
	RequestCertRenewal(ctx context.Context, peer types.PeerKey) (*admissionv1.DelegationCert, error)
	PeerDelegationCert(peer types.PeerKey) (*admissionv1.DelegationCert, bool)
	SetInviteForwarder(f InviteForwarder)
	SetInviteSigner(s *auth.DelegationSigner)
	SetInviteConsumer(c auth.InviteConsumer)
	PushCert(ctx context.Context, peer types.PeerKey, cert *admissionv1.DelegationCert) error
	JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error)
}

var _ Transport = (*QUICTransport)(nil)

type StreamType byte

const (
	StreamTypeDigest   StreamType = 1
	StreamTypeTunnel   StreamType = 2
	StreamTypeRouted   StreamType = 3
	StreamTypeArtifact StreamType = 4
	StreamTypeWorkload StreamType = 5
)

type DatagramType byte

const (
	DatagramTypeMembership DatagramType = 1
	DatagramTypeTunnel     DatagramType = 2
	DatagramTypeRouted     DatagramType = 3
)

type Stream struct{ *quic.Stream }

func (s Stream) CloseWrite() error { return s.Stream.Close() }
func (s Stream) Close() error {
	s.CancelRead(0)
	return s.Stream.Close()
}

type Packet struct {
	Data []byte
	From types.PeerKey
}

type PeerEventType int

const (
	PeerEventConnected PeerEventType = iota
	PeerEventDisconnected
	peerEventNeedsPunch
)

type PeerEvent struct {
	Addrs []netip.AddrPort
	Type  PeerEventType
	Key   types.PeerKey
}

type PeerStateCounts struct {
	Connected  uint32
	Connecting uint32
	Backoff    uint32
}

var errUnreachable = errors.New("peer unreachable")

// MaxDatagramPayload is the largest payload callers can pass to Send or
// SendTunnelDatagram. Derived from the QUIC minimum MTU (1200 bytes) minus
// worst-case QUIC packet overhead (~50 bytes for short header, DATAGRAM
// frame type, length varint, and AEAD tag). Conservative to avoid relying
// on path MTU discovery.
const MaxDatagramPayload = 1150

const (
	handshakeTimeout       = 3 * time.Second
	quicIdleTimeout        = 30 * time.Second
	quicKeepAlivePeriod    = 10 * time.Second
	maxBidiStreams         = 256
	queueBufSize           = maxBidiStreams
	probeBufSize           = 2048
	inviteRedeemTTL        = 5 * time.Minute
	sessionReapInterval    = 5 * time.Minute
	streamTypeTimeout      = 5 * time.Second
	internalConnectTimeout = 2 * time.Second
	routeHeaderSize        = 66
	defaultRouteTTL        = 16
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

type TrafficRecorder interface {
	Record(peer types.PeerKey, bytesIn, bytesOut uint64)
}

type Router interface {
	NextHop(dest types.PeerKey) (nextHop types.PeerKey, ok bool)
}

// -----------------------------------------------------------------------------
// Options & Initialization
// -----------------------------------------------------------------------------

type transportOptions struct {
	router           Router
	packetConn       net.PacketConn
	inviteConsumer   auth.InviteConsumer
	tracerProvider   trace.TracerProvider
	trafficTracker   TrafficRecorder
	isDenied         func(types.PeerKey) bool
	metrics          *metrics.MeshMetrics
	signPriv         ed25519.PrivateKey
	tlsIdentityTTL   time.Duration
	maxConnectionAge time.Duration
	peerTickInterval time.Duration
	reconnectWindow  time.Duration
	membershipTTL    time.Duration
	disableNATPunch  bool
}

type Option func(*transportOptions)

func WithSigningKey(key ed25519.PrivateKey) Option {
	return func(o *transportOptions) { o.signPriv = key }
}

func WithTLSIdentityTTL(d time.Duration) Option {
	return func(o *transportOptions) { o.tlsIdentityTTL = d }
}

func WithMembershipTTL(d time.Duration) Option {
	return func(o *transportOptions) { o.membershipTTL = d }
}

func WithReconnectWindow(d time.Duration) Option {
	return func(o *transportOptions) { o.reconnectWindow = d }
}

func WithMaxConnectionAge(d time.Duration) Option {
	return func(o *transportOptions) { o.maxConnectionAge = d }
}

func WithInviteConsumer(c auth.InviteConsumer) Option {
	return func(o *transportOptions) { o.inviteConsumer = c }
}

func WithIsDenied(fn func(types.PeerKey) bool) Option {
	return func(o *transportOptions) { o.isDenied = fn }
}
func WithMetrics(m *metrics.MeshMetrics) Option { return func(o *transportOptions) { o.metrics = m } }
func WithPacketConn(conn net.PacketConn) Option {
	return func(o *transportOptions) { o.packetConn = conn }
}
func WithDisableNATPunch() Option { return func(o *transportOptions) { o.disableNATPunch = true } }
func WithTracer(tp trace.TracerProvider) Option {
	return func(o *transportOptions) { o.tracerProvider = tp }
}

func WithPeerTickInterval(d time.Duration) Option {
	return func(o *transportOptions) { o.peerTickInterval = d }
}
func WithRouter(r Router) Option { return func(o *transportOptions) { o.router = r } }

type QUICTransport struct {
	bareCert         tls.Certificate
	inviteConsumer   auth.InviteConsumer
	router           Router
	injectedConn     net.PacketConn
	trafficTracker   TrafficRecorder
	tracer           trace.Tracer
	peers            *peerStore
	listener         *quic.Listener
	renewalCh        chan *meshv1.CertRenewalResponse
	certPushCh       chan *meshv1.CertPushResponse
	inviteSigner     *auth.DelegationSigner
	inviteForwarder  InviteForwarder
	recvCh           chan Packet
	tunnelDatagramCh chan Packet
	acceptCh         chan acceptedStream
	mainQT           *quic.Transport
	metrics          *metrics.MeshMetrics
	log              *zap.SugaredLogger
	isDenied         func(types.PeerKey) bool
	socks            *sockStoreImpl
	meshCert         atomic.Pointer[tls.Certificate]
	supervisorCh     chan PeerEvent
	peerEventCh      chan PeerEvent
	waiters          map[types.PeerKey]chan struct{}
	sessions         map[types.PeerKey]*peerSession
	rootPub          []byte
	acceptWG         sync.WaitGroup
	port             int
	membershipTTL    time.Duration
	reconnectWindow  time.Duration
	maxConnectionAge time.Duration
	peerTickInterval time.Duration
	sessionsMu       sync.RWMutex
	inviteHandlerMu  sync.RWMutex
	certPushMu       sync.Mutex
	localKey         types.PeerKey
	disableNATPunch  bool
}

func New(self types.PeerKey, creds *auth.NodeCredentials, listenAddr string, opts ...Option) (*QUICTransport, error) {
	o := transportOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.metrics == nil {
		o.metrics = metrics.NewMeshMetrics(metricnoop.NewMeterProvider())
	}
	if o.signPriv == nil {
		return nil, fmt.Errorf("signing key required")
	}

	port := 0
	if listenAddr != "" {
		_, portStr, err := net.SplitHostPort(listenAddr)
		if err != nil {
			return nil, fmt.Errorf("parse listen addr: %w", err)
		}
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("parse listen port: %w", err)
		}
	}

	meshCert, err := GenerateIdentityCert(o.signPriv, creds.Cert(), o.tlsIdentityTTL)
	if err != nil {
		return nil, fmt.Errorf("generate mesh cert: %w", err)
	}

	bareCert, err := GenerateIdentityCert(o.signPriv, nil, o.tlsIdentityTTL)
	if err != nil {
		return nil, fmt.Errorf("generate bare cert: %w", err)
	}

	peerTickInterval := o.peerTickInterval
	if peerTickInterval == 0 {
		peerTickInterval = time.Second
	}

	var tracer trace.Tracer
	if o.tracerProvider != nil {
		tracer = o.tracerProvider.Tracer("pollen/transport")
	} else {
		tracer = tracenoop.NewTracerProvider().Tracer("pollen/transport")
	}

	m := &QUICTransport{
		log:              zap.S().Named("mesh"),
		bareCert:         bareCert,
		rootPub:          creds.RootPub(),
		inviteSigner:     creds.DelegationKey(),
		inviteConsumer:   o.inviteConsumer,
		isDenied:         o.isDenied,
		localKey:         self,
		port:             port,
		membershipTTL:    o.membershipTTL,
		reconnectWindow:  o.reconnectWindow,
		maxConnectionAge: o.maxConnectionAge,
		peerTickInterval: peerTickInterval,
		injectedConn:     o.packetConn,
		disableNATPunch:  o.disableNATPunch,
		router:           o.router,
		trafficTracker:   o.trafficTracker,
		tracer:           tracer,
		sessions:         make(map[types.PeerKey]*peerSession),
		waiters:          make(map[types.PeerKey]chan struct{}),
		recvCh:           make(chan Packet, queueBufSize),
		tunnelDatagramCh: make(chan Packet, queueBufSize),
		renewalCh:        make(chan *meshv1.CertRenewalResponse, 1),
		certPushCh:       make(chan *meshv1.CertPushResponse, 1),
		peerEventCh:      make(chan PeerEvent, queueBufSize),
		supervisorCh:     make(chan PeerEvent, queueBufSize),
		acceptCh:         make(chan acceptedStream),
		metrics:          o.metrics,
	}
	m.peers = newPeerStore(m)
	m.meshCert.Store(&meshCert)
	return m, nil
}

func (m *QUICTransport) Start(ctx context.Context) error {
	var pconn net.PacketConn
	if m.injectedConn != nil {
		pconn = m.injectedConn
	} else {
		udpConn, err := net.ListenUDP("udp", &net.UDPAddr{Port: m.port})
		if err != nil {
			return fmt.Errorf("listen udp:%d: %w", m.port, err)
		}
		pconn = udpConn
	}

	qt := &quic.Transport{Conn: pconn}
	ln, err := qt.Listen(newServerTLSConfig(serverTLSParams{
		meshCertPtr:     &m.meshCert,
		inviteCert:      m.bareCert,
		rootPub:         m.rootPub,
		reconnectWindow: m.reconnectWindow,
		inviteEnabled:   m.inviteSigner != nil || m.inviteForwarder != nil,
	}), quicConfig())
	if err != nil {
		_ = pconn.Close()
		return fmt.Errorf("quic listen: %w", err)
	}

	m.mainQT = qt
	m.listener = ln

	if !m.disableNATPunch {
		m.socks = newSockStore()
		m.socks.SetMainProbeWriter(func(payload []byte, addr *net.UDPAddr) error {
			_, err := qt.WriteTo(payload, addr)
			return err
		})
		m.acceptWG.Go(func() { m.runMainProbeLoop(ctx, qt) })
	}

	m.acceptWG.Go(func() { m.acceptLoop(ctx) })
	m.acceptWG.Go(func() { m.peers.runPeerTickLoop(ctx, m.peerTickInterval) })

	if m.maxConnectionAge > 0 {
		m.acceptWG.Go(func() { m.sessionReaper(ctx) })
	}
	return nil
}

func (m *QUICTransport) Stop() error {
	m.sessionsMu.Lock()
	for _, s := range m.sessions {
		m.closeSession(s, "shutdown")
	}
	m.sessions = make(map[types.PeerKey]*peerSession)
	m.metrics.SessionsActive.Record(context.Background(), 0)
	m.sessionsMu.Unlock()

	if m.listener != nil {
		_ = m.listener.Close()
	}
	if m.mainQT != nil {
		_ = m.mainQT.Close()
		if m.mainQT.Conn != nil {
			_ = m.mainQT.Conn.Close()
		}
	}

	m.acceptWG.Wait()
	close(m.acceptCh)
	close(m.recvCh)
	close(m.tunnelDatagramCh)
	return nil
}

type peerSession struct {
	conn           *quic.Conn
	transport      *quic.Transport
	sockConn       *conn
	delegationCert *admissionv1.DelegationCert
	createdAt      time.Time
	certExpiresAt  time.Time
	outbound       bool
}

func (m *QUICTransport) addPeer(_ context.Context, s *peerSession, peerKey types.PeerKey) {
	m.sessionsMu.Lock()
	if current, ok := m.sessions[peerKey]; ok {
		if current.conn.Context().Err() == nil {
			weAreSmaller := m.localKey.Compare(peerKey) < 0
			currentPreferred := current.outbound == weAreSmaller
			if currentPreferred {
				m.sessionsMu.Unlock()
				m.closeSession(s, "duplicate")
				return
			}
		}
		m.closeSession(current, "replaced")
	}

	m.sessions[peerKey] = s
	m.metrics.SessionsActive.Record(context.Background(), float64(len(m.sessions)))

	if w, ok := m.waiters[peerKey]; ok {
		close(w)
		delete(m.waiters, peerKey)
	}
	m.sessionsMu.Unlock()

	m.acceptWG.Go(func() { m.recvDatagrams(s, peerKey) })
	m.acceptWG.Go(func() { m.acceptBidiStreams(s, peerKey) })

	addr := s.conn.RemoteAddr().(*net.UDPAddr) //nolint:forcetypeassert
	m.peers.MarkConnected(peerKey, addr.IP, addr.Port)
	m.emitPeerEvent(PeerEvent{Key: peerKey, Type: PeerEventConnected, Addrs: []netip.AddrPort{addr.AddrPort()}})
}

func (m *QUICTransport) closeSession(s *peerSession, reason string) {
	if s.conn != nil {
		_ = s.conn.CloseWithError(0, reason)
	}
	if s.transport != nil && s.transport != m.mainQT {
		_ = s.transport.Close()
	}
	if s.sockConn != nil {
		_ = s.sockConn.Close()
	}
}

func (m *QUICTransport) removePeer(pk types.PeerKey, s *peerSession, dr DisconnectReason) {
	m.sessionsMu.Lock()
	active, ok := m.sessions[pk]
	if ok && active == s {
		delete(m.sessions, pk)
		m.metrics.SessionsActive.Record(context.Background(), float64(len(m.sessions)))
	} else {
		m.sessionsMu.Unlock()
		return
	}
	m.sessionsMu.Unlock()

	m.closeSession(s, dr.String())
	m.peers.Disconnect(pk, dr)
	m.emitPeerEvent(PeerEvent{Key: pk, Type: PeerEventDisconnected})
}

func (m *QUICTransport) acceptLoop(ctx context.Context) {
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
				_ = qc.CloseWithError(0, DisconnectDenied.String())
				continue
			}
			dc := delegationCertFromConn(qc)
			m.addPeer(ctx, &peerSession{
				conn:           qc,
				transport:      m.mainQT,
				delegationCert: dc,
				createdAt:      time.Now(),
				certExpiresAt:  auth.CertExpiresAt(dc),
				outbound:       false,
			}, peerKey)
		case alpnInvite:
			m.acceptWG.Go(func() { m.handleInviteConnection(ctx, qc, peerKey) })
		default:
			_ = qc.CloseWithError(0, "unknown protocol")
		}
	}
}

func (m *QUICTransport) acceptBidiStreams(s *peerSession, peerKey types.PeerKey) {
	ctx := s.conn.Context()
	for {
		qs, err := s.conn.AcceptStream(ctx)
		if err != nil {
			return
		}

		_ = qs.SetReadDeadline(time.Now().Add(streamTypeTimeout))
		var typeBuf [1]byte
		if _, err := io.ReadFull(qs, typeBuf[:]); err != nil {
			cancelStream(qs)
			continue
		}
		_ = qs.SetReadDeadline(time.Time{})

		streamType := StreamType(typeBuf[0])
		if streamType == StreamTypeRouted {
			m.acceptWG.Go(func() { m.handleRoutedStream(ctx, qs, peerKey) })
		} else {
			select {
			case m.acceptCh <- acceptedStream{stream: Stream{qs}, stype: streamType, peerKey: peerKey}:
			case <-ctx.Done():
				cancelStream(qs)
			}
		}
	}
}

func (m *QUICTransport) recvDatagrams(s *peerSession, peerKey types.PeerKey) {
	ctx := s.conn.Context()
	for {
		payload, err := s.conn.ReceiveDatagram(ctx)
		if err != nil {
			reason := classifyQUICError(err)
			m.removePeer(peerKey, s, reason)
			return
		}
		m.metrics.DatagramsRecv.Add(ctx, 1)
		m.metrics.DatagramBytesRecv.Add(ctx, int64(len(payload)))

		switch DatagramType(payload[0]) {
		case DatagramTypeMembership:
			data := payload[1:]
			env := &meshv1.Envelope{}
			if err := env.UnmarshalVT(data); err != nil {
				continue
			}
			if resp, ok := env.GetBody().(*meshv1.Envelope_CertRenewalResponse); ok {
				select {
				case m.renewalCh <- resp.CertRenewalResponse:
				case <-ctx.Done():
					return
				}
				continue
			}
			if resp, ok := env.GetBody().(*meshv1.Envelope_CertPushResponse); ok {
				select {
				case m.certPushCh <- resp.CertPushResponse:
				case <-ctx.Done():
					return
				}
				continue
			}
			select {
			case m.recvCh <- Packet{From: peerKey, Data: data}:
			case <-ctx.Done():
				return
			}

		case DatagramTypeTunnel:
			select {
			case m.tunnelDatagramCh <- Packet{From: peerKey, Data: payload[1:]}:
			case <-ctx.Done():
				return
			}

		case DatagramTypeRouted:
			m.handleRoutedDatagram(ctx, payload[1:], peerKey)

		default:
			continue
		}
	}
}

func (m *QUICTransport) Connect(ctx context.Context, peerKey types.PeerKey, addrs []netip.AddrPort) error {
	if len(addrs) == 0 {
		return fmt.Errorf("connect to %s: no addresses", peerKey.Short())
	}

	dialCtx, cancelDial := context.WithCancel(ctx)
	defer cancelDial()

	type result struct {
		s   *peerSession
		err error
	}
	ch := make(chan result, len(addrs))

	for _, ap := range addrs {
		m.acceptWG.Go(func() {
			tlsCfg := newExpectedPeerTLSConfig(&m.meshCert, peerKey, m.rootPub, m.reconnectWindow)
			qc, err := m.mainQT.Dial(dialCtx, net.UDPAddrFromAddrPort(ap), tlsCfg, quicConfig())
			if err != nil {
				ch <- result{err: err}
				return
			}
			dc := delegationCertFromConn(qc)
			ch <- result{s: &peerSession{
				conn: qc, transport: m.mainQT, outbound: true,
				createdAt: time.Now(), certExpiresAt: auth.CertExpiresAt(dc), delegationCert: dc,
			}}
		})
	}

	var lastErr error
	consumed := 0
	for range addrs {
		select {
		case r := <-ch:
			consumed++
			if r.err != nil {
				lastErr = r.err
				continue
			}
			cancelDial()
			m.addPeer(ctx, r.s, peerKey)
			remaining := len(addrs) - consumed
			if remaining > 0 {
				m.acceptWG.Go(func() {
					for range remaining {
						if extra := <-ch; extra.s != nil {
							_ = extra.s.conn.CloseWithError(0, "duplicate")
						}
					}
				})
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return lastErr
}

func (m *QUICTransport) Punch(ctx context.Context, peerKey types.PeerKey, addr *net.UDPAddr, localNAT nat.Type) error {
	if m.disableNATPunch {
		return fmt.Errorf("NAT punch disabled")
	}

	conn, err := m.socks.Punch(ctx, addr, localNAT)
	if err != nil {
		return err
	}

	dialAddr := addr
	if peerAddr := conn.Peer(); peerAddr != nil {
		dialAddr = peerAddr
	}

	tlsCfg := newExpectedPeerTLSConfig(&m.meshCert, peerKey, m.rootPub, m.reconnectWindow)

	var qc *quic.Conn
	var qt *quic.Transport

	if conn.UDPConn == nil {
		qt = m.mainQT
		qc, err = qt.Dial(ctx, dialAddr, tlsCfg, quicConfig())
	} else {
		qt = &quic.Transport{Conn: conn.UDPConn}
		qc, err = qt.Dial(ctx, dialAddr, tlsCfg, quicConfig())
		if err != nil {
			_ = qt.Close()
			conn.Close()
		}
	}

	if err != nil {
		return err
	}

	dc := delegationCertFromConn(qc)
	s := &peerSession{
		conn: qc, transport: qt, sockConn: conn, outbound: true,
		createdAt: time.Now(), certExpiresAt: auth.CertExpiresAt(dc), delegationCert: dc,
	}
	m.addPeer(ctx, s, peerKey)
	return nil
}

func cancelStream(s *quic.Stream) {
	s.CancelRead(0)
	s.CancelWrite(0)
}

type acceptedStream struct {
	stream  Stream
	stype   StreamType
	peerKey types.PeerKey
}

func (m *QUICTransport) emitPeerEvent(ev PeerEvent) {
	select {
	case m.peerEventCh <- ev:
	default:
	}
	select {
	case m.supervisorCh <- ev:
	default:
	}
}

func (m *QUICTransport) getSession(pk types.PeerKey) (*peerSession, bool) {
	m.sessionsMu.RLock()
	defer m.sessionsMu.RUnlock()
	s, ok := m.sessions[pk]
	return s, ok
}

func (m *QUICTransport) runMainProbeLoop(ctx context.Context, qt *quic.Transport) {
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

func (m *QUICTransport) sessionReaper(ctx context.Context) {
	ticker := time.NewTicker(sessionReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			for _, pk := range m.ConnectedPeers() {
				if s, ok := m.getSession(pk); ok && now.Sub(s.createdAt) >= m.maxConnectionAge {
					m.removePeer(pk, s, DisconnectCertRotation)
				}
			}
		}
	}
}

func (m *QUICTransport) raceDirectDial(ctx context.Context, peerKey types.PeerKey, addrs []netip.AddrPort) (*peerSession, error) {
	dialCtx, cancelDial := context.WithCancel(ctx)
	defer cancelDial()

	type result struct {
		s   *peerSession
		err error
	}
	ch := make(chan result, len(addrs))

	for _, ap := range addrs {
		go func(addr netip.AddrPort) {
			tlsCfg := newExpectedPeerTLSConfig(&m.meshCert, peerKey, m.rootPub, m.reconnectWindow)
			qc, err := m.mainQT.Dial(dialCtx, net.UDPAddrFromAddrPort(addr), tlsCfg, quicConfig())
			if err != nil {
				ch <- result{err: err}
				return
			}
			dc := delegationCertFromConn(qc)
			ch <- result{s: &peerSession{
				conn: qc, transport: m.mainQT, outbound: true,
				createdAt: time.Now(), certExpiresAt: auth.CertExpiresAt(dc), delegationCert: dc,
			}}
		}(ap)
	}

	var lastErr error
	for range addrs {
		select {
		case r := <-ch:
			if r.err != nil {
				lastErr = r.err
				continue
			}
			cancelDial()
			return r.s, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

func (m *QUICTransport) AcceptStream(ctx context.Context) (Stream, StreamType, types.PeerKey, error) {
	select {
	case as := <-m.acceptCh:
		return as.stream, as.stype, as.peerKey, nil
	case <-ctx.Done():
		return Stream{}, 0, types.PeerKey{}, ctx.Err()
	}
}

func (m *QUICTransport) OpenStream(ctx context.Context, peerKey types.PeerKey, st StreamType) (Stream, error) {
	if st == StreamTypeDigest {
		return m.openTypedStream(ctx, peerKey, st)
	}

	if _, ok := m.getSession(peerKey); ok {
		return m.openTypedStream(ctx, peerKey, st)
	}
	if m.router != nil {
		if nextHop, ok := m.router.NextHop(peerKey); ok {
			if _, ok := m.getSession(nextHop); ok {
				return m.openRoutedStream(ctx, peerKey, st, nextHop)
			}
		}
	}
	return m.openTypedStream(ctx, peerKey, st)
}

func (m *QUICTransport) openTypedStream(ctx context.Context, peerKey types.PeerKey, streamType StreamType) (Stream, error) {
	s, ok := m.getSession(peerKey)
	if !ok {
		return Stream{}, errUnreachable
	}
	stream, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		return Stream{}, err
	}
	if _, err := stream.Write([]byte{byte(streamType)}); err != nil {
		cancelStream(stream)
		return Stream{}, err
	}
	return Stream{stream}, nil
}

func (m *QUICTransport) PeerEvents() <-chan PeerEvent       { return m.peerEventCh }
func (m *QUICTransport) SupervisorEvents() <-chan PeerEvent { return m.supervisorCh }

func (m *QUICTransport) ConnectedPeers() []types.PeerKey {
	m.sessionsMu.RLock()
	defer m.sessionsMu.RUnlock()
	keys := make([]types.PeerKey, 0, len(m.sessions))
	for k := range m.sessions {
		keys = append(keys, k)
	}
	return keys
}

func (m *QUICTransport) GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool) {
	s, ok := m.getSession(peer)
	if !ok {
		return nil, false
	}
	return s.conn.RemoteAddr().(*net.UDPAddr), true //nolint:forcetypeassert
}

func (m *QUICTransport) IsOutbound(peer types.PeerKey) bool {
	s, ok := m.getSession(peer)
	return ok && s.outbound
}

func (m *QUICTransport) ListenPort() int { return m.mainQT.Conn.LocalAddr().(*net.UDPAddr).Port } //nolint:forcetypeassert
func (m *QUICTransport) GetConn(peer types.PeerKey) (*quic.Conn, bool) {
	s, ok := m.getSession(peer)
	if !ok {
		return nil, false
	}
	return s.conn, true
}

func (m *QUICTransport) PeerStateCounts() PeerStateCounts    { return m.peers.stateCounts() }
func (m *QUICTransport) UpdateMeshCert(cert tls.Certificate) { m.meshCert.Store(&cert) }
func (m *QUICTransport) PeerDelegationCert(peerKey types.PeerKey) (*admissionv1.DelegationCert, bool) {
	s, ok := m.getSession(peerKey)
	if !ok {
		return nil, false
	}
	return s.delegationCert, s.delegationCert != nil
}

func (m *QUICTransport) DiscoverPeer(pk types.PeerKey, ips []net.IP, port int, lastAddr *net.UDPAddr, privatelyRoutable, publiclyAccessible bool) {
	m.peers.Discover(pk, ips, port, lastAddr, privatelyRoutable, publiclyAccessible)
}

func (m *QUICTransport) ForgetPeer(pk types.PeerKey) { m.peers.Forget(pk) }

func (m *QUICTransport) ConnectFailed(pk types.PeerKey) {
	m.peers.mu.Lock()
	m.peers.failConnectLocked(pk, time.Now())
	m.peers.mu.Unlock()
}

func (m *QUICTransport) IsPeerConnected(pk types.PeerKey) bool {
	m.peers.mu.RLock()
	defer m.peers.mu.RUnlock()
	p, ok := m.peers.m[pk]
	return ok && p.State == peerStateConnected
}

func (m *QUICTransport) IsPeerConnecting(pk types.PeerKey) bool {
	m.peers.mu.RLock()
	defer m.peers.mu.RUnlock()
	p, ok := m.peers.m[pk]
	return ok && p.State == peerStateConnecting
}

func (m *QUICTransport) ClosePeerSession(peer types.PeerKey, reason DisconnectReason) {
	if s, ok := m.getSession(peer); ok {
		m.removePeer(peer, s, reason)
	}
}

func (m *QUICTransport) sendRawDatagram(ctx context.Context, peerKey types.PeerKey, data []byte) error {
	s, ok := m.getSession(peerKey)
	if !ok {
		return fmt.Errorf("no connection to peer %s", peerKey.Short())
	}
	if err := s.conn.SendDatagram(data); err != nil {
		m.removePeer(peerKey, s, classifyQUICError(err))
		m.metrics.DatagramErrors.Add(ctx, 1)
		return err
	}
	m.metrics.DatagramsSent.Add(ctx, 1)
	m.metrics.DatagramBytesSent.Add(ctx, int64(len(data)))
	return nil
}

func (m *QUICTransport) Send(ctx context.Context, peerKey types.PeerKey, data []byte) error {
	prefixed := make([]byte, 1+len(data))
	prefixed[0] = byte(DatagramTypeMembership)
	copy(prefixed[1:], data)
	return m.sendRawDatagram(ctx, peerKey, prefixed)
}

func (m *QUICTransport) Recv(ctx context.Context) (Packet, error) {
	select {
	case p := <-m.recvCh:
		return p, nil
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	}
}

func (m *QUICTransport) sendOrRoute(ctx context.Context, dest types.PeerKey, dgType DatagramType, data []byte) error {
	if _, ok := m.getSession(dest); ok {
		prefixed := make([]byte, 1+len(data))
		prefixed[0] = byte(dgType)
		copy(prefixed[1:], data)
		return m.sendRawDatagram(ctx, dest, prefixed)
	}
	if m.router != nil {
		if nextHop, ok := m.router.NextHop(dest); ok {
			if _, ok := m.getSession(nextHop); ok {
				return m.sendRoutedDatagram(ctx, dest, dgType, data, nextHop)
			}
		}
	}
	return errUnreachable
}

func (m *QUICTransport) SendTunnelDatagram(ctx context.Context, peerKey types.PeerKey, data []byte) error {
	return m.sendOrRoute(ctx, peerKey, DatagramTypeTunnel, data)
}

func (m *QUICTransport) SendMembershipDatagram(ctx context.Context, peerKey types.PeerKey, data []byte) error {
	return m.sendOrRoute(ctx, peerKey, DatagramTypeMembership, data)
}

func (m *QUICTransport) RecvTunnelDatagram(ctx context.Context) (Packet, error) {
	select {
	case p, ok := <-m.tunnelDatagramCh:
		if !ok {
			return Packet{}, io.EOF
		}
		return p, nil
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	}
}

func (m *QUICTransport) SetRouter(r Router) {
	m.router = r
}

func (m *QUICTransport) SetTrafficTracker(t TrafficRecorder) {
	m.trafficTracker = t
}

func (m *QUICTransport) SetInviteForwarder(f InviteForwarder) {
	m.inviteHandlerMu.Lock()
	m.inviteForwarder = f
	m.inviteHandlerMu.Unlock()
}

func (m *QUICTransport) SetInviteSigner(s *auth.DelegationSigner) {
	m.inviteHandlerMu.Lock()
	m.inviteSigner = s
	m.inviteHandlerMu.Unlock()
}

func (m *QUICTransport) SetInviteConsumer(c auth.InviteConsumer) {
	m.inviteHandlerMu.Lock()
	m.inviteConsumer = c
	m.inviteHandlerMu.Unlock()
}

func (m *QUICTransport) PushCert(ctx context.Context, peerKey types.PeerKey, cert *admissionv1.DelegationCert) error {
	m.certPushMu.Lock()
	defer m.certPushMu.Unlock()

	select {
	case <-m.certPushCh:
	default:
	}

	data, err := (&meshv1.Envelope{
		Body: &meshv1.Envelope_CertPushRequest{CertPushRequest: &meshv1.CertPushRequest{Cert: cert}},
	}).MarshalVT()
	if err != nil {
		return fmt.Errorf("marshal cert push: %w", err)
	}
	if err := m.SendMembershipDatagram(ctx, peerKey, data); err != nil {
		return fmt.Errorf("send cert push: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	select {
	case resp := <-m.certPushCh:
		if !resp.GetAccepted() {
			reason := resp.GetReason()
			if reason == "" {
				reason = "cert push rejected"
			}
			return errors.New(reason)
		}
		return nil
	case <-waitCtx.Done():
		return fmt.Errorf("cert push response: %w", waitCtx.Err())
	}
}

func (m *QUICTransport) SetPeerMetrics(pm *metrics.PeerMetrics) {
	m.peers.mu.Lock()
	defer m.peers.mu.Unlock()
	m.peers.metrics = pm
}
