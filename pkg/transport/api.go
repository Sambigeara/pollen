package transport

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"

	"github.com/quic-go/quic-go"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/types"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

type TransportAPI interface {
	Start(ctx context.Context) error
	Stop() error

	Connect(ctx context.Context, key types.PeerKey, addrs []netip.AddrPort) error
	Disconnect(key types.PeerKey) error
	PeerEvents() <-chan PeerEvent
	ConnectedPeers() []types.PeerKey

	Send(ctx context.Context, peer types.PeerKey, data []byte) error
	Recv(ctx context.Context) (Packet, error)

	OpenStream(ctx context.Context, peer types.PeerKey, st StreamType) (Stream, error)
	AcceptStream(ctx context.Context) (Stream, StreamType, types.PeerKey, error)
}

var _ TransportAPI = (*QUICTransport)(nil)

type QUICTransport = impl

type StreamType byte

const (
	StreamTypeDigest      StreamType = 1
	StreamTypeTunnel      StreamType = 2
	StreamTypeRouted      StreamType = 3
	StreamTypeArtifact    StreamType = 4
	StreamTypeWorkload    StreamType = 5
	StreamTypeCertRenewal StreamType = 6
)

type Stream struct{ *quic.Stream }

func (s Stream) CloseWrite() error { return s.Stream.Close() }

func (s Stream) Close() error {
	s.CancelRead(0)
	return s.Stream.Close()
}

type TrafficRecorder interface {
	Record(peer types.PeerKey, bytesIn, bytesOut uint64)
}

type Router interface {
	NextHop(dest types.PeerKey) (nextHop types.PeerKey, ok bool)
}

type Packet struct {
	Data []byte
	From types.PeerKey
}

type PeerEventType int

const (
	PeerEventConnected    PeerEventType = iota
	PeerEventDisconnected PeerEventType = iota
	peerEventNeedsPunch   PeerEventType = iota
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

const (
	CloseReasonDenied        = "denied"
	CloseReasonTopologyPrune = "topology_prune"
	CloseReasonCertExpired   = "cert_expired"
)

var ErrUnreachable = errors.New("peer unreachable")

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

	routeHeaderSize = 66
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

type Option func(*transportOptions)

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

func WithMetrics(m *metrics.MeshMetrics) Option {
	return func(o *transportOptions) { o.metrics = m }
}

func WithPacketConn(conn net.PacketConn) Option {
	return func(o *transportOptions) { o.packetConn = conn }
}

func WithDisableNATPunch() Option {
	return func(o *transportOptions) { o.disableNATPunch = true }
}

func WithTracer(tp trace.TracerProvider) Option {
	return func(o *transportOptions) { o.tracerProvider = tp }
}

func WithPeerTickInterval(d time.Duration) Option {
	return func(o *transportOptions) { o.peerTickInterval = d }
}

func WithRouter(r Router) Option {
	return func(o *transportOptions) { o.router = r }
}

func WithTrafficTracker(t TrafficRecorder) Option {
	return func(o *transportOptions) { o.trafficTracker = t }
}

func New(self types.PeerKey, creds auth.NodeCredentials, listenAddr string, opts ...Option) (*QUICTransport, error) {
	o := transportOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.metrics == nil {
		o.metrics = metrics.NewMeshMetrics(metricnoop.NewMeterProvider())
	}
	if o.signPriv == nil {
		return nil, fmt.Errorf("signing key required (use WithSigningKey)")
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

	m := &impl{
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
		peers:            newPeerStore(),
		sessions:         newSessionRegistry(o.metrics.SessionsActive),
		recvCh:           make(chan Packet, queueBufSize),
		renewalCh:        make(chan *meshv1.CertRenewalResponse, 1),
		peerEventCh:      make(chan PeerEvent, queueBufSize),
		supervisorCh:     make(chan PeerEvent, queueBufSize),
		acceptCh:         make(chan acceptedStream),
		metrics:          o.metrics,
	}
	m.meshCert.Store(&meshCert)
	return m, nil
}
