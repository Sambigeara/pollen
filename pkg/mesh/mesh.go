package mesh

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
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
	"github.com/sambigeara/pollen/pkg/traffic"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const (
	handshakeTimeout    = 3 * time.Second
	quicIdleTimeout     = 30 * time.Second
	quicKeepAlivePeriod = 10 * time.Second
	maxBidiStreams      = 256
	queueBufSize        = maxBidiStreams
	probeBufSize        = 2048
	inviteRedeemTTL     = 5 * time.Minute
	sessionReapInterval = 5 * time.Minute
	streamTypeTimeout   = 5 * time.Second

	streamTypeClock    byte = 1
	streamTypeTunnel   byte = 2
	streamTypeRouted   byte = 3
	streamTypeArtifact byte = 4
	streamTypeWorkload byte = 5

	routeHeaderSize = 66 // 32 dest + 32 source + 1 TTL + 1 inner stream type (tunnel=2, artifact=4, workload=5 accepted at delivery)
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
	Connect(ctx context.Context, peer types.PeerKey, addrs []*net.UDPAddr) error
	Punch(ctx context.Context, peer types.PeerKey, addr *net.UDPAddr, localNAT nat.Type) error
	GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)
	GetConn(peer types.PeerKey) (*quic.Conn, bool)
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
	SetTrafficTracker(t traffic.Recorder)
	SetArtifactHandler(fn func(stream io.ReadWriteCloser, peer types.PeerKey))
	OpenArtifactStream(ctx context.Context, peer types.PeerKey) (io.ReadWriteCloser, error)
	SetWorkloadHandler(fn func(stream io.ReadWriteCloser, peer types.PeerKey))
	OpenWorkloadStream(ctx context.Context, peer types.PeerKey) (io.ReadWriteCloser, error)
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
	ctx              context.Context
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
	trafficTracker   traffic.Recorder
	artifactHandler  func(stream io.ReadWriteCloser, peer types.PeerKey)
	workloadHandler  func(stream io.ReadWriteCloser, peer types.PeerKey)
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
		trafficTracker:   traffic.Noop,
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

	m.ctx = ctx
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

func (m *impl) BroadcastDisconnect() error {
	peers := m.sessions.drainPeers()
	for _, s := range peers {
		m.closeSession(s, CloseReasonDisconnect)
	}
	return nil
}

func (m *impl) SetTracer(t *traces.Tracer) {
	m.tracer = t
}

func (m *impl) SetRouter(r Router) {
	m.router = r
}

func (m *impl) SetTrafficTracker(t traffic.Recorder) {
	m.trafficTracker = t
}

func (m *impl) SetArtifactHandler(fn func(stream io.ReadWriteCloser, peer types.PeerKey)) {
	m.artifactHandler = fn
}

func (m *impl) SetWorkloadHandler(fn func(stream io.ReadWriteCloser, peer types.PeerKey)) {
	m.workloadHandler = fn
}

func (m *impl) Close() error {
	for _, s := range m.sessions.drainPeers() {
		m.closeSession(s, CloseReasonShutdown)
	}

	m.acceptWG.Wait()
	close(m.streamCh)
	close(m.clockStreamCh)
	close(m.recvCh)

	_ = m.listener.Close()
	_ = m.mainQT.Close()
	_ = m.mainQT.Conn.Close()
	return nil
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
