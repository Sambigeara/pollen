package membership

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"maps"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
)

var ErrCertExpired = errors.New("delegation certificate has expired")

const (
	maxDatagramPayload         = transport.MaxDatagramPayload - 1 // minus 1-byte DatagramType prefix
	eventBufSize               = 64
	certCheckInterval          = 5 * time.Minute
	CertWarnThreshold          = 1 * time.Hour
	CertCriticalThreshold      = 15 * time.Minute
	certRenewalTimeout         = 10 * time.Second
	gossipStreamTimeout        = 5 * time.Second
	maxResponseSize            = 4 << 20 // 4 MB
	vivaldiWarmupDuration      = 5 * time.Second
	VivaldiErrAlpha            = 0.2
	eagerSyncCooldown          = 5 * time.Second
	eagerSyncTimeout           = 5 * time.Second
	expirySweepInterval        = 30 * time.Second
	observedAddrResendInterval = 30 * time.Second
	ipRefreshInterval          = 5 * time.Minute
)

var envelopeOverhead = (&meshv1.Envelope{Body: &meshv1.Envelope_Events{
	Events: &statev1.GossipEventBatch{},
}}).SizeVT()

type MembershipAPI interface {
	Start(ctx context.Context) error
	Stop() error

	DenyPeer(key types.PeerKey) error
	IssueCert(ctx context.Context, peerKey types.PeerKey, admin bool, attributes *structpb.Struct) error

	HandleDigestStream(ctx context.Context, stream transport.Stream, peer types.PeerKey)

	Events() <-chan state.Event
	ControlMetrics() ControlMetrics
}

var _ MembershipAPI = (*Service)(nil)

type ClusterState interface {
	Snapshot() state.Snapshot
	ApplyDelta(from types.PeerKey, data []byte) ([]state.Event, []byte, error)
	EncodeDelta(since state.Digest) []byte
	EncodeFull() []byte
	PendingNotify() <-chan struct{}
	FlushPendingGossip() []*statev1.GossipEvent
	EmitHeartbeatIfNeeded() []state.Event
	DenyPeer(key types.PeerKey) []state.Event
	SetLocalAddresses([]netip.AddrPort) []state.Event
	SetLocalCoord(coords.Coord, float64) []state.Event
	SetLocalNAT(nat.Type) []state.Event
	SetLocalReachable([]types.PeerKey) []state.Event
	SetLocalObservedAddress(string, uint32) []state.Event
}

type Network interface {
	Connect(ctx context.Context, key types.PeerKey, addrs []netip.AddrPort) error
	Send(ctx context.Context, peer types.PeerKey, data []byte) error
	Recv(ctx context.Context) (transport.Packet, error)
	ConnectedPeers() []types.PeerKey
	PeerEvents() <-chan transport.PeerEvent
}

type StreamOpener interface {
	OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (transport.Stream, error)
}

type RTTSource interface {
	GetConn(peer types.PeerKey) (*quic.Conn, bool)
}

type CertManager interface {
	UpdateMeshCert(cert tls.Certificate)
	RequestCertRenewal(ctx context.Context, peer types.PeerKey) (*admissionv1.DelegationCert, error)
	PeerDelegationCert(peer types.PeerKey) (*admissionv1.DelegationCert, bool)
	PushCert(ctx context.Context, peer types.PeerKey, cert *admissionv1.DelegationCert) error
}

type PeerAddressSource interface {
	GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)
}

type PeerSessionCloser interface {
	ClosePeerSession(peer types.PeerKey, reason transport.DisconnectReason)
}

type RoutedSender interface {
	SendMembershipDatagram(ctx context.Context, peer types.PeerKey, data []byte) error
}

type CapabilityTransitioner interface {
	UpgradeToAdmin(signer *auth.DelegationSigner)
	DowngradeToLeaf()
}

type ControlMetrics struct {
	LocalCoord        coords.Coord
	SmoothedErr       float64
	VivaldiSamples    int64
	EagerSyncs        int64
	EagerSyncFailures int64
}

type Config struct {
	RTT              RTTSource
	Certs            CertManager
	PeerAddrs        PeerAddressSource
	SessionCloser    PeerSessionCloser
	TracerProvider   trace.TracerProvider
	Streams          StreamOpener
	ShutdownCh       chan<- struct{}
	SmoothedErr      *metrics.EWMA
	NATDetector      *nat.Detector
	NodeMetrics      *metrics.NodeMetrics
	RoutedSender     RoutedSender
	CapTransition    CapabilityTransitioner
	DatagramHandler  func(ctx context.Context, from types.PeerKey, env *meshv1.Envelope)
	Log              *zap.SugaredLogger
	PollenDir        string
	AdvertisedIPs    []string
	SignPriv         ed25519.PrivateKey
	TLSIdentityTTL   time.Duration
	MembershipTTL    time.Duration
	ReconnectWindow  time.Duration
	GossipInterval   time.Duration
	GossipJitter     float64
	PeerTickInterval time.Duration
	Port             int
}

type sentAddr struct {
	at   time.Time
	addr string
}

type Service struct {
	tracer            trace.Tracer
	mesh              Network
	streams           StreamOpener
	rtt               RTTSource
	certs             CertManager
	peerAddrs         PeerAddressSource
	sessionCloser     PeerSessionCloser
	store             ClusterState
	lastSentAddr      map[types.PeerKey]sentAddr
	peerConnectTime   map[types.PeerKey]time.Time
	natDetector       *nat.Detector
	creds             *auth.NodeCredentials
	nodeMetrics       *metrics.NodeMetrics
	smoothedErr       *metrics.EWMA
	log               *zap.SugaredLogger
	cancel            context.CancelFunc
	events            chan state.Event
	lastEagerSync     map[types.PeerKey]time.Time
	shutdownCh        chan<- struct{}
	routedSender      RoutedSender
	capTransition     CapabilityTransitioner
	datagramHandler   func(ctx context.Context, from types.PeerKey, env *meshv1.Envelope)
	pollenDir         string
	advertisedIPs     []string
	signPriv          ed25519.PrivateKey
	localCoord        coords.Coord
	wg                sync.WaitGroup
	port              int
	gossipInterval    time.Duration
	eagerSyncs        atomic.Int64
	gossipJitter      float64
	reconnectWindow   time.Duration
	membershipTTL     time.Duration
	peerTickInterval  time.Duration
	vivaldiSamples    atomic.Int64
	localCoordErr     float64
	eagerSyncFailures atomic.Int64
	tlsIdentityTTL    time.Duration
	stopOnce          sync.Once
	mu                sync.Mutex
	renewalFailed     atomic.Bool
	localID           types.PeerKey
}

func New(self types.PeerKey, creds *auth.NodeCredentials, net Network, cluster ClusterState, cfg Config) *Service {
	s := &Service{
		store:            cluster,
		mesh:             net,
		streams:          cfg.Streams,
		rtt:              cfg.RTT,
		certs:            cfg.Certs,
		peerAddrs:        cfg.PeerAddrs,
		sessionCloser:    cfg.SessionCloser,
		routedSender:     cfg.RoutedSender,
		capTransition:    cfg.CapTransition,
		datagramHandler:  cfg.DatagramHandler,
		natDetector:      cfg.NATDetector,
		creds:            creds,
		localID:          self,
		log:              zap.S().Named("membership"),
		tracer:           tracenoop.NewTracerProvider().Tracer("pollen/membership"),
		nodeMetrics:      metrics.NewNodeMetrics(metricnoop.NewMeterProvider()),
		smoothedErr:      metrics.NewEWMA(VivaldiErrAlpha, 1.0),
		localCoord:       coords.RandomCoord(),
		localCoordErr:    1.0,
		lastSentAddr:     make(map[types.PeerKey]sentAddr),
		peerConnectTime:  make(map[types.PeerKey]time.Time),
		lastEagerSync:    make(map[types.PeerKey]time.Time),
		events:           make(chan state.Event, eventBufSize),
		signPriv:         cfg.SignPriv,
		advertisedIPs:    cfg.AdvertisedIPs,
		pollenDir:        cfg.PollenDir,
		port:             cfg.Port,
		tlsIdentityTTL:   cfg.TLSIdentityTTL,
		membershipTTL:    cfg.MembershipTTL,
		reconnectWindow:  cfg.ReconnectWindow,
		gossipInterval:   cfg.GossipInterval,
		gossipJitter:     cfg.GossipJitter,
		peerTickInterval: cfg.PeerTickInterval,
		shutdownCh:       cfg.ShutdownCh,
	}

	if cfg.Log != nil {
		s.log = cfg.Log
	}
	if cfg.TracerProvider != nil {
		s.tracer = cfg.TracerProvider.Tracer("pollen/membership")
	}
	if cfg.NodeMetrics != nil {
		s.nodeMetrics = cfg.NodeMetrics
	}
	if cfg.SmoothedErr != nil {
		s.smoothedErr = cfg.SmoothedErr
	}
	return s
}

func (s *Service) Start(ctx context.Context) error {
	s.store.SetLocalCoord(s.localCoord, s.localCoordErr)
	s.broadcastBatchBytes(ctx, s.store.EncodeFull())

	if s.checkCertExpiry() {
		return ErrCertExpired
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	s.spawn(runCtx, s.runGossipTicker)
	s.spawn(runCtx, s.runHeartbeatTicker)
	s.spawn(runCtx, s.runRecvLoop)
	s.spawn(runCtx, s.runPeerEventLoop)
	s.spawn(runCtx, s.runPeerTick)
	s.spawn(runCtx, s.runPendingGossipBroadcast)
	s.spawn(runCtx, s.runCertCheckTicker)

	if len(s.advertisedIPs) == 0 {
		s.spawn(runCtx, s.runIPRefresh)
	}

	return nil
}

func (s *Service) Stop() error {
	s.stopOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		s.wg.Wait()
		close(s.events)
	})
	return nil
}

func (s *Service) spawn(ctx context.Context, fn func(context.Context)) {
	s.wg.Go(func() {
		fn(ctx)
	})
}

func (s *Service) Events() <-chan state.Event {
	return s.events
}

func (s *Service) ControlMetrics() ControlMetrics {
	s.mu.Lock()
	coord := s.localCoord
	s.mu.Unlock()
	return ControlMetrics{
		LocalCoord:        coord,
		SmoothedErr:       s.smoothedErr.Value(),
		VivaldiSamples:    s.vivaldiSamples.Load(),
		EagerSyncs:        s.eagerSyncs.Load(),
		EagerSyncFailures: s.eagerSyncFailures.Load(),
	}
}

func (s *Service) DenyPeer(key types.PeerKey) error {
	events := s.store.DenyPeer(key)
	s.forwardEvents(events)
	return nil
}

func (s *Service) sendEvent(ev state.Event) {
	select {
	case s.events <- ev:
	default:
	}
}

func (s *Service) forwardEvents(events []state.Event) {
	for _, ev := range events {
		if _, ok := ev.(state.GossipApplied); ok {
			continue
		}
		s.sendEvent(ev)
	}
}

func (s *Service) runGossipTicker(ctx context.Context) {
	timer := time.NewTimer(jitter(s.gossipInterval, s.gossipJitter))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.forwardEvents(s.gossip(ctx))
			timer.Reset(jitter(s.gossipInterval, s.gossipJitter))
		}
	}
}

func (s *Service) runHeartbeatTicker(ctx context.Context) {
	ticker := time.NewTicker(state.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.store.EmitHeartbeatIfNeeded()
		}
	}
}

func (s *Service) runRecvLoop(ctx context.Context) {
	for {
		p, err := s.mesh.Recv(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Debugw("recv failed", "err", err)
			continue
		}
		s.handleDatagram(ctx, p.From, p.Data)
	}
}

func (s *Service) runPeerEventLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-s.mesh.PeerEvents():
			switch ev.Type {
			case transport.PeerEventConnected:
				s.mu.Lock()
				s.peerConnectTime[ev.Key] = time.Now()
				shouldEagerSync := time.Since(s.lastEagerSync[ev.Key]) >= eagerSyncCooldown
				s.mu.Unlock()

				if shouldEagerSync {
					s.doEagerSync(ctx, ev.Key)
				}
				if len(ev.Addrs) > 0 {
					addr := net.UDPAddrFromAddrPort(ev.Addrs[0])
					s.sendObservedAddress(ev.Key, addr)
					s.mu.Lock()
					s.lastSentAddr[ev.Key] = sentAddr{addr: addr.String(), at: time.Now()}
					s.mu.Unlock()
				}

			case transport.PeerEventDisconnected:
				s.mu.Lock()
				delete(s.peerConnectTime, ev.Key)
				delete(s.lastEagerSync, ev.Key)
				delete(s.lastSentAddr, ev.Key)
				s.mu.Unlock()
			}
			s.forwardEvents(s.store.SetLocalReachable(s.mesh.ConnectedPeers()))
		}
	}
}

func (s *Service) runPeerTick(ctx context.Context) {
	ticker := time.NewTicker(s.peerTickInterval)
	defer ticker.Stop()
	var lastExpirySweep time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap := s.store.Snapshot()
			s.updateVivaldiCoords(snap)
			s.resendObservedAddresses(snap)
			if time.Since(lastExpirySweep) >= expirySweepInterval {
				s.disconnectExpiredPeers()
				lastExpirySweep = time.Now()
			}
		}
	}
}

func (s *Service) resendObservedAddresses(snap state.Snapshot) {
	localNV, ok := snap.Nodes[snap.LocalID]
	if !ok || !localNV.PubliclyAccessible {
		return
	}
	now := time.Now()
	for _, pk := range s.mesh.ConnectedPeers() {
		addr, ok := s.peerAddrs.GetActivePeerAddress(pk)
		if !ok {
			continue
		}
		addrStr := addr.String()
		s.mu.Lock()
		prev := s.lastSentAddr[pk]
		s.mu.Unlock()
		if addrStr == prev.addr && now.Sub(prev.at) < observedAddrResendInterval {
			continue
		}
		s.sendObservedAddress(pk, addr)
		s.mu.Lock()
		s.lastSentAddr[pk] = sentAddr{addr: addrStr, at: now}
		s.mu.Unlock()
	}
}

func (s *Service) runPendingGossipBroadcast(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.store.PendingNotify():
			events := s.store.FlushPendingGossip()
			s.broadcastEvents(ctx, events)
		}
	}
}

func (s *Service) runCertCheckTicker(ctx context.Context) {
	certInterval := certCheckInterval
	if auth.IsCertExpired(s.creds.Cert(), time.Now()) {
		certInterval = expirySweepInterval
	}
	ticker := time.NewTicker(certInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.checkCertExpiry() {
				if s.shutdownCh != nil {
					s.shutdownCh <- struct{}{}
				}
				return
			}
			newInterval := certCheckInterval
			if s.renewalFailed.Load() {
				newInterval = expirySweepInterval
			}
			if newInterval != certInterval {
				certInterval = newInterval
				ticker.Reset(certInterval)
			}
		}
	}
}

func (s *Service) runIPRefresh(ctx context.Context) {
	ticker := time.NewTicker(ipRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshIPs()
		}
	}
}

func (s *Service) sendObservedAddress(pk types.PeerKey, addr *net.UDPAddr) {
	data, err := (&meshv1.Envelope{
		Body: &meshv1.Envelope_ObservedAddress{
			ObservedAddress: &meshv1.ObservedAddress{Addr: addr.String()},
		},
	}).MarshalVT()
	if err != nil {
		return
	}
	if err := s.mesh.Send(context.Background(), pk, data); err != nil {
		s.log.Debugw("failed sending observed address", "peer", pk.Short(), "err", err)
	}
}

func (s *Service) handleObservedAddress(from types.PeerKey, oa *meshv1.ObservedAddress) {
	addr, err := net.ResolveUDPAddr("udp", oa.GetAddr())
	if err != nil {
		s.log.Debugw("observed address: parse failed", "addr", oa.GetAddr(), "err", err)
		return
	}

	if addr.IP.IsPrivate() || addr.IP.IsLoopback() || addr.IP.IsLinkLocalUnicast() {
		return
	}

	s.log.Debugw("observed address received", "addr", addr.String(), "from", from.Short())

	snap := s.store.Snapshot()
	if fromNV, ok := snap.Nodes[from]; ok && fromNV.PubliclyAccessible {
		events := s.store.SetLocalObservedAddress(addr.IP.String(), uint32(addr.Port))
		if len(events) > 0 {
			s.natDetector.Reset()
			s.log.Infow("external IP changed, reset NAT observations")
		}
		s.forwardEvents(events)
	}

	if peerAddr, ok := s.peerAddrs.GetActivePeerAddress(from); ok {
		if observerIP, ok := netip.AddrFromSlice(peerAddr.IP); ok {
			observerIP = observerIP.Unmap()
			if natType, changed := s.natDetector.AddObservation(observerIP, addr.Port); changed {
				s.log.Infow("NAT type detected", "type", natType)
				s.store.SetLocalNAT(natType)
			}
		}
	}
}

func (s *Service) refreshIPs() {
	localIPs, err := transport.GetLocalInterfaceAddrs(transport.DefaultExclusions)
	if err != nil {
		s.log.Debugw("local ip refresh failed", "err", err)
	} else {
		var addrs []netip.AddrPort
		for _, ip := range localIPs {
			addr, err := netip.ParseAddr(ip)
			if err != nil {
				continue
			}
			addrs = append(addrs, netip.AddrPortFrom(addr, uint16(s.port)))
		}
		if len(s.store.SetLocalAddresses(addrs)) > 0 {
			s.log.Infow("local IPs changed", "ips", localIPs)
		}
	}

	if publicIP := transport.GetPublicIP(); publicIP != "" {
		snap := s.store.Snapshot()
		if localNV, ok := snap.Nodes[snap.LocalID]; !ok || localNV.ObservedExternalIP != publicIP {
			events := s.store.SetLocalObservedAddress(publicIP, 0)
			if len(events) > 0 {
				s.log.Infow("public IP changed", "ip", publicIP)
			}
			s.forwardEvents(events)
		}
	}
}

func (s *Service) updateVivaldiCoords(snap state.Snapshot) {
	updated := false
	now := time.Now()

	s.mu.Lock()
	localCoord := s.localCoord
	localCoordErr := s.localCoordErr
	peerConnectTimes := make(map[types.PeerKey]time.Time, len(s.peerConnectTime))
	maps.Copy(peerConnectTimes, s.peerConnectTime)
	s.mu.Unlock()

	for _, peerKey := range s.mesh.ConnectedPeers() {
		if ct, ok := peerConnectTimes[peerKey]; ok && now.Sub(ct) < vivaldiWarmupDuration {
			continue
		}
		conn, ok := s.rtt.GetConn(peerKey)
		if !ok {
			continue
		}
		rtt := conn.ConnectionStats().SmoothedRTT
		if rtt <= 0 {
			continue
		}
		nv, ok := snap.Nodes[peerKey]
		if !ok || nv.VivaldiCoord == nil {
			continue
		}

		var newErr float64
		localCoord, newErr = coords.Update(
			localCoord, localCoordErr,
			coords.Sample{RTT: rtt, PeerCoord: *nv.VivaldiCoord, PeerErr: nv.VivaldiErr},
		)
		localCoordErr = newErr
		s.smoothedErr.Update(newErr)
		s.vivaldiSamples.Add(1)
		updated = true
	}

	if updated {
		s.mu.Lock()
		s.localCoord = localCoord
		s.localCoordErr = localCoordErr
		s.mu.Unlock()

		s.forwardEvents(s.store.SetLocalCoord(localCoord, localCoordErr))
	}
}

const jitterScale = 2

func jitter(d time.Duration, percent float64) time.Duration {
	if percent <= 0 {
		return d
	}
	delta := time.Duration(float64(d) * percent)
	n := int64(delta)*jitterScale + 1
	offset := time.Duration(rand.N(n)) - delta //nolint:gosec
	return d + offset
}
