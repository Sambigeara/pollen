package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/observability/traces"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/perm"
	"github.com/sambigeara/pollen/pkg/route"
	"github.com/sambigeara/pollen/pkg/scheduler"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/sysinfo"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/traffic"
	"github.com/sambigeara/pollen/pkg/tunnel"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/util"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/sambigeara/pollen/pkg/workload"
	"go.uber.org/zap"
)

const (
	// MaxDatagramPayload is the maximum safe payload size for a QUIC datagram.
	MaxDatagramPayload = 1100
)

var ErrCertExpired = errors.New("delegation certificate has expired")

// envelopeOverhead is the serialized size of an empty Envelope wrapping
// a GossipEventBatch — covers the oneof tag, length prefixes, and batch
// wrapper framing.
var envelopeOverhead = (&meshv1.Envelope{Body: &meshv1.Envelope_Events{
	Events: &statev1.GossipEventBatch{},
}}).SizeVT()

const (
	certCheckInterval     = 5 * time.Minute
	certWarnThreshold     = 1 * time.Hour
	certCriticalThreshold = 15 * time.Minute
	certRenewalTimeout    = 10 * time.Second
	expirySweepInterval   = 30 * time.Second

	localKeysDir = "keys"

	signingKeyName    = "ed25519.key"
	signingPubKeyName = "ed25519.pub"
	pemTypePriv       = "ED25519 PRIVATE KEY"
	pemTypePub        = "ED25519 PUBLIC KEY"
	privKeyPerm       = 0o640
	pubKeyPerm        = 0o640

	directTimeout = 2 * time.Second
	punchTimeout  = 3 * time.Second

	// eagerSyncCooldown should be >= GossipInterval to avoid redundant sends.
	eagerSyncCooldown         = 5 * time.Second
	eagerSyncTimeout          = 5 * time.Second
	gossipStreamTimeout       = 5 * time.Second
	workloadInvocationTimeout = 60 * time.Second

	revokeStreakThreshold       = 3  // consecutive non-target ticks before revoking outbound
	revokeStreakThresholdPublic = 30 // public peers are stickier (coordinator stability)

	vivaldiEnterHMACThreshold = 0.6
	vivaldiExitHMACThreshold  = 0.35
	vivaldiWarmupDuration     = 5 * time.Second
	vivaldiErrAlpha           = 0.2 // ~5-sample EWMA window for smoothed vivaldi error

	routeDebounceInterval = 100 * time.Millisecond
	maxRouteDelay         = time.Second

	loopIntervalJitter = 0.1
	peerEventBufSize   = 64
	nodeWorkers        = 8
	ipRefreshInterval  = 5 * time.Minute
)

type BootstrapPeer struct {
	Addrs   []string
	PeerKey types.PeerKey
}

type Config struct {
	PacketConn          net.PacketConn
	AdvertisedIPs       []string
	BootstrapPeers      []BootstrapPeer
	TLSIdentityTTL      time.Duration
	PeerTickInterval    time.Duration
	GossipJitter        float64
	GossipInterval      time.Duration
	MembershipTTL       time.Duration
	ReconnectWindow     time.Duration
	MaxConnectionAge    time.Duration
	Port                int
	DisableGossipJitter bool
	BootstrapPublic     bool
	MetricsEnabled      bool
	DisableNATPunch     bool
}

type Node struct {
	lastExpirySweep time.Time
	mesh            mesh.Mesh
	natDetector     *nat.Detector
	store           *store.Store
	tun             *tunnel.Manager
	workloads       *workload.Manager
	casStore        *cas.Store
	nonTargetStreak map[types.PeerKey]int
	creds           *auth.NodeCredentials
	workers         *workerPool
	localPeerEvents chan peer.Input
	peers           *peer.Store
	conf            *Config
	ready           chan struct{}
	log             *zap.SugaredLogger
	lastEagerSync   map[types.PeerKey]time.Time
	peerConnectTime map[types.PeerKey]time.Time
	smoothedErr     *metrics.EWMA
	topoMetrics     *metrics.TopologyMetrics
	metricsCol      *metrics.Collector
	tracer          *traces.Tracer
	nodeMetrics     *metrics.NodeMetrics
	wasmRuntime     *wasm.Runtime
	routeTable      *route.Table
	trafficTracker  *traffic.Tracker
	sched           *scheduler.Reconciler
	routeInvalidate chan struct{}
	pollenDir       string
	signPriv        ed25519.PrivateKey
	localCoord      topology.Coord
	localCoordErr   float64
	renewalFailed   atomic.Bool
	useHMACNearest  bool

	// Always-active vivaldi convergence diagnostics (independent of MetricsEnabled).
	vivaldiSamples    atomic.Int64
	eagerSyncs        atomic.Int64
	eagerSyncFailures atomic.Int64
}

func New(conf *Config, privKey ed25519.PrivateKey, creds *auth.NodeCredentials, stateStore *store.Store, peerStore *peer.Store, pollenDir string) (*Node, error) {
	log := zap.S().Named("node")

	ips := conf.AdvertisedIPs
	if len(ips) == 0 {
		var err error
		ips, err = mesh.GetAdvertisableAddrs(mesh.DefaultExclusions)
		if err != nil {
			return nil, err
		}
	}

	stateStore.SetLocalNetwork(ips, uint32(conf.Port))

	var col *metrics.Collector
	var tracer *traces.Tracer
	if conf.MetricsEnabled {
		col = metrics.New(metrics.NewLogSink(log.Named("metrics")), metrics.Config{})
		tracer = traces.NewTracer(traces.NewLogSink(log.Named("traces")))
	}

	meshMetrics := metrics.NewMeshMetrics(col)
	peerMetrics := metrics.NewPeerMetrics(col)
	gossipMetrics := metrics.NewGossipMetrics(col)

	isDenied := func(pk types.PeerKey) bool {
		return stateStore.IsDenied(pk[:])
	}
	m, err := mesh.NewMesh(conf.Port, privKey, creds, conf.TLSIdentityTTL, conf.MembershipTTL, conf.ReconnectWindow, conf.MaxConnectionAge, isDenied, meshMetrics)
	if err != nil {
		log.Errorw("failed to load mesh", "err", err)
		return nil, err
	}

	if conf.PacketConn != nil {
		m.SetPacketConn(conf.PacketConn)
	}
	if conf.DisableNATPunch {
		m.SetDisableNATPunch(true)
	}

	m.SetTracer(tracer)
	stateStore.SetGossipMetrics(gossipMetrics)
	peerStore.SetPeerMetrics(peerMetrics)

	tun := tunnel.New(m)

	for _, svc := range stateStore.LocalServices() {
		tun.RegisterService(svc.GetPort())
	}

	casStore, err := cas.New(pollenDir)
	if err != nil {
		return nil, fmt.Errorf("create CAS store: %w", err)
	}

	n := &Node{
		log:             log,
		peers:           peerStore,
		store:           stateStore,
		mesh:            m,
		tun:             tun,
		casStore:        casStore,
		creds:           creds,
		conf:            conf,
		signPriv:        privKey,
		pollenDir:       pollenDir,
		workers:         newWorkerPool(nodeWorkers),
		localPeerEvents: make(chan peer.Input, peerEventBufSize),
		ready:           make(chan struct{}),
		lastEagerSync:   make(map[types.PeerKey]time.Time),
		peerConnectTime: make(map[types.PeerKey]time.Time),
		nonTargetStreak: make(map[types.PeerKey]int),
		natDetector:     nat.NewDetector(),
		localCoord:      topology.RandomCoord(),
		useHMACNearest:  true,
		metricsCol:      col,
		topoMetrics:     metrics.NewTopologyMetrics(col),
		nodeMetrics:     metrics.NewNodeMetrics(col),
		tracer:          tracer,
		smoothedErr:     metrics.NewEWMAFrom(vivaldiErrAlpha, 1.0),
		routeTable:      route.New(stateStore.LocalID),
		trafficTracker:  traffic.New(),
		routeInvalidate: make(chan struct{}, 1),
	}
	n.localCoordErr = 1.0

	if conf.BootstrapPublic {
		stateStore.SetLocalPubliclyAccessible(true)
	}

	return n, nil
}

func (n *Node) Start(ctx context.Context) error {
	defer n.shutdown()

	storeReady := make(chan struct{})
	go func() {
		if err := n.store.Run(ctx, storeReady); err != nil && ctx.Err() == nil {
			n.log.Errorw("store loop failed", "error", err)
		}
	}()
	<-storeReady

	hostFuncs := wasm.NewHostFunctions(n.log.Named("wasm"), n)
	wasmRT := wasm.NewRuntime(hostFuncs, 0)
	n.wasmRuntime = wasmRT
	n.workloads = workload.New(ctx, n.casStore, wasmRT)

	if n.creds.Cert != nil {
		n.store.SetLocalCertExpiry(n.creds.Cert.GetClaims().GetNotAfterUnix())
	}

	if err := n.mesh.Start(ctx); err != nil {
		return err
	}
	n.mesh.SetRouter(n.routeTable)
	n.mesh.SetTrafficTracker(n.trafficTracker)
	n.tun.SetTrafficTracker(n.trafficTracker)

	// Start scheduler reconciler for distributed workload placement.
	n.sched = scheduler.NewReconciler(
		n.store.LocalID,
		n.store,
		n.workloads,
		n.casStore,
		scheduler.NewArtifactFetcher(n.mesh, n.casStore),
		func([]*statev1.GossipEvent) {},
		n.log.Named("scheduler"),
	)
	go n.sched.Run(ctx)

	// Publish the random startup coordinate so peers receive initial Vivaldi
	// state via gossip. We intentionally do not queue the returned events here;
	// peers will pull this state via clock gossip (or eager sync) once sessions
	// are established.
	n.store.SetLocalVivaldiCoord(n.localCoord)

	close(n.ready)
	n.connectBootstrapPeers(ctx)
	go n.workers.run(ctx)
	go n.recvLoop(ctx)
	go n.streamDispatchLoop(ctx)

	jitter := loopIntervalJitter
	if n.conf.DisableGossipJitter {
		jitter = 0
	} else if n.conf.GossipJitter > 0 {
		jitter = n.conf.GossipJitter
	}
	gossipTicker := util.NewJitterTicker(ctx, n.conf.GossipInterval, jitter)
	defer gossipTicker.Stop()

	peerTicker := time.NewTicker(n.conf.PeerTickInterval)
	defer peerTicker.Stop()

	var ipRefreshCh <-chan time.Time
	if len(n.conf.AdvertisedIPs) == 0 {
		ipRefreshTicker := time.NewTicker(ipRefreshInterval)
		defer ipRefreshTicker.Stop()
		ipRefreshCh = ipRefreshTicker.C
	}

	// Speed up cert checks when the cert is already expired at startup.
	certInterval := certCheckInterval
	if auth.IsCertExpired(n.creds.Cert, time.Now()) {
		certInterval = expirySweepInterval // 30s for faster renewal attempts
	}
	certCheckTicker := time.NewTicker(certInterval)
	defer certCheckTicker.Stop()

	n.tick(ctx)
	n.recomputeRoutes()
	n.checkCertExpiry()
	n.sampleResourceTelemetry()
	n.sampleTrafficHeatmap()
	n.broadcastEvents(ctx, n.store.LocalEvents())

	// Debounced route recomputation state.
	var (
		routeDebounce          *time.Timer
		routeDebounceC         <-chan time.Time
		firstRouteInvalidation time.Time
	)

	stopDrainTimer := func(t *time.Timer) {
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-peerTicker.C:
			n.tick(ctx)
		case <-gossipTicker.C:
			n.sampleResourceTelemetry()
			n.sampleTrafficHeatmap()
			n.gossip(ctx)
		case <-ipRefreshCh:
			n.refreshIPs()
		case <-certCheckTicker.C:
			if n.checkCertExpiry() {
				return ErrCertExpired
			}
			// Speed up checks while renewal is failing.
			if n.renewalFailed.Load() && certInterval != expirySweepInterval {
				certInterval = expirySweepInterval
				certCheckTicker.Reset(certInterval)
			} else if !n.renewalFailed.Load() && certInterval != certCheckInterval {
				certInterval = certCheckInterval
				certCheckTicker.Reset(certInterval)
			}
		case ev := <-n.store.Events():
			switch e := ev.(type) {
			case store.GossipApplied:
				if len(e.Rebroadcast) > 0 {
					n.broadcastEvents(ctx, e.Rebroadcast)
				}
			case store.LocalMutationApplied:
				if len(e.Events) > 0 {
					n.broadcastEvents(ctx, e.Events)
				}
			case store.DenyApplied:
				pk := e.PeerKey
				n.log.Infow("deny event received via gossip, disconnecting peer", "peer", pk.Short())
				n.tun.DisconnectPeer(pk)
				n.mesh.ClosePeerSession(pk, mesh.CloseReasonDenied)
				n.store.RemoveDesiredConnection(pk, 0, 0)
				select {
				case n.localPeerEvents <- peer.PeerDisconnected{PeerKey: pk, Reason: peer.DisconnectDenied}:
				default:
				}
				select {
				case n.localPeerEvents <- peer.ForgetPeer{PeerKey: pk}:
				default:
				}
			case store.RouteInvalidated:
				n.signalRouteInvalidate()
			case store.WorkloadChanged:
				n.sched.Signal()
			case store.TrafficChanged:
				n.sched.SignalTraffic()
			}
		case in := <-n.mesh.Events():
			n.handlePeerInput(ctx, in)
		case in := <-n.localPeerEvents:
			n.handlePeerInput(ctx, in)
		case <-n.routeInvalidate:
			now := time.Now()
			if firstRouteInvalidation.IsZero() {
				firstRouteInvalidation = now
			}
			if now.Sub(firstRouteInvalidation) >= maxRouteDelay {
				n.recomputeRoutes()
				firstRouteInvalidation = time.Time{}
				if routeDebounce != nil {
					stopDrainTimer(routeDebounce)
					routeDebounce = nil
					routeDebounceC = nil
				}
			} else {
				if routeDebounce == nil {
					routeDebounce = time.NewTimer(routeDebounceInterval)
					routeDebounceC = routeDebounce.C
				} else {
					routeDebounce.Reset(routeDebounceInterval)
				}
			}
		case <-routeDebounceC:
			n.recomputeRoutes()
			firstRouteInvalidation = time.Time{}
			routeDebounceC = nil
			routeDebounce = nil
		}
	}
}

func (n *Node) connectBootstrapPeers(ctx context.Context) {
	for _, bp := range n.conf.BootstrapPeers {
		if n.store.IsDenied(bp.PeerKey[:]) {
			continue
		}
		addrs := make([]*net.UDPAddr, 0, len(bp.Addrs))
		for _, a := range bp.Addrs {
			addr, err := net.ResolveUDPAddr("udp", a)
			if err != nil {
				n.log.Warnw("bootstrap peer: resolve address failed", "addr", a, "err", err)
				continue
			}
			addrs = append(addrs, addr)
		}
		if len(addrs) == 0 {
			continue
		}
		if err := n.mesh.Connect(ctx, bp.PeerKey, addrs); err != nil {
			n.log.Warnw("bootstrap peer: connect failed", "peer", bp.PeerKey.Short(), "err", err)
		}
	}
}

func (n *Node) recvLoop(ctx context.Context) {
	for {
		p, err := n.mesh.Recv(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			n.log.Debugw("recv failed", "err", err)
			continue
		}
		n.handleDatagram(ctx, p.Peer, p.Envelope)
	}
}

func (n *Node) streamDispatchLoop(ctx context.Context) {
	for {
		stream, stype, peerKey, err := n.mesh.AcceptAllStreams(ctx)
		if err != nil {
			return
		}
		switch stype {
		case mesh.StreamTypeClock:
			go n.handleClockStream(ctx, peerKey, stream)
		case mesh.StreamTypeTunnel:
			n.tun.HandleIncoming(stream, peerKey)
		case mesh.StreamTypeArtifact:
			go n.handleArtifactStream(stream)
		case mesh.StreamTypeWorkload:
			go n.handleWorkloadStream(ctx, stream)
		default:
			n.log.Warnw("unknown stream type", "type", uint8(stype))
			stream.Close()
		}
	}
}

func (n *Node) handleArtifactStream(stream io.ReadWriteCloser) {
	scheduler.HandleArtifactStream(stream, n.casStore)
}

func (n *Node) handleWorkloadStream(ctx context.Context, stream io.ReadWriteCloser) {
	scheduler.HandleWorkloadStream(ctx, stream, n.workloads, workloadInvocationTimeout)
}

func (n *Node) handleClockStream(_ context.Context, from types.PeerKey, stream io.ReadWriteCloser) {
	defer stream.Close()
	span := n.tracer.Start("gossip.handleClock")
	span.SetAttr("peer", from.Short())
	defer span.End()

	const maxClockSize = 256 << 10 // 256 KB
	b, err := io.ReadAll(io.LimitReader(stream, maxClockSize+1))
	if err != nil {
		n.log.Debugw("read clock stream failed", "peer", from.Short(), "err", err)
		return
	}
	if len(b) > maxClockSize {
		n.log.Warnw("clock stream exceeded size limit", "peer", from.Short(), "size", len(b))
		return
	}

	digest := &statev1.GossipStateDigest{}
	if err := digest.UnmarshalVT(b); err != nil {
		n.log.Debugw("unmarshal clock stream failed", "peer", from.Short(), "err", err)
		return
	}

	events := n.store.MissingFor(digest)
	if len(events) == 0 {
		return
	}

	batch := &statev1.GossipEventBatch{Events: events}
	resp, err := batch.MarshalVT()
	if err != nil {
		n.log.Debugw("marshal clock response failed", "peer", from.Short(), "err", err)
		return
	}
	if _, err := stream.Write(resp); err != nil {
		n.log.Debugw("write clock response failed", "peer", from.Short(), "err", err)
	}
}

func (n *Node) handleDatagram(ctx context.Context, from types.PeerKey, env *meshv1.Envelope) {
	switch body := env.GetBody().(type) {
	case *meshv1.Envelope_Events:
		span := n.tracer.StartFromRemote("gossip.applyEvents", env.GetTraceId())
		span.SetAttr("peer", from.Short())
		n.store.ApplyEvents(body.Events.GetEvents(), body.Events.GetIsResponse())
		span.End()
	case *meshv1.Envelope_PunchCoordRequest:
		span := n.tracer.StartFromRemote("punch.coordRequest", env.GetTraceId())
		span.SetAttr("peer", from.Short())
		n.handlePunchCoordRequest(ctx, from, body.PunchCoordRequest)
		span.End()
	case *meshv1.Envelope_PunchCoordTrigger:
		span := n.tracer.StartFromRemote("punch.coordTrigger", env.GetTraceId())
		span.SetAttr("peer", from.Short())
		n.handlePunchCoordTrigger(ctx, body.PunchCoordTrigger)
		span.End()
	case *meshv1.Envelope_ObservedAddress:
		n.handleObservedAddress(from, body.ObservedAddress)
	case *meshv1.Envelope_CertRenewalRequest:
		n.handleCertRenewalRequest(ctx, from, body.CertRenewalRequest)
	default:
		n.log.Debugw("unknown datagram type", "peer", from.Short())
	}
}

func (n *Node) broadcastEvents(ctx context.Context, events []*statev1.GossipEvent) {
	if len(events) == 0 {
		return
	}

	n.broadcastGossipBatches(ctx, n.GetConnectedPeers(), batchEvents(events, MaxDatagramPayload))
}

func (n *Node) broadcastGossipBatches(ctx context.Context, peerIDs []types.PeerKey, batches [][]*statev1.GossipEvent) {
	failed := make(map[types.PeerKey]struct{})
	for _, batch := range batches {
		env := &meshv1.Envelope{Body: &meshv1.Envelope_Events{
			Events: &statev1.GossipEventBatch{Events: batch},
		}}
		for _, peerID := range peerIDs {
			if peerID == n.store.LocalID {
				continue
			}
			if _, ok := failed[peerID]; ok {
				continue
			}
			if err := n.mesh.Send(ctx, peerID, env); err != nil {
				n.log.Debugw("event gossip send failed", "peer", peerID.Short(), "err", err)
				failed[peerID] = struct{}{}
			}
		}
	}
}

const maxResponseSize = 4 << 20 // 4 MB

func (n *Node) sendClockViaStream(ctx context.Context, peerID types.PeerKey, digest *statev1.GossipStateDigest) error {
	stream, err := n.mesh.OpenClockStream(ctx, peerID)
	if err != nil {
		return fmt.Errorf("open clock stream to %s: %w", peerID.Short(), err)
	}

	b, err := digest.MarshalVT()
	if err != nil {
		stream.Close()
		return fmt.Errorf("marshal digest: %w", err)
	}
	if _, err := stream.Write(b); err != nil {
		stream.Close()
		return fmt.Errorf("write clock to %s: %w", peerID.Short(), err)
	}

	// Half-close the write side so the responder sees EOF.
	if err := stream.CloseWrite(); err != nil {
		return fmt.Errorf("half-close clock stream to %s: %w", peerID.Short(), err)
	}

	resp, err := io.ReadAll(io.LimitReader(stream, maxResponseSize+1))
	if err != nil {
		return fmt.Errorf("read clock response from %s: %w", peerID.Short(), err)
	}
	if len(resp) == 0 {
		return nil
	}
	if len(resp) > maxResponseSize {
		return fmt.Errorf("clock response from %s exceeded size limit (%d bytes)", peerID.Short(), len(resp))
	}

	batch := &statev1.GossipEventBatch{}
	if err := batch.UnmarshalVT(resp); err != nil {
		return fmt.Errorf("unmarshal clock response from %s: %w", peerID.Short(), err)
	}

	n.store.ApplyEvents(batch.GetEvents(), true)
	return nil
}

// eventWireSize returns the on-wire size of a single event inside a
// GossipEventBatch. Uses proto's own size calculation to avoid fragile
// hand-computed varint arithmetic.
func eventWireSize(event *statev1.GossipEvent) int {
	single := &statev1.GossipEventBatch{Events: []*statev1.GossipEvent{event}}
	empty := &statev1.GossipEventBatch{}
	return single.SizeVT() - empty.SizeVT()
}

// batchEvents packs events into groups that each fit within maxSize when
// serialized as an Envelope. Events that individually exceed maxSize are
// placed in their own batch.
func batchEvents(events []*statev1.GossipEvent, maxSize int) [][]*statev1.GossipEvent {
	if len(events) == 0 {
		return nil
	}

	var batches [][]*statev1.GossipEvent
	var current []*statev1.GossipEvent
	currentSize := envelopeOverhead

	for _, event := range events {
		eventSize := eventWireSize(event)
		if len(current) > 0 && currentSize+eventSize > maxSize {
			batches = append(batches, current)
			current = nil
			currentSize = envelopeOverhead
		}
		current = append(current, event)
		currentSize += eventSize
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}

func (n *Node) gossip(ctx context.Context) {
	digest := n.store.Clock()
	peers := n.GetConnectedPeers()
	var wg sync.WaitGroup
	for _, peerID := range peers {
		if peerID == n.store.LocalID {
			continue
		}
		wg.Go(func() {
			gossipCtx, cancel := context.WithTimeout(ctx, gossipStreamTimeout)
			defer cancel()
			if err := n.sendClockViaStream(gossipCtx, peerID, digest); err != nil {
				n.log.Debugw("clock gossip send failed", "peer", peerID.Short(), "err", err)
			}
		})
	}
	wg.Wait()
}

func (n *Node) sampleResourceTelemetry() {
	cpuPct, memPct, memTotal, numCPU := sysinfo.Sample()
	n.store.SetLocalResourceTelemetry(cpuPct, memPct, memTotal, numCPU)
}

func (n *Node) sampleTrafficHeatmap() {
	snapshot, changed := n.trafficTracker.RotateAndSnapshot()
	if !changed {
		return
	}
	rates := make(map[types.PeerKey]store.TrafficSnapshot, len(snapshot))
	for pk, pt := range snapshot {
		rates[pk] = store.TrafficSnapshot{BytesIn: pt.BytesIn, BytesOut: pt.BytesOut}
	}
	n.store.SetLocalTrafficHeatmap(rates)
	n.sched.SignalTraffic()
}

func (n *Node) refreshIPs() {
	newIPs, err := mesh.GetAdvertisableAddrs(mesh.DefaultExclusions)
	if err != nil {
		n.log.Debugw("ip refresh failed", "err", err)
		return
	}

	if len(n.store.SetLocalNetwork(newIPs, uint32(n.conf.Port))) > 0 {
		n.log.Infow("advertised IPs changed", "ips", newIPs)
	}
}

func (n *Node) handlePeerInput(ctx context.Context, in peer.Input) {
	switch d := in.(type) {
	case peer.PeerDisconnected:
		n.store.SetLocalConnected(d.PeerKey, false)
		n.signalRouteInvalidate()
		if n.sched != nil {
			n.sched.Signal() // reachability change affects workload claim visibility
		}
		delete(n.lastEagerSync, d.PeerKey)
		delete(n.peerConnectTime, d.PeerKey)
	case peer.ForgetPeer:
		delete(n.lastEagerSync, d.PeerKey)
		delete(n.peerConnectTime, d.PeerKey)
	}

	outputs := n.peers.Step(time.Now(), in)
	n.handleOutputs(ctx, outputs)
}

func (n *Node) tick(ctx context.Context) {
	snap := n.store.Snapshot()
	n.updateVivaldiCoords(snap)
	n.syncPeersFromState(ctx, snap)
	if time.Since(n.lastExpirySweep) >= expirySweepInterval {
		n.disconnectExpiredPeers(ctx)
		n.lastExpirySweep = time.Now()
	}
	n.reconcileConnections(snap)
	n.reconcileDesiredConnections(snap)

	now := time.Now()
	outputs := n.peers.Step(now, peer.Tick{
		MaxConnect: topology.DefaultInfraMax + topology.DefaultNearestK + topology.DefaultRandomR,
	})
	n.handleOutputs(ctx, outputs)
}

func (n *Node) updateVivaldiCoords(snap store.Snapshot) {
	updated := false
	now := time.Now()
	for _, peerKey := range n.GetConnectedPeers() {
		if ct, ok := n.peerConnectTime[peerKey]; ok && now.Sub(ct) < vivaldiWarmupDuration {
			continue
		}
		conn, ok := n.mesh.GetConn(peerKey)
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
		peerCoord := nv.VivaldiCoord
		var newErr float64
		n.localCoord, newErr = topology.Update(
			n.localCoord, n.localCoordErr,
			topology.Sample{RTT: rtt, PeerCoord: *peerCoord},
		)
		n.localCoordErr = newErr
		n.smoothedErr.Update(newErr)
		n.vivaldiSamples.Add(1)
		updated = true
	}
	if updated {
		if len(n.store.SetLocalVivaldiCoord(n.localCoord)) > 0 {
			n.signalRouteInvalidate()
		}
	}
}

func (n *Node) syncPeersFromState(ctx context.Context, snap store.Snapshot) {
	knownPeers := knownPeersFromSnapshot(snap)

	// Build topology peer infos for target selection.
	peerInfos := make([]topology.PeerInfo, 0, len(knownPeers))
	peerMap := make(map[types.PeerKey]store.KnownPeer, len(knownPeers))
	for _, kp := range knownPeers {
		peerMap[kp.PeerID] = kp
		peerInfos = append(peerInfos, topology.PeerInfo{
			Key:                kp.PeerID,
			Coord:              kp.VivaldiCoord,
			IPs:                kp.IPs,
			NatType:            kp.NatType,
			ObservedExternalIP: kp.ObservedExternalIP,
			PubliclyAccessible: kp.PubliclyAccessible,
		})
	}

	// Collect current outbound peer keys so the topology layer can apply
	// hysteresis, keeping incumbents unless a challenger is meaningfully closer.
	connectedPeers := n.GetConnectedPeers()
	currentOutbound := make(map[types.PeerKey]struct{}, len(connectedPeers))
	for _, pk := range connectedPeers {
		if n.mesh.IsOutbound(pk) {
			currentOutbound[pk] = struct{}{}
		}
	}

	smoothed := n.smoothedErr.Value()
	if n.useHMACNearest {
		if smoothed < vivaldiExitHMACThreshold {
			n.useHMACNearest = false
		}
	} else {
		if smoothed > vivaldiEnterHMACThreshold {
			n.useHMACNearest = true
		}
	}

	epoch := time.Now().Unix() / topology.EpochSeconds
	localNV := snap.Nodes[snap.LocalID]
	localIPs := localNV.IPs
	shape := summarizeTopologyShape(localIPs, knownPeers)
	activePeerCount := len(connectedPeers)
	params := adaptiveTopologyParams(epoch, shape)
	params.PreferFullMesh = activePeerCount <= tinyClusterPeerThreshold
	params.LocalIPs = localIPs
	params.CurrentOutbound = currentOutbound
	params.LocalNATType = n.natDetector.Type()
	params.LocalObservedExternalIP = localNV.ObservedExternalIP
	params.UseHMACNearest = n.useHMACNearest
	targets := topology.ComputeTargetPeers(snap.LocalID, n.localCoord, peerInfos, params)

	n.topoMetrics.VivaldiError.Set(smoothed)
	if n.useHMACNearest {
		n.topoMetrics.HMACNearestEnabled.Set(1.0)
	} else {
		n.topoMetrics.HMACNearestEnabled.Set(0.0)
	}

	targetSet := buildTargetPeerSet(targets, snap.Connections)

	// Discover peers that are topology targets or needed by local service
	// forwards.
	for pk := range targetSet {
		kp, ok := peerMap[pk]
		if !ok {
			continue
		}

		ips := make([]net.IP, 0, len(kp.IPs))
		for _, ipStr := range kp.IPs {
			ip := net.ParseIP(ipStr)
			if ip == nil {
				n.log.Error("unable to parse IP")
				continue
			}
			ips = append(ips, ip)
		}

		var lastAddr *net.UDPAddr
		if kp.LastAddr != "" {
			var err error
			lastAddr, err = net.ResolveUDPAddr("udp", kp.LastAddr)
			if err != nil {
				n.log.Debugw("invalid last addr", "peer", kp.PeerID.Short(), "addr", kp.LastAddr, "err", err)
			}
		}

		if len(ips) == 0 && lastAddr == nil {
			continue
		}

		reachability := inferReachability(params.LocalIPs, kp.IPs, kp.PubliclyAccessible)

		n.peers.Step(time.Now(), peer.DiscoverPeer{
			PeerKey:            kp.PeerID,
			Ips:                ips,
			Port:               int(kp.LocalPort),
			LastAddr:           lastAddr,
			PrivatelyRoutable:  reachability == reachabilitySameSitePrivate,
			PubliclyAccessible: kp.PubliclyAccessible,
		})
	}

	// Reset streak counters for peers that are back in the target set.
	for pk := range targetSet {
		delete(n.nonTargetStreak, pk)
	}

	// Prune non-targeted peers. Inbound connections are left alone — they'll
	// expire naturally. Outbound connections to former targets are delayed by
	// a streak threshold to absorb Vivaldi coordinate jitter.
	for _, kp := range knownPeers {
		if _, targeted := targetSet[kp.PeerID]; targeted {
			continue
		}
		connected := n.peers.InState(kp.PeerID, peer.PeerStateConnected)
		if connected && !n.mesh.IsOutbound(kp.PeerID) {
			continue
		}
		if connected {
			n.nonTargetStreak[kp.PeerID]++
			threshold := revokeStreakThreshold
			if kp.PubliclyAccessible {
				threshold = revokeStreakThresholdPublic
			}
			if n.nonTargetStreak[kp.PeerID] < threshold {
				continue
			}
			n.mesh.ClosePeerSession(kp.PeerID, mesh.CloseReasonTopologyPrune)
			n.topoMetrics.TopologyPrunes.Inc()
			n.handlePeerInput(ctx, peer.PeerDisconnected{PeerKey: kp.PeerID, Reason: peer.DisconnectGraceful})
		}
		delete(n.nonTargetStreak, kp.PeerID)
		n.handlePeerInput(ctx, peer.ForgetPeer{PeerKey: kp.PeerID})
	}
}

func buildTargetPeerSet(targets []types.PeerKey, desired []store.Connection) map[types.PeerKey]struct{} {
	out := make(map[types.PeerKey]struct{}, len(targets)+len(desired))
	for _, pk := range targets {
		out[pk] = struct{}{}
	}
	for _, conn := range desired {
		out[conn.PeerID] = struct{}{}
	}
	return out
}

func (n *Node) handleOutputs(ctx context.Context, outputs []peer.Output) {
	for _, out := range outputs {
		switch e := out.(type) {
		case peer.PeerConnected:
			addr := &net.UDPAddr{IP: e.IP, Port: e.ObservedPort}
			n.store.SetLastAddr(e.PeerKey, addr.String())
			n.peerConnectTime[e.PeerKey] = time.Now()
			n.store.SetLocalConnected(e.PeerKey, true)
			n.signalRouteInvalidate()
			if n.sched != nil {
				n.sched.Signal()
			}
			if time.Since(n.lastEagerSync[e.PeerKey]) >= eagerSyncCooldown {
				n.lastEagerSync[e.PeerKey] = time.Now()
				clock := n.store.EagerSyncClock()
				pk := e.PeerKey
				n.workers.submit(ctx, func() {
					eagerCtx, cancel := context.WithTimeout(context.Background(), eagerSyncTimeout)
					defer cancel()
					if err := n.sendClockViaStream(eagerCtx, pk, clock); err != nil {
						n.log.Debugw("eager sync failed", "peer", pk.Short(), "err", err)
						n.eagerSyncFailures.Add(1)
					} else {
						n.eagerSyncs.Add(1)
					}
				})
			}

			n.sendObservedAddress(e.PeerKey, addr)

			n.log.Infow("peer connected", "peer_id", e.PeerKey.Short(), "ip", e.IP, "observedPort", e.ObservedPort)
		case peer.AttemptEagerConnect:
			pk, addr := e.PeerKey, e.Addr
			n.workers.submit(ctx, func() { n.doConnect(pk, []*net.UDPAddr{addr}) })
		case peer.AttemptConnect:
			pk, addrs := e.PeerKey, n.buildPeerAddrs(e.PeerKey, e.Ips, e.Port)
			n.workers.submit(ctx, func() { n.doConnect(pk, addrs) })
		case peer.RequestPunchCoordination:
			pk := e.PeerKey
			n.workers.submit(ctx, func() { n.requestPunchCoordination(pk) })
		}
	}
}

func (n *Node) reconcileConnections(snap store.Snapshot) {
	for _, conn := range n.tun.ListConnections() {
		if snapshotHasServicePort(snap, conn.PeerID, conn.RemotePort) {
			continue
		}
		n.log.Infow("removing stale forward", "peer", conn.PeerID.Short(), "port", conn.RemotePort)
		n.tun.DisconnectRemoteService(conn.PeerID, conn.RemotePort)
		n.store.RemoveDesiredConnection(conn.PeerID, conn.RemotePort, 0)
	}
}

func (n *Node) reconcileDesiredConnections(snap store.Snapshot) {
	if len(snap.Connections) == 0 {
		return
	}

	existing := make(map[string]struct{})
	for _, conn := range n.tun.ListConnections() {
		existing[store.Connection{PeerID: conn.PeerID, RemotePort: conn.RemotePort, LocalPort: conn.LocalPort}.Key()] = struct{}{}
	}

	for _, desiredConn := range snap.Connections {
		if _, ok := existing[desiredConn.Key()]; ok {
			continue
		}

		if !snapshotHasServicePort(snap, desiredConn.PeerID, desiredConn.RemotePort) {
			continue
		}

		if _, err := n.ConnectService(desiredConn.PeerID, desiredConn.RemotePort, desiredConn.LocalPort); err != nil {
			n.log.Debugw("failed restoring desired connection", "peer", desiredConn.PeerID.Short(), "remotePort", desiredConn.RemotePort, "localPort", desiredConn.LocalPort, "err", err)
		}
	}
}

func (n *Node) doConnect(peerKey types.PeerKey, addrs []*net.UDPAddr) {
	ctx, cancel := context.WithTimeout(context.Background(), directTimeout)
	defer cancel()

	if err := n.mesh.Connect(ctx, peerKey, addrs); err != nil {
		if n.peers.InState(peerKey, peer.PeerStateConnecting) {
			if errors.Is(err, mesh.ErrIdentityMismatch) {
				n.log.Warnw("connect failed: peer identity mismatch", "peer", peerKey.Short(), "err", err)
			} else {
				n.log.Debugw("connect failed", "peer", peerKey.Short(), "err", err)
			}
			n.localPeerEvents <- peer.ConnectFailed{PeerKey: peerKey}
		}
	}
}

func (n *Node) buildPeerAddrs(peerKey types.PeerKey, ips []net.IP, port int) []*net.UDPAddr {
	snap := n.store.Snapshot()
	var extPort int
	if nv, ok := snap.Nodes[peerKey]; ok && nv.ExternalPort != 0 {
		extPort = int(nv.ExternalPort)
	}

	return orderPeerAddrs(snap.Nodes[snap.LocalID].IPs, ips, port, extPort)
}

func (n *Node) coordinatorPeers(target types.PeerKey) []types.PeerKey {
	snap := n.store.Snapshot()
	localIPs := snap.Nodes[snap.LocalID].IPs
	targetIPs := snap.Nodes[target].IPs
	connectedPeers := n.GetConnectedPeers()
	filtered := make([]types.PeerKey, 0, len(connectedPeers))
	for _, key := range connectedPeers {
		if key == target {
			continue
		}
		filtered = append(filtered, key)
	}
	return rankCoordinators(localIPs, targetIPs, target, filtered, snap)
}

func (n *Node) requestPunchCoordination(target types.PeerKey) {
	coordinators := n.coordinatorPeers(target)
	if len(coordinators) == 0 {
		n.log.Debugw("no coordinators available for punch", "peer", target.Short())
		return
	}

	req := &meshv1.PunchCoordRequest{PeerId: target.Bytes()}
	env := &meshv1.Envelope{
		Body: &meshv1.Envelope_PunchCoordRequest{PunchCoordRequest: req},
	}

	// TODO(saml) down the line, we should probably cycle through potential coordinators per request
	coord := coordinators[0]
	if err := n.mesh.Send(context.Background(), coord, env); err != nil {
		n.log.Debugw("punch coord request send failed", "coordinator", coord.Short(), "err", err)
	}
}

func (n *Node) handlePunchCoordRequest(ctx context.Context, from types.PeerKey, req *meshv1.PunchCoordRequest) {
	targetKey := types.PeerKeyFromBytes(req.PeerId)

	fromAddr, fromOk := n.mesh.GetActivePeerAddress(from)
	targetAddr, targetOk := n.mesh.GetActivePeerAddress(targetKey)
	if !fromOk || !targetOk {
		n.log.Debugw("punch coord: missing address",
			"from", from.Short(), "fromOk", fromOk,
			"target", targetKey.Short(), "targetOk", targetOk)
		return
	}

	n.workers.submit(ctx, func() {
		if err := n.mesh.Send(ctx, from, &meshv1.Envelope{Body: &meshv1.Envelope_PunchCoordTrigger{PunchCoordTrigger: &meshv1.PunchCoordTrigger{
			PeerId:   req.PeerId,
			SelfAddr: fromAddr.String(),
			PeerAddr: targetAddr.String(),
		}}}); err != nil {
			n.log.Debugw("punch coord trigger send failed", "to", from.Short(), "err", err)
		}
	})
	n.workers.submit(ctx, func() {
		if err := n.mesh.Send(ctx, targetKey, &meshv1.Envelope{Body: &meshv1.Envelope_PunchCoordTrigger{PunchCoordTrigger: &meshv1.PunchCoordTrigger{
			PeerId:   from.Bytes(),
			SelfAddr: targetAddr.String(),
			PeerAddr: fromAddr.String(),
		}}}); err != nil {
			n.log.Debugw("punch coord trigger send failed", "to", targetKey.Short(), "err", err)
		}
	})
}

func (n *Node) handlePunchCoordTrigger(ctx context.Context, trigger *meshv1.PunchCoordTrigger) {
	peerKey := types.PeerKeyFromBytes(trigger.PeerId)
	if n.peers.InState(peerKey, peer.PeerStateConnected) {
		return
	}

	peerAddr, err := net.ResolveUDPAddr("udp", trigger.PeerAddr)
	if err != nil {
		n.log.Debugw("punch coord trigger: bad peer addr", "addr", trigger.PeerAddr, "err", err)
		return
	}

	n.log.Infow("punch coord trigger received", "peer", peerKey.Short(), "peerAddr", peerAddr.String())

	n.workers.submit(ctx, func() {
		if n.peers.InState(peerKey, peer.PeerStateConnected) {
			return
		}

		localNAT := n.natDetector.Type()
		n.nodeMetrics.PunchAttempts.Inc()

		punchCtx, cancel := context.WithTimeout(context.Background(), punchTimeout)
		err := n.mesh.Punch(punchCtx, peerKey, peerAddr, localNAT)
		cancel()

		if err != nil && n.peers.InState(peerKey, peer.PeerStateConnecting) {
			if errors.Is(err, mesh.ErrIdentityMismatch) {
				n.log.Warnw("punch failed: peer identity mismatch", "peer", peerKey.Short(), "err", err)
			} else {
				n.log.Debugw("punch failed", "peer", peerKey.Short(), "err", err)
			}
			n.nodeMetrics.PunchFailures.Inc()
			n.localPeerEvents <- peer.ConnectFailed{PeerKey: peerKey}
		}
	})
}

func (n *Node) sendObservedAddress(pk types.PeerKey, addr *net.UDPAddr) {
	if err := n.mesh.Send(context.Background(), pk, &meshv1.Envelope{
		Body: &meshv1.Envelope_ObservedAddress{
			ObservedAddress: &meshv1.ObservedAddress{Addr: addr.String()},
		},
	}); err != nil {
		n.log.Debugw("failed sending observed address", "peer", pk.Short(), "err", err)
	}
}

func (n *Node) handleObservedAddress(from types.PeerKey, oa *meshv1.ObservedAddress) {
	addr, err := net.ResolveUDPAddr("udp", oa.GetAddr())
	if err != nil {
		n.log.Debugw("observed address: parse failed", "addr", oa.GetAddr(), "err", err)
		return
	}

	if addr.IP.IsPrivate() || addr.IP.IsLoopback() || addr.IP.IsLinkLocalUnicast() {
		return
	}

	n.log.Debugw("observed address received", "addr", addr.String(), "from", from.Short())
	n.store.SetObservedExternalIP(addr.IP.String())
	n.store.SetExternalPort(uint32(addr.Port))

	if peerAddr, ok := n.mesh.GetActivePeerAddress(from); ok {
		if observerIP, ok := netip.AddrFromSlice(peerAddr.IP); ok {
			observerIP = observerIP.Unmap()
			if natType, changed := n.natDetector.AddObservation(observerIP, addr.Port); changed {
				n.log.Infow("NAT type detected", "type", natType)
				n.store.SetLocalNatType(natType)
			}
		}
	}
}

// RouteCall implements wasm.InvocationRouter. It invokes a function on the
// target workload — locally if compiled here, otherwise over the mesh to a
// node that claims it.
func (n *Node) RouteCall(ctx context.Context, targetHash, function string, input []byte) ([]byte, error) {
	// Local-first: if we have it compiled, call directly.
	if n.workloads.IsRunning(targetHash) {
		return n.workloads.Call(ctx, targetHash, function, input)
	}

	// Find claimants from gossip.
	snap := n.store.Snapshot()
	claimants := snap.Claims[targetHash]
	if len(claimants) == 0 {
		return nil, fmt.Errorf("no node claims workload %s: %w", targetHash[:min(12, len(targetHash))], workload.ErrNotRunning) //nolint:mnd
	}

	// Sort claimants deterministically and try each until one succeeds.
	sorted := sortedClaimants(claimants)
	var lastErr error
	for _, target := range sorted {
		stream, err := n.mesh.OpenWorkloadStream(ctx, target)
		if err != nil {
			n.log.Debugw("workload stream failed, trying next claimant",
				"target", target.Short(), "hash", targetHash[:min(12, len(targetHash))], "err", err) //nolint:mnd
			lastErr = err
			continue
		}

		out, err := scheduler.InvokeOverStream(ctx, stream, targetHash, function, input)
		if err != nil {
			n.log.Warnw("workload invocation failed, trying next claimant",
				"target", target.Short(), "hash", targetHash[:min(12, len(targetHash))], "err", err) //nolint:mnd
			lastErr = err
			continue
		}
		return out, nil
	}
	return nil, fmt.Errorf("all %d claimants failed for %s: %w", len(sorted), targetHash[:min(12, len(targetHash))], lastErr) //nolint:mnd
}

func sortedClaimants(claimants map[types.PeerKey]struct{}) []types.PeerKey {
	keys := make([]types.PeerKey, 0, len(claimants))
	for pk := range claimants {
		keys = append(keys, pk)
	}
	slices.SortFunc(keys, func(a, b types.PeerKey) int { return a.Compare(b) })
	return keys
}

func (n *Node) ConnectService(peerID types.PeerKey, remotePort, localPort uint32) (uint32, error) {
	snap := n.store.Snapshot()
	if nv, ok := snap.Nodes[peerID]; !ok || len(nv.IdentityPub) == 0 {
		return 0, errors.New("peerID not recognised")
	}

	port, err := n.tun.ConnectService(peerID, remotePort, localPort)
	if err != nil {
		return 0, err
	}
	n.store.AddDesiredConnection(peerID, remotePort, port)

	n.localPeerEvents <- peer.RetryPeer{PeerKey: peerID}

	return port, nil
}

func (n *Node) DisconnectService(localPort uint32) error {
	snap := n.store.Snapshot()
	for _, conn := range snap.Connections {
		if conn.LocalPort == localPort {
			n.tun.DisconnectLocalPort(localPort)
			n.store.RemoveDesiredConnection(conn.PeerID, conn.RemotePort, conn.LocalPort)
			return nil
		}
	}
	return fmt.Errorf("no connection on local port %d", localPort)
}

// checkCertExpiry checks the local node's delegation cert and logs warnings.
// Returns true if the cert has expired beyond the reconnect window and the
// node should shut down.
func (n *Node) checkCertExpiry() bool {
	now := time.Now()
	expired := auth.IsCertExpired(n.creds.Cert, now)
	remaining := time.Until(auth.CertExpiresAt(n.creds.Cert))
	n.nodeMetrics.CertExpirySeconds.Set(remaining.Seconds())

	if expired {
		// Attempt renewal before giving up.
		if n.attemptCertRenewal() {
			n.renewalFailed.Store(false)
			return false
		}
		n.renewalFailed.Store(true)

		if !auth.IsCertWithinReconnectWindow(n.creds.Cert, now, n.conf.ReconnectWindow) {
			n.log.Errorw("delegation certificate expired beyond reconnect window, shutting down — rejoin the cluster or contact a cluster admin",
				"expired_at", auth.CertExpiresAt(n.creds.Cert))
			return true
		}
		n.log.Warnw("delegation certificate expired — running in degraded mode, will keep retrying renewal",
			"expired_at", auth.CertExpiresAt(n.creds.Cert),
			"reconnect_window_remaining", auth.CertExpiresAt(n.creds.Cert).Add(n.conf.ReconnectWindow).Sub(now).Truncate(time.Minute))
		return false
	}

	if remaining <= certWarnThreshold {
		failed := !n.attemptCertRenewal()
		n.renewalFailed.Store(failed)
		if !failed {
			return false
		}
	}

	switch {
	case remaining <= certCriticalThreshold:
		n.log.Warnw("delegation certificate expiring soon — auto-renewal failed — rejoin the cluster or contact a cluster admin",
			"expires_in", remaining.Truncate(time.Minute))
	case remaining <= certWarnThreshold:
		n.log.Infow("delegation certificate approaching expiry — auto-renewal attempted but failed, will retry",
			"expires_in", remaining.Truncate(time.Minute))
	}
	return false
}

func (n *Node) attemptCertRenewal() bool {
	connectedPeers := n.GetConnectedPeers()
	if len(connectedPeers) == 0 {
		n.log.Warnw("delegation certificate renewal failed: no connected peers")
		return false
	}

	n.log.Infow("renewing delegation certificate")

	ctx, cancel := context.WithTimeout(context.Background(), certRenewalTimeout)
	defer cancel()

	for _, peerKey := range connectedPeers {
		newCert, err := n.mesh.RequestCertRenewal(ctx, peerKey)
		if err != nil {
			n.log.Debugw("delegation certificate renewal failed", "peer", peerKey.Short(), "err", err)
			continue
		}

		if err := n.applyCertRenewal(newCert); err != nil {
			n.log.Warnw("delegation certificate renewal failed: invalid cert", "peer", peerKey.Short(), "err", err)
			continue
		}

		n.log.Infow("delegation certificate renewed",
			"expires_at", auth.CertExpiresAt(newCert))
		n.nodeMetrics.CertRenewals.Inc()
		return true
	}

	n.log.Warnw("delegation certificate renewal failed: all peers refused or returned errors")
	n.nodeMetrics.CertRenewalsFailed.Inc()
	return false
}

func (n *Node) applyCertRenewal(newCert *admissionv1.DelegationCert) error {
	now := time.Now()
	pubKey := n.signPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	if err := auth.VerifyDelegationCert(newCert, n.creds.Trust, now, pubKey); err != nil {
		return err
	}

	tlsCert, err := mesh.GenerateIdentityCert(n.signPriv, newCert, n.conf.TLSIdentityTTL)
	if err != nil {
		return err
	}

	n.mesh.UpdateMeshCert(tlsCert)
	n.creds.Cert = newCert

	if err := auth.SaveNodeCredentials(n.pollenDir, n.creds); err != nil {
		n.log.Warnw("failed to persist renewed credentials", "err", err)
	}

	n.store.SetLocalCertExpiry(newCert.GetClaims().GetNotAfterUnix())
	return nil
}

func (n *Node) handleCertRenewalRequest(ctx context.Context, from types.PeerKey, req *meshv1.CertRenewalRequest) {
	sendReject := func(reason string) {
		_ = n.mesh.Send(ctx, from, &meshv1.Envelope{
			Body: &meshv1.Envelope_CertRenewalResponse{
				CertRenewalResponse: &meshv1.CertRenewalResponse{Reason: reason},
			},
		})
	}

	if !bytes.Equal(req.GetSubjectPub(), from.Bytes()) {
		sendReject("subject_pub does not match sender")
		return
	}

	signer := n.creds.DelegationKey
	if signer == nil {
		sendReject("this node is not an admin")
		return
	}

	if n.store.IsDenied(req.GetSubjectPub()) {
		sendReject("subject has been denied")
		return
	}

	ttl := n.conf.MembershipTTL
	var accessDeadline time.Time
	if peerCert, ok := n.mesh.PeerDelegationCert(from); ok {
		ttl = auth.CertTTL(peerCert)
		if dl, hasDeadline := auth.CertAccessDeadline(peerCert); hasDeadline {
			if time.Now().After(dl) {
				sendReject("access deadline has passed")
				return
			}
			accessDeadline = dl
		}
	}

	now := time.Now()

	notAfter := now.Add(ttl)
	if !accessDeadline.IsZero() && notAfter.After(accessDeadline) {
		notAfter = accessDeadline
	}

	parentChain := make([]*admissionv1.DelegationCert, 0, 1+len(signer.Issuer.GetChain()))
	parentChain = append(parentChain, signer.Issuer)
	parentChain = append(parentChain, signer.Issuer.GetChain()...)
	newCert, err := auth.IssueDelegationCert(
		signer.Priv,
		parentChain,
		signer.Trust.GetClusterId(),
		req.GetSubjectPub(),
		auth.LeafCapabilities(),
		now.Add(-time.Minute),
		notAfter,
		accessDeadline,
	)
	if err != nil {
		sendReject(err.Error())
		return
	}

	_ = n.mesh.Send(ctx, from, &meshv1.Envelope{
		Body: &meshv1.Envelope_CertRenewalResponse{CertRenewalResponse: &meshv1.CertRenewalResponse{
			Accepted: true,
			Cert:     newCert,
		}},
	})
}

func (n *Node) disconnectExpiredPeers(ctx context.Context) {
	now := time.Now()
	for _, peerKey := range n.mesh.ConnectedPeers() {
		dc, ok := n.mesh.PeerDelegationCert(peerKey)
		if !ok || dc == nil {
			continue
		}
		if !auth.IsCertExpired(dc, now) {
			continue
		}
		// Allow peers within the reconnect window to stay connected so
		// they can renew their cert.
		if auth.IsCertWithinReconnectWindow(dc, now, n.conf.ReconnectWindow) {
			continue
		}
		n.log.Warnw("disconnecting peer with expired delegation cert beyond reconnect window",
			"peer", peerKey.Short(), "expired_at", auth.CertExpiresAt(dc))
		n.tun.DisconnectPeer(peerKey)
		n.mesh.ClosePeerSession(peerKey, mesh.CloseReasonCertExpired)
		n.handlePeerInput(ctx, peer.PeerDisconnected{PeerKey: peerKey, Reason: peer.DisconnectCertExpired})
		n.handlePeerInput(ctx, peer.ForgetPeer{PeerKey: peerKey})
	}
}

func (n *Node) shutdown() {
	n.workers.wait()

	if err := n.store.Close(); err != nil {
		n.log.Errorw("failed to close state store", "err", err)
	}

	if n.workloads != nil {
		n.workloads.Close()
	}
	if n.wasmRuntime != nil {
		n.wasmRuntime.Close()
	}

	n.tun.Close()

	if err := n.mesh.BroadcastDisconnect(); err != nil {
		n.log.Errorw("failed to broadcast disconnect", "err", err)
	}

	if err := n.mesh.Close(); err != nil {
		n.log.Errorw("failed to shut down mesh", "err", err)
	} else {
		n.log.Info("successfully shut down mesh")
	}

	n.metricsCol.Close()

	n.log.Debug("successfully shutdown Node")
}

func (n *Node) signalRouteInvalidate() {
	select {
	case n.routeInvalidate <- struct{}{}:
	default:
	}
}

func (n *Node) recomputeRoutes() {
	snap := n.store.Snapshot()
	nodeInfos := make(map[types.PeerKey]route.NodeInfo, len(snap.Nodes))
	for pk, nv := range snap.Nodes {
		nodeInfos[pk] = route.NodeInfo{
			Reachable: nv.Reachable,
			Coord:     nv.VivaldiCoord,
		}
	}

	directPeers := make(map[types.PeerKey]struct{})
	for _, pk := range n.mesh.ConnectedPeers() {
		directPeers[pk] = struct{}{}
	}

	routes := route.Recompute(snap.LocalID, &n.localCoord, directPeers, nodeInfos)
	n.routeTable.Update(routes)
}

// knownPeersFromSnapshot builds a []store.KnownPeer from a snapshot,
// applying the same filters as store.KnownPeers(): excludes the local node
// and nodes without any address information.
func knownPeersFromSnapshot(snap store.Snapshot) []store.KnownPeer {
	known := make([]store.KnownPeer, 0, len(snap.Nodes))
	for pk, nv := range snap.Nodes {
		if pk == snap.LocalID {
			continue
		}
		if nv.LastAddr == "" && (len(nv.IPs) == 0 || nv.LocalPort == 0) {
			continue
		}
		known = append(known, store.KnownPeer{
			PeerID:             pk,
			LocalPort:          nv.LocalPort,
			ExternalPort:       nv.ExternalPort,
			ObservedExternalIP: nv.ObservedExternalIP,
			NatType:            nv.NatType,
			IdentityPub:        nv.IdentityPub,
			IPs:                nv.IPs,
			LastAddr:           nv.LastAddr,
			PubliclyAccessible: nv.PubliclyAccessible,
			VivaldiCoord:       nv.VivaldiCoord,
		})
	}
	slices.SortFunc(known, func(a, b store.KnownPeer) int {
		return a.PeerID.Compare(b.PeerID)
	})
	return known
}

// snapshotHasServicePort checks whether a peer in the snapshot has a service
// registered on the given port.
func snapshotHasServicePort(snap store.Snapshot, peerID types.PeerKey, port uint32) bool {
	nv, ok := snap.Nodes[peerID]
	if !ok {
		return false
	}
	for _, svc := range nv.Services {
		if svc.GetPort() == port {
			return true
		}
	}
	return false
}

func (n *Node) Ready() <-chan struct{} {
	return n.ready
}

func (n *Node) ListenPort() int {
	return n.mesh.ListenPort()
}

// GetConnectedPeers returns all currently connected peer keys.
func (n *Node) GetConnectedPeers() []types.PeerKey {
	return n.peers.GetAll(peer.PeerStateConnected)
}

// ConnectPeer establishes a direct mesh connection to a peer at the given addresses.
func (n *Node) ConnectPeer(ctx context.Context, pk types.PeerKey, addrs []*net.UDPAddr) error {
	return n.mesh.Connect(ctx, pk, addrs)
}

func (n *Node) UpsertService(port uint32, name string) {
	n.tun.RegisterService(port)
	n.store.UpsertLocalService(port, name)
}

func (n *Node) RemoveService(name string, port uint32) {
	n.store.RemoveLocalServices(name)
	n.tun.UnregisterService(port)
}

func (n *Node) DenyPeer(pk types.PeerKey) {
	n.store.DenyPeer(pk.Bytes())
	n.tun.DisconnectPeer(pk)
	n.store.RemoveDesiredConnection(pk, 0, 0)
	n.mesh.ClosePeerSession(pk, mesh.CloseReasonDenied)
	n.localPeerEvents <- peer.PeerDisconnected{PeerKey: pk, Reason: peer.DisconnectDenied}
	n.localPeerEvents <- peer.ForgetPeer{PeerKey: pk}
	if err := n.store.Save(); err != nil {
		n.log.Warnw("failed to save state after deny", "err", err)
	}
}

func (n *Node) SetWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) error {
	_, err := n.store.SetLocalWorkloadSpec(hash, replicas, memoryPages, timeoutMs)
	return err
}

// SeedWorkload stores WASM bytes in CAS, compiles the module, publishes the
// workload spec, and claims it locally — the same path as NodeService.SeedWorkload.
func (n *Node) SeedWorkload(wasmBytes []byte, replicas, memoryPages, timeoutMs uint32) (string, error) {
	if n.workloads == nil {
		return "", fmt.Errorf("workload manager not initialized")
	}

	cfg := wasm.PluginConfig{
		MemoryPages: memoryPages,
		Timeout:     time.Duration(timeoutMs) * time.Millisecond,
	}
	hash, err := n.workloads.Seed(wasmBytes, cfg)
	if err != nil && !errors.Is(err, workload.ErrAlreadyRunning) {
		return "", err
	}

	if replicas == 0 {
		replicas = 1
	}

	if _, err := n.store.SetLocalWorkloadSpec(hash, replicas, memoryPages, timeoutMs); err != nil {
		return "", err
	}
	n.store.SetLocalWorkloadClaim(hash, true)

	return hash, nil
}

func (n *Node) Credentials() *auth.NodeCredentials {
	return n.creds
}

// --- Control-service delegation methods ---
// These thin wrappers satisfy the nodeController interface so svc.go
// never reaches into Node's internal fields.

func (n *Node) Snapshot() store.Snapshot {
	return n.store.Snapshot()
}

func (n *Node) ListConnections() []tunnel.ConnectionInfo {
	return n.tun.ListConnections()
}

func (n *Node) PeerStateCounts() peer.PeerStateCounts {
	return n.peers.StateCounts()
}

func (n *Node) GetActivePeerAddress(pk types.PeerKey) (*net.UDPAddr, bool) {
	return n.mesh.GetActivePeerAddress(pk)
}

func (n *Node) PeerRTT(pk types.PeerKey) (time.Duration, bool) {
	conn, ok := n.mesh.GetConn(pk)
	if !ok {
		return 0, false
	}
	rtt := conn.ConnectionStats().SmoothedRTT
	if rtt <= 0 {
		return 0, false
	}
	return rtt, true
}

func (n *Node) MeshConnect(ctx context.Context, pk types.PeerKey, addrs []*net.UDPAddr) error {
	return n.mesh.Connect(ctx, pk, addrs)
}

func (n *Node) ReconnectWindowDuration() time.Duration {
	return n.conf.ReconnectWindow
}

func (n *Node) HasWorkloads() bool {
	return n.workloads != nil
}

func (n *Node) WorkloadList() []workload.Summary {
	if n.workloads == nil {
		return nil
	}
	return n.workloads.List()
}

func (n *Node) WorkloadSeed(wasmBytes []byte, cfg wasm.PluginConfig) (string, error) {
	return n.workloads.Seed(wasmBytes, cfg)
}

func (n *Node) WorkloadUnseed(hash string) error {
	return n.workloads.Unseed(hash)
}

func (n *Node) WorkloadIsRunning(hash string) bool {
	return n.workloads.IsRunning(hash)
}

func (n *Node) ResolveWorkloadPrefix(prefix string) (hash string, ambiguous, found bool) {
	return n.store.ResolveWorkloadPrefix(prefix)
}

func (n *Node) SetLocalWorkloadClaim(hash string, claimed bool) {
	n.store.SetLocalWorkloadClaim(hash, claimed)
}

func (n *Node) RemoveLocalWorkloadSpec(hash string) {
	n.store.RemoveLocalWorkloadSpec(hash)
}

func (n *Node) ControlMetrics() controlMetrics {
	nm := n.nodeMetrics
	gm := n.store.GossipMetrics()
	return controlMetrics{
		CertExpirySeconds:  nm.CertExpirySeconds.Value(),
		CertRenewals:       uint64(nm.CertRenewals.Value()),       //nolint:gosec
		CertRenewalsFailed: uint64(nm.CertRenewalsFailed.Value()), //nolint:gosec
		PunchAttempts:      uint64(nm.PunchAttempts.Value()),      //nolint:gosec
		PunchFailures:      uint64(nm.PunchFailures.Value()),      //nolint:gosec
		GossipApplied:      uint64(gm.EventsApplied.Value()),      //nolint:gosec
		GossipStale:        uint64(gm.EventsStale.Value()),        //nolint:gosec
		SmoothedVivaldiErr: n.smoothedErr.Value(),
		VivaldiSamples:     uint64(n.vivaldiSamples.Load()),    //nolint:gosec
		EagerSyncs:         uint64(n.eagerSyncs.Load()),        //nolint:gosec
		EagerSyncFailures:  uint64(n.eagerSyncFailures.Load()), //nolint:gosec
	}
}

func (n *Node) Log() *zap.SugaredLogger {
	return n.log
}

func (n *Node) JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error) {
	return n.mesh.JoinWithInvite(ctx, token)
}

func (n *Node) AddDesiredConnection(pk types.PeerKey, remotePort, localPort uint32) {
	n.store.AddDesiredConnection(pk, remotePort, localPort)
}

func (n *Node) RemoveDesiredConnection(pk types.PeerKey, remotePort, localPort uint32) {
	n.store.RemoveDesiredConnection(pk, remotePort, localPort)
}

func GenIdentityKey(pollenDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	dir := filepath.Join(pollenDir, localKeysDir)
	privPath := filepath.Join(dir, signingKeyName)
	pubPath := filepath.Join(dir, signingPubKeyName)

	if priv, pub, err := loadIdentityKey(privPath, pubPath); err == nil {
		return priv, pub, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}

	if err := perm.EnsureDir(dir); err != nil {
		return nil, nil, err
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	privFile, err := os.OpenFile(privPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, privKeyPerm)
	if err != nil {
		return nil, nil, err
	}
	defer privFile.Close()

	if err := pem.Encode(privFile, &pem.Block{
		Type:  pemTypePriv,
		Bytes: priv.Seed(),
	}); err != nil {
		return nil, nil, err
	}

	if err := perm.SetGroupReadable(privPath); err != nil {
		return nil, nil, err
	}

	pubFile, err := os.OpenFile(pubPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, pubKeyPerm)
	if err != nil {
		return nil, nil, err
	}
	defer pubFile.Close()

	if err := pem.Encode(pubFile, &pem.Block{
		Type:  pemTypePub,
		Bytes: pub,
	}); err != nil {
		return nil, nil, err
	}

	if err := perm.SetGroupReadable(pubPath); err != nil {
		return nil, nil, err
	}

	return priv, pub, nil
}

// ReadIdentityPub reads the public key from disk without requiring private key access.
func ReadIdentityPub(pollenDir string) (ed25519.PublicKey, error) {
	pubPath := filepath.Join(pollenDir, localKeysDir, signingPubKeyName)
	pubEnc, err := os.ReadFile(pubPath)
	if err != nil {
		return nil, err
	}
	return decodePubKeyPEM(pubEnc)
}

func loadIdentityKey(privPath, pubPath string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	keyEnc, err := os.ReadFile(privPath)
	if err != nil {
		return nil, nil, err
	}
	pubEnc, err := os.ReadFile(pubPath)
	if err != nil {
		return nil, nil, err
	}
	block, _ := pem.Decode(keyEnc)
	if block == nil || block.Type != pemTypePriv {
		return nil, nil, errors.New("invalid private key PEM")
	}
	pub, err := decodePubKeyPEM(pubEnc)
	if err != nil {
		return nil, nil, err
	}
	return ed25519.NewKeyFromSeed(block.Bytes), pub, nil
}

func decodePubKeyPEM(data []byte) (ed25519.PublicKey, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != pemTypePub {
		return nil, errors.New("invalid public key PEM")
	}
	return ed25519.PublicKey(block.Bytes), nil
}
