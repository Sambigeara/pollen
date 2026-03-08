package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	plnconfig "github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/observability/traces"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/perm"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/sysinfo"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/tunnel"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/util"
	"go.uber.org/zap"
)

const (
	// MaxDatagramPayload is the maximum safe payload size for a QUIC datagram.
	MaxDatagramPayload = 1100
)

var ErrCertExpired = errors.New("membership certificate has expired")

// envelopeOverhead is the worst-case serialized size of an Envelope wrapping
// a GossipEventBatch — covers the oneof tag, length prefixes, batch wrapper
// framing, and the IsResponse tag.
var envelopeOverhead = (&meshv1.Envelope{Body: &meshv1.Envelope_Events{
	Events: &statev1.GossipEventBatch{IsResponse: true},
}}).SizeVT()

const (
	certCheckInterval     = 1 * time.Hour
	certWarnThreshold     = 30 * 24 * time.Hour
	certCriticalThreshold = 7 * 24 * time.Hour
	certRenewalTimeout    = 10 * time.Second
	expirySweepInterval   = 30 * time.Second

	localKeysDir = "keys"

	signingKeyName    = "ed25519.key"
	signingPubKeyName = "ed25519.pub"
	pemTypePriv       = "ED25519 PRIVATE KEY"
	pemTypePub        = "ED25519 PUBLIC KEY"
	privKeyPerm       = 0o600
	pubKeyPerm        = 0o640

	directTimeout = 2 * time.Second
	punchTimeout  = 3 * time.Second

	// eagerSyncCooldown should be >= GossipInterval to avoid redundant sends.
	eagerSyncCooldown = 5 * time.Second

	revokeStreakThreshold       = 3  // consecutive non-target ticks before revoking outbound
	revokeStreakThresholdPublic = 30 // public peers are stickier (coordinator stability)

	vivaldiEnterHMACThreshold = 0.6
	vivaldiExitHMACThreshold  = 0.35
	vivaldiWarmupDuration     = 5 * time.Second

	loopIntervalJitter = 0.1
	peerEventBufSize   = 64
	gossipEventBufSize = 64
	punchChBufSize     = 32
	punchWorkers       = 3 // max concurrent punches; bounds socket usage to N×256
	ipRefreshInterval  = 5 * time.Minute
)

type BootstrapPeer struct {
	Addrs   []string
	PeerKey types.PeerKey
}

type Config struct {
	AdvertisedIPs       []string
	BootstrapPeers      []BootstrapPeer
	Port                int
	GossipInterval      time.Duration
	PeerTickInterval    time.Duration
	GossipJitter        float64
	TLSIdentityTTL      time.Duration
	MembershipTTL       time.Duration
	MaxConnectionAge    time.Duration
	DisableGossipJitter bool
	BootstrapPublic     bool
	MetricsEnabled      bool
}

type punchRequest struct {
	ip      net.IP
	port    int
	peerKey types.PeerKey
}

type Node struct {
	lastExpirySweep time.Time
	mesh            mesh.Mesh
	lastEagerSync   map[types.PeerKey]time.Time
	peerConnectTime map[types.PeerKey]time.Time
	tun             *tunnel.Manager
	nonTargetStreak map[types.PeerKey]int
	creds           *auth.NodeCredentials
	localPeerEvents chan peer.Input
	peers           *peer.Store
	conf            *Config
	ready           chan struct{}
	store           *store.Store
	natDetector     *nat.Detector
	punchCh         chan punchRequest
	gossipEvents    chan []*statev1.GossipEvent
	log             *zap.SugaredLogger
	nodeMetrics     *metrics.NodeMetrics
	topoMetrics     *metrics.TopologyMetrics
	metricsCol      *metrics.Collector
	tracer          *traces.Tracer
	pollenDir       string
	signPriv        ed25519.PrivateKey
	localCoord      topology.Coord
	localCoordErr   atomic.Uint64 // float64 via Float64bits; accessed from event loop + gRPC
	renewalFailed   atomic.Bool
	useHMACNearest  bool
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

	var metricsSink metrics.Sink
	var tracesSink traces.Sink
	if conf.MetricsEnabled {
		metricsSink = metrics.NewLogSink(log.Named("metrics"))
		tracesSink = traces.NewLogSink(log.Named("traces"))
	}
	col := metrics.New(metricsSink, metrics.Config{})
	tracer := traces.NewTracer(tracesSink)

	meshMetrics := metrics.NewMeshMetrics(col)
	peerMetrics := metrics.NewPeerMetrics(col)
	gossipMetrics := metrics.NewGossipMetrics(col)

	m, err := mesh.NewMesh(conf.Port, privKey, creds, conf.TLSIdentityTTL, conf.MembershipTTL, conf.MaxConnectionAge, stateStore.IsSubjectRevoked, meshMetrics)
	if err != nil {
		log.Error("failed to load mesh", zap.Error(err))
		return nil, err
	}

	m.SetTracer(tracer)
	stateStore.SetGossipMetrics(gossipMetrics)
	peerStore.SetPeerMetrics(peerMetrics)

	tun := tunnel.New(m)

	for _, svc := range stateStore.LocalServices() {
		tun.RegisterService(svc.GetPort())
	}

	n := &Node{
		log:             log,
		peers:           peerStore,
		store:           stateStore,
		mesh:            m,
		tun:             tun,
		creds:           creds,
		conf:            conf,
		signPriv:        privKey,
		pollenDir:       pollenDir,
		localPeerEvents: make(chan peer.Input, peerEventBufSize),
		punchCh:         make(chan punchRequest, punchChBufSize),
		gossipEvents:    make(chan []*statev1.GossipEvent, gossipEventBufSize),
		ready:           make(chan struct{}),
		lastEagerSync:   make(map[types.PeerKey]time.Time),
		peerConnectTime: make(map[types.PeerKey]time.Time),
		nonTargetStreak: make(map[types.PeerKey]int),
		natDetector:     nat.NewDetector(),
		useHMACNearest:  true,
		metricsCol:      col,
		topoMetrics:     metrics.NewTopologyMetrics(col),
		nodeMetrics:     metrics.NewNodeMetrics(col),
		tracer:          tracer,
	}
	n.localCoordErr.Store(math.Float64bits(1.0))

	stateStore.OnRevocation(func(subject types.PeerKey) {
		n.tun.DisconnectPeer(subject)
		stateStore.RemoveDesiredConnection(subject, 0, 0)
		go n.mesh.ClosePeerSession(subject, mesh.CloseReasonRevoked)
		select {
		case n.localPeerEvents <- peer.ForgetPeer{PeerKey: subject}:
		default:
		}
		if cfg, err := plnconfig.Load(n.pollenDir); err != nil {
			n.log.Warnw("load config for bootstrap peer cleanup failed", "err", err)
		} else {
			cfg.ForgetBootstrapPeer(subject[:])
			if err := plnconfig.Save(n.pollenDir, cfg); err != nil {
				n.log.Warnw("persist bootstrap peer removal failed", "err", err)
			}
		}
	})

	if conf.BootstrapPublic {
		stateStore.SetLocalPubliclyAccessible(true)
	}

	return n, nil
}

func (n *Node) Start(ctx context.Context) error {
	defer n.shutdown()

	if n.creds.Cert != nil {
		n.store.SetLocalCertExpiry(n.creds.Cert.GetClaims().GetNotAfterUnix())
	}

	if err := n.mesh.Start(ctx); err != nil {
		return err
	}

	// Persist a startup coordinate so fresh clusters can bootstrap Vivaldi state
	// without waiting for pre-existing peer coordinates. We intentionally do not
	// queue the returned events here; peers will pull this state via clock gossip
	// (or eager sync) once sessions are established.
	n.store.SetLocalVivaldiCoord(n.localCoord)

	close(n.ready)
	n.connectBootstrapPeers(ctx)
	n.tun.Start(ctx)
	go n.recvLoop(ctx)
	for range punchWorkers {
		go n.punchLoop(ctx)
	}

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

	certCheckTicker := time.NewTicker(certCheckInterval)
	defer certCheckTicker.Stop()

	n.tick()
	n.checkCertExpiry()         // seed the cert-expiry gauge; the ticker only fires hourly
	n.sampleResourceTelemetry() // seed telemetry before first gossip round

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-peerTicker.C:
			n.tick()
		case <-gossipTicker.C:
			n.sampleResourceTelemetry()
			n.gossip(ctx)
		case <-ipRefreshCh:
			n.refreshIPs()
		case <-certCheckTicker.C:
			if n.checkCertExpiry() {
				return ErrCertExpired
			}
		case events := <-n.gossipEvents:
			n.broadcastEvents(ctx, events)
		case in := <-n.mesh.Events():
			n.handlePeerInput(in)
		case in := <-n.localPeerEvents:
			n.handlePeerInput(in)
		}
	}
}

func (n *Node) connectBootstrapPeers(ctx context.Context) {
	for _, bp := range n.conf.BootstrapPeers {
		if n.store.IsSubjectRevoked(bp.PeerKey[:]) {
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

func (n *Node) queueGossipEvents(events []*statev1.GossipEvent) {
	if len(events) == 0 {
		return
	}

	select {
	case n.gossipEvents <- events:
	default:
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

func (n *Node) handleDatagram(ctx context.Context, from types.PeerKey, env *meshv1.Envelope) {
	switch body := env.GetBody().(type) {
	case *meshv1.Envelope_Clock:
		span := n.tracer.StartFromRemote("gossip.handleClock", env.GetTraceId())
		span.SetAttr("peer", from.Short())
		events := n.store.MissingFor(body.Clock)
		n.sendGossipBatchesToPeer(ctx, from, batchEvents(events, MaxDatagramPayload), true)
		span.End()
	case *meshv1.Envelope_Events:
		span := n.tracer.StartFromRemote("gossip.applyEvents", env.GetTraceId())
		span.SetAttr("peer", from.Short())
		result := n.store.ApplyEvents(body.Events.GetEvents(), body.Events.GetIsResponse())
		if len(result.Rebroadcast) > 0 && !body.Events.GetIsResponse() {
			n.queueGossipEvents(result.Rebroadcast)
		}
		span.End()
	case *meshv1.Envelope_PunchCoordRequest:
		span := n.tracer.StartFromRemote("punch.coordRequest", env.GetTraceId())
		span.SetAttr("peer", from.Short())
		n.handlePunchCoordRequest(ctx, from, body.PunchCoordRequest)
		span.End()
	case *meshv1.Envelope_PunchCoordTrigger:
		span := n.tracer.StartFromRemote("punch.coordTrigger", env.GetTraceId())
		span.SetAttr("peer", from.Short())
		n.handlePunchCoordTrigger(body.PunchCoordTrigger)
		span.End()
	case *meshv1.Envelope_ObservedAddress:
		n.handleObservedAddress(from, body.ObservedAddress)
	case *meshv1.Envelope_CertRenewalRequest:
		n.handleCertRenewalRequest(ctx, from, body.CertRenewalRequest)
	}
}

func (n *Node) broadcastEvents(ctx context.Context, events []*statev1.GossipEvent) {
	if len(events) == 0 {
		return
	}

	n.broadcastGossipBatches(ctx, n.GetConnectedPeers(), batchEvents(events, MaxDatagramPayload))
}

func (n *Node) sendGossipBatchesToPeer(ctx context.Context, peerID types.PeerKey, batches [][]*statev1.GossipEvent, isResponse bool) {
	for _, batch := range batches {
		env := &meshv1.Envelope{Body: &meshv1.Envelope_Events{
			Events: &statev1.GossipEventBatch{Events: batch, IsResponse: isResponse},
		}}
		if err := n.mesh.Send(ctx, peerID, env); err != nil {
			n.log.Debugw("failed sending gossip events", "to", peerID.Short(), "err", err)
			return
		}
	}
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

func (n *Node) sendClockBatchesToPeer(ctx context.Context, peerID types.PeerKey, batches []*statev1.GossipVectorClock) {
	for _, batch := range batches {
		if err := n.mesh.Send(ctx, peerID, &meshv1.Envelope{
			Body: &meshv1.Envelope_Clock{Clock: batch},
		}); err != nil {
			n.log.Debugw("eager sync failed", "peer", peerID.Short(), "err", err)
			return
		}
	}
}

// eventWireSize returns the on-wire size of a single event inside the
// repeated field: the varint-encoded length prefix plus the message bytes.
func eventWireSize(event *statev1.GossipEvent) int {
	n := event.SizeVT()
	// Tag for repeated field (1 byte) + varint-encoded length.
	overhead := 1
	for v := uint(n); v >= 0x80; v >>= 7 {
		overhead++
	}
	return overhead + n
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

// clockEnvelopeSize returns the exact serialized size of an Envelope wrapping
// a GossipVectorClock with the given counters.
func clockEnvelopeSize(counters map[string]uint64) int {
	return (&meshv1.Envelope{Body: &meshv1.Envelope_Clock{
		Clock: &statev1.GossipVectorClock{Counters: counters},
	}}).SizeVT()
}

// batchClocks splits a GossipVectorClock into multiple clocks that each fit
// within maxSize when serialized as an Envelope. Clock entries are idempotent
// (receiver takes max per peer), so partial clocks need no reassembly.
func batchClocks(clock *statev1.GossipVectorClock, maxSize int) []*statev1.GossipVectorClock {
	if clock == nil {
		return nil
	}
	counters := clock.GetCounters()
	if len(counters) == 0 {
		return []*statev1.GossipVectorClock{clock}
	}

	var batches []*statev1.GossipVectorClock
	current := make(map[string]uint64)

	for key, val := range counters {
		current[key] = val
		if clockEnvelopeSize(current) > maxSize {
			if len(current) > 1 {
				delete(current, key)
				batches = append(batches, &statev1.GossipVectorClock{Counters: current})
				current = map[string]uint64{key: val}
			}
			// current now has exactly one entry; if it still exceeds
			// maxSize the entry is individually too large to send.
			if clockEnvelopeSize(current) > maxSize {
				zap.S().Warnw("clock entry exceeds max datagram size, skipping", "peer", key)
				current = make(map[string]uint64)
			}
		}
	}
	if len(current) > 0 {
		batches = append(batches, &statev1.GossipVectorClock{Counters: current})
	}
	return batches
}

func (n *Node) gossip(ctx context.Context) {
	batches := batchClocks(n.store.Clock(), MaxDatagramPayload)
	connectedPeers := n.GetConnectedPeers()
	for _, peerID := range connectedPeers {
		if peerID == n.store.LocalID {
			continue
		}
		for _, batch := range batches {
			if err := n.mesh.Send(ctx, peerID, &meshv1.Envelope{
				Body: &meshv1.Envelope_Clock{Clock: batch},
			}); err != nil {
				n.log.Debugw("clock gossip send failed", "peer", peerID.Short(), "err", err)
			}
		}
	}
}

func (n *Node) sampleResourceTelemetry() {
	cpuPct, memPct, memTotal := sysinfo.Sample()
	n.queueGossipEvents(n.store.SetLocalResourceTelemetry(cpuPct, memPct, memTotal))
}

func (n *Node) refreshIPs() {
	newIPs, err := mesh.GetAdvertisableAddrs(mesh.DefaultExclusions)
	if err != nil {
		n.log.Debugw("ip refresh failed", "err", err)
		return
	}

	events := n.store.SetLocalNetwork(newIPs, uint32(n.conf.Port))
	if len(events) > 0 {
		n.log.Infow("advertised IPs changed", "ips", newIPs)
		n.queueGossipEvents(events)
	}
}

func (n *Node) handlePeerInput(in peer.Input) {
	switch d := in.(type) {
	case peer.PeerDisconnected:
		n.queueGossipEvents(n.store.SetLocalConnected(d.PeerKey, false))
		delete(n.lastEagerSync, d.PeerKey)
		delete(n.peerConnectTime, d.PeerKey)
	case peer.ForgetPeer:
		delete(n.lastEagerSync, d.PeerKey)
		delete(n.peerConnectTime, d.PeerKey)
	}

	outputs := n.peers.Step(time.Now(), in)
	n.handleOutputs(outputs)
}

func (n *Node) tick() {
	n.updateVivaldiCoords()
	n.syncPeersFromState()
	if time.Since(n.lastExpirySweep) >= expirySweepInterval {
		n.disconnectExpiredPeers()
		n.lastExpirySweep = time.Now()
	}
	n.reconcileConnections()
	n.reconcileDesiredConnections()

	now := time.Now()
	outputs := n.peers.Step(now, peer.Tick{})
	n.handleOutputs(outputs)
}

func (n *Node) updateVivaldiCoords() {
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
		peerCoord, ok := n.store.PeerVivaldiCoord(peerKey)
		if !ok || peerCoord == nil || peerCoord.IsZero() {
			continue
		}
		coordErr := math.Float64frombits(n.localCoordErr.Load())
		var newErr float64
		n.localCoord, newErr = topology.Update(
			n.localCoord, coordErr,
			topology.Sample{RTT: rtt, PeerCoord: *peerCoord},
		)
		n.localCoordErr.Store(math.Float64bits(newErr))
		updated = true
	}
	if updated {
		n.queueGossipEvents(n.store.SetLocalVivaldiCoord(n.localCoord))
	}
}

func (n *Node) syncPeersFromState() {
	knownPeers := n.store.KnownPeers()

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

	coordErr := math.Float64frombits(n.localCoordErr.Load())
	if n.useHMACNearest {
		if coordErr < vivaldiExitHMACThreshold {
			n.useHMACNearest = false
		}
	} else {
		if coordErr > vivaldiEnterHMACThreshold {
			n.useHMACNearest = true
		}
	}

	epoch := time.Now().Unix() / topology.EpochSeconds
	localIPs := n.store.NodeIPs(n.store.LocalID)
	shape := summarizeTopologyShape(localIPs, knownPeers)
	activePeerCount := len(connectedPeers)
	if activePeerCount == 0 {
		activePeerCount = len(knownPeers)
	}
	params := adaptiveTopologyParams(epoch, shape)
	params.PreferFullMesh = activePeerCount <= tinyClusterPeerThreshold
	params.LocalIPs = localIPs
	params.CurrentOutbound = currentOutbound
	params.LocalNATType = n.natDetector.Type()
	if localRec, ok := n.store.Get(n.store.LocalID); ok {
		params.LocalObservedExternalIP = localRec.ObservedExternalIP
	}
	params.UseHMACNearest = n.useHMACNearest
	targets := topology.ComputeTargetPeers(n.store.LocalID, n.localCoord, peerInfos, params)

	n.topoMetrics.VivaldiError.Set(coordErr)
	if n.useHMACNearest {
		n.topoMetrics.HMACNearestEnabled.Set(1.0)
	} else {
		n.topoMetrics.HMACNearestEnabled.Set(0.0)
	}

	targetSet := buildTargetPeerSet(targets, n.store.DesiredConnections())

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
			n.handlePeerInput(peer.PeerDisconnected{PeerKey: kp.PeerID, Reason: peer.DisconnectGraceful})
		}
		delete(n.nonTargetStreak, kp.PeerID)
		n.handlePeerInput(peer.ForgetPeer{PeerKey: kp.PeerID})
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

func (n *Node) handleOutputs(outputs []peer.Output) {
	for _, out := range outputs {
		switch e := out.(type) {
		case peer.PeerConnected:
			addr := &net.UDPAddr{IP: e.IP, Port: e.ObservedPort}
			n.store.SetLastAddr(e.PeerKey, addr.String())
			n.peerConnectTime[e.PeerKey] = time.Now()
			n.queueGossipEvents(n.store.SetLocalConnected(e.PeerKey, true))
			if time.Since(n.lastEagerSync[e.PeerKey]) >= eagerSyncCooldown {
				n.sendClockBatchesToPeer(context.Background(), e.PeerKey, batchClocks(n.store.EagerSyncClock(), MaxDatagramPayload))
				n.lastEagerSync[e.PeerKey] = time.Now()
			}

			n.sendObservedAddress(e.PeerKey, addr)

			n.log.Infow("peer connected", "peer_id", e.PeerKey.Short(), "ip", e.IP, "observedPort", e.ObservedPort)
		case peer.AttemptEagerConnect:
			go n.doConnect(e.PeerKey, []*net.UDPAddr{e.Addr})
		case peer.AttemptConnect:
			go n.doConnect(e.PeerKey, n.buildPeerAddrs(e.PeerKey, e.Ips, e.Port))
		case peer.RequestPunchCoordination:
			go n.requestPunchCoordination(e.PeerKey)
		}
	}
}

func (n *Node) reconcileConnections() {
	for _, conn := range n.tun.ListConnections() {
		if n.store.HasServicePort(conn.PeerID, conn.RemotePort) {
			continue
		}
		n.log.Infow("removing stale forward", "peer", conn.PeerID.Short(), "port", conn.RemotePort)
		n.tun.DisconnectRemoteService(conn.PeerID, conn.RemotePort)
		n.store.RemoveDesiredConnection(conn.PeerID, conn.RemotePort, 0)
	}
}

func (n *Node) reconcileDesiredConnections() {
	desired := n.store.DesiredConnections()
	if len(desired) == 0 {
		return
	}

	existing := make(map[string]struct{})
	for _, conn := range n.tun.ListConnections() {
		existing[store.Connection{PeerID: conn.PeerID, RemotePort: conn.RemotePort, LocalPort: conn.LocalPort}.Key()] = struct{}{}
	}

	for _, desiredConn := range desired {
		if _, ok := existing[desiredConn.Key()]; ok {
			continue
		}

		if !n.store.HasServicePort(desiredConn.PeerID, desiredConn.RemotePort) {
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
	var extPort int
	if rec, ok := n.store.Get(peerKey); ok && rec.ExternalPort != 0 {
		extPort = int(rec.ExternalPort)
	}

	return orderPeerAddrs(n.store.NodeIPs(n.store.LocalID), ips, port, extPort)
}

func (n *Node) coordinatorPeers(target types.PeerKey) []types.PeerKey {
	localIPs := n.store.NodeIPs(n.store.LocalID)
	targetIPs := n.store.NodeIPs(target)
	connectedPeers := n.GetConnectedPeers()
	filtered := make([]types.PeerKey, 0, len(connectedPeers))
	for _, key := range connectedPeers {
		if key == target {
			continue
		}
		filtered = append(filtered, key)
	}
	return rankCoordinators(localIPs, targetIPs, target, filtered, n.store)
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

	go func() {
		if err := n.mesh.Send(ctx, from, &meshv1.Envelope{Body: &meshv1.Envelope_PunchCoordTrigger{PunchCoordTrigger: &meshv1.PunchCoordTrigger{
			PeerId:   req.PeerId,
			SelfAddr: fromAddr.String(),
			PeerAddr: targetAddr.String(),
		}}}); err != nil {
			n.log.Debugw("punch coord trigger send failed", "to", from.Short(), "err", err)
		}
	}()
	go func() {
		if err := n.mesh.Send(ctx, targetKey, &meshv1.Envelope{Body: &meshv1.Envelope_PunchCoordTrigger{PunchCoordTrigger: &meshv1.PunchCoordTrigger{
			PeerId:   from.Bytes(),
			SelfAddr: targetAddr.String(),
			PeerAddr: fromAddr.String(),
		}}}); err != nil {
			n.log.Debugw("punch coord trigger send failed", "to", targetKey.Short(), "err", err)
		}
	}()
}

func (n *Node) punchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-n.punchCh:
			if n.peers.InState(req.peerKey, peer.PeerStateConnected) {
				continue
			}

			localNAT := n.natDetector.Type()
			n.nodeMetrics.PunchAttempts.Inc()

			ctx, cancel := context.WithTimeout(context.Background(), punchTimeout)
			err := n.mesh.Punch(ctx, req.peerKey, &net.UDPAddr{IP: req.ip, Port: req.port}, localNAT)
			cancel()

			if err != nil && n.peers.InState(req.peerKey, peer.PeerStateConnecting) {
				if errors.Is(err, mesh.ErrIdentityMismatch) {
					n.log.Warnw("punch failed: peer identity mismatch", "peer", req.peerKey.Short(), "err", err)
				} else {
					n.log.Debugw("punch failed", "peer", req.peerKey.Short(), "err", err)
				}
				n.nodeMetrics.PunchFailures.Inc()
				n.localPeerEvents <- peer.ConnectFailed{PeerKey: req.peerKey}
			}
		}
	}
}

func (n *Node) handlePunchCoordTrigger(trigger *meshv1.PunchCoordTrigger) {
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

	select {
	case n.punchCh <- punchRequest{peerKey: peerKey, ip: peerAddr.IP, port: peerAddr.Port}:
	default:
		n.log.Debugw("punch queue full, dropping", "peer", peerKey.Short())
	}
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
	n.queueGossipEvents(n.store.SetObservedExternalIP(addr.IP.String()))
	n.queueGossipEvents(n.store.SetExternalPort(uint32(addr.Port)))

	if peerAddr, ok := n.mesh.GetActivePeerAddress(from); ok {
		if observerIP, ok := netip.AddrFromSlice(peerAddr.IP); ok {
			observerIP = observerIP.Unmap()
			if natType, changed := n.natDetector.AddObservation(observerIP, addr.Port); changed {
				n.log.Infow("NAT type detected", "type", natType)
				n.queueGossipEvents(n.store.SetLocalNatType(natType))
			}
		}
	}
}

func (n *Node) ConnectService(peerID types.PeerKey, remotePort, localPort uint32) (uint32, error) {
	if _, ok := n.store.IdentityPub(peerID); !ok {
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
	for _, conn := range n.store.DesiredConnections() {
		if conn.LocalPort == localPort {
			n.tun.DisconnectLocalPort(localPort)
			n.store.RemoveDesiredConnection(conn.PeerID, conn.RemotePort, conn.LocalPort)
			return nil
		}
	}
	return fmt.Errorf("no connection on local port %d", localPort)
}

// checkCertExpiry checks the local node's membership cert and logs warnings.
// Returns true if the cert has expired and the node should shut down.
func (n *Node) checkCertExpiry() bool {
	now := time.Now()
	if auth.IsMembershipCertExpired(n.creds.Cert, now) {
		msg := "membership certificate has expired, shutting down — rejoin the cluster or contact a cluster admin"
		if auth.IsCertNonRenewable(n.creds.Cert) {
			msg = "guest membership certificate has expired, shutting down — request a new invite from a cluster admin"
		}
		n.log.Errorw(msg, "expired_at", auth.CertExpiresAt(n.creds.Cert))
		return true
	}

	remaining := time.Until(auth.CertExpiresAt(n.creds.Cert))
	n.nodeMetrics.CertExpirySeconds.Set(remaining.Seconds())

	if auth.IsCertNonRenewable(n.creds.Cert) {
		if remaining <= certWarnThreshold {
			n.log.Warnw("non-renewable guest certificate approaching expiry — request a new invite to continue",
				"expires_in", remaining.Truncate(time.Minute))
		}
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
		n.log.Warnw("membership certificate expiring soon — auto-renewal failed — rejoin the cluster or contact a cluster admin",
			"expires_in", remaining.Truncate(time.Minute))
	case remaining <= certWarnThreshold:
		n.log.Infow("membership certificate approaching expiry — auto-renewal attempted but failed, will retry",
			"expires_in", remaining.Truncate(time.Minute))
	}
	return false
}

func (n *Node) attemptCertRenewal() bool {
	connectedPeers := n.GetConnectedPeers()
	if len(connectedPeers) == 0 {
		n.log.Warnw("membership certificate renewal failed: no connected peers")
		return false
	}

	n.log.Infow("renewing membership certificate")

	ctx, cancel := context.WithTimeout(context.Background(), certRenewalTimeout)
	defer cancel()

	for _, peerKey := range connectedPeers {
		newCert, err := n.mesh.RequestCertRenewal(ctx, peerKey)
		if err != nil {
			n.log.Debugw("membership certificate renewal failed", "peer", peerKey.Short(), "err", err)
			continue
		}

		if err := n.applyCertRenewal(newCert); err != nil {
			n.log.Warnw("membership certificate renewal failed: invalid cert", "peer", peerKey.Short(), "err", err)
			continue
		}

		n.log.Infow("membership certificate renewed",
			"expires_at", auth.CertExpiresAt(newCert))
		n.nodeMetrics.CertRenewals.Inc()
		return true
	}

	n.log.Warnw("membership certificate renewal failed: all peers refused or returned errors")
	n.nodeMetrics.CertRenewalsFailed.Inc()
	return false
}

func (n *Node) applyCertRenewal(newCert *admissionv1.MembershipCert) error {
	now := time.Now()
	pubKey := n.signPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	if err := auth.VerifyMembershipCert(newCert, n.creds.Trust, now, pubKey); err != nil {
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

	n.queueGossipEvents(n.store.SetLocalCertExpiry(newCert.GetClaims().GetNotAfterUnix()))
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

	signer := n.creds.InviteSigner
	if signer == nil {
		sendReject("this node is not an admin")
		return
	}

	if n.store.IsSubjectRevoked(req.GetSubjectPub()) {
		sendReject("subject has been revoked")
		return
	}

	if mc, ok := n.mesh.PeerMembershipCert(from); ok && auth.IsCertNonRenewable(mc) {
		sendReject("certificate is non-renewable — request a new invite from a cluster admin")
		return
	}

	now := time.Now()
	newCert, err := auth.IssueMembershipCertWithIssuer(
		signer.Priv,
		signer.Issuer,
		signer.Trust.GetClusterId(),
		req.GetSubjectPub(),
		now.Add(-time.Minute),
		now.Add(n.conf.MembershipTTL),
		false,
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

func (n *Node) disconnectExpiredPeers() {
	now := time.Now()
	for _, peerKey := range n.mesh.ConnectedPeers() {
		expiry, ok := n.mesh.PeerCertExpiresAt(peerKey)
		if !ok || expiry.IsZero() {
			continue
		}
		if auth.IsCertExpiredAt(expiry, now) {
			n.log.Warnw("disconnecting peer with expired membership cert",
				"peer", peerKey.Short(), "expired_at", expiry)
			n.tun.DisconnectPeer(peerKey)
			n.mesh.ClosePeerSession(peerKey, mesh.CloseReasonCertExpired)
			n.handlePeerInput(peer.PeerDisconnected{PeerKey: peerKey, Reason: peer.DisconnectCertExpired})
			n.handlePeerInput(peer.ForgetPeer{PeerKey: peerKey})
		}
	}
}

func (n *Node) shutdown() {
	if err := n.store.Save(); err != nil {
		n.log.Errorf("failed to save state: %v", err)
	} else {
		n.log.Info("state saved to disk")
	}
	if err := n.store.Close(); err != nil {
		n.log.Errorf("failed to close state store: %v", err)
	}

	n.tun.Close()

	if err := n.mesh.BroadcastDisconnect(); err != nil {
		n.log.Errorf("failed to broadcast disconnect: %v", err)
	}

	if err := n.mesh.Close(); err != nil {
		n.log.Errorf("failed to shut down sock: %v", err)
	} else {
		n.log.Info("successfully shut down mesh")
	}

	n.metricsCol.Close()

	n.log.Debug("successfully shutdown Node")
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

	if err := perm.SetPrivate(privPath); err != nil {
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
