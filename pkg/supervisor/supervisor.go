package supervisor

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/netip"
	"path/filepath"
	"slices"
	"sync"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/control"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/observability/traces"
	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/plnfs"
	"github.com/sambigeara/pollen/pkg/routing"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/tunneling"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	stateSaveInterval     = 30 * time.Second
	routeDebounceInterval = 100 * time.Millisecond
	maxRouteDelay         = time.Second
	loopIntervalJitter    = 0.1
	nodeWorkers           = 8
)

type Supervisor struct {
	trafficRecorder  transport.TrafficRecorder
	membership       membership.MembershipAPI
	placement        placement.PlacementAPI
	tunneling        tunneling.TunnelingAPI
	mesh             transport.Transport
	tracer           trace.Tracer
	store            state.StateStore
	inviteConsumer   auth.InviteConsumer
	ready            chan struct{}
	log              *zap.SugaredLogger
	router           *atomicRouter
	nonTargetStreak  map[types.PeerKey]int
	vivaldiErr       *metrics.EWMA
	nodeMetrics      *metrics.NodeMetrics
	routeInvalidate  chan struct{}
	natDetector      *nat.Detector
	casStore         *cas.Store
	topoMetrics      *metrics.TopologyMetrics
	tracesProviders  *traces.Provider
	controlSrv       *control.Server
	wasmRuntime      *wasm.Runtime
	punchSem         chan struct{}
	metricsProviders *metrics.Provider
	creds            *auth.NodeCredentials
	shutdownCh       chan struct{}
	inviteWaiters    map[types.PeerKey]chan *meshv1.InviteRedeemResponse
	socketPath       string
	pollenDir        string
	bootstrapPeers   []BootstrapTarget
	signPriv         ed25519.PrivateKey
	wg               sync.WaitGroup
	peerTickInterval time.Duration
	reconnectWindow  time.Duration
	membershipTTL    time.Duration
	inviteWaitersMu  sync.Mutex
	useHMACNearest   bool
}

func New(opts Options, creds *auth.NodeCredentials, inviteConsumer auth.InviteConsumer) (*Supervisor, error) {
	log := zap.S().Named("supervisor")
	privKey := opts.SigningKey
	pubKey := privKey.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	pollenDir := opts.PollenDir
	self := types.PeerKeyFromBytes(pubKey)

	stateStore := state.New(self)
	if opts.BootstrapPublic {
		stateStore.SetPublic()
	}
	if creds.DelegationKey() != nil {
		stateStore.SetAdmin()
	}
	if rs := opts.RuntimeState; rs != nil {
		if gs := rs.GetGossipState(); len(gs) > 0 {
			if err := stateStore.LoadGossipState(gs); err != nil {
				log.Warnw("failed to restore gossip state from disk", zap.Error(err))
			}
		}
		lastAddrs := make(map[types.PeerKey]string, len(rs.GetPeers()))
		for _, ps := range rs.GetPeers() {
			if addr := ps.GetLastAddr(); addr != "" {
				lastAddrs[types.PeerKeyFromBytes(ps.GetPeerPub())] = addr
			}
		}
		stateStore.LoadLastAddrs(lastAddrs)
	}
	for _, svc := range opts.InitialServices {
		stateStore.SetService(svc.Port, svc.Name, svc.Protocol)
	}
	if opts.NodeName != "" {
		stateStore.SetNodeName(opts.NodeName)
	}

	if err := initLocalAddresses(stateStore, opts); err != nil {
		return nil, err
	}

	mp, tp := metrics.NewNoopProvider(), traces.NewNoopProvider()
	if opts.MetricsEnabled {
		mp, tp = metrics.NewProvider(), traces.NewProvider()
	}

	router := newAtomicRouter()
	meshOpts := []transport.Option{
		transport.WithSigningKey(privKey),
		transport.WithTLSIdentityTTL(config.DefaultTLSIdentityTTL),
		transport.WithMembershipTTL(config.DefaultMembershipTTL),
		transport.WithReconnectWindow(config.DefaultReconnectWindow),
		transport.WithMaxConnectionAge(opts.MaxConnectionAge),
		transport.WithPeerTickInterval(opts.PeerTickInterval),
		transport.WithIsDenied(func(pk types.PeerKey) bool {
			return slices.Contains(stateStore.Snapshot().DeniedPeers(), pk)
		}),
		transport.WithMetrics(metrics.NewMeshMetrics(mp.Meter())),
		transport.WithTracer(tp.Tracer()),
		transport.WithRouter(router),
	}
	if opts.PacketConn != nil {
		meshOpts = append(meshOpts, transport.WithPacketConn(opts.PacketConn))
	}
	if opts.DisableNATPunch {
		meshOpts = append(meshOpts, transport.WithDisableNATPunch())
	}
	if inviteConsumer != nil {
		meshOpts = append(meshOpts, transport.WithInviteConsumer(inviteConsumer))
	}

	m, err := transport.New(self, creds, fmt.Sprintf(":%d", opts.ListenPort), meshOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load mesh transport: %w", err)
	}
	m.SetPeerMetrics(metrics.NewPeerMetrics(mp.Meter()))

	casStore, err := cas.New(pollenDir)
	if err != nil {
		return nil, fmt.Errorf("create CAS store: %w", err)
	}

	vivaldiErr := metrics.NewEWMA(membership.VivaldiErrAlpha, 1.0)
	nodeMetrics := metrics.NewNodeMetrics(mp.Meter())
	shutdownCh := make(chan struct{}, 1)

	n := &Supervisor{
		log:              log,
		store:            stateStore,
		mesh:             m,
		casStore:         casStore,
		creds:            creds,
		signPriv:         privKey,
		pollenDir:        pollenDir,
		socketPath:       opts.SocketPath,
		peerTickInterval: opts.PeerTickInterval,
		bootstrapPeers:   opts.BootstrapPeers,
		reconnectWindow:  config.DefaultReconnectWindow,
		punchSem:         make(chan struct{}, nodeWorkers),
		ready:            make(chan struct{}),
		shutdownCh:       shutdownCh,
		nonTargetStreak:  make(map[types.PeerKey]int),
		natDetector:      nat.NewDetector(),
		useHMACNearest:   true,
		metricsProviders: mp,
		tracesProviders:  tp,
		topoMetrics:      metrics.NewTopologyMetrics(mp.Meter()),
		nodeMetrics:      nodeMetrics,
		tracer:           tp.Tracer().Tracer("pollen/supervisor"),
		router:           router,
		vivaldiErr:       vivaldiErr,
		routeInvalidate:  make(chan struct{}, 1),
		inviteConsumer:   inviteConsumer,
		inviteWaiters:    make(map[types.PeerKey]chan *meshv1.InviteRedeemResponse),
		membershipTTL:    config.DefaultMembershipTTL,
	}

	if creds.DelegationKey() == nil {
		m.SetInviteForwarder(n.forwardInviteToAdmin)
	}

	capTrans := &capTransitioner{
		mesh:               m,
		store:              stateStore,
		fwd:                n.forwardInviteToAdmin,
		supervisorConsumer: &n.inviteConsumer,
	}

	streamAdapter := &streamOpenAdapter{t: m}
	datagramAdapter := &datagramOpenAdapter{t: m}

	n.tunneling = tunneling.New(
		self, stateStore, streamAdapter, datagramAdapter, router,
		tunneling.WithTrafficTracking(),
		tunneling.WithLogger(log.Named("tunneling")),
		tunneling.WithSampleInterval(opts.GossipInterval),
		tunneling.WithReconcileInterval(opts.PeerTickInterval),
	)

	gossipJitter := loopIntervalJitter
	if opts.DisableGossipJitter {
		gossipJitter = 0
	} else if opts.GossipJitter > 0 {
		gossipJitter = opts.GossipJitter
	}

	n.membership = membership.New(
		self, creds, m, stateStore,
		membership.Config{
			Streams:          m,
			RTT:              m,
			Certs:            m,
			PeerAddrs:        m,
			SessionCloser:    m,
			RoutedSender:     m,
			CapTransition:    capTrans,
			SmoothedErr:      vivaldiErr,
			TracerProvider:   tp.Tracer(),
			NATDetector:      n.natDetector,
			NodeMetrics:      nodeMetrics,
			SignPriv:         privKey,
			PollenDir:        pollenDir,
			Log:              log.Named("membership"),
			Port:             opts.ListenPort,
			TLSIdentityTTL:   config.DefaultTLSIdentityTTL,
			MembershipTTL:    config.DefaultMembershipTTL,
			ReconnectWindow:  config.DefaultReconnectWindow,
			GossipInterval:   opts.GossipInterval,
			GossipJitter:     gossipJitter,
			PeerTickInterval: opts.PeerTickInterval,
			AdvertisedIPs:    opts.AdvertisedIPs,
			ShutdownCh:       shutdownCh,
			DatagramHandler: func(ctx context.Context, from types.PeerKey, env *meshv1.Envelope) {
				switch body := env.GetBody().(type) {
				case *meshv1.Envelope_PunchCoordRequest:
					sc := traces.SpanContextFromTraceID(env.GetTraceId())
					spanCtx := trace.ContextWithRemoteSpanContext(ctx, sc)
					spanCtx, span := n.tracer.Start(spanCtx, "punch.coordRequest")
					span.SetAttributes(attribute.String("peer", from.Short()))
					n.handlePunchCoordRequest(spanCtx, from, body.PunchCoordRequest)
					span.End()
				case *meshv1.Envelope_PunchCoordTrigger:
					sc := traces.SpanContextFromTraceID(env.GetTraceId())
					spanCtx := trace.ContextWithRemoteSpanContext(ctx, sc)
					spanCtx, span := n.tracer.Start(spanCtx, "punch.coordTrigger")
					span.SetAttributes(attribute.String("peer", from.Short()))
					n.handlePunchCoordTrigger(spanCtx, body.PunchCoordTrigger)
					span.End()
				case *meshv1.Envelope_ForwardedInviteRequest:
					n.handleForwardedInviteRequest(ctx, from, body.ForwardedInviteRequest)
				case *meshv1.Envelope_ForwardedInviteResponse:
					n.handleForwardedInviteResponse(from, body.ForwardedInviteResponse)
				default:
					n.log.Debugw("unknown datagram type", "peer", from.Short())
				}
			},
		},
	)

	wasmRT, err := wasm.NewRuntime(wasm.NewHostFunctions(log.Named("wasm"), n), nodeWorkers)
	if err != nil {
		return nil, fmt.Errorf("wasm runtime: %w", err)
	}
	n.wasmRuntime = wasmRT

	var placementOpener placement.StreamOpener = streamAdapter
	if tr := n.tunneling.TrafficRecorder(); tr != nil {
		n.trafficRecorder = tr
		m.SetTrafficTracker(tr)
		placementOpener = &trafficCountedOpener{inner: streamAdapter, recorder: tr}
	}

	n.placement = placement.New(
		self, stateStore, casStore, wasmRT,
		placement.WithMesh(placementOpener),
		placement.WithLogger(log.Named("placement")),
		placement.WithResourceBudget(opts.CPUBudgetPercent, opts.MemBudgetPercent),
	)

	controlOpts := []control.Option{
		control.WithCredentials(creds),
		control.WithTransportInfo(n),
		control.WithMetricsSource(n),
		control.WithMeshConnector(n),
	}
	if opts.ShutdownFunc != nil {
		controlOpts = append(controlOpts, control.WithShutdown(opts.ShutdownFunc))
	}

	n.controlSrv = control.New(n.membership, n.placement, n.tunneling, stateStore, controlOpts...)

	for _, conn := range opts.InitialConnections {
		n.AddDesiredConnection(conn.PeerKey, conn.RemotePort, conn.LocalPort, conn.Protocol)
	}

	return n, nil
}

// spawn safely manages goroutine lifecycles against the Supervisor's waitgroup.
func (n *Supervisor) spawn(fn func()) {
	n.wg.Go(func() {
		fn()
	})
}

func parseAddrPorts(ips []string, port int) []netip.AddrPort {
	addrs := make([]netip.AddrPort, 0, len(ips))
	for _, ipStr := range ips {
		if addr, err := netip.ParseAddr(ipStr); err == nil {
			addrs = append(addrs, netip.AddrPortFrom(addr, uint16(port)))
		}
	}
	return addrs
}

func initLocalAddresses(store state.StateStore, opts Options) error {
	if len(opts.AdvertisedIPs) > 0 {
		store.SetLocalAddresses(parseAddrPorts(opts.AdvertisedIPs, opts.ListenPort))
		return nil
	}

	localIPs, err := transport.GetLocalInterfaceAddrs(transport.DefaultExclusions)
	publicIP := transport.GetPublicIP()
	if err != nil && publicIP == "" {
		return fmt.Errorf("failed to discover any addresses: %w", err)
	}
	if err == nil {
		store.SetLocalAddresses(parseAddrPorts(localIPs, opts.ListenPort))
	}
	if publicIP != "" {
		snap := store.Snapshot()
		if localNV, ok := snap.Nodes[snap.LocalID]; !ok || localNV.ObservedExternalIP != publicIP {
			store.SetLocalObservedAddress(publicIP, 0)
		}
	}
	return nil
}

func (n *Supervisor) Run(ctx context.Context) error {
	defer n.shutdown()

	if err := n.mesh.Start(ctx); err != nil {
		return fmt.Errorf("mesh start: %w", err)
	}
	if err := n.membership.Start(ctx); err != nil {
		return fmt.Errorf("membership start: %w", err)
	}
	if err := n.tunneling.Start(ctx); err != nil {
		return fmt.Errorf("tunneling start: %w", err)
	}
	if err := n.placement.Start(ctx); err != nil {
		return fmt.Errorf("placement start: %w", err)
	}

	n.spawn(func() {
		if err := n.controlSrv.Start(n.socketPath); err != nil {
			n.log.Warnw("control server failed", zap.Error(err))
		}
	})

	close(n.ready)
	n.connectBootstrapPeers(ctx)
	n.spawn(func() { n.streamDispatchLoop(ctx) })
	n.spawn(func() { n.tunnelDatagramLoop(ctx) })

	peerTicker := time.NewTicker(n.peerTickInterval)
	defer peerTicker.Stop()

	saveTicker := time.NewTicker(stateSaveInterval)
	defer saveTicker.Stop()

	n.syncPeersFromState(ctx, n.store.Snapshot())
	n.recomputeRoutes()

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
		case <-n.shutdownCh:
			return membership.ErrCertExpired
		case <-peerTicker.C:
			n.syncPeersFromState(ctx, n.store.Snapshot())
		case ev := <-n.membership.Events():
			n.dispatchEvents(ctx, []state.Event{ev})
		case ev := <-n.mesh.SupervisorEvents():
			n.handlePeerEvent(ctx, ev)
		case <-saveTicker.C:
			n.saveState()
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

func (n *Supervisor) connectBootstrapPeers(ctx context.Context) {
	for _, bp := range n.bootstrapPeers {
		if slices.Contains(n.store.Snapshot().DeniedPeers(), bp.PeerKey) {
			continue
		}
		addrs := make([]netip.AddrPort, 0, len(bp.Addrs))
		for _, a := range bp.Addrs {
			ap, err := netip.ParseAddrPort(a)
			if err != nil {
				n.log.Warnw("bootstrap peer: resolve address failed", "addr", a, zap.Error(err))
				continue
			}
			addrs = append(addrs, ap)
		}
		if len(addrs) > 0 {
			if err := n.mesh.Connect(ctx, bp.PeerKey, addrs); err != nil {
				n.log.Warnw("bootstrap peer: connect failed", "peer", bp.PeerKey.Short(), zap.Error(err))
			}
		}
	}
}

func (n *Supervisor) streamDispatchLoop(ctx context.Context) {
	for {
		stream, stype, peerKey, err := n.mesh.AcceptStream(ctx)
		if err != nil {
			return
		}
		switch stype {
		case transport.StreamTypeDigest:
			n.spawn(func() { n.membership.HandleDigestStream(ctx, stream, peerKey) })
		case transport.StreamTypeTunnel:
			n.tunneling.HandleTunnelStream(stream, peerKey)
		case transport.StreamTypeArtifact:
			s := wrapTrafficStream(stream, n.trafficRecorder, peerKey)
			n.spawn(func() { n.placement.HandleArtifactStream(s, peerKey) })
		case transport.StreamTypeWorkload:
			s := wrapTrafficStream(stream, n.trafficRecorder, peerKey)
			n.spawn(func() { n.placement.HandleWorkloadStream(s, peerKey) })
		default:
			n.log.Warnw("unknown stream type", "type", uint8(stype))
			stream.Close()
		}
	}
}

func (n *Supervisor) tunnelDatagramLoop(ctx context.Context) {
	for {
		p, err := n.mesh.RecvTunnelDatagram(ctx)
		if err != nil {
			return
		}
		n.tunneling.HandleTunnelDatagram(p.Data, p.From)
	}
}

func (n *Supervisor) dispatchEvents(_ context.Context, events []state.Event) {
	for _, ev := range events {
		switch e := ev.(type) {
		case state.PeerDenied:
			n.log.Infow("deny event received via gossip, disconnecting peer", "peer", e.Key.Short())
			n.tunneling.HandlePeerDenied(e.Key)
			n.mesh.ClosePeerSession(e.Key, transport.DisconnectDenied)
			n.mesh.ForgetPeer(e.Key)
		case state.TopologyChanged:
			n.signalRouteInvalidate()
			n.placement.Signal()
		case state.WorkloadChanged:
			n.placement.Signal()
		}
	}
}

func (n *Supervisor) handlePeerEvent(_ context.Context, ev transport.PeerEvent) {
	switch ev.Type {
	case transport.PeerEventConnected:
		n.signalRouteInvalidate()
		n.store.SetLocalReachable(n.GetConnectedPeers())
		n.store.SetPeerLastAddr(ev.Key, ev.Addrs[0].String())
		n.log.Infow("peer connected", "peer_id", ev.Key.Short())
	case transport.PeerEventDisconnected:
		n.signalRouteInvalidate()
		n.store.SetLocalReachable(n.GetConnectedPeers())
		n.placement.Signal()
	default:
		n.spawn(func() {
			n.punchSem <- struct{}{}
			defer func() { <-n.punchSem }()
			n.requestPunchCoordination(ev.Key)
		})
	}
}

func (n *Supervisor) signalRouteInvalidate() {
	select {
	case n.routeInvalidate <- struct{}{}:
	default:
	}
}

func (n *Supervisor) recomputeRoutes() {
	snap := n.store.Snapshot()
	topology := make([]routing.PeerTopology, 0, len(snap.Nodes))
	localCoord := n.membership.ControlMetrics().LocalCoord
	liveSet := make(map[types.PeerKey]struct{}, len(snap.PeerKeys))
	for _, pk := range snap.PeerKeys {
		liveSet[pk] = struct{}{}
	}

	for pk, nv := range snap.Nodes {
		if _, live := liveSet[pk]; !live && pk != snap.LocalID {
			continue
		}
		c := nv.VivaldiCoord
		if pk == snap.LocalID {
			c = &localCoord
		}
		topology = append(topology, routing.PeerTopology{
			Key:       pk,
			Reachable: nv.Reachable,
			Coord:     c,
		})
	}
	n.router.set(routing.Build(snap.LocalID, topology, n.mesh.ConnectedPeers()))
}

func (n *Supervisor) saveState() {
	lastAddrs := n.store.ExportLastAddrs()
	peers := make([]*statev1.PeerState, 0, len(lastAddrs))
	for pk, addr := range lastAddrs {
		peers = append(peers, &statev1.PeerState{
			PeerPub:  pk.Bytes(),
			LastAddr: addr,
		})
	}

	rs := &statev1.RuntimeState{
		GossipState: n.store.EncodeFull(),
		Peers:       peers,
	}
	if n.inviteConsumer != nil {
		rs.ConsumedInvites = n.inviteConsumer.Export()
	}

	blob, err := rs.MarshalVT()
	if err != nil {
		n.log.Warnw("failed to marshal state", zap.Error(err))
		return
	}
	if err := plnfs.WriteGroupReadable(filepath.Join(n.pollenDir, "state.pb"), blob); err != nil {
		n.log.Warnw("failed to save state", zap.Error(err))
	}
}

func (n *Supervisor) shutdown() {
	n.controlSrv.Stop()

	// Close QUIC sessions before waiting for goroutines — stream handlers
	// (HandleDigestStream, HandleArtifactStream, HandleWorkloadStream) block
	// on stream reads that only unblock when sessions close.
	if err := n.mesh.Stop(); err != nil {
		n.log.Errorw("failed to shut down mesh", zap.Error(err))
	}

	n.wg.Wait()

	if err := n.membership.Stop(); err != nil {
		n.log.Warnw("membership stop failed", zap.Error(err))
	}

	n.saveState()

	if err := n.placement.Stop(); err != nil {
		n.log.Warnw("placement stop failed", zap.Error(err))
	}
	n.wasmRuntime.Close(context.Background())

	if err := n.tunneling.Stop(); err != nil {
		n.log.Warnw("tunneling stop failed", zap.Error(err))
	}

	shutdownCtx := context.Background()
	if err := n.metricsProviders.Shutdown(shutdownCtx); err != nil {
		n.log.Warnw("metrics shutdown failed", zap.Error(err))
	}
	if err := n.tracesProviders.Shutdown(shutdownCtx); err != nil {
		n.log.Warnw("traces shutdown failed", zap.Error(err))
	}
}

// --- Interface implementations directly mapped for indirection removal ---

func (n *Supervisor) PeerStateCounts() transport.PeerStateCounts { return n.mesh.PeerStateCounts() }

func (n *Supervisor) GetActivePeerAddress(pk types.PeerKey) (*net.UDPAddr, bool) {
	return n.mesh.GetActivePeerAddress(pk)
}
func (n *Supervisor) ReconnectWindowDuration() time.Duration { return n.reconnectWindow }
func (n *Supervisor) PeerRTT(pk types.PeerKey) (time.Duration, bool) {
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

func (n *Supervisor) ControlMetrics() control.Metrics {
	snap, err := n.metricsProviders.CollectSnapshot(context.Background())
	if err != nil {
		n.log.Warnw("metric snapshot collection failed", zap.Error(err))
	}
	cm := n.membership.ControlMetrics()
	return control.Metrics{
		CertExpirySeconds:  snap.CertExpirySeconds,
		CertRenewals:       uint64(snap.CertRenewals),
		CertRenewalsFailed: uint64(snap.CertRenewalsFailed),
		PunchAttempts:      uint64(snap.PunchAttempts),
		PunchFailures:      uint64(snap.PunchFailures),
		SmoothedVivaldiErr: cm.SmoothedErr,
		VivaldiSamples:     uint64(cm.VivaldiSamples),
		EagerSyncs:         uint64(cm.EagerSyncs),
		EagerSyncFailures:  uint64(cm.EagerSyncFailures),
	}
}

// --- Public API ---

func (n *Supervisor) RouteCall(ctx context.Context, targetHash, function string, input []byte) ([]byte, error) {
	// Preserve existing caller identity for inter-workload call chains.
	// Only stamp the local node's identity on the first hop.
	if _, ok := wasm.CallerInfoFromContext(ctx); !ok {
		if cert := n.creds.Cert(); cert != nil {
			info := wasm.CallerInfo{
				PeerKey: types.PeerKeyFromBytes(cert.GetClaims().GetSubjectPub()),
			}
			if attrs := cert.GetClaims().GetCapabilities().GetAttributes(); attrs != nil {
				info.Attributes = attrs.AsMap()
			}
			ctx = wasm.WithCallerInfo(ctx, info)
		}
	}
	return n.placement.Call(ctx, targetHash, function, input)
}

func (n *Supervisor) Connect(ctx context.Context, pk types.PeerKey, addrs []netip.AddrPort) error {
	return n.mesh.Connect(ctx, pk, addrs)
}
func (n *Supervisor) ControlService() *control.Service     { return n.controlSrv.Service() }
func (n *Supervisor) Membership() membership.MembershipAPI { return n.membership }
func (n *Supervisor) Tunneling() tunneling.TunnelingAPI    { return n.tunneling }
func (n *Supervisor) StateStore() state.StateStore         { return n.store }
func (n *Supervisor) Ready() <-chan struct{}               { return n.ready }
func (n *Supervisor) ListenPort() int                      { return n.mesh.ListenPort() }
func (n *Supervisor) GetConnectedPeers() []types.PeerKey   { return n.mesh.ConnectedPeers() }
func (n *Supervisor) SeedWorkload(wasmBytes []byte, name string, replicas, memoryPages, timeoutMs uint32, spread float32) (string, error) {
	h := sha256.Sum256(wasmBytes)
	hash := hex.EncodeToString(h[:])
	if name == "" {
		name = hash
	}
	if err := n.placement.Seed(name, hash, wasmBytes, replicas, memoryPages, timeoutMs, spread); err != nil {
		return "", err
	}
	return hash, nil
}
func (n *Supervisor) UnseedWorkload(hash string) error   { return n.placement.Unseed(hash) }
func (n *Supervisor) Credentials() *auth.NodeCredentials { return n.creds }
func (n *Supervisor) JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error) {
	return n.mesh.JoinWithInvite(ctx, token)
}

func (n *Supervisor) AddDesiredConnection(pk types.PeerKey, remotePort, localPort uint32, protocol statev1.ServiceProtocol) {
	if _, err := n.tunneling.Connect(context.Background(), pk, remotePort, localPort, protocol); err != nil {
		n.log.Warnw("failed to add desired connection", "peer", pk.Short(), "remotePort", remotePort, zap.Error(err))
	}
}

func (n *Supervisor) DesiredConnections() []tunneling.ConnectionInfo {
	return n.tunneling.ListDesiredConnections()
}

// --- Internal Stream Wrappers ---

type streamOpenAdapter struct {
	t transport.Transport
}

func (a *streamOpenAdapter) OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error) {
	return a.t.OpenStream(ctx, peer, st)
}

type datagramOpenAdapter struct {
	t transport.Transport
}

func (a *datagramOpenAdapter) SendTunnelDatagram(ctx context.Context, peer types.PeerKey, data []byte) error {
	return a.t.SendTunnelDatagram(ctx, peer, data)
}

type trafficCountedStream struct {
	io.ReadWriteCloser
	recorder transport.TrafficRecorder
	peer     types.PeerKey
}

func (s *trafficCountedStream) Read(p []byte) (int, error) {
	n, err := s.ReadWriteCloser.Read(p)
	if n > 0 {
		s.recorder.Record(s.peer, uint64(n), 0)
	}
	return n, err
}

func (s *trafficCountedStream) Write(p []byte) (int, error) {
	n, err := s.ReadWriteCloser.Write(p)
	if n > 0 {
		s.recorder.Record(s.peer, 0, uint64(n))
	}
	return n, err
}

func wrapTrafficStream(stream io.ReadWriteCloser, recorder transport.TrafficRecorder, peer types.PeerKey) io.ReadWriteCloser {
	if recorder == nil {
		return stream
	}
	return &trafficCountedStream{ReadWriteCloser: stream, recorder: recorder, peer: peer}
}

type trafficCountedOpener struct {
	inner    placement.StreamOpener
	recorder transport.TrafficRecorder
}

func (a *trafficCountedOpener) OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error) {
	stream, err := a.inner.OpenStream(ctx, peer, st)
	if err != nil {
		return nil, err
	}
	return wrapTrafficStream(stream, a.recorder, peer), nil
}

// Compile-time interface compliance checks.
var (
	_ membership.ClusterState   = state.StateStore(nil)
	_ membership.Network        = (*transport.QUICTransport)(nil)
	_ placement.WorkloadState   = state.StateStore(nil)
	_ tunneling.ServiceState    = state.StateStore(nil)
	_ control.MembershipControl = (*membership.Service)(nil)
	_ control.PlacementControl  = (*placement.Service)(nil)
	_ control.TunnelingControl  = (*tunneling.Service)(nil)
	_ control.StateReader       = state.StateStore(nil)
	_ placement.WASMRuntime     = (*wasm.Runtime)(nil)
	_ control.TransportInfo     = (*Supervisor)(nil)
	_ control.MetricsSource     = (*Supervisor)(nil)
	_ control.MeshConnector     = (*Supervisor)(nil)
)
