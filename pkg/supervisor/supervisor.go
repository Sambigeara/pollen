package supervisor

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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
	stateSaveInterval = 30 * time.Second

	routeDebounceInterval = 100 * time.Millisecond
	maxRouteDelay         = time.Second

	loopIntervalJitter = 0.1
	nodeWorkers        = 8
)

type Supervisor struct {
	trafficRecorder  transport.TrafficRecorder
	membership       membership.MembershipAPI
	placement        placement.PlacementAPI
	tunneling        tunneling.TunnelingAPI
	mesh             Transport
	meshInternal     TransportInternal
	tracer           trace.Tracer
	store            state.StateStore
	inviteConsumer   auth.InviteConsumer
	tracesProviders  *traces.Providers
	punchSem         chan struct{}
	router           *atomicRouter
	nonTargetStreak  map[types.PeerKey]int
	vivaldiErr       *metrics.EWMA
	nodeMetrics      *metrics.NodeMetrics
	routeInvalidate  chan struct{}
	natDetector      *nat.Detector
	casStore         *cas.Store
	topoMetrics      *metrics.TopologyMetrics
	ready            chan struct{}
	controlSrv       *control.Server
	wasmRuntime      *wasm.Runtime
	log              *zap.SugaredLogger
	metricsProviders *metrics.Providers
	creds            *auth.NodeCredentials
	shutdownCh       chan struct{}
	pollenDir        string
	socketPath       string
	bootstrapPeers   []BootstrapTarget
	signPriv         ed25519.PrivateKey
	wg               sync.WaitGroup
	peerTickInterval time.Duration
	reconnectWindow  time.Duration
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
		stateStore.SetBootstrapPublic()
	}
	if rs := opts.RuntimeState; rs != nil {
		if gs := rs.GetGossipState(); len(gs) > 0 {
			if _, _, err := stateStore.ApplyDelta(self, gs); err != nil {
				log.Warnw("failed to restore gossip state from disk", "err", err)
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
		stateStore.SetService(svc.Port, svc.Name)
	}
	ips := opts.AdvertisedIPs
	if len(ips) == 0 {
		var err error
		ips, err = transport.GetAdvertisableAddrs(transport.DefaultExclusions)
		if err != nil {
			return nil, err
		}
	}
	addrs := make([]netip.AddrPort, 0, len(ips))
	for _, ipStr := range ips {
		addr, err := netip.ParseAddr(ipStr)
		if err != nil {
			continue
		}
		addrs = append(addrs, netip.AddrPortFrom(addr, uint16(opts.ListenPort)))
	}
	stateStore.SetLocalAddresses(addrs)

	var mp *metrics.Providers
	var tp *traces.Providers
	if opts.MetricsEnabled {
		mp = metrics.NewProviders()
		tp = traces.NewProviders()
	} else {
		mp = metrics.NewNoopProviders()
		tp = traces.NewNoopProviders()
	}
	meshMetrics := metrics.NewMeshMetrics(mp.Meter())
	peerMetrics := metrics.NewPeerMetrics(mp.Meter())
	isDenied := func(pk types.PeerKey) bool {
		return slices.Contains(stateStore.Snapshot().DeniedPeers(), pk)
	}
	router := newAtomicRouter()
	meshOpts := []transport.Option{
		transport.WithSigningKey(privKey),
		transport.WithTLSIdentityTTL(config.DefaultTLSIdentityTTL),
		transport.WithMembershipTTL(config.DefaultMembershipTTL),
		transport.WithReconnectWindow(config.DefaultReconnectWindow),
		transport.WithMaxConnectionAge(opts.MaxConnectionAge),
		transport.WithPeerTickInterval(opts.PeerTickInterval),
		transport.WithIsDenied(isDenied),
		transport.WithMetrics(meshMetrics),
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
	m, err := transport.New(self, *creds, fmt.Sprintf(":%d", opts.ListenPort), meshOpts...)
	if err != nil {
		log.Errorw("failed to load mesh", "err", err)
		return nil, err
	}
	m.SetPeerMetrics(peerMetrics)

	casStore, err := cas.New(pollenDir)
	if err != nil {
		return nil, fmt.Errorf("create CAS store: %w", err)
	}
	natDetector := nat.NewDetector()
	nodeMetrics := metrics.NewNodeMetrics(mp.Meter())
	streamAdapter := &streamOpenAdapter{m}

	shutdownCh := make(chan struct{}, 1)
	vivaldiErr := metrics.NewEWMA(membership.VivaldiErrAlpha, 1.0)

	n := &Supervisor{
		log:              log,
		store:            stateStore,
		mesh:             m,
		meshInternal:     m,
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
		natDetector:      natDetector,
		useHMACNearest:   true,
		metricsProviders: mp,
		tracesProviders:  tp,
		topoMetrics:      metrics.NewTopologyMetrics(mp.Meter()),
		nodeMetrics:      nodeMetrics,
		tracer:           tp.Tracer().Tracer("pollen/supervisor"),
		router:           router,
		vivaldiErr:       vivaldiErr,
		routeInvalidate:  make(chan struct{}, 1),
	}

	n.inviteConsumer = inviteConsumer

	n.tunneling = tunneling.New(
		self,
		stateStore,
		streamAdapter,
		router,
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
		membership.WithStreamOpener(m),
		membership.WithRTTSource(m),
		membership.WithCertManager(m),
		membership.WithPeerAddressSource(m),
		membership.WithPeerSessionCloser(m),
		membership.WithSmoothedErr(vivaldiErr),
		membership.WithTracer(tp.Tracer()),
		membership.WithNATDetector(natDetector),
		membership.WithNodeMetrics(nodeMetrics),
		membership.WithSigningKey(privKey),
		membership.WithPollenDir(pollenDir),
		membership.WithLogger(log.Named("membership")),
		membership.WithPort(opts.ListenPort),
		membership.WithTLSIdentityTTL(config.DefaultTLSIdentityTTL),
		membership.WithMembershipTTL(config.DefaultMembershipTTL),
		membership.WithReconnectWindow(config.DefaultReconnectWindow),
		membership.WithGossipInterval(opts.GossipInterval),
		membership.WithGossipJitter(gossipJitter),
		membership.WithPeerTickInterval(opts.PeerTickInterval),
		membership.WithAdvertisedIPs(opts.AdvertisedIPs),
		membership.WithShutdownSignal(shutdownCh),
		membership.WithDatagramHandler(func(ctx context.Context, from types.PeerKey, env *meshv1.Envelope) {
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
			default:
				n.log.Debugw("unknown datagram type", "peer", from.Short())
			}
		}),
	)

	hostFuncs := wasm.NewHostFunctions(log.Named("wasm"), n)
	wasmRT, err := wasm.NewRuntime(hostFuncs, nodeWorkers)
	if err != nil {
		return nil, fmt.Errorf("wasm runtime: %w", err)
	}
	n.wasmRuntime = wasmRT

	var placementMesh placement.StreamOpener = streamAdapter
	if tr := n.tunneling.TrafficRecorder(); tr != nil {
		n.trafficRecorder = tr
		m.SetTrafficTracker(tr)
		placementMesh = &trafficCountedOpener{inner: streamAdapter, recorder: tr}
	}
	n.placement = placement.New(
		self, stateStore, casStore, wasmRT,
		placement.WithMesh(placementMesh),
		placement.WithLogger(log.Named("placement")),
	)

	controlOpts := []control.Option{
		control.WithCredentials(creds),
		control.WithTransportInfo(&supervisorTransportInfo{n}),
		control.WithMetricsSource(&supervisorMetricsSource{providers: mp, membership: n.membership}),
		control.WithMeshConnector(n),
	}
	if opts.ShutdownFunc != nil {
		controlOpts = append(controlOpts, control.WithShutdown(opts.ShutdownFunc))
	}
	n.controlSrv = control.New(
		n.membership, n.placement, n.tunneling, stateStore,
		controlOpts...,
	)

	for _, conn := range opts.InitialConnections {
		n.tunneling.SeedDesiredConnection(conn.PeerKey, conn.RemotePort, conn.LocalPort)
	}

	return n, nil
}

func (n *Supervisor) Run(ctx context.Context) error {
	defer n.shutdown()

	if err := n.mesh.Start(ctx); err != nil {
		return err
	}
	if err := n.membership.Start(ctx); err != nil {
		return err
	}
	if err := n.tunneling.Start(ctx); err != nil {
		return err
	}
	if err := n.placement.Start(ctx); err != nil {
		return err
	}

	n.wg.Go(func() {
		if err := n.controlSrv.Start(n.socketPath); err != nil {
			n.log.Warnw("control server failed", "err", err)
		}
	})

	close(n.ready)
	n.connectBootstrapPeers(ctx)
	n.wg.Go(func() { n.streamDispatchLoop(ctx) })

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
		case ev := <-n.meshInternal.SupervisorEvents():
			n.handlePeerEvent(ctx, ev)
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
		case <-saveTicker.C:
			n.saveState()
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
				n.log.Warnw("bootstrap peer: resolve address failed", "addr", a, "err", err)
				continue
			}
			addrs = append(addrs, ap)
		}
		if len(addrs) == 0 {
			continue
		}
		if err := n.mesh.Connect(ctx, bp.PeerKey, addrs); err != nil {
			n.log.Warnw("bootstrap peer: connect failed", "peer", bp.PeerKey.Short(), "err", err)
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
			n.wg.Go(func() { n.membership.HandleDigestStream(ctx, stream, peerKey) })
		case transport.StreamTypeTunnel:
			n.tunneling.HandleTunnelStream(stream, peerKey)
		case transport.StreamTypeArtifact:
			var s io.ReadWriteCloser = stream
			if n.trafficRecorder != nil {
				s = wrapTrafficStream(stream, n.trafficRecorder, peerKey)
			}
			n.wg.Go(func() { n.placement.HandleArtifactStream(s, peerKey) })
		case transport.StreamTypeWorkload:
			var s io.ReadWriteCloser = stream
			if n.trafficRecorder != nil {
				s = wrapTrafficStream(stream, n.trafficRecorder, peerKey)
			}
			n.wg.Go(func() { n.placement.HandleWorkloadStream(s, peerKey) })
		case transport.StreamTypeCertRenewal:
			n.wg.Go(func() { n.membership.HandleCertRenewalStream(stream, peerKey) })
		default:
			n.log.Warnw("unknown stream type", "type", uint8(stype))
			stream.Close()
		}
	}
}

func (n *Supervisor) dispatchEvents(_ context.Context, events []state.Event) {
	for _, ev := range events {
		switch e := ev.(type) {
		case state.PeerDenied:
			n.log.Infow("deny event received via gossip, disconnecting peer", "peer", e.Key.Short())
			n.tunneling.HandlePeerDenied(e.Key)
			n.meshInternal.ClosePeerSession(e.Key, transport.CloseReasonDenied)
			n.meshInternal.ForgetPeer(e.Key)
		case state.PeerLeft, state.TopologyChanged:
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
		n.submitPunch(func() { n.requestPunchCoordination(ev.Key) })
	}
}

func (n *Supervisor) submitPunch(fn func()) {
	n.wg.Go(func() {
		n.punchSem <- struct{}{}
		defer func() { <-n.punchSem }()
		fn()
	})
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
	for pk, nv := range snap.Nodes {
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
		n.log.Warnw("failed to marshal state", "err", err)
		return
	}
	if err := plnfs.WriteGroupReadable(filepath.Join(n.pollenDir, "state.pb"), blob); err != nil {
		n.log.Warnw("failed to save state", "err", err)
	}
}

func (n *Supervisor) shutdown() {
	n.controlSrv.Stop()
	n.wg.Wait()
	if err := n.membership.Stop(); err != nil {
		n.log.Warnw("membership stop failed", "err", err)
	}

	n.saveState()

	if err := n.placement.Stop(); err != nil {
		n.log.Warnw("placement stop failed", "err", err)
	}
	n.wasmRuntime.Close(context.Background())

	if err := n.tunneling.Stop(); err != nil {
		n.log.Warnw("tunneling stop failed", "err", err)
	}

	if err := n.mesh.Stop(); err != nil {
		n.log.Errorw("failed to shut down mesh", "err", err)
	}

	shutdownCtx := context.Background()
	if err := n.metricsProviders.Shutdown(shutdownCtx); err != nil {
		n.log.Warnw("metrics shutdown failed", "err", err)
	}
	if err := n.tracesProviders.Shutdown(shutdownCtx); err != nil {
		n.log.Warnw("traces shutdown failed", "err", err)
	}
}

func (n *Supervisor) RouteCall(ctx context.Context, targetHash, function string, input []byte) ([]byte, error) {
	return n.placement.Call(ctx, targetHash, function, input)
}

func (n *Supervisor) Connect(ctx context.Context, pk types.PeerKey, addrs []netip.AddrPort) error {
	return n.mesh.Connect(ctx, pk, addrs)
}

func (n *Supervisor) ControlService() *control.Service {
	return n.controlSrv.Service()
}

func (n *Supervisor) Membership() membership.MembershipAPI {
	return n.membership
}

func (n *Supervisor) Tunneling() tunneling.TunnelingAPI {
	return n.tunneling
}

func (n *Supervisor) StateStore() state.StateStore {
	return n.store
}

func (n *Supervisor) Ready() <-chan struct{} {
	return n.ready
}

func (n *Supervisor) ListenPort() int {
	return n.meshInternal.ListenPort()
}

func (n *Supervisor) GetConnectedPeers() []types.PeerKey {
	return n.mesh.ConnectedPeers()
}

func (n *Supervisor) SeedWorkload(wasmBytes []byte, replicas, memoryPages, timeoutMs uint32) (string, error) {
	h := sha256.Sum256(wasmBytes)
	hash := hex.EncodeToString(h[:])
	if err := n.placement.Seed(hash, wasmBytes, replicas, memoryPages, timeoutMs); err != nil {
		return "", err
	}
	return hash, nil
}

func (n *Supervisor) UnseedWorkload(hash string) error {
	return n.placement.Unseed(hash)
}

func (n *Supervisor) Credentials() *auth.NodeCredentials {
	return n.creds
}

func (n *Supervisor) JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error) {
	return n.meshInternal.JoinWithInvite(ctx, token)
}

func (n *Supervisor) AddDesiredConnection(pk types.PeerKey, remotePort, localPort uint32) {
	n.tunneling.SeedDesiredConnection(pk, remotePort, localPort)
}

func (n *Supervisor) DesiredConnections() []tunneling.ConnectionInfo {
	return n.tunneling.ListDesiredConnections()
}
