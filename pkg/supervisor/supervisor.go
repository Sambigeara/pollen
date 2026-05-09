// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

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

	"github.com/prometheus/client_golang/prometheus"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/blobs"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/control"
	"github.com/sambigeara/pollen/pkg/gate"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/observability/traces"
	"github.com/sambigeara/pollen/pkg/peercache"
	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/plnfs"
	"github.com/sambigeara/pollen/pkg/routing"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/static"
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
	maxConcurrentPunches  = 8
	blobPruneInterval     = 5 * time.Minute
	// blobPruneGrace must comfortably exceed the worst-case duration of a
	// directory seed so a janitor tick mid-seed can't race the spec
	// publish that claims the just-uploaded file blobs.
	blobPruneGrace = 5 * time.Minute
)

type Supervisor struct {
	inviteConsumer   auth.InviteConsumer
	membership       membership.MembershipAPI
	placement        placement.PlacementAPI
	tunneling        tunneling.TunnelingAPI
	static           static.StaticAPI
	blobs            blobs.BlobsAPI
	trafficRecorder  transport.TrafficRecorder
	mesh             transport.Transport
	tracer           trace.Tracer
	store            state.StateStore
	natDetector      *nat.Detector
	wasmRuntime      *wasm.Runtime
	log              *zap.SugaredLogger
	router           *atomicRouter
	gate             *gate.Gate
	peerCache        *peercache.Store
	nonTargetStreak  map[types.PeerKey]int
	vivaldiErr       *metrics.EWMA
	nodeMetrics      *metrics.NodeMetrics
	routeInvalidate  chan struct{}
	promRegistry     *prometheus.Registry
	staticSvc        *static.Service
	topoMetrics      *metrics.TopologyMetrics
	tracesProviders  *traces.Provider
	controlSrv       *control.Server
	ready            chan struct{}
	punchSem         chan struct{}
	metricsProviders *metrics.Provider
	creds            *auth.NodeCredentials
	shutdownCh       chan struct{}
	staticAddr       string
	httpAddr         string
	socketPath       string
	controlAddr      string
	pollenDir        string
	signPriv         ed25519.PrivateKey
	wg               sync.WaitGroup
	peerTickInterval time.Duration
	reconnectWindow  time.Duration
	membershipTTL    time.Duration
	localID          types.PeerKey
	useHMACNearest   bool
}

func New(opts Options, creds *auth.NodeCredentials, inviteConsumer auth.InviteConsumer) (*Supervisor, error) {
	log := zap.S().Named("supervisor")
	privKey := opts.SigningKey
	pubKey := privKey.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	pollenDir := opts.PollenDir
	self := types.PeerKeyFromBytes(pubKey)

	peerCache := opts.PeerCache
	if peerCache == nil {
		pc, err := peercache.Open(pollenDir)
		if err != nil {
			return nil, fmt.Errorf("open peer cache: %w", err)
		}
		peerCache = pc
	}

	stateStore := state.New(self, creds.RootPub())
	runtimeGate := gate.New(creds.RootPub(), stateStore)
	stateStore.SetMutationValidator(runtimeGate.Admit)
	if signer := creds.DelegationKey(); signer != nil {
		stateStore.SetLocalSigner(signer)
	}
	if opts.BootstrapPublic {
		stateStore.SetPublic()
	}
	if creds.DelegationKey() != nil {
		stateStore.SetAdmin()
	}
	if opts.StaticAddr != "" {
		stateStore.SetStaticCapable()
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
		if _, err := stateStore.SetService(svc.Port, svc.Name, svc.Protocol, svc.Properties, nil); err != nil {
			return nil, err
		}
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

	var promRegistry *prometheus.Registry
	if opts.HTTPAddr != "" {
		promRegistry = prometheus.NewRegistry()
		exporter, promErr := promexporter.New(promexporter.WithRegisterer(promRegistry))
		if promErr != nil {
			return nil, fmt.Errorf("prometheus exporter: %w", promErr)
		}
		mp = metrics.NewProvider(exporter)
		tp = traces.NewProvider()
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

	streamAdapter := &streamOpenAdapter{t: m}

	blobsSvc, err := blobs.New(pollenDir, self, streamAdapter, stateStore, runtimeGate)
	if err != nil {
		return nil, fmt.Errorf("create blob store: %w", err)
	}
	if err := blobsSvc.Rescan(); err != nil {
		log.Warnw("scan local blobs", zap.Error(err))
	}

	vivaldiErr := metrics.NewEWMA(membership.VivaldiErrAlpha, 1.0)
	nodeMetrics := metrics.NewNodeMetrics(mp.Meter())
	shutdownCh := make(chan struct{}, 1)

	n := &Supervisor{
		log:              log,
		localID:          self,
		store:            stateStore,
		mesh:             m,
		blobs:            blobsSvc,
		creds:            creds,
		signPriv:         privKey,
		pollenDir:        pollenDir,
		socketPath:       opts.SocketPath,
		controlAddr:      opts.ControlAddr,
		peerTickInterval: opts.PeerTickInterval,
		peerCache:        peerCache,
		reconnectWindow:  config.DefaultReconnectWindow,
		punchSem:         make(chan struct{}, maxConcurrentPunches),
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
		membershipTTL:    config.DefaultMembershipTTL,
		promRegistry:     promRegistry,
		httpAddr:         opts.HTTPAddr,
	}

	m.SetInviteForwarder(n.forwardInviteToAdmin)

	capTrans := &capTransitioner{
		mesh:               m,
		store:              stateStore,
		fwd:                n.forwardInviteToAdmin,
		supervisorConsumer: &n.inviteConsumer,
	}

	n.tunneling = tunneling.New(
		self, stateStore, streamAdapter, m, router,
		tunneling.WithTrafficTracking(),
		tunneling.WithLogger(log.Named("tunneling")),
		tunneling.WithSampleInterval(opts.GossipInterval),
		tunneling.WithReconcileInterval(opts.PeerTickInterval),
	)

	gossipJitter := loopIntervalJitter
	if opts.GossipJitter > 0 {
		gossipJitter = opts.GossipJitter
	} else if opts.GossipJitter < 0 {
		gossipJitter = 0
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
				default:
					n.log.Debugw("unknown datagram type", "peer", from.Short())
				}
			},
		},
	)

	if tr := n.tunneling.TrafficRecorder(); tr != nil {
		n.trafficRecorder = tr
		m.SetTrafficTracker(tr)
	}

	if opts.RelayOnly {
		n.placement = placement.NewNoopService()
	} else {
		var wasmOpts []wasm.RuntimeOption
		if opts.IdleInstanceTTL > 0 {
			wasmOpts = append(wasmOpts, wasm.WithIdleTTL(opts.IdleInstanceTTL))
		}
		wasmRT, err := wasm.NewRuntime(wasm.NewHostFunctions(log.Named("wasm"), n), wasmOpts...)
		if err != nil {
			return nil, fmt.Errorf("wasm runtime: %w", err)
		}
		n.wasmRuntime = wasmRT

		var placementOpener placement.StreamOpener = streamAdapter
		if n.trafficRecorder != nil {
			placementOpener = &trafficCountedOpener{inner: streamAdapter, recorder: n.trafficRecorder}
		}

		n.placement = placement.New(
			self, stateStore, blobsSvc, wasmRT,
			placement.WithMesh(placementOpener),
			placement.WithLogger(log.Named("placement")),
			placement.WithGate(runtimeGate),
		)
	}

	n.gate = runtimeGate

	staticSvc := static.New(self, stateStore, blobsSvc, opts.StaticAddr != "", log.Named("static"))
	n.static = staticSvc
	n.staticSvc = staticSvc
	n.staticAddr = opts.StaticAddr

	controlOpts := []control.Option{
		control.WithCredentials(creds),
		control.WithTransportInfo(n),
		control.WithMetricsSource(n),
		control.WithMeshConnector(n),
		control.WithOperatorGate(runtimeGate),
	}
	if opts.ShutdownFunc != nil {
		controlOpts = append(controlOpts, control.WithShutdown(opts.ShutdownFunc))
	}

	n.controlSrv = control.New(n.membership, n.placement, n.tunneling, n.blobs, n.static, stateStore, controlOpts...)
	if opts.ControlToken != "" {
		n.controlSrv.SetToken(opts.ControlToken)
	}

	for _, conn := range opts.InitialConnections {
		n.AddDesiredConnection(conn.PeerKey, conn.RemotePort, conn.LocalPort, conn.Protocol)
	}

	return n, nil
}

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
	if err := n.static.Start(ctx); err != nil {
		return fmt.Errorf("static start: %w", err)
	}

	n.spawn(func() {
		if err := n.controlSrv.Start(n.socketPath); err != nil {
			n.log.Warnw("control server failed", zap.Error(err))
		}
	})

	if n.controlAddr != "" {
		n.spawn(func() {
			if err := n.controlSrv.StartTCP(n.controlAddr); err != nil {
				n.log.Warnw("control tcp server failed", zap.Error(err))
			}
		})
	}

	if n.promRegistry != nil {
		n.spawn(func() {
			if err := n.startPrometheus(ctx, n.httpAddr); err != nil {
				n.log.Warnw("prometheus server failed", zap.Error(err))
			}
		})
	}

	if n.staticAddr != "" {
		n.spawn(func() {
			if err := n.startStaticHTTP(ctx, n.staticAddr); err != nil {
				n.log.Warnw("static http server failed", zap.Error(err))
			}
		})
	}

	close(n.ready)
	n.spawn(func() { n.streamDispatchLoop(ctx) })
	n.spawn(func() { n.tunnelDatagramLoop(ctx) })

	peerTicker := time.NewTicker(n.peerTickInterval)
	defer peerTicker.Stop()

	saveTicker := time.NewTicker(stateSaveInterval)
	defer saveTicker.Stop()

	pruneTicker := time.NewTicker(blobPruneInterval)
	defer pruneTicker.Stop()

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
		case ev := <-n.static.Events():
			n.dispatchEvents(ctx, []state.Event{ev})
		case ev := <-n.mesh.SupervisorEvents():
			n.handlePeerEvent(ctx, ev)
		case <-saveTicker.C:
			n.saveState()
		case <-pruneTicker.C:
			n.pruneOrphanBlobs()
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

func (n *Supervisor) dispatchBlobFetch(stream io.ReadWriteCloser, peerKey types.PeerKey) {
	hash, err := blobs.ReadHash(stream)
	if err != nil {
		stream.Close() //nolint:errcheck
		return
	}
	if err := n.gate.Fetch(peerKey, hash); err != nil {
		stream.Close() //nolint:errcheck
		return
	}
	n.blobs.Serve(stream, hash)
}

// Denied connects close the stream without a distinguishing reply so
// service existence is not leaked to the caller.
func (n *Supervisor) dispatchServiceConnect(stream io.ReadWriteCloser, peerKey types.PeerKey) {
	port, err := tunneling.ReadPort(stream)
	if err != nil {
		stream.Close() //nolint:errcheck
		return
	}
	if err := n.gate.Connect(peerKey, n.localID, port); err != nil {
		stream.Close() //nolint:errcheck
		return
	}
	n.tunneling.Serve(stream, peerKey, port)
}

func (n *Supervisor) dispatchWorkloadCall(stream io.ReadWriteCloser, peerKey types.PeerKey) {
	n.placement.Serve(stream, peerKey)
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
		case transport.StreamTypeMembership:
			n.spawn(func() { n.handleMembershipStream(ctx, stream, peerKey) })
		case transport.StreamTypeTunnel:
			n.spawn(func() { n.dispatchServiceConnect(stream, peerKey) })
		case transport.StreamTypeBlob:
			s := transport.WrapTrafficStream(stream, n.trafficRecorder, peerKey)
			n.spawn(func() { n.dispatchBlobFetch(s, peerKey) })
		case transport.StreamTypeWorkload:
			s := transport.WrapTrafficStream(stream, n.trafficRecorder, peerKey)
			n.spawn(func() { n.dispatchWorkloadCall(s, peerKey) })
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

func (n *Supervisor) dispatchEvents(ctx context.Context, events []state.Event) {
	for _, ev := range events {
		switch e := ev.(type) {
		case state.PeerDenied:
			n.log.Infow("deny event received via gossip, disconnecting peer", "peer", e.Key.Short())
			n.tunneling.HandlePeerDenied(e.Key)
			n.mesh.ClosePeerSession(e.Key, transport.DisconnectDenied)
			n.mesh.ForgetPeer(e.Key)
			n.peerCache.Forget(e.Key)
			// Refresh routes and placement explicitly so non-connected
			// cascade victims (denied peers we never had a session with,
			// e.g. routed-only or workload destinations) are removed
			// from routing/placement decisions. The connected path
			// piggybacks on PeerEventDisconnected, but that event only
			// fires when ClosePeerSession actually closed something.
			n.signalRouteInvalidate()
			n.placement.Signal()
		case state.TopologyChanged:
			n.signalRouteInvalidate()
			n.placement.Signal()
		case state.AddressesChanged:
			snap := n.store.Snapshot()
			n.syncPeersFromState(ctx, snap)
			if e.Peer == snap.LocalID {
				for _, kp := range knownPeers(snap, n.peerCache) {
					if !n.mesh.IsPeerConnected(kp.PeerID) {
						n.mesh.NudgePeer(kp.PeerID)
					}
				}
				continue
			}
			if !n.mesh.IsPeerConnected(e.Peer) {
				n.mesh.NudgePeer(e.Peer)
			}
		case state.WorkloadChanged:
			n.placement.Signal()
		case state.StaticChanged:
			n.static.Signal()
		}
	}
}

func (n *Supervisor) handlePeerEvent(_ context.Context, ev transport.PeerEvent) {
	switch ev.Type {
	case transport.PeerEventConnected:
		n.signalRouteInvalidate()
		n.store.SetLocalReachable(n.GetConnectedPeers())
		n.store.SetPeerLastAddr(ev.Key, ev.Addrs[0].String())
		addrs := make([]string, len(ev.Addrs))
		for i, a := range ev.Addrs {
			addrs[i] = a.String()
		}
		n.peerCache.Upsert(ev.Key, addrs, time.Now())
		n.log.Infow("peer connected", "peer_id", ev.Key.Short())
	case transport.PeerEventDisconnected:
		n.signalRouteInvalidate()
		n.store.SetLocalReachable(n.GetConnectedPeers())
		n.placement.Signal()
		n.log.Infow("peer disconnected", "peer_id", ev.Key.Short(), "reason", ev.Reason.String())
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
	if err := n.peerCache.Flush(); err != nil {
		n.log.Warnw("failed to flush peer cache", zap.Error(err))
	}
}

func (n *Supervisor) pruneOrphanBlobs() {
	keep := blobs.KeepSet(n.store.Snapshot(), n.static.StaticBlobs())
	removed, err := n.blobs.Prune(keep, blobPruneGrace)
	if err != nil {
		n.log.Warnw("prune orphan blobs failed", zap.Error(err))
	}
	if len(removed) > 0 {
		n.log.Infow("evicted orphan blobs", "count", len(removed))
	}
}

func (n *Supervisor) shutdown() {
	n.controlSrv.Stop()

	// Stream handlers block on reads that only unblock when sessions close.
	if err := n.mesh.Stop(); err != nil {
		n.log.Errorw("failed to shut down mesh", zap.Error(err))
	}

	n.wg.Wait()

	if err := n.membership.Stop(); err != nil {
		n.log.Warnw("membership stop failed", zap.Error(err))
	}

	n.saveState()

	if err := n.static.Stop(); err != nil {
		n.log.Warnw("static stop failed", zap.Error(err))
	}
	if err := n.placement.Stop(); err != nil {
		n.log.Warnw("placement stop failed", zap.Error(err))
	}
	if n.wasmRuntime != nil {
		n.wasmRuntime.Close(context.Background())
	}

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

func (n *Supervisor) RouteRequest(ctx context.Context, uri wasm.URI, input []byte) ([]byte, error) {
	info, ok := wasm.CallerInfoFromContext(ctx)
	if !ok {
		if cert := n.creds.Cert(); cert != nil {
			info = wasm.CallerInfo{
				PeerKey: types.PeerKeyFromBytes(cert.GetClaims().GetSubjectPub()),
			}
			if attrs := cert.GetClaims().GetCapabilities().GetAttributes(); attrs != nil {
				info.Attributes = attrs.AsMap()
			}
		}
	}

	ctx = wasm.WithCallerInfo(ctx, info)

	switch uri.Scheme {
	case wasm.SchemeSeed:
		return n.placement.Call(ctx, uri.Name, uri.Function, input)
	case wasm.SchemeService:
		return n.routeServiceRequest(ctx, info.PeerKey, uri.Name, input)
	default:
		return nil, fmt.Errorf("unsupported URI scheme: %s", uri.Scheme)
	}
}

func (n *Supervisor) routeServiceRequest(ctx context.Context, callerKey types.PeerKey, name string, input []byte) ([]byte, error) {
	snap := n.store.Snapshot()
	var candidates []state.ServiceInfo
	for _, svc := range snap.Services() {
		if svc.Name == name {
			candidates = append(candidates, svc)
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no provider for service %q: %w", name, wasm.ErrTargetNotFound)
	}

	svc := pickNearestService(snap, candidates)
	if err := n.gate.Connect(callerKey, svc.Peer, svc.Port); err != nil {
		return nil, fmt.Errorf("connect %s: %w", name, wasm.ErrTargetNotFound)
	}
	if svc.Peer == snap.LocalID {
		return dialLocalService(ctx, svc.Port, input)
	}
	return n.tunneling.RequestService(ctx, svc.Peer, svc.Port, input)
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
func (n *Supervisor) SeedWorkload(wasmBytes []byte, spec state.WorkloadSpec) (string, error) {
	h := sha256.Sum256(wasmBytes)
	hash := hex.EncodeToString(h[:])
	spec.Hash = hash
	if spec.Name == "" {
		spec.Name = hash
	}
	if err := n.placement.Seed(wasmBytes, spec, nil); err != nil {
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

type streamOpenAdapter struct {
	t transport.Transport
}

func (a *streamOpenAdapter) OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error) {
	return a.t.OpenStream(ctx, peer, st)
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
	return transport.WrapTrafficStream(stream, a.recorder, peer), nil
}

var (
	_ membership.ClusterState   = state.StateStore(nil)
	_ membership.Network        = (*transport.QUICTransport)(nil)
	_ placement.WorkloadState   = state.StateStore(nil)
	_ tunneling.ServiceState    = state.StateStore(nil)
	_ control.MembershipControl = (*membership.Service)(nil)
	_ control.PlacementControl  = (*placement.Service)(nil)
	_ control.TunnelingControl  = (*tunneling.Service)(nil)
	_ control.BlobsControl      = (*blobs.Service)(nil)
	_ control.StaticControl     = (*static.Service)(nil)
	_ control.StateReader       = state.StateStore(nil)
	_ placement.WASMRuntime     = (*wasm.Runtime)(nil)
	_ control.TransportInfo     = (*Supervisor)(nil)
	_ control.MetricsSource     = (*Supervisor)(nil)
	_ control.MeshConnector     = (*Supervisor)(nil)
)
