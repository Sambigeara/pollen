package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"net"
	"os"
	"path/filepath"
	"time"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/tunnel"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/util"
	"go.uber.org/zap"
)

const (
	// MaxDatagramPayload is the maximum safe payload size for a QUIC datagram.
	MaxDatagramPayload = 1100
)

// envelopeOverhead is the serialized size of an empty Envelope wrapping an
// empty GossipEventBatch — covers the oneof tag, length prefixes, and batch
// wrapper framing.
var envelopeOverhead = (&meshv1.Envelope{Body: &meshv1.Envelope_Events{
	Events: &statev1.GossipEventBatch{},
}}).SizeVT()

const (
	localKeysDir = "keys"

	signingKeyName    = "ed25519.key"
	signingPubKeyName = "ed25519.pub"
	pemTypePriv       = "ED25519 PRIVATE KEY"
	pemTypePub        = "ED25519 PUBLIC KEY"
	keyDirPerm        = 0o700

	directTimeout = 2 * time.Second
	punchTimeout  = 3 * time.Second

	eagerSyncCooldown = 5 * time.Second

	loopIntervalJitter = 0.1
	peerEventBufSize   = 64
	gossipEventBufSize = 64
	punchChBufSize     = 16
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
	DisableGossipJitter bool
}

type HandlerFn func(ctx context.Context, from types.PeerKey, payload []byte) error

type punchRequest struct {
	ip      net.IP
	port    int
	peerKey types.PeerKey
}

type Node struct {
	peers           *peer.Store
	store           *store.Store
	mesh            mesh.Mesh
	tun             *tunnel.Manager
	localPeerEvents chan peer.Input
	punchCh         chan punchRequest
	conf            *Config
	log             *zap.SugaredLogger
	gossipEvents    chan []*statev1.GossipEvent
	ready           chan struct{}
	lastEagerSync   map[types.PeerKey]time.Time
}

func New(conf *Config, privKey ed25519.PrivateKey, creds *auth.NodeCredentials, stateStore *store.Store, peerStore *peer.Store) (*Node, error) {
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

	m, err := mesh.NewMesh(conf.Port, privKey, creds, conf.TLSIdentityTTL, conf.MembershipTTL, stateStore.IsSubjectRevoked)
	if err != nil {
		log.Error("failed to load mesh", zap.Error(err))
		return nil, err
	}

	tun := tunnel.New(m)

	stateStore.OnRevocation(func(subject types.PeerKey) {
		tun.DisconnectPeer(subject)
		stateStore.RemoveDesiredConnection(subject, 0, 0)
		go m.ClosePeerSession(subject)
	})
	for _, svc := range stateStore.LocalServices() {
		tun.RegisterService(svc.GetPort())
	}

	n := &Node{
		log:             log,
		peers:           peerStore,
		store:           stateStore,
		mesh:            m,
		tun:             tun,
		conf:            conf,
		localPeerEvents: make(chan peer.Input, peerEventBufSize),
		punchCh:         make(chan punchRequest, punchChBufSize),
		gossipEvents:    make(chan []*statev1.GossipEvent, gossipEventBufSize),
		ready:           make(chan struct{}),
		lastEagerSync:   make(map[types.PeerKey]time.Time),
	}

	return n, nil
}

func (n *Node) Start(ctx context.Context) error {
	defer n.shutdown()

	if err := n.mesh.Start(ctx); err != nil {
		return err
	}
	close(n.ready)
	n.connectBootstrapPeers(ctx)
	n.tun.Start(ctx)
	go n.recvLoop(ctx)
	for range punchWorkers {
		go n.punchLoop(ctx)
	}

	gossipTicker := util.NewJitterTicker(ctx, n.conf.GossipInterval, n.gossipJitter())
	defer gossipTicker.Stop()

	peerTicker := time.NewTicker(n.conf.PeerTickInterval)
	defer peerTicker.Stop()

	var ipRefreshCh <-chan time.Time
	if len(n.conf.AdvertisedIPs) == 0 {
		ipRefreshTicker := time.NewTicker(ipRefreshInterval)
		defer ipRefreshTicker.Stop()
		ipRefreshCh = ipRefreshTicker.C
	}

	n.tick()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-peerTicker.C:
			n.tick()
		case <-gossipTicker.C:
			n.gossip(ctx)
		case <-ipRefreshCh:
			n.refreshIPs()
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
	if env == nil {
		return
	}

	switch body := env.GetBody().(type) {
	case *meshv1.Envelope_Clock:
		events := n.store.MissingFor(body.Clock)
		batches := batchEvents(events, MaxDatagramPayload)
		for _, batch := range batches {
			env := &meshv1.Envelope{Body: &meshv1.Envelope_Events{
				Events: &statev1.GossipEventBatch{Events: batch},
			}}
			if err := n.mesh.Send(ctx, from, env); err != nil {
				n.log.Debugw("failed sending gossip events", "to", from.Short(), "err", err)
			}
		}
	case *meshv1.Envelope_Events:
		result := n.store.ApplyEvents(body.Events.GetEvents())
		if len(result.Rebroadcast) > 0 {
			n.queueGossipEvents(result.Rebroadcast)
		}
	case *meshv1.Envelope_PunchCoordRequest:
		n.handlePunchCoordRequest(ctx, from, body.PunchCoordRequest)
	case *meshv1.Envelope_PunchCoordTrigger:
		n.handlePunchCoordTrigger(body.PunchCoordTrigger)
	case *meshv1.Envelope_ObservedAddress:
		n.handleObservedAddress(body.ObservedAddress)
	}
}

func (n *Node) broadcastEvents(ctx context.Context, events []*statev1.GossipEvent) {
	if len(events) == 0 {
		return
	}

	connectedPeers := n.GetConnectedPeers()
	batches := batchEvents(events, MaxDatagramPayload)
	for _, batch := range batches {
		env := &meshv1.Envelope{Body: &meshv1.Envelope_Events{
			Events: &statev1.GossipEventBatch{Events: batch},
		}}
		for _, peerID := range connectedPeers {
			if peerID == n.store.LocalID {
				continue
			}
			if err := n.mesh.Send(ctx, peerID, env); err != nil {
				n.log.Debugw("event gossip send failed", "peer", peerID.Short(), "err", err)
			}
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

func (n *Node) gossip(ctx context.Context) {
	clock := n.store.Clock()
	connectedPeers := n.GetConnectedPeers()
	for _, peerID := range connectedPeers {
		if peerID == n.store.LocalID {
			continue
		}
		if err := n.mesh.Send(ctx, peerID, &meshv1.Envelope{
			Body: &meshv1.Envelope_Clock{Clock: clock},
		}); err != nil {
			n.log.Debugw("clock gossip send failed", "peer", peerID.Short(), "err", err)
		}
	}
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
	if d, ok := in.(peer.PeerDisconnected); ok {
		n.queueGossipEvents(n.store.SetLocalConnected(d.PeerKey, false))
		delete(n.lastEagerSync, d.PeerKey)
	}

	outputs := n.peers.Step(time.Now(), in)
	n.handleOutputs(outputs)
}

func (n *Node) tick() {
	n.syncPeersFromState()
	n.reconcileConnections()
	n.reconcileDesiredConnections()

	now := time.Now()
	outputs := n.peers.Step(now, peer.Tick{})
	n.handleOutputs(outputs)
}

func (n *Node) syncPeersFromState() {
	for _, kp := range n.store.KnownPeers() {
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

		n.peers.Step(time.Now(), peer.DiscoverPeer{
			PeerKey:  kp.PeerID,
			Ips:      ips,
			Port:     int(kp.LocalPort),
			LastAddr: lastAddr,
		})
	}
}

func (n *Node) handleOutputs(outputs []peer.Output) {
	for _, out := range outputs {
		switch e := out.(type) {
		case peer.PeerConnected:
			addr := &net.UDPAddr{IP: e.IP, Port: e.ObservedPort}
			n.store.SetLastAddr(e.PeerKey, addr.String())
			n.queueGossipEvents(n.store.SetLocalConnected(e.PeerKey, true))
			if time.Since(n.lastEagerSync[e.PeerKey]) >= eagerSyncCooldown {
				clock := n.store.Clock()
				if !n.store.HasRemoteState() {
					clock = n.store.ZeroClock()
				}
				if err := n.mesh.Send(context.Background(), e.PeerKey, &meshv1.Envelope{
					Body: &meshv1.Envelope_Clock{Clock: clock},
				}); err != nil {
					n.log.Debugw("eager sync failed", "peer", e.PeerKey.Short(), "err", err)
				}
				n.lastEagerSync[e.PeerKey] = time.Now()
			}

			if err := n.mesh.Send(context.Background(), e.PeerKey, &meshv1.Envelope{
				Body: &meshv1.Envelope_ObservedAddress{
					ObservedAddress: &meshv1.ObservedAddress{Addr: addr.String()},
				},
			}); err != nil {
				n.log.Debugw("failed sending observed address", "peer", e.PeerKey.Short(), "err", err)
			}

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
	connections := n.tun.ListConnections()

	missing := make(map[types.PeerKey]map[uint32]struct{}, len(connections))
	for _, conn := range connections {
		if !n.store.HasServicePort(conn.PeerID, conn.RemotePort) {
			ports, ok := missing[conn.PeerID]
			if !ok {
				ports = make(map[uint32]struct{})
				missing[conn.PeerID] = ports
			}
			ports[conn.RemotePort] = struct{}{}
		}
	}

	for peerID, ports := range missing {
		for port := range ports {
			n.log.Infow("removing stale forward", "peer", peerID.String()[:8], "port", port)
			n.tun.DisconnectRemoteService(peerID, port)
			n.store.RemoveDesiredConnection(peerID, port, 0)
		}
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
	if err := n.connectPeer(peerKey, addrs); err != nil {
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

func (n *Node) connectPeer(peerKey types.PeerKey, addrs []*net.UDPAddr) error {
	ctx, cancel := context.WithTimeout(context.Background(), directTimeout)
	defer cancel()
	return n.mesh.Connect(ctx, peerKey, addrs)
}

func (n *Node) buildPeerAddrs(peerKey types.PeerKey, ips []net.IP, port int) []*net.UDPAddr {
	var extPort int
	if rec, ok := n.store.Get(peerKey); ok && rec.ExternalPort != 0 {
		extPort = int(rec.ExternalPort)
	}

	addrs := make([]*net.UDPAddr, 0, len(ips))
	for _, ip := range ips {
		if ip == nil {
			continue
		}
		p := port
		if extPort != 0 && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
			p = extPort
		}
		addrs = append(addrs, &net.UDPAddr{IP: ip, Port: p})
	}
	return addrs
}

func (n *Node) punchPeer(peerKey types.PeerKey, ip net.IP, port int) error {
	addr := &net.UDPAddr{IP: ip, Port: port}

	ctx, cancel := context.WithTimeout(context.Background(), punchTimeout)
	defer cancel()
	return n.mesh.Punch(ctx, peerKey, addr)
}

func (n *Node) coordinatorPeers(target types.PeerKey) []types.PeerKey {
	localIPs := n.store.NodeIPs(n.store.LocalID)
	connectedPeers := n.GetConnectedPeers()
	peers := make([]types.PeerKey, 0, len(connectedPeers))

	for _, key := range connectedPeers {
		if key == target {
			continue
		}
		candidateIPs := n.store.NodeIPs(key)
		// TODO(saml) we shouldn't skip if coordinators share the same LAN, but we need to communicate with
		// the punching peer outside of the shared LAN that the IP is the same as the coordinator, somehow
		if len(candidateIPs) == 0 || sharesLAN(localIPs, candidateIPs) {
			continue
		}
		if !n.store.IsConnected(key, target) {
			continue
		}
		peers = append(peers, key)
	}
	return peers
}

func (n *Node) requestPunchCoordination(target types.PeerKey) {
	coordinators := n.coordinatorPeers(target)
	if len(coordinators) == 0 {
		n.log.Debugw("no coordinators available for punch", "peer", target.Short())
		n.localPeerEvents <- peer.ConnectFailed{PeerKey: target}
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
		n.localPeerEvents <- peer.ConnectFailed{PeerKey: target}
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
			// Skip if peer connected while queued.
			if n.peers.InState(req.peerKey, peer.PeerStateConnected) {
				continue
			}
			if err := n.punchPeer(req.peerKey, req.ip, req.port); err != nil {
				if n.peers.InState(req.peerKey, peer.PeerStateConnecting) {
					if errors.Is(err, mesh.ErrIdentityMismatch) {
						n.log.Warnw("punch failed: peer identity mismatch", "peer", req.peerKey.Short(), "err", err)
					} else {
						n.log.Debugw("punch failed", "peer", req.peerKey.Short(), "err", err)
					}
					n.localPeerEvents <- peer.ConnectFailed{PeerKey: req.peerKey}
				}
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
		n.localPeerEvents <- peer.ConnectFailed{PeerKey: peerKey}
	}
}

func (n *Node) handleObservedAddress(oa *meshv1.ObservedAddress) {
	addr, err := net.ResolveUDPAddr("udp", oa.GetAddr())
	if err != nil {
		n.log.Debugw("observed address: parse failed", "addr", oa.GetAddr(), "err", err)
		return
	}

	if addr.IP.IsPrivate() || addr.IP.IsLoopback() || addr.IP.IsLinkLocalUnicast() {
		return
	}

	n.log.Infow("observed address received", "addr", addr.String())
	n.queueGossipEvents(n.store.SetExternalPort(uint32(addr.Port)))
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

func (n *Node) gossipJitter() float64 {
	if n.conf.DisableGossipJitter {
		return 0
	}
	if n.conf.GossipJitter > 0 {
		return n.conf.GossipJitter
	}
	return loopIntervalJitter
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

	n.log.Debug("successfully shutdown Node")
}

func sharesLAN(aIPs, bIPs []string) bool {
	for _, aStr := range aIPs {
		a := net.ParseIP(aStr)
		if a == nil || !a.IsPrivate() {
			continue
		}
		for _, bStr := range bIPs {
			b := net.ParseIP(bStr)
			if b == nil || !b.IsPrivate() {
				continue
			}
			if a4, b4 := a.To4(), b.To4(); a4 != nil && b4 != nil {
				if a4[0] == b4[0] && a4[1] == b4[1] && a4[2] == b4[2] {
					return true
				}
			} else if a16, b16 := a.To16(), b.To16(); a16 != nil && b16 != nil {
				if string(a16[:8]) == string(b16[:8]) {
					return true
				}
			}
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

func GenIdentityKey(pollenDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	dir := filepath.Join(pollenDir, localKeysDir)

	privPath := filepath.Join(dir, signingKeyName)
	pubPath := filepath.Join(dir, signingPubKeyName)

	requireRegen := false
	keyEnc, err := os.ReadFile(privPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			requireRegen = true
		} else {
			return nil, nil, err
		}
	}

	var pubEnc []byte
	if !requireRegen { //nolint:nestif
		pubEnc, err = os.ReadFile(pubPath)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, nil, err
			}
		} else {
			block, _ := pem.Decode(keyEnc)
			if block == nil || block.Type != pemTypePriv {
				return nil, nil, errors.New("invalid private key PEM")
			}
			priv := ed25519.NewKeyFromSeed(block.Bytes)

			block, _ = pem.Decode(pubEnc)
			if block == nil || block.Type != pemTypePub {
				return nil, nil, errors.New("invalid public key PEM")
			}
			pub := ed25519.PublicKey(block.Bytes)

			return priv, pub, nil
		}
	}

	if err := os.MkdirAll(dir, keyDirPerm); err != nil {
		return nil, nil, err
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	privFile, err := os.Create(privPath)
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

	pubFile, err := os.Create(pubPath)
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

	return priv, pub, err
}
