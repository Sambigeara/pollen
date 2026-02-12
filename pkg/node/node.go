package node

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/sock"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/tunnel"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/crypto/ed25519"
)

var _ sock.LocalCrypto = (*localCrypto)(nil)

const (
	localKeysDir = "keys"

	noiseKeyName      = "noise.key"
	noisePubKeyName   = "noise.pub"
	signingKeyName    = "ed25519.key"
	signingPubKeyName = "ed25519.pub"
	pemTypePriv       = "ED25519 PRIVATE KEY"
	pemTypePub        = "ED25519 PUBLIC KEY"
	keyDirPerm        = 0o700
	keyFilePerm       = 0o600

	udpDirectTimeout = 2 * time.Second
	udpPunchTimeout  = 3 * time.Second
	tcpDirectTimeout = 2 * time.Second
	tcpPunchTimeout  = 5 * time.Second

	loopIntervalJitter = 0.1
	peerEventBufSize   = 64
	gossipEventBufSize = 64
)

type Config struct {
	PollenDir           string
	AdvertisedIPs       []string
	Port                int
	GossipInterval      time.Duration
	PeerTickInterval    time.Duration
	GossipJitter        float64
	DisableGossipJitter bool
}

type HandlerFn func(ctx context.Context, from types.PeerKey, payload []byte) error

type Node struct {
	invites         admission.Store
	sock            sock.SuperSock
	tcpPeers        *peer.Store
	store           *store.Store
	localPeerEvents chan peer.Input
	conf            *Config
	tun             *tunnel.Manager
	udpPeers        *peer.Store
	log             *zap.SugaredLogger
	crypto          *localCrypto
	tcpPeerEvents   chan peer.Input
	gossipEvents    chan *statev1.GossipNode
	handlers        map[types.MsgType]HandlerFn
	handlersMu      sync.RWMutex
	requestFullOnce sync.Once
}

func New(conf *Config) (*Node, error) {
	log := zap.S().Named("node")

	cs := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)

	noiseKey, err := genNoiseKey(cs, conf.PollenDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load noise key: %w", err)
	}
	nodeID := types.PeerKeyFromBytes(noiseKey.Public)

	privKey, pubKey, err := genIdentityKey(conf.PollenDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load signing keys: %w", err)
	}

	stateStore, err := store.Load(conf.PollenDir, nodeID, pubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	invitesStore, err := admission.Load(conf.PollenDir)
	if err != nil {
		log.Error("failed to load peers", zap.Error(err))
		return nil, err
	}

	ips := conf.AdvertisedIPs
	if len(ips) == 0 {
		var err error
		ips, err = sock.GetAdvertisableAddrs()
		if err != nil {
			return nil, err
		}
	}

	crypto := &localCrypto{
		noisePubKey:    noiseKey.Public,
		identityPubKey: pubKey,
	}

	s := sock.NewTransport(conf.Port, &cs, noiseKey, crypto, invitesStore)

	stateStore.SetLocalNetwork(ips, uint32(conf.Port))

	n := &Node{
		log:             log,
		sock:            s,
		handlers:        make(map[types.MsgType]HandlerFn),
		udpPeers:        peer.NewStore(),
		tcpPeers:        peer.NewStore(),
		store:           stateStore,
		invites:         invitesStore,
		crypto:          crypto,
		conf:            conf,
		localPeerEvents: make(chan peer.Input, peerEventBufSize),
		tcpPeerEvents:   make(chan peer.Input, peerEventBufSize),
		gossipEvents:    make(chan *statev1.GossipNode, gossipEventBufSize),
	}

	n.tun = tunnel.New(
		n.sendEnvelope,
		&Directory{store: stateStore},
		s,
		privKey,
		ips,
	)

	for _, svc := range stateStore.LocalServices() {
		n.tun.RegisterService(svc.GetPort())
	}

	n.tun.Sessions().SetOnDisconnect(func(peerKey types.PeerKey) {
		n.tcpPeerEvents <- peer.PeerDisconnected{PeerKey: peerKey}
	})

	return n, nil
}

func (n *Node) Start(ctx context.Context, token *peerv1.Invite) error {
	defer n.shutdown()

	n.registerHandlers()

	if err := n.sock.Start(ctx); err != nil {
		return err
	}
	go n.recvLoop(ctx)

	if token != nil {
		if err := n.sock.JoinWithInvite(ctx, token); err != nil {
			return fmt.Errorf("join with invite: %w", err)
		}
	}

	gossipTicker := util.NewJitterTicker(ctx, n.conf.GossipInterval, n.gossipJitter())
	defer gossipTicker.Stop()

	peerTicker := time.NewTicker(n.conf.PeerTickInterval)
	defer peerTicker.Stop()

	n.tick()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-peerTicker.C:
			n.tick()
		case <-gossipTicker.C:
			n.gossip(ctx)
		case node := <-n.gossipEvents:
			n.broadcastNode(ctx, node)
		// UDP FSM events
		case in := <-n.sock.Events():
			n.handleUDPPeerInput(in)
		case in := <-n.localPeerEvents:
			n.handleUDPPeerInput(in)
		// TCP FSM events
		case in := <-n.tcpPeerEvents:
			n.handleTCPPeerInput(in)
		}
	}
}

func (n *Node) queueGossipNode(node *statev1.GossipNode) {
	if node == nil {
		return
	}

	select {
	case n.gossipEvents <- node:
	default:
	}
}

func (n *Node) Handle(t types.MsgType, h HandlerFn) {
	n.handlersMu.Lock()
	defer n.handlersMu.Unlock()
	n.handlers[t] = h
}

func (n *Node) recvLoop(ctx context.Context) {
	for {
		p, err := n.sock.Recv(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			n.log.Debugw("recv failed", "err", err)
			continue
		}
		n.handleApp(ctx, p)
	}
}

func (n *Node) handleApp(ctx context.Context, p sock.Packet) {
	n.handlersMu.RLock()
	h := n.handlers[p.Typ]
	n.handlersMu.RUnlock()
	if h != nil {
		if err := h(ctx, p.Peer, p.Payload); err != nil {
			n.log.Debugf("handler error: %w", err)
		}
	}
}

func (n *Node) registerHandlers() {
	// Session handlers (multiplexed tunnels)
	n.Handle(types.MsgTypeSessionRequest, n.tun.HandleSessionRequest)
	n.Handle(types.MsgTypeSessionResponse, n.tun.HandleSessionResponse)

	// TCP punch handlers
	n.Handle(types.MsgTypeTCPPunchRequest, n.tun.HandlePunchRequest)
	n.Handle(types.MsgTypeTCPPunchTrigger, n.tun.HandlePunchTrigger)
	n.Handle(types.MsgTypeTCPPunchReady, n.tun.HandlePunchReady)
	n.Handle(types.MsgTypeTCPPunchResponse, n.tun.HandlePunchResponse)
	n.Handle(types.MsgTypeTCPPunchProbeRequest, n.tun.HandlePunchProbeRequest)
	n.Handle(types.MsgTypeTCPPunchProbeOffer, n.tun.HandlePunchProbeOffer)
	n.Handle(types.MsgTypeTCPPunchProbeResult, n.tun.HandlePunchProbeResult)
	n.Handle(types.MsgTypeUDPRelay, n.handleUDPRelay)
	n.Handle(types.MsgTypeGossip, n.handleGossip)
}

func (n *Node) handleGossip(ctx context.Context, from types.PeerKey, plaintext []byte) error {
	env := &statev1.GossipEnvelope{}
	if err := env.UnmarshalVT(plaintext); err != nil {
		return fmt.Errorf("malformed gossip payload: %w", err)
	}

	switch body := env.GetBody().(type) {
	case *statev1.GossipEnvelope_Clock:
		for _, node := range n.store.MissingFor(body.Clock) {
			if err := n.sendNode(ctx, from, node); err != nil {
				n.log.Debugw("failed sending gossip node", "to", from.Short(), "err", err)
			}
		}
	case *statev1.GossipEnvelope_Node:
		result := n.store.ApplyNode(body.Node)
		if result.Rebroadcast != nil {
			n.queueGossipNode(result.Rebroadcast)
		}
	}

	return nil
}

func (n *Node) sendClock(ctx context.Context, to types.PeerKey, clock *statev1.GossipVectorClock) error {
	env := &statev1.GossipEnvelope{
		Body: &statev1.GossipEnvelope_Clock{Clock: clock},
	}
	b, err := env.MarshalVT()
	if err != nil {
		return err
	}

	return n.sendEnvelope(ctx, to, types.Envelope{Type: types.MsgTypeGossip, Payload: b})
}

func (n *Node) sendNode(ctx context.Context, to types.PeerKey, node *statev1.GossipNode) error {
	env := &statev1.GossipEnvelope{
		Body: &statev1.GossipEnvelope_Node{Node: node},
	}
	b, err := env.MarshalVT()
	if err != nil {
		return err
	}

	return n.sendEnvelope(ctx, to, types.Envelope{Type: types.MsgTypeGossip, Payload: b})
}

func (n *Node) broadcastNode(ctx context.Context, node *statev1.GossipNode) {
	if node == nil {
		return
	}

	connectedPeers := n.GetConnectedPeers()
	for _, peerID := range connectedPeers {
		if peerID == n.store.LocalID {
			continue
		}
		if err := n.sendNode(ctx, peerID, node); err != nil {
			n.log.Debugw("node gossip send failed", "peer", peerID.Short(), "err", err)
		}
	}
}

func (n *Node) gossip(ctx context.Context) {
	clock := n.store.Clock()
	connectedPeers := n.GetConnectedPeers()
	for _, peerID := range connectedPeers {
		if peerID == n.store.LocalID {
			continue
		}
		if err := n.sendClock(ctx, peerID, clock); err != nil {
			n.log.Debugw("clock gossip send failed", "peer", peerID.Short(), "err", err)
		}
	}
}

func (n *Node) handleUDPPeerInput(in peer.Input) {
	if d, ok := in.(peer.PeerDisconnected); ok {
		n.queueGossipNode(n.store.SetLocalConnected(d.PeerKey, false))
	}

	outputs := n.udpPeers.Step(time.Now(), in)
	n.handleUDPOutputs(outputs)
}

func (n *Node) handleTCPPeerInput(in peer.Input) {
	outputs := n.tcpPeers.Step(time.Now(), in)
	n.handleTCPOutputs(outputs)
}

func (n *Node) tick() {
	// First sync any new peers and connections from gossip state
	n.syncPeersFromState()
	n.reconcileConnections()
	n.reconcileDesiredConnections()

	// Tick both FSMs
	now := time.Now()
	udpOutputs := n.udpPeers.Step(now, peer.Tick{})
	n.handleUDPOutputs(udpOutputs)

	now = time.Now()
	tcpOutputs := n.tcpPeers.Step(now, peer.Tick{})
	n.handleTCPOutputs(tcpOutputs)
}

func (n *Node) syncPeersFromState() {
	for _, kp := range n.store.KnownPeers() {
		if kp.PeerID == n.store.LocalID {
			continue
		}

		if len(kp.IPs) == 0 {
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

		if len(ips) == 0 {
			continue
		}

		n.udpPeers.Step(time.Now(), peer.DiscoverPeer{
			PeerKey: kp.PeerID,
			Ips:     ips,
			Port:    int(kp.LocalPort),
		})
	}
}

func (n *Node) handleUDPOutputs(outputs []peer.Output) {
	for _, out := range outputs {
		switch e := out.(type) {
		case peer.PeerConnected:
			n.queueGossipNode(n.store.SetLocalConnected(e.PeerKey, true))
			n.requestFullOnce.Do(func() {
				if err := n.sendClock(context.Background(), e.PeerKey, n.store.ZeroClock()); err != nil {
					n.log.Debugw("failed requesting full state", "peer", e.PeerKey.Short(), "err", err)
				}
			})
			n.log.Infow("udp peer connected", "peer_id", e.PeerKey.Short(), "ip", e.IP, "observedPort", e.ObservedPort)
		case peer.AttemptConnect:
			go func() {
				if err := n.udpDirectConnect(e); err != nil {
					n.log.Debugw("udp direct connect failed", "peer", e.PeerKey.String()[:8], "ips", e.Ips, "port", e.Port, "err", err)
					n.localPeerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
				}
			}()
		case peer.RequestPunchCoordination:
			coordinator, ok := n.FindCoordinator(e.PeerKey)
			if !ok {
				n.log.Debugw("no coordinator available for udp punch", "peer", e.PeerKey.Short())
				go func() {
					n.localPeerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
				}()
				continue
			}
			go func() {
				if err := n.udpPunchCoordination(e, coordinator); err != nil {
					n.localPeerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
				}
			}()
			continue
		}
	}
}

func (n *Node) handleTCPOutputs(outputs []peer.Output) {
	for _, out := range outputs {
		switch e := out.(type) {
		case peer.PeerConnected:
			n.log.Infow("tcp peer connected", "peer_id", e.PeerKey.Short())
		case peer.AttemptConnect:
			go func() {
				if err := n.tcpDirectConnect(e); err != nil {
					n.log.Debugw("tcp direct connect failed", "peer", e.PeerKey.Short(), "err", err)
					n.tcpPeerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
					return
				}
				n.tcpPeerEvents <- peer.ConnectPeer{PeerKey: e.PeerKey}
			}()
		case peer.RequestPunchCoordination:
			coordinator, ok := n.FindCoordinator(e.PeerKey)
			if !ok {
				n.log.Debugw("no coordinator available for tcp punch", "peer", e.PeerKey.Short())
				go func() {
					n.tcpPeerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
				}()
				continue
			}
			go func() {
				if err := n.tcpPunchCoordination(e, coordinator); err != nil {
					n.log.Debugw("tcp punch failed", "peer", e.PeerKey.Short(), "err", err)
					n.tcpPeerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
					return
				}
				n.tcpPeerEvents <- peer.ConnectPeer{PeerKey: e.PeerKey}
			}()
		}
	}
}

// FindCoordinator returns a connected UDP peer that can coordinate a punch
// to the given target. Skips LAN peers and verifies the coordinator is
// connected to the target via gossip state.
func (n *Node) FindCoordinator(target types.PeerKey) (types.PeerKey, bool) {
	localIPs := n.store.NodeIPs(n.store.LocalID)
	connectedPeers := n.GetConnectedPeers()
	sort.Slice(connectedPeers, func(i, j int) bool {
		return connectedPeers[i].Less(connectedPeers[j])
	})

	for _, key := range connectedPeers {
		if key == target {
			continue
		}

		candidateIPs := n.store.NodeIPs(key)
		if len(candidateIPs) == 0 {
			continue
		}
		if sharesLAN(localIPs, candidateIPs) {
			continue
		}
		if !n.store.IsConnected(key, target) {
			continue
		}
		return key, true
	}

	return types.PeerKey{}, false
}

func (n *Node) reconcileConnections() {
	connections := n.tun.ListConnections()

	nodes := n.store.Nodes()
	missing := make(map[types.PeerKey]map[uint32]struct{}, len(connections))
	for _, conn := range connections {
		node := nodes[conn.PeerID]
		if !hasServicePort(node.Services, conn.RemotePort) {
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

	// After removing stale forwards, check which peers still have active
	// connections. Remove any TCP FSM peers that have no remaining tunnels.
	remaining := n.tun.ListConnections()
	activePeers := make(map[types.PeerKey]bool, len(remaining))
	for _, conn := range remaining {
		activePeers[conn.PeerID] = true
	}
	for _, st := range []peer.PeerState{
		peer.PeerStateConnected,
		peer.PeerStateDiscovered,
		peer.PeerStateConnecting,
		peer.PeerStateUnreachable,
	} {
		for _, peerKey := range n.tcpPeers.GetAll(st) {
			if !activePeers[peerKey] {
				n.tcpPeers.Step(time.Now(), peer.RemovePeer{PeerKey: peerKey})
			}
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
		existing[desiredConnectionKey(conn.PeerID, conn.RemotePort, conn.LocalPort)] = struct{}{}
	}

	nodes := n.store.Nodes()
	for _, desiredConn := range desired {
		if _, ok := existing[desiredConnectionKey(desiredConn.PeerID, desiredConn.RemotePort, desiredConn.LocalPort)]; ok {
			continue
		}

		node, ok := nodes[desiredConn.PeerID]
		if !ok || !hasServicePort(node.Services, desiredConn.RemotePort) {
			continue
		}

		if _, err := n.ConnectService(desiredConn.PeerID, desiredConn.RemotePort, desiredConn.LocalPort); err != nil {
			n.log.Debugw("failed restoring desired connection", "peer", desiredConn.PeerID.Short(), "remotePort", desiredConn.RemotePort, "localPort", desiredConn.LocalPort, "err", err)
		}
	}
}

func hasServicePort(services []*statev1.Service, port uint32) bool {
	for _, svc := range services {
		if svc.GetPort() == port {
			return true
		}
	}
	return false
}

func desiredConnectionKey(peerID types.PeerKey, remotePort, localPort uint32) string {
	return fmt.Sprintf("%s:%d:%d", peerID.String(), remotePort, localPort)
}

func (n *Node) udpDirectConnect(e peer.AttemptConnect) error {
	addrs := make([]*net.UDPAddr, len(e.Ips))
	for i, ip := range e.Ips {
		addrs[i] = &net.UDPAddr{
			IP:   ip,
			Port: e.Port,
		}
	}

	n.log.Debugw("attempting direct connection", "peer", e.PeerKey.Short())

	ctx, cancel := context.WithTimeout(context.Background(), udpDirectTimeout)
	defer cancel()

	return n.sock.EnsurePeer(ctx, e.PeerKey, addrs, false)
}

func (n *Node) tcpDirectConnect(e peer.AttemptConnect) error {
	ctx, cancel := context.WithTimeout(context.Background(), tcpDirectTimeout)
	defer cancel()

	conn, err := n.tun.DialSessionDirect(ctx, e.PeerKey)
	if err != nil {
		return err
	}

	if _, err := n.tun.Sessions().RegisterOutbound(conn, e.PeerKey); err != nil {
		n.log.Warnw("tcp session register failed", "peer", e.PeerKey.Short(), "err", err)
		_ = conn.Close()
		return err
	}

	return nil
}

func (n *Node) udpPunchCoordination(e peer.RequestPunchCoordination, coordinator types.PeerKey) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), udpPunchTimeout)
	defer cancel()

	req := &peerv1.PunchCoordRequest{
		PeerId: e.PeerKey.Bytes(),
	}
	b, err := req.MarshalVT()
	if err != nil {
		n.log.Errorw("failed to marshal punch request", "err", err)
		return err
	}

	n.log.Infow("requesting punch coordination", "peer", e.PeerKey.Short(), "coordinator", coordinator.String()[:8])

	if err := n.sendEnvelope(ctx, coordinator, types.Envelope{
		Type:    types.MsgTypeUDPPunchCoordRequest,
		Payload: b,
	}); err != nil {
		n.log.Debugw("punch coord request failed", "peer", e.PeerKey.String()[:8], "err", err)
		return err
	}

	return nil
}

func (n *Node) tcpPunchCoordination(e peer.RequestPunchCoordination, coordinator types.PeerKey) error {
	ctx, cancel := context.WithTimeout(context.Background(), tcpPunchTimeout)
	defer cancel()

	conn, err := n.tun.DialSessionWithPunch(ctx, e.PeerKey, coordinator)
	if err != nil {
		return err
	}

	if _, err := n.tun.Sessions().RegisterOutbound(conn, e.PeerKey); err != nil {
		n.log.Warnw("tcp session register failed", "peer", e.PeerKey.Short(), "err", err)
		_ = conn.Close()
		return err
	}

	return nil
}

// ConnectService wraps tunnel.Manager.ConnectService and injects the peer
// into the TCP FSM for proactive session establishment.
func (n *Node) ConnectService(peerID types.PeerKey, remotePort, localPort uint32) (uint32, error) {
	port, err := n.tun.ConnectService(peerID, remotePort, localPort)
	if err != nil {
		return 0, err
	}
	n.store.AddDesiredConnection(peerID, remotePort, port)

	n.tcpPeerEvents <- peer.RetryPeer{PeerKey: peerID}

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
	activeConns := n.tun.ListConnections()
	desired := make([]store.Connection, 0, len(activeConns))
	for _, conn := range activeConns {
		desired = append(desired, store.Connection{
			PeerID:     conn.PeerID,
			RemotePort: conn.RemotePort,
			LocalPort:  conn.LocalPort,
		})
	}
	n.store.ReplaceDesiredConnections(desired)

	if err := n.store.Save(); err != nil {
		n.log.Errorf("failed to save state: %v", err)
	} else {
		n.log.Info("state saved to disk")
	}
	if err := n.store.Close(); err != nil {
		n.log.Errorf("failed to close state store: %v", err)
	}

	// Notify peers we're shutting down (fire-and-forget)
	n.sock.BroadcastDisconnect()

	if err := n.sock.Close(); err != nil {
		n.log.Errorf("failed to shut down sock: %v", err)
	} else {
		n.log.Info("successfully shut down mesh")
	}

	n.log.Debug("successfully shutdown Node")
}

// sharesLAN reports whether two nodes likely share a LAN by checking
// if any of their private IPs fall in the same /24 (v4) or /64 (v6) subnet.
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

// GetConnectedPeers returns all currently connected peer keys.
func (n *Node) GetConnectedPeers() []types.PeerKey {
	return n.udpPeers.GetAll(peer.PeerStateConnected)
}

func genNoiseKey(cs noise.CipherSuite, pollenDir string) (noise.DHKey, error) {
	dir := filepath.Join(pollenDir, localKeysDir)

	keyPath := filepath.Join(dir, noiseKeyName)
	pubKeyPath := filepath.Join(dir, noisePubKeyName)

	requireRegen := false
	keyEnc, err := os.ReadFile(keyPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			requireRegen = true
		} else {
			return noise.DHKey{}, err
		}
	}

	enc := base64.StdEncoding
	var pubEnc []byte
	//nolint:nestif
	if !requireRegen {
		pubEnc, err = os.ReadFile(pubKeyPath)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return noise.DHKey{}, err
			}
		} else {
			var key [32]byte
			keyLen, err := enc.Decode(key[:], keyEnc)
			if err != nil {
				return noise.DHKey{}, err
			}
			var pub [32]byte
			pubLen, err := enc.Decode(pub[:], pubEnc)
			if err != nil {
				return noise.DHKey{}, err
			}
			return noise.DHKey{Private: key[:keyLen], Public: pub[:pubLen]}, nil
		}
	}

	if err := os.MkdirAll(dir, keyDirPerm); err != nil {
		return noise.DHKey{}, err
	}

	keyPair, err := cs.GenerateKeypair(rand.Reader)
	if err != nil {
		return noise.DHKey{}, err
	}

	newKeyEnc := make([]byte, enc.EncodedLen(len(keyPair.Private)))
	enc.Encode(newKeyEnc, keyPair.Private)
	if err := os.WriteFile(keyPath, newKeyEnc, keyFilePerm); err != nil {
		return noise.DHKey{}, err
	}

	newPubEnc := make([]byte, enc.EncodedLen(len(keyPair.Public)))
	enc.Encode(newPubEnc, keyPair.Public)
	if err := os.WriteFile(pubKeyPath, newPubEnc, keyFilePerm); err != nil {
		return noise.DHKey{}, err
	}

	return keyPair, nil
}

func genIdentityKey(pollenDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
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
	//nolint:nestif
	if !requireRegen {
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

type localCrypto struct {
	identityPubKey ed25519.PublicKey
	noisePubKey    []byte
}

func (c *localCrypto) NoisePub() []byte {
	return c.noisePubKey
}

func (c *localCrypto) IdentityPub() []byte {
	return c.identityPubKey
}
