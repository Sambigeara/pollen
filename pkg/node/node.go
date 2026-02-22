package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
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
	localKeysDir = "keys"

	signingKeyName    = "ed25519.key"
	signingPubKeyName = "ed25519.pub"
	pemTypePriv       = "ED25519 PRIVATE KEY"
	pemTypePub        = "ED25519 PUBLIC KEY"
	keyDirPerm        = 0o700

	directTimeout = 2 * time.Second
	punchTimeout  = 3 * time.Second

	loopIntervalJitter = 0.1
	peerEventBufSize   = 64
	gossipEventBufSize = 64
	ipRefreshInterval  = 5 * time.Minute
)

type BootstrapPeer struct {
	PeerKey types.PeerKey
	Addrs   []string
}

type Config struct {
	AdvertisedIPs       []string
	Port                int
	GossipInterval      time.Duration
	PeerTickInterval    time.Duration
	GossipJitter        float64
	DisableGossipJitter bool
	BootstrapPeers      []BootstrapPeer
}

type HandlerFn func(ctx context.Context, from types.PeerKey, payload []byte) error

type punchRequest struct {
	peerKey types.PeerKey
	ip      net.IP
	port    int
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
	gossipEvents    chan *statev1.GossipNode
	requestFullOnce sync.Once
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

	m, err := mesh.NewMesh(conf.Port, privKey, creds)
	if err != nil {
		log.Error("failed to load mesh", zap.Error(err))
		return nil, err
	}

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
		conf:            conf,
		localPeerEvents: make(chan peer.Input, peerEventBufSize),
		punchCh:         make(chan punchRequest, 16),
		gossipEvents:    make(chan *statev1.GossipNode, gossipEventBufSize),
	}

	return n, nil
}

func (n *Node) Start(ctx context.Context) error {
	defer n.shutdown()

	if err := n.mesh.Start(ctx); err != nil {
		return err
	}
	n.connectBootstrapPeers(ctx)
	n.tun.Start(ctx)
	go n.recvLoop(ctx)
	go n.punchLoop(ctx)

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
		case node := <-n.gossipEvents:
			n.broadcastNode(ctx, node)
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

func (n *Node) queueGossipNode(node *statev1.GossipNode) {
	if node == nil {
		return
	}

	select {
	case n.gossipEvents <- node:
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
		for _, node := range n.store.MissingFor(body.Clock) {
			if err := n.mesh.Send(ctx, from, &meshv1.Envelope{
				Body: &meshv1.Envelope_Node{Node: node},
			}); err != nil {
				n.log.Debugw("failed sending gossip node", "to", from.Short(), "err", err)
			}
		}
	case *meshv1.Envelope_Node:
		result := n.store.ApplyNode(body.Node)
		if result.Rebroadcast != nil {
			n.queueGossipNode(result.Rebroadcast)
		}
	case *meshv1.Envelope_PunchCoordRequest:
		n.handlePunchCoordRequest(ctx, from, body.PunchCoordRequest)
	case *meshv1.Envelope_PunchCoordTrigger:
		n.handlePunchCoordTrigger(body.PunchCoordTrigger)
	}
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
		if err := n.mesh.Send(ctx, peerID, &meshv1.Envelope{
			Body: &meshv1.Envelope_Node{Node: node},
		}); err != nil {
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

	gossipNode := n.store.SetLocalNetwork(newIPs, uint32(n.conf.Port))
	if gossipNode != nil {
		n.log.Infow("advertised IPs changed", "ips", newIPs)
		n.queueGossipNode(gossipNode)
	}
}

func (n *Node) handlePeerInput(in peer.Input) {
	if d, ok := in.(peer.PeerDisconnected); ok {
		n.queueGossipNode(n.store.SetLocalConnected(d.PeerKey, false))
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

		n.peers.Step(time.Now(), peer.DiscoverPeer{
			PeerKey: kp.PeerID,
			Ips:     ips,
			Port:    int(kp.LocalPort),
		})
	}
}

func (n *Node) handleOutputs(outputs []peer.Output) {
	for _, out := range outputs {
		switch e := out.(type) {
		case peer.PeerConnected:
			n.queueGossipNode(n.store.SetLocalConnected(e.PeerKey, true))
			n.requestFullOnce.Do(func() {
				if err := n.mesh.Send(context.Background(), e.PeerKey, &meshv1.Envelope{
					Body: &meshv1.Envelope_Clock{Clock: n.store.ZeroClock()},
				}); err != nil {
					n.log.Debugw("failed requesting full state", "peer", e.PeerKey.Short(), "err", err)
				}
			})

			n.log.Infow("peer connected", "peer_id", e.PeerKey.Short(), "ip", e.IP, "observedPort", e.ObservedPort)
		case peer.AttemptConnect:
			go func() {
				if err := n.connectPeer(e.PeerKey, e.Ips, e.Port); err != nil {
					if n.peers.InState(e.PeerKey, peer.PeerStateConnecting) {
						n.log.Debugw("connect failed", "peer", e.PeerKey.Short(), "err", err)
						n.localPeerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
					}
				}
			}()
		case peer.RequestPunchCoordination:
			go n.requestPunchCoordination(e.PeerKey)
		}
	}
}

func (n *Node) reconcileConnections() {
	connections := n.tun.ListConnections()

	missing := make(map[types.PeerKey]map[uint32]struct{}, len(connections))
	for _, conn := range connections {
		node, ok := n.store.Get(conn.PeerID)
		if !ok {
			continue
		}
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

	for _, desiredConn := range desired {
		if _, ok := existing[desiredConnectionKey(desiredConn.PeerID, desiredConn.RemotePort, desiredConn.LocalPort)]; ok {
			continue
		}

		node, ok := n.store.Get(desiredConn.PeerID)
		if !ok || !hasServicePort(node.Services, desiredConn.RemotePort) {
			continue
		}

		if _, err := n.ConnectService(desiredConn.PeerID, desiredConn.RemotePort, desiredConn.LocalPort); err != nil {
			n.log.Debugw("failed restoring desired connection", "peer", desiredConn.PeerID.Short(), "remotePort", desiredConn.RemotePort, "localPort", desiredConn.LocalPort, "err", err)
		}
	}
}

func hasServicePort(services map[string]*statev1.Service, port uint32) bool {
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

func (n *Node) connectPeer(peerKey types.PeerKey, ips []net.IP, port int) error {
	addrs := make([]*net.UDPAddr, 0, len(ips))
	for _, ip := range ips {
		if ip != nil {
			addrs = append(addrs, &net.UDPAddr{IP: ip, Port: port})
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), directTimeout)
	defer cancel()

	return n.mesh.Connect(ctx, peerKey, addrs)
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
			if n.peers.InState(req.peerKey, peer.PeerStateConnected) {
				continue
			}
			if err := n.punchPeer(req.peerKey, req.ip, req.port); err != nil {
				if n.peers.InState(req.peerKey, peer.PeerStateConnecting) {
					n.log.Debugw("punch failed", "peer", req.peerKey.Short(), "err", err)
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
