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
	"sort"
	"sync"
	"time"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/quic"
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
	peers           *peer.Store
	store           *store.Store
	localPeerEvents chan peer.Input
	conf            *Config
	tun             *tunnel.Manager
	sock            *quic.SuperSockImpl
	log             *zap.SugaredLogger
	gossipEvents    chan *statev1.GossipNode
	identityPub     ed25519.PublicKey
	requestFullOnce sync.Once
}

func New(conf *Config) (*Node, error) {
	log := zap.S().Named("node")

	privKey, pubKey, err := genIdentityKey(conf.PollenDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load signing keys: %w", err)
	}

	stateStore, err := store.Load(conf.PollenDir, pubKey)
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
		ips, err = quic.GetAdvertisableAddrs()
		if err != nil {
			return nil, err
		}
	}

	dir := NewDirectory(stateStore)
	s := quic.NewSuperSock(conf.Port, privKey, dir, invitesStore)

	stateStore.SetLocalNetwork(ips, uint32(conf.Port))

	n := &Node{
		log:             log,
		sock:            s,
		peers:           peer.NewStore(),
		store:           stateStore,
		invites:         invitesStore,
		identityPub:     pubKey,
		conf:            conf,
		localPeerEvents: make(chan peer.Input, peerEventBufSize),
		gossipEvents:    make(chan *statev1.GossipNode, gossipEventBufSize),
	}

	n.tun = tunnel.New(dir)

	for _, svc := range stateStore.LocalServices() {
		n.tun.RegisterService(svc.GetPort())
	}

	return n, nil
}

func (n *Node) Start(ctx context.Context, token *peerv1.Invite) error {
	defer n.shutdown()

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
		case in := <-n.sock.Events():
			n.handlePeerInput(in)
		case in := <-n.localPeerEvents:
			n.handlePeerInput(in)
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
		p, err := n.sock.Recv(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			n.log.Debugw("recv failed", "err", err)
			continue
		}
		if err := n.handleApp(ctx, p); err != nil {
			n.log.Debugw("handle app failed", "err", err)
		}
	}
}

func (n *Node) handleApp(ctx context.Context, p quic.Packet) error {
	var fn HandlerFn
	switch p.Typ {
	case types.MsgTypeUDPRelay:
		fn = n.handleUDPRelay
	case types.MsgTypeGossip:
		fn = n.handleGossip
	case types.MsgTypeUDPPunchCoordRequest:
		fn = n.handlePunchCoordRequest
	case types.MsgTypeUDPPunchCoordResponse:
		fn = n.handlePunchCoordTrigger
	}
	return fn(ctx, p.Peer, p.Payload)
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
				if err := n.sendClock(context.Background(), e.PeerKey, n.store.ZeroClock()); err != nil {
					n.log.Debugw("failed requesting full state", "peer", e.PeerKey.Short(), "err", err)
				}
			})

			// Register QUIC connection as a tunnel session.
			if conn, ok := n.sock.GetConn(e.PeerKey); ok {
				if _, err := n.tun.Sessions().Register(conn, e.PeerKey); err != nil {
					n.log.Warnw("session register failed", "peer", e.PeerKey.Short(), "err", err)
				}
			}

			n.log.Infow("peer connected", "peer_id", e.PeerKey.Short(), "ip", e.IP, "observedPort", e.ObservedPort)
		case peer.AttemptConnect:
			go func() {
				if err := n.directConnect(e); err != nil {
					n.log.Debugw("direct connect failed", "peer", e.PeerKey.String()[:8], "ips", e.Ips, "port", e.Port, "err", err)
					n.localPeerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
				}
			}()
		case peer.RequestPunchCoordination:
			coordinator, ok := n.FindCoordinator(e.PeerKey)
			if !ok {
				n.log.Debugw("no coordinator available for punch", "peer", e.PeerKey.Short())
				go func() {
					n.localPeerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
				}()
				continue
			}
			go func() {
				if err := n.punchCoordination(e, coordinator); err != nil {
					n.localPeerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
				}
			}()
		}
	}
}

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

func (n *Node) directConnect(e peer.AttemptConnect) error {
	addrs := make([]*net.UDPAddr, len(e.Ips))
	for i, ip := range e.Ips {
		addrs[i] = &net.UDPAddr{
			IP:   ip,
			Port: e.Port,
		}
	}

	n.log.Debugw("attempting direct connection", "peer", e.PeerKey.Short())

	ctx, cancel := context.WithTimeout(context.Background(), directTimeout)
	defer cancel()

	return n.sock.EnsurePeer(ctx, e.PeerKey, addrs)
}

func (n *Node) punchCoordination(e peer.RequestPunchCoordination, coordinator types.PeerKey) error {
	ctx, cancel := context.WithTimeout(context.Background(), punchTimeout)
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

// handlePunchCoordRequest is called on the coordinator. It tells both peers to
// dial each other by sending PunchCoordTrigger to each.
func (n *Node) handlePunchCoordRequest(ctx context.Context, from types.PeerKey, payload []byte) error {
	req := &peerv1.PunchCoordRequest{}
	if err := req.UnmarshalVT(payload); err != nil {
		return fmt.Errorf("malformed punch coord request: %w", err)
	}

	targetKey := types.PeerKeyFromBytes(req.PeerId)

	// Look up observed addresses for both peers.
	fromAddr, fromOK := n.sock.GetActivePeerAddress(from)
	targetAddr, targetOK := n.sock.GetActivePeerAddress(targetKey)
	if !fromOK || !targetOK {
		n.log.Debugw("punch coord: missing peer address", "from_ok", fromOK, "target_ok", targetOK)
		return nil
	}

	// Tell the requester about the target's address.
	triggerA := &peerv1.PunchCoordTrigger{
		PeerId:   targetKey.Bytes(),
		PeerAddr: targetAddr.String(),
		SelfAddr: fromAddr.String(),
	}
	if b, err := triggerA.MarshalVT(); err == nil {
		_ = n.sendEnvelope(ctx, from, types.Envelope{
			Type:    types.MsgTypeUDPPunchCoordResponse,
			Payload: b,
		})
	}

	// Tell the target about the requester's address.
	triggerB := &peerv1.PunchCoordTrigger{
		PeerId:   from.Bytes(),
		PeerAddr: fromAddr.String(),
		SelfAddr: targetAddr.String(),
	}
	if b, err := triggerB.MarshalVT(); err == nil {
		_ = n.sendEnvelope(ctx, targetKey, types.Envelope{
			Type:    types.MsgTypeUDPPunchCoordResponse,
			Payload: b,
		})
	}

	n.log.Infow("coordinated punch", "from", from.Short(), "target", targetKey.Short())
	return nil
}

// handlePunchCoordTrigger is called on a peer told to initiate a simultaneous dial.
func (n *Node) handlePunchCoordTrigger(ctx context.Context, _ types.PeerKey, payload []byte) error {
	trigger := &peerv1.PunchCoordTrigger{}
	if err := trigger.UnmarshalVT(payload); err != nil {
		return fmt.Errorf("malformed punch coord trigger: %w", err)
	}

	peerKey := types.PeerKeyFromBytes(trigger.PeerId)
	peerAddr, err := net.ResolveUDPAddr("udp", trigger.PeerAddr)
	if err != nil {
		return fmt.Errorf("invalid peer addr in punch trigger: %w", err)
	}

	n.log.Infow("received punch trigger, attempting dial", "peer", peerKey.Short(), "addr", peerAddr)

	go func() {
		dialCtx, cancel := context.WithTimeout(context.Background(), punchTimeout)
		defer cancel()

		if err := n.sock.EnsurePeerPunch(dialCtx, peerKey, peerAddr); err != nil {
			n.log.Debugw("punch dial failed", "peer", peerKey.Short(), "err", err)
			n.localPeerEvents <- peer.ConnectFailed{PeerKey: peerKey}
		}
	}()

	return nil
}

// ConnectService wraps tunnel.Manager.ConnectService.
func (n *Node) ConnectService(peerID types.PeerKey, remotePort, localPort uint32) (uint32, error) {
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

	n.tun.Close()

	n.sock.BroadcastDisconnect()

	if err := n.sock.Close(); err != nil {
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
