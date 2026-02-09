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
	"github.com/sambigeara/pollen/pkg/state"
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

	directAttemptDuration    = 2 * time.Second
	punchCoordRequestTimeout = 3 * time.Second

	loopIntervalJitter = 0.1
	peerEventBufSize   = 64
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
	log  *zap.SugaredLogger
	conf *Config

	crypto  *localCrypto
	invites admission.Store
	peers   *peer.Store
	sock    sock.SuperSock
	storage *state.Persistence
	tun     *tunnel.Manager

	localPeerEvents chan peer.Input
	gossipTrigger   chan struct{}
	handlers        map[types.MsgType]HandlerFn
	handlersMu      sync.RWMutex
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

	stateStore, err := state.Load(conf.PollenDir, nodeID)
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

	cluster := stateStore.Cluster

	tun := tunnel.New(
		s.Send,
		&Directory{cluster: cluster},
		privKey,
		ips,
	)

	cluster.Nodes.Set(nodeID, &statev1.Node{
		Id:        nodeID.String(),
		Ips:       ips,
		LocalPort: uint32(conf.Port),
		Keys: &statev1.Keys{
			NoisePub:    noiseKey.Public,
			IdentityPub: pubKey,
		},
	})

	return &Node{
		log:             log,
		sock:            s,
		handlers:        make(map[types.MsgType]HandlerFn),
		peers:           peer.NewStore(),
		storage:         stateStore,
		invites:         invitesStore,
		tun:             tun,
		crypto:          crypto,
		conf:            conf,
		localPeerEvents: make(chan peer.Input, peerEventBufSize),
		gossipTrigger:   make(chan struct{}),
	}, nil
}

func (n *Node) Start(ctx context.Context, token *peerv1.Invite) error {
	defer n.shutdown()

	n.registerHandlers()

	// for _, node := range n.storage.Cluster.Nodes.GetAll() {
	// 	n.log.Debugw("Initial node state", "isSelf", node.Id == n.storage.Cluster.LocalID.String(), "node", node)
	// }

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

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-peerTicker.C:
			n.tick()
		case <-gossipTicker.C:
			n.gossip(ctx)
		case <-n.gossipTrigger:
			n.gossip(ctx)
		// external origin events
		case in := <-n.sock.Events():
			n.handlePeerInput(in)
		// internal origin events
		case in := <-n.localPeerEvents:
			n.handlePeerInput(in)
		}
	}
}

// TODO(saml) this is a result of an LLM scaffolded project and will definitely be removed.
func (n *Node) triggerGossip() {
	select {
	case n.gossipTrigger <- struct{}{}:
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
	// Set peer provider for TCP punch coordination
	n.tun.SetPeerProvider(n)

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

	n.Handle(types.MsgTypeGossip, func(_ context.Context, peer types.PeerKey, plaintext []byte) error {
		delta := &statev1.DeltaState{}
		if err := delta.UnmarshalVT(plaintext); err != nil {
			return fmt.Errorf("malformed gossip payload: %w", err)
		}

		n.log.Debugw("received gossip", "from", peer.Short(), "nodeRecords", len(delta.Nodes))

		n.storage.Hydrate(delta)
		return nil
	})
}

func (n *Node) gossip(ctx context.Context) {
	// TODO(saml) this is a hack because I've realised that I was exceeding MTU and packets were being dropped on CGNAT (mobile)
	for k, v := range state.ToNodeDelta(n.storage.Cluster.Nodes) {
		delta := &statev1.DeltaState{
			Nodes: map[string]*statev1.NodeRecord{k: v},
		}

		payload, err := delta.MarshalVT()
		if err != nil {
			n.log.Errorf("failed to marshal gossip: %v", err)
			return
		}

		connectedPeers := n.GetConnectedPeers()
		connectedPeerKeys := make([]string, 0, len(connectedPeers))
		for _, peerKey := range connectedPeers {
			connectedPeerKeys = append(connectedPeerKeys, peerKey.Short())
		}
		sort.Strings(connectedPeerKeys)

		stateNodes := n.storage.Cluster.Nodes.GetAll()
		stateEntries := make([]string, 0, len(stateNodes))
		for key, node := range stateNodes {
			stateEntries = append(stateEntries, fmt.Sprintf("k=%s id=%s noise=%s", key.Short(), shortNodeID(node.GetId()), shortNodeKey(node.GetKeys())))
		}
		sort.Strings(stateEntries)

		for _, k := range connectedPeers {
			if k == n.storage.Cluster.LocalID {
				continue
			}

			if err := n.sock.Send(ctx, k, types.Envelope{
				Type:    types.MsgTypeGossip,
				Payload: payload,
			}); err != nil {
				n.log.Debugw("gossip send failed", "peer", k.Short(), "err", err)
			}
		}
	}
}

func (n *Node) handlePeerInput(in peer.Input) {
	outputs := n.peers.Step(time.Now(), in)
	n.handleOutputs(outputs)
}

func (n *Node) tick() {
	// First sync any new peers and connections from gossip state
	n.syncPeersFromState()
	n.reconcileConnections()

	// Then tick the state machine to get pending actions
	outputs := n.peers.Step(time.Now(), peer.Tick{})
	n.handleOutputs(outputs)
}

func (n *Node) syncPeersFromState() {
	for _, node := range n.storage.Cluster.Nodes.GetAll() {
		if node.Id == n.storage.Cluster.LocalID.String() {
			continue
		}
		// if node.Id[:8] != "5d9019cc" && node.Id[:8] != "e916928b" {
		// 	continue
		// }
		if len(node.Keys.NoisePub) == 0 || len(node.Ips) == 0 {
			continue
		}

		ips := make([]net.IP, len(node.Ips))
		for i, ipStr := range node.Ips {
			ip := net.ParseIP(ipStr)
			if ip == nil {
				n.log.Error("unable to parse IP")
				continue
			}
			ips[i] = ip
		}

		peerKey := types.PeerKeyFromBytes(node.Keys.NoisePub)
		if node.Id != "" && node.Id != peerKey.String() {
			n.log.Debugw("state identity mismatch", "nodeId", shortNodeID(node.Id), "noiseKey", peerKey.Short(), "ips", node.Ips, "localPort", node.LocalPort)
		}

		n.peers.Step(time.Now(), peer.DiscoverPeer{
			PeerKey: peerKey,
			Ips:     ips,
			Port:    int(node.LocalPort),
		})
	}
}

func (n *Node) handleOutputs(outputs []peer.Output) {
	for _, out := range outputs {
		switch e := out.(type) {
		case peer.PeerConnected:
			n.storage.ConnectNode(e.PeerKey)
			n.triggerGossip()
			n.log.Infow("peer connected", "peer_id", e.PeerKey.Short(), "ip", e.IP, "observedPort", e.ObservedPort)
		case peer.AttemptConnect:
			go func() {
				if err := n.attemptDirectConnect(e); err != nil {
					n.log.Debugw("direct connect failed", "peer", e.PeerKey.String()[:8], "ips", e.Ips, "port", e.Port, "err", err)
					n.localPeerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
				}
			}()
		case peer.RequestPunchCoordination:
			// Select coordinator in main loop to avoid race on Peers.GetAll
			// TODO(saml) coordinator selection strategy - currently just picks first connected peer
			connected := n.GetConnectedPeers()
			if len(connected) == 0 {
				n.log.Debugw("no coordinator available for punch", "peer", e.PeerKey.String()[:8])
				continue
			}
			go func() {
				if err := n.requestPunchCoordination(e, connected[0]); err != nil {
					n.localPeerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
				}
			}()
			// for _, key := range n.GetConnectedPeers() {
			// 	if p, ok := n.Store.Cluster.Nodes.Get(key); ok {
			// 		if _, ok := p.Node.Connected[e.PeerKey.String()]; ok {
			// 			go n.requestPunchCoordination(e, key)
			// 			continue
			// 		}
			// 	}
			// }
			// n.log.Debugw("no coordinator available for punch", "peer", e.PeerKey.String()[:8])
		}
	}
}

func (n *Node) reconcileConnections() {
	connections := n.tun.ListConnections()
	if len(connections) == 0 {
		return
	}

	nodes := n.storage.Cluster.Nodes.GetAll()
	missing := make(map[types.PeerKey]map[uint32]struct{}, len(connections))
	for _, conn := range connections {
		node := nodes[conn.PeerID]
		if node == nil || !hasServicePort(node, conn.RemotePort) {
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
		}
	}
}

func hasServicePort(node *statev1.Node, port uint32) bool {
	for _, svc := range node.Services {
		if svc.GetPort() == port {
			return true
		}
	}
	return false
}

func (n *Node) attemptDirectConnect(e peer.AttemptConnect) error {
	addrs := make([]*net.UDPAddr, len(e.Ips))
	for i, ip := range e.Ips {
		addrs[i] = &net.UDPAddr{
			IP:   ip,
			Port: e.Port,
		}
	}

	n.log.Debugw("attempting direct connection", "peer", e.PeerKey.Short())

	ctx, cancel := context.WithTimeout(context.Background(), directAttemptDuration)
	defer cancel()

	return n.sock.EnsurePeer(ctx, e.PeerKey, addrs, false)
}

func (n *Node) requestPunchCoordination(e peer.RequestPunchCoordination, coordinator types.PeerKey) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), punchCoordRequestTimeout)
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

	if err := n.sock.Send(ctx, coordinator, types.Envelope{
		Type:    types.MsgTypeUDPPunchCoordRequest,
		Payload: b,
	}); err != nil {
		n.log.Debugw("punch coord request failed", "peer", e.PeerKey.String()[:8], "err", err)
		return err
	}

	return nil
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
	if err := n.storage.Save(); err != nil {
		n.log.Errorf("failed to save state: %v", err)
	} else {
		n.log.Info("state saved to disk")
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

// GetConnectedPeers returns all currently connected peer keys.
// Implements tunnel.ConnectedPeersProvider.
func (n *Node) GetConnectedPeers() []types.PeerKey {
	return n.peers.GetAll(peer.PeerStateConnected)
}

// GetActivePeerAddress returns the active address for a connected peer.
// Implements tunnel.ConnectedPeersProvider.
func (n *Node) GetActivePeerAddress(peerKey types.PeerKey) (*net.UDPAddr, bool) {
	return n.sock.GetActivePeerAddress(peerKey)
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

func (c *localCrypto) GetStateKeys() *statev1.Keys {
	return &statev1.Keys{
		NoisePub:    c.noisePubKey,
		IdentityPub: c.identityPubKey,
	}
}

func (c *localCrypto) NoisePub() []byte {
	return c.noisePubKey
}

func (c *localCrypto) IdentityPub() []byte {
	return c.identityPubKey
}

func shortNodeID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func shortNodeKey(keys *statev1.Keys) string {
	if keys == nil || len(keys.NoisePub) == 0 {
		return "<missing>"
	}
	return types.PeerKeyFromBytes(keys.NoisePub).Short()
}
