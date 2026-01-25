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
	"time"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/link"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/tunnel"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/crypto/ed25519"
)

var _ link.LocalCrypto = (*localCrypto)(nil)

const (
	localKeysDir = "keys"

	noiseKeyName         = "noise.key"
	noisePubKeyName      = "noise.pub"
	signingKeyName       = "ed25519.key"
	signingPubKeyName    = "ed25519.pub"
	pemTypePriv          = "ED25519 PRIVATE KEY"
	pemTypePub           = "ED25519 PUBLIC KEY"
	keyDirPerm           = 0o700
	keyFilePerm          = 0o600
	punchAttemptDuration = 3 * time.Second
	loopIntervalJitter   = 0.1
	peerEventBufSize     = 64
)

type Config struct {
	PollenDir        string
	AdvertisedIPs    []string
	Port             int
	GossipInterval   time.Duration
	PeerTickInterval time.Duration
}

type Node struct {
	log            *zap.SugaredLogger
	conf           *Config
	Link           link.Link
	Store          *state.Persistence
	Peers          *peer.Store
	AdmissionStore admission.Store
	Tunnel         *tunnel.Manager
	Directory      *Directory
	crypto         *localCrypto
	peerEvents     chan peer.Input
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
		ips, err = transport.GetAdvertisableAddrs()
		if err != nil {
			return nil, err
		}
	}

	addrs := make([]string, 0, len(ips))
	for _, ip := range ips {
		addrs = append(addrs, net.JoinHostPort(ip, fmt.Sprintf("%d", conf.Port)))
	}

	crypto := &localCrypto{
		noisePubKey:    noiseKey.Public,
		identityPubKey: pubKey,
	}

	link, err := link.NewLink(conf.Port, &cs, noiseKey, crypto, invitesStore)
	if err != nil {
		return nil, err
	}

	cluster := stateStore.Cluster

	tun := tunnel.New(
		link.Send,
		&Directory{cluster: cluster},
		privKey,
		ips,
	)

	cluster.Nodes.Set(nodeID, &statev1.Node{
		Id:        nodeID.String(),
		Addresses: addrs,
		Keys: &statev1.Keys{
			NoisePub:    noiseKey.Public,
			IdentityPub: pubKey,
		},
	})

	return &Node{
		log:            log,
		Peers:          peer.NewStore(),
		Store:          stateStore,
		Link:           link,
		AdmissionStore: invitesStore,
		Tunnel:         tun,
		Directory:      &Directory{cluster: cluster},
		crypto:         crypto,
		conf:           conf,
		peerEvents:     make(chan peer.Input, peerEventBufSize),
	}, nil
}

func (n *Node) Start(ctx context.Context, token *peerv1.Invite) error {
	tickCh := make(chan struct{}, 1)
	n.registerHandlers(tickCh)

	if err := n.Link.Start(ctx); err != nil {
		return err
	}
	defer n.shutdown()

	if token != nil {
		if err := n.Link.JoinWithInvite(ctx, token); err != nil {
			return fmt.Errorf("join with invite: %w", err)
		}
	}

	trigger(tickCh)

	gossipTicker := util.NewJitterTicker(ctx, n.conf.GossipInterval, loopIntervalJitter)
	defer gossipTicker.Stop()

	peerTicker := time.NewTicker(n.conf.PeerTickInterval)
	defer peerTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-peerTicker.C:
			n.tick(ctx)
		case <-tickCh:
			n.tick(ctx)
		case <-gossipTicker.C:
			n.gossip(ctx)
		case in := <-n.Link.Events():
			n.handlePeerInput(in)
		case in := <-n.peerEvents:
			n.handlePeerInput(in)
		}
	}
}

func (n *Node) registerHandlers(reconcileCh chan<- struct{}) {
	// Set peer provider for TCP punch coordination
	n.Tunnel.SetPeerProvider(n)

	// Session handlers (multiplexed tunnels)
	n.Link.Handle(types.MsgTypeSessionRequest, n.Tunnel.HandleSessionRequest)
	n.Link.Handle(types.MsgTypeSessionResponse, n.Tunnel.HandleSessionResponse)

	// TCP punch handlers
	n.Link.Handle(types.MsgTypeTCPPunchRequest, n.Tunnel.HandlePunchRequest)
	n.Link.Handle(types.MsgTypeTCPPunchTrigger, n.Tunnel.HandlePunchTrigger)
	n.Link.Handle(types.MsgTypeTCPPunchReady, n.Tunnel.HandlePunchReady)
	n.Link.Handle(types.MsgTypeTCPPunchResponse, n.Tunnel.HandlePunchResponse)

	n.Link.Handle(types.MsgTypeGossip, func(_ context.Context, _ types.PeerKey, plaintext []byte) error {
		delta := &statev1.DeltaState{}
		if err := delta.UnmarshalVT(plaintext); err != nil {
			return fmt.Errorf("malformed gossip payload: %w", err)
		}

		n.Store.Hydrate(delta)

		trigger(reconcileCh)

		return nil
	})
}

func (n *Node) gossip(ctx context.Context) {
	delta := &statev1.DeltaState{
		Nodes: state.ToNodeDelta(n.Store.Cluster.Nodes),
	}

	payload, err := delta.MarshalVT()
	if err != nil {
		n.log.Errorf("failed to marshal gossip: %v", err)
		return
	}

	for _, k := range n.Peers.GetAll(peer.PeerStateConnected) {
		if k == n.Store.Cluster.LocalID {
			continue
		}

		if err := n.Link.Send(ctx, k, types.Envelope{
			Type:    types.MsgTypeGossip,
			Payload: payload,
		}); err != nil {
			n.log.Debugw("gossip send failed", "peer", k.String(), "err", err)
		}
	}
}

func (n *Node) handlePeerInput(in peer.Input) {
	outputs := n.Peers.Step(time.Now(), in)
	n.handleOutputs(outputs)
}

func (n *Node) tick(_ context.Context) {
	// First sync any new peers from gossip state
	n.syncPeersFromGossip()

	// Then tick the state machine to get pending actions
	outputs := n.Peers.Step(time.Now(), peer.Tick{})
	n.handleOutputs(outputs)
}

func (n *Node) syncPeersFromGossip() {
	for _, node := range n.Store.Cluster.Nodes.GetAll() {
		if node.Id == n.Store.Cluster.LocalID.String() {
			continue
		}
		if len(node.Keys.NoisePub) == 0 || len(node.Addresses) == 0 {
			continue
		}

		peerKey := types.PeerKeyFromBytes(node.Keys.NoisePub)
		n.Peers.Step(time.Now(), peer.DiscoverPeer{
			PeerKey: peerKey,
			Addrs:   node.Addresses,
		})
	}
}

func (n *Node) handleOutputs(outputs []peer.Output) {
	for _, out := range outputs {
		switch e := out.(type) {
		case peer.PeerConnected:
			n.log.Infow("peer connected", "peer_id", e.PeerKey.String()[:8])
		case peer.AttemptConnect:
			go n.attemptConnect(e)
		case peer.RequestPunchCoordination:
			// Select coordinator in main loop to avoid race on Peers.GetAll
			// TODO(saml) coordinator selection strategy - currently just picks first connected peer
			connected := n.Peers.GetAll(peer.PeerStateConnected)
			if len(connected) == 0 {
				n.log.Debugw("no coordinator available for punch", "peer", e.PeerKey.String()[:8])
				n.peerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
				continue
			}
			go n.requestPunchCoordination(e, connected[0])
		}
	}
}

func (n *Node) attemptConnect(e peer.AttemptConnect) {
	ctx, cancel := context.WithTimeout(context.Background(), punchAttemptDuration)
	defer cancel()

	if err := n.Link.EnsurePeer(ctx, e.PeerKey, e.Addrs); err != nil {
		n.log.Debugw("direct connect failed", "peer", e.PeerKey.String()[:8], "err", err)
		n.peerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
	}
}

func (n *Node) requestPunchCoordination(e peer.RequestPunchCoordination, coordinator types.PeerKey) {
	req := &peerv1.PunchCoordRequest{
		PeerId: e.PeerKey.Bytes(),
	}
	b, err := req.MarshalVT()
	if err != nil {
		n.log.Errorw("failed to marshal punch request", "err", err)
		n.peerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
		return
	}

	n.log.Infow("requesting punch coordination", "peer", e.PeerKey.String()[:8], "coordinator", coordinator.String()[:8])

	if err := n.Link.Send(context.Background(), coordinator, types.Envelope{
		Type:    types.MsgTypeUDPPunchCoordRequest,
		Payload: b,
	}); err != nil {
		n.log.Debugw("punch coord request failed", "peer", e.PeerKey.String()[:8], "err", err)
		n.peerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
		return
	}

	// Signal failure after timeout. If peer connected, ConnectPeer will be
	// processed first (via Link.Events), and connectFailed will no-op.
	time.AfterFunc(punchAttemptDuration, func() {
		n.log.Debugw("punch attempt timed out", "peer", e.PeerKey.String()[:8])
		n.peerEvents <- peer.ConnectFailed{PeerKey: e.PeerKey}
	})
}

func trigger(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (n *Node) shutdown() {
	if err := n.Store.Save(); err != nil {
		n.log.Errorf("failed to save state: %v", err)
	} else {
		n.log.Info("state saved to disk")
	}

	// Notify peers we're shutting down (fire-and-forget)
	n.Link.BroadcastDisconnect()

	if err := n.Link.Close(); err != nil {
		n.log.Errorf("failed to shut down link: %v", err)
	} else {
		n.log.Info("successfully shut down mesh")
	}
}

// GetConnectedPeers returns all currently connected peer keys.
// Implements tunnel.ConnectedPeersProvider.
func (n *Node) GetConnectedPeers() []types.PeerKey {
	return n.Peers.GetAll(peer.PeerStateConnected)
}

// GetActivePeerAddress returns the active address for a connected peer.
// Implements tunnel.ConnectedPeersProvider.
func (n *Node) GetActivePeerAddress(peerKey types.PeerKey) (string, bool) {
	return n.Link.GetActivePeerAddress(peerKey)
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
