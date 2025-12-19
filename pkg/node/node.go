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
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/tunnel"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
	"golang.org/x/crypto/ed25519"
)

var _ link.LocalCrypto = (*localCrypto)(nil)

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
	ensurePeerTimeout = 400 * time.Millisecond
)

type Config struct {
	PollenDir             string
	AdvertisedIPs         []string
	Port                  int
	GossipInterval        time.Duration
	PeerReconcileInterval time.Duration
}

type Node struct {
	log            *zap.SugaredLogger
	conf           *Config
	link           link.Link
	Store          *state.Persistence
	AdmissionStore admission.Store
	Tunnel         *tunnel.Manager
	Directory      *Directory
	crypto         *localCrypto
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

	link, err := link.NewLink(conf.Port, &cs, noiseKey, pubKey, crypto, invitesStore)
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
		Store:          stateStore,
		link:           link,
		AdmissionStore: invitesStore,
		Tunnel:         tun,
		Directory:      &Directory{cluster: cluster},
		crypto:         crypto,
		conf:           conf,
	}, nil
}

func (n *Node) Start(ctx context.Context, token *peerv1.Invite) error {
	n.registerHandlers(ctx)

	if err := n.link.Start(ctx); err != nil {
		return err
	}

	if token != nil {
		if err := n.link.JoinWithInvite(ctx, token); err != nil {
			return fmt.Errorf("join with invite: %w", err)
		}
	}

	// initial synchronous run
	go n.reconcilePeers(ctx)

	// continual watch
	go n.maintainConnections(ctx)
	go n.startGossip(ctx)

	go n.handlePeerEvents(ctx)

	<-ctx.Done()

	n.shutdown()

	return nil
}

func (n *Node) registerHandlers(ctx context.Context) {
	n.link.Handle(types.MsgTypeTCPTunnelRequest, n.Tunnel.HandleRequest)
	n.link.Handle(types.MsgTypeTCPTunnelResponse, n.Tunnel.HandleResponse)

	n.link.Handle(types.MsgTypeGossip, func(_ context.Context, _ types.PeerKey, plaintext []byte) error {
		delta := &statev1.DeltaState{}
		if err := delta.UnmarshalVT(plaintext); err != nil {
			return fmt.Errorf("malformed gossip payload: %w", err)
		}

		n.Store.Hydrate(delta)

		n.log.Debugw("gossip merged", "records", len(delta.Nodes))

		// eager reconciliation in case a new peer arrived in the gossip
		go n.reconcilePeers(ctx)

		return nil
	})
}

func (n *Node) startGossip(ctx context.Context) {
	ticker := time.NewTicker(n.conf.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.gossip(ctx)
		}
	}
}

func (n *Node) handlePeerEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-n.link.Events():
			switch ev.Kind {
			case types.PeerEventKindUp:
				id := ev.Peer
				n.Store.Cluster.Nodes.SetPlaceholder(id, &statev1.Node{
					Id:        id.String(),
					Addresses: []string{ev.Addr},
					Keys: &statev1.Keys{
						NoisePub:    ev.Peer.Bytes(),
						IdentityPub: ev.IdentityPub,
					},
				})
				n.log.Infow("peer connected", "peer_id", id.String()[:8], "addr", ev.Addr)
			case types.PeerEventKindDown:
			}
		}
	}
}

func (n *Node) gossip(ctx context.Context) {
	// TODO(saml) for now we're just gossiping to all, but down the line, this'll be much more selective
	peers := n.Store.Cluster.Nodes.GetAll()

	// we're the only peer
	if len(peers) <= 1 {
		return
	}

	delta := &statev1.DeltaState{
		Nodes: state.ToNodeDelta(n.Store.Cluster.Nodes),
	}

	payload, err := delta.MarshalVT()
	if err != nil {
		n.log.Errorf("failed to marshal gossip: %v", err)
		return
	}

	for _, peer := range peers {
		if peer.Id == n.Store.Cluster.LocalID.String() {
			continue
		}
		if err := n.link.Send(ctx, types.PeerKeyFromBytes(peer.Keys.NoisePub), types.Envelope{
			Type:    types.MsgTypeGossip,
			Payload: payload,
		}); err != nil {
			n.log.Debugw("gossip send failed", "peer", peer.Id, "err", err)
		}
	}
}

func (n *Node) maintainConnections(ctx context.Context) {
	ticker := time.NewTicker(n.conf.PeerReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.reconcilePeers(ctx)
		}
	}
}

func (n *Node) reconcilePeers(ctx context.Context) {
	for _, node := range n.Store.Cluster.Nodes.GetAll() {
		if node.Id == n.Store.Cluster.LocalID.String() {
			continue
		}

		if len(node.Keys.NoisePub) == 0 || len(node.Addresses) == 0 {
			continue
		}

		// Tie-breaker to prevent simultaneous open race conditions.
		// Only initiate if our ID is "smaller" than the peer's ID.
		// The peer with the "larger" ID will wait for the incoming handshake.
		// TODO(saml) this requires a much more balanced and robust approach
		if n.Store.Cluster.LocalID.String() > node.Id {
			continue
		}

		dialCtx, cancel := context.WithTimeout(ctx, ensurePeerTimeout)
		err := n.link.EnsurePeer(dialCtx, types.PeerKeyFromBytes(node.Keys.NoisePub), node.Addresses)
		cancel()

		// Ignore timeouts; next reconcile tick will retry.
		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			n.log.Debugw("ensure peer failed", "peer", node.Id, "err", err)
		}
	}
}

func (n *Node) shutdown() {
	if err := n.Store.Save(); err != nil {
		n.log.Errorf("failed to save state: %v", err)
	} else {
		n.log.Info("state saved to disk")
	}

	if err := n.link.Close(); err != nil {
		n.log.Errorf("failed to shut down link: %v", err)
	} else {
		n.log.Info("successfully shut down mesh")
	}
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
