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
	"github.com/sambigeara/pollen/pkg/invites"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/tunnel"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
	"golang.org/x/crypto/ed25519"
)

const (
	localKeysDir = "keys"

	noiseKeyName      = "noise.key"
	noisePubKeyName   = "noise.pub"
	signingKeyName    = "ed25519.key"
	signingPubKeyName = "ed25519.pub"
	pemTypePriv       = "ED25519 PRIVATE KEY"
	pemTypePub        = "ED25519 PUBLIC KEY"
)

type Config struct {
	Mesh                  *mesh.Config
	GossipInterval        time.Duration
	PeerReconcileInterval time.Duration
	PollenDir             string
}

type Node struct {
	log            *zap.SugaredLogger
	conf           *Config
	Mesh           *mesh.Mesh
	Store          *state.Persistence
	Invites        *invites.InviteStore
	Tunnel         *tunnel.Manager
	Directory      *Directory
	identityPubKey ed25519.PublicKey
	noisePubKey    []byte
}

func New(conf *Config) (*Node, error) {
	log := zap.S().Named("node")

	cs := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)

	noiseKey, err := genNoiseKey(cs, conf.PollenDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load noise key: %v", err)
	}
	nodeID, _ := types.IDFromBytes(noiseKey.Public)

	privKey, pubKey, err := genIdentityKey(conf.PollenDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load signing keys: %v", err)
	}

	stateStore, err := state.Load(conf.PollenDir, nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	invitesStore, err := invites.Load(conf.PollenDir)
	if err != nil {
		log.Error("failed to load peers", zap.Error(err))
		return nil, err
	}

	m, err := mesh.New(conf.Mesh, &cs, noiseKey, privKey, pubKey, invitesStore)
	if err != nil {
		return nil, err
	}

	// TODO(saml) this seems to be everywhere. Needs to be managed on the highest common layer and propogated down
	ips, err := mesh.GetAdvertisableIPs()
	if err != nil {
		return nil, err
	}

	cluster := stateStore.Cluster

	tun := tunnel.New(
		m.Send,
		Directory{cluster: cluster},
		privKey,
		ips,
	)

	m.On(mesh.MessageTypeTCPTunnelRequest, tun.HandleRequest)
	m.On(mesh.MessageTypeTCPTunnelResponse, tun.HandleResponse)

	m.On(mesh.MessageTypeGossip, func(peerNoisePub []byte, plaintext []byte) error {
		delta := &statev1.DeltaState{}
		if err := delta.UnmarshalVT(plaintext); err != nil {
			return fmt.Errorf("malformed gossip payload: %w", err)
		}

		stateStore.Hydrate(delta)

		log.Debugw("gossip merged", "nodes", len(delta.Nodes))
		return nil
	})

	m.SetPeerJoinHandler(func(noiseKey, identityKey []byte, addr *net.UDPAddr) {
		id, _ := types.IDFromBytes(noiseKey)
		cluster.Nodes.SetPlaceholder(id, &statev1.Node{
			Id:        id.String(),
			Addresses: []string{addr.String()},
			Keys: &statev1.Keys{
				NoisePub:    noiseKey,
				IdentityPub: identityKey,
			},
		})

		log.Infow("peer added to state via handshake", "peer_id", id[:8])
	})

	cluster.Nodes.Set(nodeID, &statev1.Node{
		Id:        nodeID.String(),
		Addresses: m.GetAdvertisableAddrs(),
		Keys: &statev1.Keys{
			NoisePub:    noiseKey.Public,
			IdentityPub: pubKey,
		},
	})

	return &Node{
		log:            log,
		Mesh:           m,
		Store:          stateStore,
		Invites:        invitesStore,
		Tunnel:         tun,
		Directory:      &Directory{cluster: cluster},
		identityPubKey: pubKey,
		noisePubKey:    noiseKey.Public,
		conf:           conf,
	}, nil
}

func (n *Node) Start(ctx context.Context, token *peerv1.Invite) error {
	if err := n.Mesh.Start(ctx, token); err != nil {
		return err
	}

	// initial synchronous run
	n.reconcilePeers()

	// continual watch
	go n.maintainConnections(ctx)
	go n.startGossip(ctx)

	<-ctx.Done()

	n.shutdown()

	return nil
}

func (n *Node) startGossip(ctx context.Context) {
	ticker := time.NewTicker(n.conf.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.gossip()
		}
	}
}

func (n *Node) gossip() {
	// TODO(saml) for now we're just gossiping to all, but down the line, this'll be much more selective
	peers := n.Store.Cluster.Nodes.GetAll()

	// we're the only peer
	if len(peers) == 1 {
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
		n.Mesh.Send(peer.Keys.NoisePub, payload, mesh.MessageTypeGossip)
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
			n.reconcilePeers()
		}
	}
}

func (n *Node) reconcilePeers() {
	for _, node := range n.Store.Cluster.Nodes.GetAll() {
		if node.Id == n.Store.Cluster.LocalID.String() {
			continue
		}

		if len(node.Keys.NoisePub) == 0 || len(node.Addresses) == 0 {
			continue
		}

		if err := n.Mesh.RejoinPeer(node); err != nil {
			continue
		}
	}
}

func (n *Node) shutdown() {
	if err := n.Store.Save(); err != nil {
		n.log.Errorf("failed to save state: %v", err)
	} else {
		n.log.Info("state saved to disk")
	}

	if err := n.Mesh.Shutdown(); err != nil {
		n.log.Errorf("failed to shut down mesh: %v", err)
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

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return noise.DHKey{}, err
	}

	keyPair, err := cs.GenerateKeypair(rand.Reader)
	if err != nil {
		return noise.DHKey{}, err
	}

	newKeyEnc := make([]byte, enc.EncodedLen(len(keyPair.Private)))
	enc.Encode(newKeyEnc, keyPair.Private)
	if err := os.WriteFile(keyPath, newKeyEnc, 0o600); err != nil {
		return noise.DHKey{}, err
	}

	newPubEnc := make([]byte, enc.EncodedLen(len(keyPair.Public)))
	enc.Encode(newPubEnc, keyPair.Public)
	if err := os.WriteFile(pubKeyPath, newPubEnc, 0o644); err != nil {
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

	if err := os.MkdirAll(dir, 0o700); err != nil {
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

	pem.Encode(privFile, &pem.Block{
		Type:  pemTypePriv,
		Bytes: priv.Seed(),
	})

	pubFile, err := os.Create(pubPath)
	if err != nil {
		return nil, nil, err
	}
	defer pubFile.Close()

	pem.Encode(pubFile, &pem.Block{
		Type:  pemTypePub,
		Bytes: pub,
	})

	return priv, pub, err
}

func (n *Node) GetStateKeys() *statev1.Keys {
	return &statev1.Keys{
		NoisePub:    n.noisePubKey,
		IdentityPub: n.identityPubKey,
	}
}
