package node

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/invites"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/state"
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
	Mesh      *mesh.Config
	PollenDir string
}

type Node struct {
	log           *zap.SugaredLogger
	conf          *Config
	Mesh          *mesh.Mesh
	Store         *state.Persistence
	Invites       *invites.InviteStore
	signingPubKey ed25519.PublicKey
	noisePubKey   []byte
}

func New(conf *Config) (*Node, error) {
	log := zap.S().Named("node")

	cs := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)

	noiseKey, err := genNoiseKey(cs, conf.PollenDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load noise key: %v", err)
	}
	nodeID := hex.EncodeToString(noiseKey.Public)

	privKey, pubKey, err := genSigningKey(conf.PollenDir)
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

	cluster := stateStore.Cluster

	m, err := mesh.New(&cs, noiseKey, privKey, pubKey, cluster, invitesStore, conf.Mesh)
	if err != nil {
		return nil, err
	}

	m.SetGossipHandler(func(payload []byte) error {
		delta := &statev1.DeltaState{}
		if err := delta.UnmarshalVT(payload); err != nil {
			return fmt.Errorf("malformed gossip payload: %w", err)
		}

		stateStore.Hydrate(delta)

		log.Debugw("gossip merged", "nodes", len(delta.Nodes))
		return nil
	})

	cluster.Nodes.Set(nodeID, &statev1.Node{
		Id: nodeID,
		// Name:      "local-node", // TODO(saml) Allow configuring hostname/alias
		Addresses: m.GetAdvertisableAddrs(),
		Keys: &statev1.Keys{
			NoisePub:    noiseKey.Public,
			IdentityPub: pubKey,
		},
	})

	return &Node{
		log:           log,
		Mesh:          m,
		Store:         stateStore,
		Invites:       invitesStore,
		signingPubKey: pubKey,
		noisePubKey:   noiseKey.Public,
	}, nil
}

func (n *Node) Start(ctx context.Context, token *peerv1.Invite) error {
	if err := n.Mesh.Start(ctx, token); err != nil {
		return err
	}

	<-ctx.Done()

	return n.shutdown(ctx)
}

func (n *Node) shutdown(ctx context.Context) error {
	if err := n.Store.Save(); err != nil {
		n.log.Errorf("failed to save state: %v", err)
	} else {
		n.log.Info("state saved to disk")
	}

	if err := n.Mesh.Shutdown(ctx); err != nil {
		n.log.Errorf("failed to shut down mesh: %v", err)
	} else {
		n.log.Info("successfully shut down mesh")
	}

	return ctx.Err()
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

func genSigningKey(pollenDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
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
		IdentityPub: n.signingPubKey,
	}
}
