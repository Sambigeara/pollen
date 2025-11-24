package node

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/invites"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/peers"
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

type Node struct {
	log  *zap.SugaredLogger
	mesh *mesh.Mesh
}

func New(port int, pollenDir string) (*Node, error) {
	log := zap.S().Named("node")

	peersStore, err := peers.Load(pollenDir)
	if err != nil {
		log.Error("failed to load peers", zap.Error(err))
		return nil, err
	}

	invitesStore, err := invites.Load(pollenDir)
	if err != nil {
		log.Error("failed to load peers", zap.Error(err))
		return nil, err
	}

	cs := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)

	noiseKey, err := genNoiseKey(cs, pollenDir)
	if err != nil {
		log.Fatalf("failed to load noise key: %v", err)
	}

	privKey, pubKey, err := genSigningKey(pollenDir)
	if err != nil {
		log.Fatalf("failed to load signing keys: %v", err)
	}

	mesh, err := mesh.New(&cs, noiseKey, privKey, pubKey, peersStore, invitesStore, port)
	if err != nil {
		return nil, err
	}

	return &Node{
		mesh: mesh,
	}, nil
}

func (m *Node) Start(ctx context.Context, token *peerv1.Invite) error {
	if err := m.mesh.Start(ctx, token); err != nil {
		return err
	}

	<-ctx.Done()

	return m.shutdown(ctx)
}

func (m *Node) shutdown(ctx context.Context) error {
	if err := m.mesh.Shutdown(ctx); err != nil {
		return err
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
