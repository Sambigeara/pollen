package node

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"

	"github.com/flynn/noise"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/invites"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/peers"
	"go.uber.org/zap"
)

const (
	localKeysDir = "keys"

	staticKeyName = "static.key"
	staticPubName = "static.pub"
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

	noiseKey, err := genStaticKey(cs, pollenDir)
	if err != nil {
		log.Fatalf("failed to load noise key: %v", err)
	}

	mesh, err := mesh.New(&cs, noiseKey, peersStore, invitesStore, port)
	if err != nil {
		return nil, err
	}

	return &Node{
		log:  log,
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

func genStaticKey(cs noise.CipherSuite, pollenDir string) (noise.DHKey, error) {
	dir := filepath.Join(pollenDir, localKeysDir)

	staticKeyPath := filepath.Join(dir, staticKeyName)
	staticPubPath := filepath.Join(dir, staticPubName)

	requireRegen := false
	keyEnc, err := os.ReadFile(staticKeyPath)
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
		pubEnc, err = os.ReadFile(staticPubPath)
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
	if err := os.WriteFile(staticKeyPath, newKeyEnc, 0o600); err != nil {
		return noise.DHKey{}, err
	}

	newPubEnc := make([]byte, enc.EncodedLen(len(keyPair.Public)))
	enc.Encode(newPubEnc, keyPair.Public)
	if err := os.WriteFile(staticPubPath, newPubEnc, 0o644); err != nil {
		return noise.DHKey{}, err
	}

	return keyPair, nil
}
