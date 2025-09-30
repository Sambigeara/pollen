package node

import (
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"

	"github.com/flynn/noise"
	"google.golang.org/protobuf/proto"

	cryptov1 "github.com/sambigeara/pollen/api/genpb/pollen/crypto/v1"
)

const noiseKeyFile = "noise.key"

var handshakePrologue = []byte("pollenv1")

func GenLocalStaticKey(cs noise.CipherSuite, dir string) (noise.DHKey, error) {
	path := filepath.Join(dir, noiseKeyFile)

	data, err := os.ReadFile(path)
	if err == nil {
		k := &cryptov1.NoiseStaticKey{}
		if err := proto.Unmarshal(data, k); err != nil {
			return noise.DHKey{}, err
		}

		return noise.DHKey{Private: k.Priv, Public: k.Pub}, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return noise.DHKey{}, err
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return noise.DHKey{}, err
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return noise.DHKey{}, err
	}
	defer f.Close()

	key, err := cs.GenerateKeypair(rand.Reader)
	if err != nil {
		return noise.DHKey{}, err
	}

	protoKey := &cryptov1.NoiseStaticKey{
		Priv: key.Private,
		Pub:  key.Public,
	}

	b, err := proto.Marshal(protoKey)
	if err != nil {
		return noise.DHKey{}, err
	}

	if _, err := f.Write(b); err != nil {
		return noise.DHKey{}, err
	}

	return key, nil
}
