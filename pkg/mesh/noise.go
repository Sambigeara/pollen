package mesh

import (
	"crypto/rand"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/flynn/noise"
	"google.golang.org/protobuf/proto"

	cryptov1 "github.com/sambigeara/pollen/api/genpb/pollen/crypto/v1"
)

const noiseStaticFile = "noise_static.pb"

// TODO(saml) implement send.Rekey() on both every 1<<20 bytes or 1000 messages or whatever Wireguards standard is.
type noiseConn struct {
	peerAddr *net.UDPAddr
	send     *noise.CipherState
	recv     *noise.CipherState
}

type cipherStateStore struct {
	m  map[uint32]*noiseConn
	mu sync.RWMutex
}

func newCipherStateStore() *cipherStateStore {
	return &cipherStateStore{
		m: make(map[uint32]*noiseConn),
	}
}

func (s *cipherStateStore) set(peerID uint32, conn *noiseConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[peerID] = conn
}

func (s *cipherStateStore) get(peerID uint32) (*noiseConn, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.m[peerID]
	return c, ok
}

func GenStaticKey(cs noise.CipherSuite, dir string) (*noise.DHKey, error) {
	path := filepath.Join(dir, noiseStaticFile)

	data, err := os.ReadFile(path)
	if err == nil {
		k := &cryptov1.NoiseStaticKey{}
		if err := proto.Unmarshal(data, k); err != nil {
			return nil, err
		}

		return &noise.DHKey{Private: k.Priv, Public: k.Pub}, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	key, err := cs.GenerateKeypair(rand.Reader)
	if err != nil {
		return nil, err
	}

	protoKey := &cryptov1.NoiseStaticKey{
		Priv: key.Private,
		Pub:  key.Public,
	}

	b, err := proto.Marshal(protoKey)
	if err != nil {
		return nil, err
	}

	if _, err := f.Write(b); err != nil {
		return nil, err
	}

	return &key, nil
}
