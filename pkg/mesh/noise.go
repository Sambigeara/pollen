package mesh

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/bluele/gcache"
	"github.com/flynn/noise"
	"google.golang.org/protobuf/proto"

	cryptov1 "github.com/sambigeara/pollen/api/genpb/pollen/crypto/v1"
)

const (
	noiseStaticFile             = "noise_static.pb"
	noiseSessionRefreshInterval = time.Second * 120                // new IK handshake every 2 mins
	noiseSessionDuration        = noiseSessionRefreshInterval + 10 // give 10 second overlap to deal with inflight packets
	maxConnections              = 256
)

type cipherStore struct {
	c gcache.Cache
}

func newCipherStore() *cipherStore {
	return &cipherStore{
		c: gcache.New(maxConnections).
			LRU().
			Expiration(noiseSessionDuration).
			EvictedFunc(func(key, value any) {
				// TODO(saml)
				fmt.Printf("expired key=%v value=%v\n", key, value)
			}).
			Build(),
	}
}

func (s *cipherStore) set(peerID uint32, conn *noiseConn) error {
	if err := s.c.Set(peerID, conn); err != nil {
		return err
	}
	return nil
}


type noiseConn struct {
	createdAt     time.Time
	peerAddr      *net.UDPAddr
	send          *noise.CipherState
	recv          *noise.CipherState
	peerStaticKey []byte
}

func newNoiseConn(peerAddr *net.UDPAddr, peerStaticKey []byte, send, recv *noise.CipherState) *noiseConn {
	return &noiseConn{
		createdAt:     time.Now(),
		peerAddr:      peerAddr,
		peerStaticKey: peerStaticKey,
		send:          send,
		recv:          recv,
	}
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
