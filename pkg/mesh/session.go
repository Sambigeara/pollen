package mesh

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/flynn/noise"
	"google.golang.org/protobuf/proto"

	cryptov1 "github.com/sambigeara/pollen/api/genpb/pollen/crypto/v1"
)

const (
	staticKeyFile = "static_keys.pb"

	// Noise enforces a MaxNonce of 2^64-1. We set a "slightly" lower limit.
	maxNonce = uint32(math.MaxUint32) - 1
)

type sessionStore struct {
	idSessMap map[uint32]*session
	keyIDMap  map[string]uint32
	mu        sync.RWMutex
}

func newSessionStore() *sessionStore {
	return &sessionStore{
		idSessMap: make(map[uint32]*session),
		keyIDMap:  make(map[string]uint32),
	}
}

func (s *sessionStore) set(staticKey []byte, sessionID uint32, conn *session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := hex.EncodeToString(staticKey)
	if prevSessID, ok := s.keyIDMap[k]; ok {
		delete(s.idSessMap, prevSessID)
	}

	s.keyIDMap[k] = sessionID
	s.idSessMap[sessionID] = conn
}

func (s *sessionStore) get(peerID uint32) (*session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sess, ok := s.idSessMap[peerID]
	return sess, ok
}

type pingFn func(uint32) error

type session struct {
	peerAddr *net.UDPAddr
	send     *noise.CipherState
	recv     *noise.CipherState
}

func newSession(peerAddr *net.UDPAddr, send, recv *noise.CipherState) *session {
	return &session{
		peerAddr: peerAddr,
		send:     send,
		recv:     recv,
	}
}

func (s *session) Decrypt(msg []byte) ([]byte, bool, error) {
	pt, err := s.recv.Decrypt(nil, nil, msg)
	if err != nil {
		return nil, false, err
	}

	return pt, s.shouldRekey(), nil
}

func (s *session) Encrypt(conn *UDPConn, msg []byte) ([]byte, bool, error) {
	enc, err := s.send.Encrypt(nil, nil, msg)
	if err != nil {
		return nil, false, err
	}

	return enc, s.shouldRekey(), nil
}

func (s *session) shouldRekey() bool {
	return s.send.Nonce() >= uint64(maxNonce)
}

func GenStaticKey(cs noise.CipherSuite, dir string) (*noise.DHKey, error) {
	path := filepath.Join(dir, staticKeyFile)

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
