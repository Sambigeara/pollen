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
	"time"

	"github.com/flynn/noise"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	cryptov1 "github.com/sambigeara/pollen/api/genpb/pollen/crypto/v1"
)

const (
	staticKeyFile          = "static_keys.pb"
	sessionRefreshInterval = time.Second * 15            // new IK handshake every 2 mins
	sessionDuration        = sessionRefreshInterval + 10 // give 10 second overlap to deal with inflight packets

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

	if prevSessID, ok := s.keyIDMap[hex.EncodeToString(staticKey)]; ok {
		if prevConn, ok := s.idSessMap[prevSessID]; ok {
			prevConn.close()
			delete(s.idSessMap, prevSessID)
		}
	}

	s.keyIDMap[hex.EncodeToString(staticKey)] = sessionID
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
	fnCh    chan pingFn
	bumpCh  chan struct{}
	closeCh chan struct{}
	doneCh  chan struct{}

	peerAddr *net.UDPAddr
	peerID   uint32
	send     *noise.CipherState
	recv     *noise.CipherState
}

func newSession(peerAddr *net.UDPAddr, peerID uint32, send, recv *noise.CipherState) *session {
	s := &session{
		peerAddr: peerAddr,
		peerID:   peerID,
		send:     send,
		recv:     recv,
		fnCh:     make(chan pingFn),
		bumpCh:   make(chan struct{}),
		closeCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
	}

	zap.L().Named("session").Debug("Creating new session...")

	go func() {
		timer := time.NewTimer(sessionRefreshInterval)
		fn := func(uint32) error { return nil }
		for {
			select {
			case fn = <-s.fnCh:
			case <-timer.C:
				_ = fn(s.peerID)
				timer.Reset(sessionRefreshInterval)
			case <-s.bumpCh:
				timer.Reset(sessionRefreshInterval)
			case <-s.closeCh:
				close(s.doneCh)
				zap.L().Named("session").Debug("Closing session...")
				return
			}
		}
	}()

	return s
}

func (s *session) setPingFn(fn func(uint32) error) {
	go func() {
		select {
		case s.fnCh <- fn:
		case <-s.doneCh:
		}
	}()
}

func (s *session) reset() {
	go func() {
		select {
		case s.bumpCh <- struct{}{}:
		case <-s.doneCh:
		}
	}()
}

func (s *session) close() {
	s.closeCh <- struct{}{}
}

func (s *session) Decrypt(msg []byte) ([]byte, bool, error) {
	pt, err := s.recv.Decrypt(nil, nil, msg)
	if err != nil {
		return nil, false, err
	}

	s.reset()
	return pt, s.shouldRekey(), nil
}

func (s *session) Send(conn *net.UDPConn, msg []byte) (bool, error) {
	enc, err := s.send.Encrypt(nil, nil, msg)
	if err != nil {
		return false, err
	}

	if err := write(conn, s.peerAddr, messageTypeTransportData, 0, s.peerID, enc); err != nil {
		return s.shouldRekey(), err
	}

	s.reset()
	return s.shouldRekey(), nil
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
