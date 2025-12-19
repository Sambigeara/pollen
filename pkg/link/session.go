package link

import (
	"math"
	"sync"
	"time"

	"github.com/flynn/noise"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	// Noise enforces a MaxNonce of 2^64-1. We set a "slightly" lower limit.
	maxNonce = uint32(math.MaxUint32) - 1

	rekeyGracePeriod = time.Second * 3 // old sessions hang around for 3 seconds to allow inflight packets targetting the old sessionID to land
)

type sessionStore struct {
	byPeer    map[types.PeerKey]*session
	byLocalID map[uint32]*session
	mu        sync.RWMutex
}

func newSessionStore() *sessionStore {
	return &sessionStore{
		byPeer:    make(map[types.PeerKey]*session),
		byLocalID: make(map[uint32]*session),
	}
}

// set registers a session under both the local and peer-assigned session IDs.
func (s *sessionStore) set(sess *session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := types.PeerKeyFromBytes(sess.peerNoiseKey)

	if prev, ok := s.byPeer[k]; ok && prev.localSessionID != sess.localSessionID {
		oldID := prev.localSessionID
		// deletions occur after a grace period so that inflight packets have a higher chance of being processed
		defer time.AfterFunc(rekeyGracePeriod, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.byLocalID, oldID)
		})
	}

	s.byPeer[k] = sess
	s.byLocalID[sess.localSessionID] = sess
}

func (s *sessionStore) getByPeer(peer types.PeerKey) (*session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sess, ok := s.byPeer[peer]
	return sess, ok
}

func (s *sessionStore) getByLocalID(localID uint32) (*session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	id, ok := s.byLocalID[localID]
	return id, ok
}

type pingFn func(uint32) error

type session struct {
	localSessionID uint32
	peerSessionID  uint32
	peerAddr       string
	peerNoiseKey   []byte
	send           *noise.CipherState
	recv           *noise.CipherState
	sendMu, recvMu sync.RWMutex
}

func newSession(localSessionID uint32, noiseKey []byte, send, recv *noise.CipherState) *session {
	return &session{
		localSessionID: localSessionID,
		peerNoiseKey:   noiseKey,
		send:           send,
		recv:           recv,
	}
}

func (s *session) Encrypt(msg []byte) ([]byte, bool, error) {
	s.sendMu.Lock()
	enc, err := s.send.Encrypt(nil, nil, msg)
	if err != nil {
		s.sendMu.Unlock()
		return nil, false, err
	}
	// Check shouldRekey while still holding the writer lock to pair with Nonce mutation
	should := s.send.Nonce() >= uint64(maxNonce)
	s.sendMu.Unlock()

	return enc, should, nil
}

func (s *session) Decrypt(msg []byte) ([]byte, bool, error) {
	s.recvMu.Lock()
	pt, err := s.recv.Decrypt(nil, nil, msg)
	s.recvMu.Unlock()
	if err != nil {
		return nil, false, err
	}

	// Read send nonce under read lock to avoid racing with Encrypt
	s.sendMu.RLock()
	should := s.send.Nonce() >= uint64(maxNonce)
	s.sendMu.RUnlock()

	return pt, should, nil
}

func (s *session) shouldRekey() bool {
	s.sendMu.RLock()
	defer s.sendMu.RUnlock()
	return s.send.Nonce() >= uint64(maxNonce)
}
