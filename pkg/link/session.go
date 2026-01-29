package link

import (
	"bytes"
	"math"
	"sync"
	"time"

	"github.com/flynn/noise"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	// Noise enforces a MaxNonce of 2^64-1. We set a "slightly" lower limit.
	maxNonce = uint32(math.MaxUint32) - 1

	rekeyGracePeriod    = time.Second * 3  // old sessions hang around for 3 seconds to allow inflight packets targeting the old sessionID to land
	sessionStaleTimeout = time.Second * 60 // sessions with no received messages for this duration are considered stale
)

type sessionStore struct {
	byPeer        map[types.PeerKey]*session
	byLocalID     map[uint32]*session
	localNoiseKey []byte // our noise public key for tie-breaking
	mu            sync.RWMutex
}

func newSessionStore(localNoiseKey []byte) *sessionStore {
	return &sessionStore{
		byPeer:        make(map[types.PeerKey]*session),
		byLocalID:     make(map[uint32]*session),
		localNoiseKey: localNoiseKey,
	}
}

// set registers a session under both the local and peer-assigned session IDs.
// When simultaneous handshakes occur, deterministic tie-breaking ensures both
// peers keep the same winning session: the peer with the lexicographically
// smaller noise public key should be the initiator of the winning handshake.
func (s *sessionStore) set(sess *session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := types.PeerKeyFromBytes(sess.peerNoiseKey)

	if prev, ok := s.byPeer[k]; ok && prev.localSessionID != sess.localSessionID {
		// Deterministic tie-breaking: smaller key = initiator wins
		shouldBeInitiator := bytes.Compare(s.localNoiseKey, sess.peerNoiseKey) < 0

		// If prev matches expected role and new doesn't, keep prev
		if prev.isInitiator == shouldBeInitiator && sess.isInitiator != shouldBeInitiator {
			// Register new session by localID for incoming messages during transition
			s.byLocalID[sess.localSessionID] = sess
			newID := sess.localSessionID
			time.AfterFunc(rekeyGracePeriod, func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				delete(s.byLocalID, newID)
			})
			return // Don't replace byPeer
		}

		// Otherwise, new session wins - proceed with replacement
		oldID := prev.localSessionID
		// deletions occur after a grace period so that inflight packets have a higher chance of being processed
		time.AfterFunc(rekeyGracePeriod, func() {
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

// getStaleAndRemove finds sessions that haven't received messages recently,
// removes them from the store, and returns their peer keys.
func (s *sessionStore) getStaleAndRemove(now time.Time) []types.PeerKey {
	s.mu.Lock()
	defer s.mu.Unlock()

	var stale []types.PeerKey
	for peerKey, sess := range s.byPeer {
		if sess.isStale(now) {
			stale = append(stale, peerKey)
			delete(s.byPeer, peerKey)
			delete(s.byLocalID, sess.localSessionID)
		}
	}
	return stale
}

func (s *sessionStore) removeByPeerKey(k types.PeerKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byPeer[k]
	if !ok {
		return
	}
	delete(s.byPeer, k)
	delete(s.byLocalID, sess.localSessionID)
}

// getAllPeers returns all peer keys with active sessions.
func (s *sessionStore) getAllPeers() []types.PeerKey {
	s.mu.RLock()
	defer s.mu.RUnlock()

	peers := make([]types.PeerKey, 0, len(s.byPeer))
	for pk := range s.byPeer {
		peers = append(peers, pk)
	}
	return peers
}

type session struct {
	lastRecvTime   time.Time
	send           *noise.CipherState
	recv           *noise.CipherState
	peerAddr       string
	peerNoiseKey   []byte
	sendMu         sync.RWMutex
	recvMu         sync.RWMutex
	localSessionID uint32
	peerSessionID  uint32
	isInitiator    bool // true if we initiated this handshake
}

func newSession(localSessionID uint32, noiseKey []byte, send, recv *noise.CipherState, isInitiator bool) *session {
	return &session{
		localSessionID: localSessionID,
		peerNoiseKey:   noiseKey,
		send:           send,
		recv:           recv,
		lastRecvTime:   time.Now(),
		isInitiator:    isInitiator,
	}
}

func (s *session) touchRecv() {
	s.recvMu.Lock()
	s.lastRecvTime = time.Now()
	s.recvMu.Unlock()
}

func (s *session) isStale(now time.Time) bool {
	s.recvMu.RLock()
	defer s.recvMu.RUnlock()
	return now.Sub(s.lastRecvTime) > sessionStaleTimeout
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
