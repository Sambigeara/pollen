package sock

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"sync"
	"time"

	"github.com/flynn/noise"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const (
	// Noise enforces a MaxNonce of 2^64-1. We set a "slightly" lower limit.
	maxNonce = uint32(math.MaxUint32) - 1

	noncePrefixSize = 8
	recvWindowSize  = 1024
	recvWindowWords = recvWindowSize / 64

	rekeyGracePeriod    = time.Second * 3  // old sessions hang around for 3 seconds to allow inflight packets targeting the old sessionID to land
	sessionStaleTimeout = time.Second * 30 // sessions with no received messages for this duration are considered stale
)

var (
	ErrReplay          = errors.New("replay detected")
	ErrTooOld          = errors.New("message too old")
	ErrShortCiphertext = errors.New("ciphertext too short")
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
	log := zap.S().Named("sessionStore").With("localSessionID", sess.localSessionID, "sess.isInitiator", sess.isInitiator, "peer", k.Short())

	if prev, ok := s.byPeer[k]; ok && prev.localSessionID != sess.localSessionID {
		// Deterministic tie-breaking: smaller key = initiator wins
		shouldBeInitiator := bytes.Compare(s.localNoiseKey, sess.peerNoiseKey) < 0
		log.Debugw("tie breaker", "shouldBeInitiator", shouldBeInitiator)

		// If prev matches expected role and new doesn't, keep prev
		if prev.isInitiator == shouldBeInitiator && sess.isInitiator != shouldBeInitiator {
			log.Debugw("tie breaker override")
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
	sendCipher     noise.Cipher
	recvCipher     noise.Cipher
	peerAddr       *net.UDPAddr
	peerNoiseKey   []byte
	peerAddrMu     sync.RWMutex
	sendMu         sync.RWMutex
	recvMu         sync.RWMutex
	localSessionID uint32
	peerSessionID  uint32
	isInitiator    bool // true if we initiated this handshake
	sendNonce      uint64
	recvWindow     nonceWindow
}

func (s *session) setPeerAddr(addr *net.UDPAddr) {
	s.peerAddrMu.Lock()
	s.peerAddr = addr
	s.peerAddrMu.Unlock()
}

// TODO(saml) is this even necessary?
func (s *session) peerAddrValue() *net.UDPAddr {
	s.peerAddrMu.RLock()
	addr := s.peerAddr
	s.peerAddrMu.RUnlock()
	return addr
}

type nonceWindow struct {
	max         uint64
	seen        [recvWindowWords]uint64
	initialized bool
}

func newSession(localSessionID uint32, noiseKey []byte, send, recv *noise.CipherState, isInitiator bool) *session {
	return &session{
		localSessionID: localSessionID,
		peerNoiseKey:   noiseKey,
		sendCipher:     send.Cipher(),
		recvCipher:     recv.Cipher(),
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
	nonce := s.sendNonce
	enc := s.sendCipher.Encrypt(nil, nonce, nil, msg)
	s.sendNonce++
	// Check shouldRekey while still holding the writer lock to pair with Nonce mutation
	should := s.sendNonce >= uint64(maxNonce)
	s.sendMu.Unlock()

	out := make([]byte, noncePrefixSize+len(enc))
	binary.BigEndian.PutUint64(out[:noncePrefixSize], nonce)
	copy(out[noncePrefixSize:], enc)

	return out, should, nil
}

func (s *session) Decrypt(msg []byte) ([]byte, bool, error) {
	s.recvMu.Lock()
	if len(msg) < noncePrefixSize {
		s.recvMu.Unlock()
		return nil, false, ErrShortCiphertext
	}

	nonce := binary.BigEndian.Uint64(msg[:noncePrefixSize])
	ciphertext := msg[noncePrefixSize:]
	if err := s.recvWindow.checkAndMark(nonce); err != nil {
		s.recvMu.Unlock()
		return nil, false, err
	}
	pt, err := s.recvCipher.Decrypt(nil, nonce, nil, ciphertext)
	s.recvMu.Unlock()
	if err != nil {
		return nil, false, err
	}

	// Read send nonce under read lock to avoid racing with Encrypt
	s.sendMu.RLock()
	should := s.sendNonce >= uint64(maxNonce)
	s.sendMu.RUnlock()
	if nonce >= uint64(maxNonce) {
		should = true
	}

	return pt, should, nil
}

func (w *nonceWindow) checkAndMark(nonce uint64) error {
	if !w.initialized {
		w.max = nonce
		w.seen[0] = 1
		w.initialized = true
		return nil
	}

	if nonce > w.max {
		delta := nonce - w.max
		if delta >= uint64(recvWindowSize) {
			w.clear()
		} else {
			w.shift(delta)
		}
		w.max = nonce
		w.setBit(0)
		return nil
	}

	offset := w.max - nonce
	if offset >= uint64(recvWindowSize) {
		return ErrTooOld
	}
	if w.hasBit(offset) {
		return ErrReplay
	}
	w.setBit(offset)
	return nil
}

func (w *nonceWindow) clear() {
	for i := range w.seen {
		w.seen[i] = 0
	}
}

func (w *nonceWindow) shift(delta uint64) {
	if delta == 0 {
		return
	}
	if delta >= uint64(recvWindowSize) {
		w.clear()
		return
	}

	var next [recvWindowWords]uint64
	wordShift := int(delta / 64)
	bitShift := uint(delta % 64)
	for i := recvWindowWords - 1; i >= 0; i-- {
		src := i - wordShift
		if src < 0 {
			next[i] = 0
			continue
		}
		val := w.seen[src] << bitShift
		if bitShift > 0 && src > 0 {
			val |= w.seen[src-1] >> (64 - bitShift)
		}
		next[i] = val
	}
	w.seen = next
}

func (w *nonceWindow) setBit(offset uint64) {
	idx := int(offset / 64)
	shift := uint(offset % 64)
	w.seen[idx] |= uint64(1) << shift
}

func (w *nonceWindow) hasBit(offset uint64) bool {
	idx := int(offset / 64)
	shift := uint(offset % 64)
	return w.seen[idx]&(uint64(1)<<shift) != 0
}
