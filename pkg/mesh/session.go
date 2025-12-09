package mesh

import (
	"encoding/hex"
	"math"
	"net"
	"sync"
	"time"

	"github.com/flynn/noise"
)

const (
	// Noise enforces a MaxNonce of 2^64-1. We set a "slightly" lower limit.
	maxNonce = uint32(math.MaxUint32) - 1

	rekeyGracePeriod = time.Second * 3 // old sessions hang around for 3 seconds to allow inflight packets targetting the old sessionID to land
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

// set registers a session under both the local and peer-assigned session IDs.
//   - keyIDMap maps staticKey -> peerSessionID (used when sending to set receiverID)
//   - idSessMap maps both peerSessionID and localSessionID -> session (so inbound
//     frames addressed to our local session ID can be decrypted)
func (s *sessionStore) set(staticKey []byte, localSessionID uint32, peerSessionID uint32, conn *session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := hex.EncodeToString(staticKey)
	if prevSessID, ok := s.keyIDMap[k]; ok {
		// the chances of collision are hilariously low, but let's cover this for hypothetical scenarios
		// of 100,000 nodes running over a decade :-)
		if prevSessID != peerSessionID {
			// deletions occur after a grace period so that inflight packets have a higher chance of being processed
			defer time.AfterFunc(rekeyGracePeriod, func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				delete(s.idSessMap, prevSessID)
			})
		}
	}

	// map static key to the peer's session ID (used when sending)
	s.keyIDMap[k] = peerSessionID
	// store by both peer and local IDs so we can resolve sessions in both directions
	s.idSessMap[peerSessionID] = conn
	s.idSessMap[localSessionID] = conn

	// TODO(saml): Also evict the previous localSessionID mapping after a rekey.
	// Currently we only schedule deletion of the prior peerSessionID entry. To fully
	// avoid stale entries in idSessMap, track the previous localSessionID for this
	// staticKey and delete s.idSessMap[oldLocalID] after rekeyGracePeriod.
}

func (s *sessionStore) get(peerID uint32) (*session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sess, ok := s.idSessMap[peerID]
	return sess, ok
}

func (s *sessionStore) getID(staticKey []byte) (uint32, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	k := hex.EncodeToString(staticKey)
	id, ok := s.keyIDMap[k]
	return id, ok
}

type pingFn func(uint32) error

type session struct {
	peerAddr     *net.UDPAddr
	peerNoiseKey []byte
	send         *noise.CipherState
	recv         *noise.CipherState
}

func newSession(peerAddr *net.UDPAddr, noiseKey []byte, send, recv *noise.CipherState) *session {
	return &session{
		peerAddr:     peerAddr,
		peerNoiseKey: noiseKey,
		send:         send,
		recv:         recv,
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
