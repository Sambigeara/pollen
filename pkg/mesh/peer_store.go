package mesh

import (
	"encoding/hex"
	"sync"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
)

// TODO(saml) remove this file once the functionality is ported across to the store

// PeerNoiseKey is a type alias for the raw bytes of a key.
type PeerNoiseKey []byte

// PeerSig is a type alias for the Ed25519 public key.
type PeerSig []byte

// PeerStore is an in-memory cache of active peers.
// It serves as the "ARP Table" for the mesh: resolving Static Keys -> IP Addresses.
// It is NOT persisted to disk (pkg/state handles persistence).
type PeerStore struct {
	peers map[string]*peerv1.Known
	mu    sync.RWMutex
}

func newPeerStore() *PeerStore {
	return &PeerStore{
		peers: make(map[string]*peerv1.Known),
	}
}

// Add inserts or updates a peer in the cache.
func (s *PeerStore) Add(peer *peerv1.Known) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := hex.EncodeToString(peer.NoisePub)
	s.peers[key] = peer
}

// Get retrieves a peer by their Noise static key.
func (s *PeerStore) Get(key PeerNoiseKey) (*peerv1.Known, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	k := hex.EncodeToString(key)
	peer, ok := s.peers[k]
	return peer, ok
}

// PromoteToPeer upgrades a handshake attempt to a full peer entry.
func (s *PeerStore) PromoteToPeer(key PeerNoiseKey, sig PeerSig, peerAddr string) {
	s.Add(&peerv1.Known{
		NoisePub: key,
		SigPub:   sig,
		Addr:     peerAddr,
	})
}

// GetAllKnown returns a snapshot of all peers.
// Used by the Control API (ListPeers) and during Handshake init.
func (s *PeerStore) GetAllKnown() []*peerv1.Known {
	s.mu.RLock()
	defer s.mu.RUnlock()

	known := make([]*peerv1.Known, 0, len(s.peers))
	for _, k := range s.peers {
		known = append(known, k)
	}
	return known
}
