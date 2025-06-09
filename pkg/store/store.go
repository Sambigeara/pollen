package store

import (
	"sync"
	"time"
)

type Function struct {
	ID        string
	Timestamp time.Time
	Hash      string
	Deleted   bool
}

type Peer struct {
	Addr    string
	ID      string // TODO(saml) currently redundant, requires thought
	Deleted bool
}

type NodeStore struct {
	OriginAddr string
	mu         sync.RWMutex
	Functions  map[string]*Function
	Peers      map[string]*Peer
}

func NewNodeStore(addr string, peers []string) *NodeStore {
	selfID := NewID().String()

	peerMap := map[string]*Peer{
		addr: {
			Addr: addr,
			ID:   selfID,
		},
	}

	for _, p := range peers {
		peerMap[p] = &Peer{
			Addr: p,
		}
	}

	return &NodeStore{
		OriginAddr: addr,
		Functions:  make(map[string]*Function),
		Peers:      peerMap,
	}
}

func (s *NodeStore) AddFunction(id, hash string) {
	fn := &Function{
		ID:        id,
		Hash:      hash,
		Timestamp: time.Now(),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.Functions[id]; !ok || fn.Timestamp.After(existing.Timestamp) {
		s.Functions[id] = fn
	}
}

func (s *NodeStore) Merge(other *NodeStore) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, fn := range other.Functions {
		if existing, ok := s.Functions[id]; !ok || fn.Timestamp.After(existing.Timestamp) {
			s.Functions[id] = fn
		}
	}

	for addr, peer := range other.Peers {
		if addr == s.OriginAddr {
			continue
		}
		s.Peers[addr] = peer
	}
}

func (s *NodeStore) GetPeers() []*Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	addrs := make([]*Peer, 0, len(s.Peers))
	for _, p := range s.Peers {
		addrs = append(addrs, p)
	}

	return addrs
}
