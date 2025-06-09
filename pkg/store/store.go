package store

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type Function struct {
	ID        string
	Timestamp time.Time
	Hash      string
	Deleted   bool
}

type Peer struct {
	ID        string // TODO(saml) currently redundant, requires thought
	Addr      string
	Timestamp time.Time
	Active    bool
}

type NodeStore struct {
	log        logrus.FieldLogger
	OriginAddr string
	mu         sync.RWMutex
	Functions  map[string]*Function
	Peers      map[string]*Peer
}

func NewNodeStore(log logrus.FieldLogger, addr string, peers []string) *NodeStore {
	selfID := NewID().String()

	peerMap := map[string]*Peer{
		addr: {
			ID:        selfID,
			Timestamp: time.Now(),
			Addr:      addr,
			Active:    true,
		},
	}

	for _, p := range peers {
		peerMap[p] = &Peer{
			Addr:   p,
			Active: true, // TODO(saml) not strictly true, but works for now
		}
	}

	return &NodeStore{
		log:        log,
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
		if existing, ok := s.Peers[addr]; ok && peer.Timestamp.Before(existing.Timestamp) {
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
		if p.Active {
			addrs = append(addrs, p)
		}
	}

	return addrs
}

func (s *NodeStore) DisablePeer(addr string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if p, ok := s.Peers[addr]; ok {
		s.log.Debugf("Disabling peer %s", addr)
		p.Active = false
		p.Timestamp = time.Now()
	}
}
