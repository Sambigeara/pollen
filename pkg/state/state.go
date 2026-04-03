package state

import (
	"net/netip"
	"sync"
	"sync/atomic"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

type Event interface{ stateEvent() }

type (
	PeerJoined     struct{ Key types.PeerKey }
	PeerDenied     struct{ Key types.PeerKey }
	ServiceChanged struct {
		Name string
		Peer types.PeerKey
	}
	WorkloadChanged struct{ Hash string }
	TopologyChanged struct{ Peer types.PeerKey }
	GossipApplied   struct{}
)

func (PeerJoined) stateEvent()      {}
func (PeerDenied) stateEvent()      {}
func (ServiceChanged) stateEvent()  {}
func (WorkloadChanged) stateEvent() {}
func (TopologyChanged) stateEvent() {}
func (GossipApplied) stateEvent()   {}

type StateStore interface {
	Snapshot() Snapshot
	ApplyDelta(from types.PeerKey, data []byte) ([]Event, []byte, error)
	EncodeDelta(since Digest) []byte
	EncodeFull() []byte
	PendingNotify() <-chan struct{}
	FlushPendingGossip() []*statev1.GossipEvent

	DenyPeer(key types.PeerKey) []Event
	SetLocalAddresses(addrs []netip.AddrPort) []Event
	SetLocalNAT(t nat.Type) []Event
	SetLocalCoord(c coords.Coord, coordErr float64) []Event
	SetLocalReachable(peers []types.PeerKey) []Event
	SetLocalObservedAddress(ip string, port uint32) []Event

	SetWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) []Event
	ClaimWorkload(hash string) []Event
	ReleaseWorkload(hash string) []Event
	SetLocalResources(cpu, mem float64) []Event

	SetService(port uint32, name string) []Event
	RemoveService(name string) []Event
	SetLocalTraffic(peer types.PeerKey, in, out uint64) []Event

	SetPeerLastAddr(pk types.PeerKey, addr string)
	SetBootstrapPublic()
	ExportLastAddrs() map[types.PeerKey]string
	LoadLastAddrs(addrs map[types.PeerKey]string)
}

type store struct {
	nodes         map[types.PeerKey]nodeRecord
	denied        map[types.PeerKey]struct{}
	pendingNotify chan struct{}
	snap          atomic.Pointer[Snapshot]
	pendingGossip []*statev1.GossipEvent
	mu            sync.Mutex
	localID       types.PeerKey
}

func New(self types.PeerKey) StateStore {
	s := &store{
		localID:       self,
		nodes:         map[types.PeerKey]nodeRecord{self: newNodeRecord()},
		denied:        make(map[types.PeerKey]struct{}),
		pendingNotify: make(chan struct{}, 1),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := s.nodes[self]
	s.tombstoneStaleAttrsLocked(&rec)
	s.nodes[self] = rec
	s.updateSnapshotLocked()

	return s
}

func (s *store) Snapshot() Snapshot {
	return *s.snap.Load()
}

func (s *store) PendingNotify() <-chan struct{} {
	return s.pendingNotify
}

func (s *store) FlushPendingGossip() []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.pendingGossip
	s.pendingGossip = nil
	return events
}

func (s *store) notify() {
	select {
	case s.pendingNotify <- struct{}{}:
	default:
	}
}

func (s *store) updateSnapshotLocked() {
	snap := s.buildSnapshot()
	s.snap.Store(&snap)
}

func (s *store) SetPeerLastAddr(pk types.PeerKey, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.nodes[pk]; ok {
		rec.LastAddr = addr
		s.nodes[pk] = rec
		s.updateSnapshotLocked()
	}
}

func (s *store) ExportLastAddrs() map[types.PeerKey]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[types.PeerKey]string)
	for pk, rec := range s.nodes {
		if pk != s.localID && rec.LastAddr != "" {
			out[pk] = rec.LastAddr
		}
	}
	return out
}

func (s *store) LoadLastAddrs(addrs map[types.PeerKey]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for pk, addr := range addrs {
		if rec, ok := s.nodes[pk]; ok {
			rec.LastAddr = addr
			s.nodes[pk] = rec
		}
	}
	s.updateSnapshotLocked()
}
