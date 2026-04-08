package state

import (
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

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

	SetWorkloadSpec(name, hash string, replicas, memoryPages, timeoutMs uint32, spread float32) []Event
	ClaimWorkload(hash string) []Event
	ReleaseWorkload(hash string) []Event
	SetLocalResources(cpu, mem float64, memTotalBytes uint64, numCPU, cpuBudgetPct, memBudgetPct uint32) []Event
	SetSeedLoad(rates map[string]float32) []Event
	SetSeedDemand(rates map[string]float32) []Event

	SetService(port uint32, name string, protocol statev1.ServiceProtocol) []Event
	RemoveService(name string) []Event
	SetLocalTraffic(peer types.PeerKey, in, out uint64) []Event

	EmitHeartbeatIfNeeded() []Event
	LoadGossipState(data []byte) error

	SetPeerLastAddr(pk types.PeerKey, addr string)
	SetPublic()
	SetAdmin()
	ClearAdmin()
	SetNodeName(name string)
	ExportLastAddrs() map[types.PeerKey]string
	LoadLastAddrs(addrs map[types.PeerKey]string)
}

type store struct {
	lastLocalEmit time.Time
	nodes         map[types.PeerKey]nodeRecord
	denied        map[types.PeerKey]struct{}
	pendingNotify chan struct{}
	snap          atomic.Pointer[Snapshot]
	nowFunc       func() time.Time
	pendingGossip []*statev1.GossipEvent
	mu            sync.Mutex
	localID       types.PeerKey
}

func New(self types.PeerKey) StateStore {
	now := time.Now()
	s := &store{
		localID:       self,
		nodes:         map[types.PeerKey]nodeRecord{self: newNodeRecord()},
		denied:        make(map[types.PeerKey]struct{}),
		pendingNotify: make(chan struct{}, 1),
		nowFunc:       time.Now,
		lastLocalEmit: now,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := s.nodes[self]
	rec.lastEventAt = now
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

const HeartbeatInterval = 15 * time.Second

func (s *store) EmitHeartbeatIfNeeded() []Event {
	s.mu.Lock()
	if s.nowFunc().Sub(s.lastLocalEmit) < HeartbeatInterval {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	return s.mutateLocal(func(_ *nodeRecord) ([]*statev1.GossipEvent, []Event) {
		return []*statev1.GossipEvent{
			{Change: &statev1.GossipEvent_Heartbeat{Heartbeat: &statev1.HeartbeatChange{}}},
		}, nil
	})
}

func (s *store) LoadGossipState(data []byte) error {
	var batch statev1.GossipEventBatch
	if err := batch.UnmarshalVT(data); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.applyBatchLocked(batch.Events, false)

	// Rebuild denied set from all loaded deny events (including self-events
	// processed via handleSelfConflictLocked which skip the per-event check).
	clear(s.denied)
	for _, rec := range s.nodes {
		for key, ev := range rec.log {
			if key.kind == attrDeny && !ev.Deleted {
				s.denied[types.PeerKeyFromBytes(ev.GetDeny().PeerPub)] = struct{}{}
			}
		}
	}

	s.updateSnapshotLocked()
	return nil
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
