// Package state provides the CRDT-based distributed state store with
// immutable snapshots, typed projections, and synchronous event emission.
package state

import (
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

// StateStore is the producer-side interface. Consumers define narrower subsets.
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

var _ StateStore = (*store)(nil)

var errSpecOwnedRemotely = errors.New("spec published by another node")

// store is the CRDT-based in-memory state store. Mutations are mutex-protected;
// Snapshot() is lock-free via atomic.Pointer.
type store struct {
	nodes         map[types.PeerKey]nodeRecord
	denied        map[types.PeerKey]struct{}
	snap          atomic.Pointer[Snapshot]
	pendingNotify chan struct{}
	pendingGossip []*statev1.GossipEvent
	mu            sync.Mutex
	localID       types.PeerKey
}

func New(self types.PeerKey) StateStore {
	s := &store{
		localID:       self,
		pendingNotify: make(chan struct{}, 1),
		nodes: map[types.PeerKey]nodeRecord{
			self: newNodeRecord(self),
		},
		denied: make(map[types.PeerKey]struct{}),
	}

	local := s.nodes[self]
	tombstoneStaleAttrs(&local)
	s.nodes[self] = local

	s.updateSnapshot()
	return s
}

func (s *store) SetPeerLastAddr(pk types.PeerKey, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.nodes[pk]
	if !ok {
		return
	}
	rec.LastAddr = addr
	s.nodes[pk] = rec
	s.updateSnapshot()
}

func (s *store) ExportLastAddrs() map[types.PeerKey]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[types.PeerKey]string)
	for pk, rec := range s.nodes {
		if pk == s.localID || rec.LastAddr == "" {
			continue
		}
		out[pk] = rec.LastAddr
	}
	return out
}

func (s *store) LoadLastAddrs(addrs map[types.PeerKey]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for pk, addr := range addrs {
		rec, ok := s.nodes[pk]
		if !ok {
			continue
		}
		rec.LastAddr = addr
		s.nodes[pk] = rec
	}
	s.updateSnapshot()
}

func (s *store) SetBootstrapPublic() {
	s.setLocalPubliclyAccessible(true)
}

func (s *store) Snapshot() Snapshot {
	if p := s.snap.Load(); p != nil {
		return *p
	}
	return Snapshot{}
}

func (s *store) PendingNotify() <-chan struct{} {
	return s.pendingNotify
}

func (s *store) FlushPendingGossip() []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pendingGossip) == 0 {
		return nil
	}
	events := s.pendingGossip
	s.pendingGossip = nil
	return events
}

func (s *store) do(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn()
}

func (s *store) updateSnapshot() {
	snap := s.snapshotLocked()
	s.snap.Store(&snap)
}

func (s *store) enqueuePendingGossip(events []*statev1.GossipEvent) {
	if len(events) == 0 {
		return
	}
	s.mu.Lock()
	s.pendingGossip = append(s.pendingGossip, events...)
	s.mu.Unlock()
	select {
	case s.pendingNotify <- struct{}{}:
	default:
	}
}

func (s *store) validNodesLocked() map[types.PeerKey]nodeRecord {
	now := time.Now()
	out := make(map[types.PeerKey]nodeRecord, len(s.nodes))
	for k, v := range s.nodes {
		if _, denied := s.denied[k]; denied {
			continue
		}
		if v.CertExpiry != 0 && auth.IsExpiredAt(time.Unix(v.CertExpiry, 0), now) {
			continue
		}
		out[k] = v.clone()
	}
	return out
}

func (s *store) isValidOwnerLocked(peerID types.PeerKey) bool {
	if _, denied := s.denied[peerID]; denied {
		return false
	}
	rec, ok := s.nodes[peerID]
	if !ok {
		return false
	}
	if rec.CertExpiry != 0 && auth.IsExpiredAt(time.Unix(rec.CertExpiry, 0), time.Now()) {
		return false
	}
	return true
}

func (s *store) tombstoneLosingLocalSpec(remotePeer types.PeerKey, hash string) []*statev1.GossipEvent {
	if remotePeer == s.localID {
		return nil
	}
	if !s.isValidOwnerLocked(remotePeer) {
		return nil
	}
	if s.localID.Compare(remotePeer) < 0 {
		return nil
	}
	local := s.nodes[s.localID]
	if _, has := local.WorkloadSpecs[hash]; !has {
		return nil
	}

	delete(local.WorkloadSpecs, hash)

	key := attrKey{kind: attrWorkloadSpec, name: hash}
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter, Deleted: true}
	s.nodes[s.localID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.localID.String(),
		Counter: counter,
		Deleted: true,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: hash},
		},
	}}
}

func (s *store) ApplyDelta(from types.PeerKey, data []byte) ([]Event, []byte, error) {
	var batch statev1.GossipEventBatch
	if err := batch.UnmarshalVT(data); err != nil {
		return nil, nil, fmt.Errorf("unmarshal gossip batch: %w", err)
	}

	result := s.applyEvents(batch.GetEvents())

	var rebroadcast []byte
	if len(result.rebroadcast) > 0 {
		rb := &statev1.GossipEventBatch{Events: result.rebroadcast}
		rebroadcast, _ = rb.MarshalVT()
	}

	return result.events(), rebroadcast, nil
}

func (s *store) EncodeDelta(since Digest) []byte {
	batch := &statev1.GossipEventBatch{Events: s.missingFor(since.proto)}
	data, _ := batch.MarshalVT()
	return data
}

func (s *store) EncodeFull() []byte {
	return s.EncodeDelta(Digest{})
}
