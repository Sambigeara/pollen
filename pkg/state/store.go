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
	EncodeDelta(since Clock) []byte
	FullState() []byte
	PendingNotify() <-chan struct{}
	FlushPendingGossip() []*statev1.GossipEvent

	DenyPeer(key types.PeerKey) []Event
	SetLocalAddresses(addrs []netip.AddrPort) []Event
	SetLocalNAT(t nat.Type) []Event
	SetLocalCoord(c coords.Coord) []Event
	SetLocalReachable(peers []types.PeerKey) []Event
	SetLocalObservedExternalIP(ip string) []Event
	SetLocalExternalPort(port uint32) []Event

	SetWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) []Event
	ClaimWorkload(hash string) []Event
	ReleaseWorkload(hash string) []Event
	SetLocalResources(cpu, mem float64) []Event

	SetService(port uint32, name string) []Event
	RemoveService(name string) []Event
	SetLocalTraffic(peer types.PeerKey, in, out uint64) []Event

	SetPeerLastAddr(pk types.PeerKey, addr string)
	SetBootstrapPublic()
}

var _ StateStore = (*Store)(nil)

var errSpecOwnedRemotely = errors.New("spec published by another node")

// Store is the CRDT-based in-memory state store. Mutations are mutex-protected;
// Snapshot() is lock-free via atomic.Pointer.
type Store struct {
	nodes         map[types.PeerKey]nodeRecord
	denied        map[types.PeerKey]struct{}
	snap          atomic.Pointer[Snapshot]
	pendingNotify chan struct{}
	pendingGossip []*statev1.GossipEvent
	mu            sync.Mutex
	LocalID       types.PeerKey
}

func New(self types.PeerKey) *Store {
	s := &Store{
		LocalID:       self,
		pendingNotify: make(chan struct{}, 1),
		nodes: map[types.PeerKey]nodeRecord{
			self: {
				NodeView: NodeView{
					IdentityPub:    append([]byte(nil), self[:]...),
					Reachable:      make(map[types.PeerKey]struct{}),
					Services:       make(map[string]*statev1.Service),
					WorkloadSpecs:  make(map[string]*statev1.WorkloadSpecChange),
					WorkloadClaims: make(map[string]struct{}),
				},
				maxCounter: 1,
				log: map[attrKey]logEntry{
					{kind: attrIdentity}: {Counter: 1},
				},
			},
		},
		denied: make(map[types.PeerKey]struct{}),
	}

	local := s.nodes[self]
	tombstoneStaleAttrs(&local)
	s.nodes[self] = local

	s.updateSnapshot()
	return s
}

func (s *Store) SetPeerLastAddr(pk types.PeerKey, addr string) {
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

func (s *Store) SetBootstrapPublic() {
	s.setLocalPubliclyAccessible(true)
}

func (s *Store) Snapshot() Snapshot {
	if p := s.snap.Load(); p != nil {
		return *p
	}
	return Snapshot{}
}

func (s *Store) PendingNotify() <-chan struct{} {
	return s.pendingNotify
}

func (s *Store) FlushPendingGossip() []*statev1.GossipEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pendingGossip) == 0 {
		return nil
	}
	events := s.pendingGossip
	s.pendingGossip = nil
	return events
}

func (s *Store) do(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn()
}

func (s *Store) updateSnapshot() {
	snap := s.snapshotLocked()
	s.snap.Store(&snap)
}

func (s *Store) enqueuePendingGossip(events []*statev1.GossipEvent) {
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

func (s *Store) validNodesLocked() map[types.PeerKey]nodeRecord {
	now := time.Now()
	out := make(map[types.PeerKey]nodeRecord, len(s.nodes))
	for k, v := range s.nodes {
		if _, denied := s.denied[k]; denied {
			continue
		}
		if v.CertExpiry != 0 && auth.IsCertExpiredAt(time.Unix(v.CertExpiry, 0), now) {
			continue
		}
		out[k] = v.clone()
	}
	return out
}

func (s *Store) isValidOwnerLocked(peerID types.PeerKey) bool {
	if _, denied := s.denied[peerID]; denied {
		return false
	}
	rec, ok := s.nodes[peerID]
	if !ok {
		return false
	}
	if rec.CertExpiry != 0 && auth.IsCertExpiredAt(time.Unix(rec.CertExpiry, 0), time.Now()) {
		return false
	}
	return true
}

func (s *Store) tombstoneLosingLocalSpec(remotePeer types.PeerKey, hash string) []*statev1.GossipEvent {
	if remotePeer == s.LocalID {
		return nil
	}
	if !s.isValidOwnerLocked(remotePeer) {
		return nil
	}
	if s.LocalID.Compare(remotePeer) < 0 {
		return nil
	}
	local := s.nodes[s.LocalID]
	if _, has := local.WorkloadSpecs[hash]; !has {
		return nil
	}

	delete(local.WorkloadSpecs, hash)

	key := attrKey{kind: attrWorkloadSpec, name: hash}
	local.maxCounter++
	counter := local.maxCounter
	local.log[key] = logEntry{Counter: counter, Deleted: true}
	s.nodes[s.LocalID] = local

	return []*statev1.GossipEvent{{
		PeerId:  s.LocalID.String(),
		Counter: counter,
		Deleted: true,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: hash},
		},
	}}
}

func (s *Store) ApplyDelta(from types.PeerKey, data []byte) ([]Event, []byte, error) {
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

func (s *Store) EncodeDelta(since Clock) []byte {
	batch := &statev1.GossipEventBatch{Events: s.missingFor(since.digest)}
	data, _ := batch.MarshalVT()
	return data
}

func (s *Store) FullState() []byte {
	return s.EncodeDelta(Clock{})
}
