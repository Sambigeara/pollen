// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"errors"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	ErrSpecOwnedByPeer = errors.New("spec already published by another peer")
	ErrInvalidDigest   = errors.New("invalid manifest digest")
)

type Event interface{ stateEvent() }

type (
	PeerJoined     struct{ Key types.PeerKey }
	PeerDenied     struct{ Key types.PeerKey }
	ServiceChanged struct {
		Name string
		Peer types.PeerKey
	}
	WorkloadChanged  struct{ Hash string }
	TopologyChanged  struct{ Peer types.PeerKey }
	AddressesChanged struct{ Peer types.PeerKey }
	StaticChanged    struct{ Name string }
)

func (PeerJoined) stateEvent()       {}
func (PeerDenied) stateEvent()       {}
func (ServiceChanged) stateEvent()   {}
func (WorkloadChanged) stateEvent()  {}
func (TopologyChanged) stateEvent()  {}
func (AddressesChanged) stateEvent() {}
func (StaticChanged) stateEvent()    {}

type LocalSigner interface {
	IssueSpecAuth(resource *admissionv1.ResourceID, body auth.SpecBody, policy *admissionv1.Predicate, deleted bool) (*admissionv1.SpecAuth, error)
}

type MutationValidator func(*statev1.SpecChange) error

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

	PublishWorkload(spec WorkloadSpec, policy *admissionv1.Predicate) ([]Event, error)
	DeleteWorkloadSpec(hash string) ([]Event, error)
	ClaimWorkload(hash string) []Event
	MarkWorkloadDraining(hash string) []Event
	ReleaseWorkload(hash string) []Event
	SetLocalResources(r NodeResources) []Event
	SetBackoffTTL(expiresAt time.Time) []Event
	SetPerSeedCallCounts(counts map[string]uint64) []Event
	SetLocalBlobs(digests []string) []Event

	SetStaticSpec(spec StaticSpec, policy *admissionv1.Predicate) ([]Event, error)
	DeleteStaticSpec(name string) ([]Event, error)
	ClaimStatic(name string) []Event
	ReleaseStatic(name string) []Event

	SetBlobSpec(spec BlobSpec, policy *admissionv1.Predicate) ([]Event, error)
	DeleteBlobSpec(digest string) ([]Event, error)

	SetBlobWrapping(wrapping *statev1.BlobWrappingChange) []Event

	SetService(port uint32, name string, protocol statev1.ServiceProtocol, properties *structpb.Struct, policy *admissionv1.Predicate) ([]Event, error)
	RemoveService(name string) ([]Event, error)
	SetLocalTraffic(peer types.PeerKey, in, out uint64) []Event

	EmitHeartbeatIfNeeded() []Event
	LoadGossipState(data []byte) error

	SetPeerLastAddr(pk types.PeerKey, addr string)
	SetPublic()
	SetAdmin()
	ClearAdmin()
	SetStaticCapable()
	SetNodeName(name string)
	SetLocalDelegationCert(cert *admissionv1.DelegationCert, subjectSig []byte) []Event
	SetLocalSigner(signer LocalSigner)
	SetMutationValidator(v MutationValidator)
	ExportLastAddrs() map[types.PeerKey]string
	LoadLastAddrs(addrs map[types.PeerKey]string)
}

type store struct {
	lastLocalEmit time.Time
	signer        LocalSigner
	validate      MutationValidator
	nodes         map[types.PeerKey]nodeRecord
	denied        map[types.PeerKey]struct{}
	pendingNotify chan struct{}
	snap          atomic.Pointer[Snapshot]
	nowFunc       func() time.Time
	pendingGossip []*statev1.GossipEvent
	rootPub       []byte
	mu            sync.Mutex
	localID       types.PeerKey
}

func New(self types.PeerKey, rootPub []byte) StateStore {
	if len(rootPub) == 0 {
		panic("state.New: rootPub is required")
	}
	now := time.Now()
	s := &store{
		localID:       self,
		nodes:         map[types.PeerKey]nodeRecord{self: newNodeRecord()},
		denied:        make(map[types.PeerKey]struct{}),
		pendingNotify: make(chan struct{}, 1),
		nowFunc:       time.Now,
		lastLocalEmit: now,
		rootPub:       rootPub,
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

	// Derive the denied set from the freshly-loaded cert + deny graph.
	// applyBatchLocked skips this when live=false (replay/restore path),
	// so we trigger it explicitly here.
	s.recomputeDeniedLocked()

	s.updateSnapshotLocked()
	return nil
}

func (s *store) SetLocalSigner(signer LocalSigner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.signer = signer
}

func (s *store) SetMutationValidator(v MutationValidator) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.validate = v
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
