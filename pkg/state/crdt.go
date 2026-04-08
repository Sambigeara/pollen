package state

import (
	"cmp"
	"encoding/binary"
	"slices"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
)

type attrKind uint8

const (
	attrNetwork attrKind = iota + 1
	attrObservedAddress
	attrCertExpiry
	attrService
	attrReachability
	attrDeny
	attrPubliclyAccessible
	attrVivaldi
	attrNatType
	attrResourceTelemetry
	attrWorkloadSpec
	attrWorkloadClaim
	attrTrafficHeatmap
	attrHeartbeat
	attrSeedLoad
	attrAdminCapable
	attrNodeName
	attrSeedDemand
)

type attrKey struct {
	name string
	peer types.PeerKey
	kind attrKind
}

type nodeRecord struct {
	lastEventAt time.Time
	log         map[attrKey]*statev1.GossipEvent
	LastAddr    string
	maxCounter  uint64
}

func newNodeRecord() nodeRecord {
	return nodeRecord{log: make(map[attrKey]*statev1.GossipEvent)}
}

func (s *store) ApplyDelta(from types.PeerKey, data []byte) ([]Event, []byte, error) {
	var batch statev1.GossipEventBatch
	if err := batch.UnmarshalVT(data); err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	events, rebroadcast := s.applyBatchLocked(batch.Events, true)
	s.updateSnapshotLocked()

	var rbData []byte
	if len(rebroadcast) > 0 {
		events = append(events, GossipApplied{})
		rbBatch := &statev1.GossipEventBatch{Events: rebroadcast}
		rbData, _ = rbBatch.MarshalVT()
	}

	return events, rbData, nil
}

// applyBatchLocked is the shared core for ApplyDelta and LoadGossipState.
// When live is true (normal gossip), it stamps lastEventAt, generates domain
// events, and collects rebroadcast entries. When false (disk restore), it
// inserts events without liveness stamps and produces no domain events.
func (s *store) applyBatchLocked(events []*statev1.GossipEvent, live bool) ([]Event, []*statev1.GossipEvent) {
	var domainEvents []Event
	var rebroadcast []*statev1.GossipEvent

	for _, ev := range events {
		pk, err := types.PeerKeyFromString(ev.PeerId)
		if err != nil {
			continue
		}

		if pk == s.localID {
			if live {
				rebroadcast = append(rebroadcast, s.handleSelfConflictLocked(ev)...)
			} else {
				s.handleSelfConflictLocked(ev)
			}
			continue
		}

		key, ok := getAttrKey(ev)
		if !ok {
			continue
		}

		rec, exists := s.nodes[pk]
		if !exists {
			rec = newNodeRecord()
			if live {
				domainEvents = append(domainEvents, PeerJoined{Key: pk})
			}
		}

		if old, ok := rec.log[key]; ok && ev.Counter <= old.Counter {
			if ev.Counter > rec.maxCounter {
				rec.maxCounter = ev.Counter
				s.nodes[pk] = rec
			}
			continue
		}

		rec.log[key] = ev
		if ev.Counter > rec.maxCounter {
			rec.maxCounter = ev.Counter
		}
		if live {
			rec.lastEventAt = s.nowFunc()
			rebroadcast = append(rebroadcast, ev)
		}
		s.nodes[pk] = rec

		if !live {
			continue
		}

		if key.kind == attrDeny && !ev.Deleted {
			subject := types.PeerKeyFromBytes(ev.GetDeny().PeerPub)
			if _, already := s.denied[subject]; !already {
				s.denied[subject] = struct{}{}
				domainEvents = append(domainEvents, PeerDenied{Key: subject})
			}
		}
		if key.kind == attrService {
			domainEvents = append(domainEvents, ServiceChanged{Peer: pk, Name: key.name})
		}
		if key.kind == attrReachability || key.kind == attrVivaldi || key.kind == attrNetwork || key.kind == attrObservedAddress {
			domainEvents = append(domainEvents, TopologyChanged{Peer: pk})
		}
		if key.kind == attrWorkloadClaim || key.kind == attrWorkloadSpec {
			domainEvents = append(domainEvents, WorkloadChanged{Hash: key.name})
		}
	}

	return domainEvents, rebroadcast
}

func (s *store) handleSelfConflictLocked(ev *statev1.GossipEvent) []*statev1.GossipEvent {
	rec := s.nodes[s.localID]

	// Adopt persistent attrs we don't have locally. Ephemeral attrs (claims,
	// reachability) that we don't have locally are tombstoned so the deletion
	// propagates to peers still holding stale state. All adopted/tombstoned
	// entries get a counter immediately so they're visible to EncodeDelta.
	key, ok := getAttrKey(ev)
	if ok && !ev.Deleted {
		if _, exists := rec.log[key]; !exists {
			switch key.kind { //nolint:exhaustive
			case attrWorkloadSpec, attrService, attrNetwork, attrCertExpiry, attrDeny, attrNodeName:
				rec.maxCounter++
				rec.log[key] = &statev1.GossipEvent{
					PeerId:  s.localID.String(),
					Counter: rec.maxCounter,
					Change:  ev.Change,
				}
			case attrWorkloadClaim, attrReachability, attrHeartbeat, attrSeedLoad, attrSeedDemand:
				rec.maxCounter++
				rec.log[key] = &statev1.GossipEvent{
					PeerId:  s.localID.String(),
					Counter: rec.maxCounter,
					Deleted: true,
					Change:  ev.Change,
				}
			}
		}
	}

	if ev.Counter <= rec.maxCounter {
		s.nodes[s.localID] = rec
		return nil
	}

	rec.maxCounter = ev.Counter
	var evs []*statev1.GossipEvent
	for key, stored := range rec.log {
		rec.maxCounter++
		clone := &statev1.GossipEvent{
			PeerId:  stored.PeerId,
			Counter: rec.maxCounter,
			Deleted: stored.Deleted,
			Change:  stored.Change,
		}
		rec.log[key] = clone
		evs = append(evs, clone)
	}
	s.nodes[s.localID] = rec
	return evs
}

func (s *store) EncodeDelta(since Digest) []byte {
	return s.encodeDelta(since, s.snap.Load().live)
}

func (s *store) EncodeFull() []byte {
	return s.encodeDelta(Digest{proto: &statev1.Digest{}}, nil)
}

// encodeDelta computes events the remote is missing. When live is non-nil,
// only peers in the live set are included (topology-masked gossip). When nil,
// all peers are included (state persistence and initial broadcast).
func (s *store) encodeDelta(since Digest, live map[types.PeerKey]struct{}) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	var events []*statev1.GossipEvent
	remote := since.proto.GetPeers()

	for pk, rec := range s.nodes {
		if live != nil {
			if _, ok := live[pk]; !ok {
				continue
			}
		}

		rd := remote[pk.String()]

		if rd == nil || s.computePeerHash(rec) != rd.StateHash || rec.maxCounter > rd.MaxCounter {
			floor := uint64(0)
			if rd != nil && rec.maxCounter > rd.MaxCounter && s.isPrefixConsistent(rec, rd) {
				floor = rd.MaxCounter
			}

			for _, ev := range rec.log {
				if ev.Counter > floor {
					events = append(events, ev)
				}
			}
		}
	}

	slices.SortFunc(events, func(a, b *statev1.GossipEvent) int {
		if c := cmp.Compare(a.PeerId, b.PeerId); c != 0 {
			return c
		}
		return cmp.Compare(a.Counter, b.Counter)
	})

	batch := &statev1.GossipEventBatch{Events: events}
	data, _ := batch.MarshalVT()
	return data
}

func (s *store) isValidOwnerLocked(peerID types.PeerKey) bool {
	if _, denied := s.denied[peerID]; denied {
		return false
	}
	rec, ok := s.nodes[peerID]
	if !ok {
		return false
	}
	if ev, ok := rec.log[attrKey{kind: attrCertExpiry}]; ok && !ev.Deleted {
		if auth.IsExpiredAt(time.Unix(ev.GetCertExpiry().ExpiryUnix, 0), time.Now()) {
			return false
		}
	}
	return true
}

func (s *store) tombstoneStaleAttrsLocked(rec *nodeRecord) {
	ephemeral := []*statev1.GossipEvent{
		{Change: &statev1.GossipEvent_ObservedAddress{ObservedAddress: &statev1.ObservedAddressChange{}}},
		{Change: &statev1.GossipEvent_PubliclyAccessible{PubliclyAccessible: &statev1.PubliclyAccessibleChange{}}},
		{Change: &statev1.GossipEvent_NatType{NatType: &statev1.NatTypeChange{}}},
		{Change: &statev1.GossipEvent_ResourceTelemetry{ResourceTelemetry: &statev1.ResourceTelemetryChange{}}},
		{Change: &statev1.GossipEvent_TrafficHeatmap{TrafficHeatmap: &statev1.TrafficHeatmapChange{}}},
		{Change: &statev1.GossipEvent_Heartbeat{Heartbeat: &statev1.HeartbeatChange{}}},
		{Change: &statev1.GossipEvent_SeedLoad{SeedLoad: &statev1.SeedLoadChange{}}},
		{Change: &statev1.GossipEvent_SeedDemand{SeedDemand: &statev1.SeedDemandChange{}}},
		{Change: &statev1.GossipEvent_AdminCapable{AdminCapable: &statev1.AdminCapableChange{}}},
	}
	for _, ev := range ephemeral {
		rec.maxCounter++
		ev.PeerId = s.localID.String()
		ev.Counter = rec.maxCounter
		ev.Deleted = true
		key, _ := getAttrKey(ev)
		rec.log[key] = ev
	}
	for key := range rec.log {
		if key.kind == attrReachability {
			rec.maxCounter++
			ev := &statev1.GossipEvent{PeerId: s.localID.String(), Counter: rec.maxCounter, Deleted: true, Change: &statev1.GossipEvent_Reachability{Reachability: &statev1.ReachabilityChange{PeerId: key.peer.String()}}}
			rec.log[key] = ev
		}
	}
}

// CRDT Mapping & Hashing.
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

func (s *store) computePeerHash(rec nodeRecord) uint64 {
	var h uint64
	for k, ev := range rec.log {
		h ^= s.hashEntry(k, ev.Counter, ev.Deleted)
	}
	return h
}

func (s *store) hashEntry(key attrKey, counter uint64, deleted bool) uint64 {
	h := uint64(fnvOffset64)
	mix := func(b []byte) {
		for _, v := range b {
			h ^= uint64(v)
			h *= fnvPrime64
		}
	}
	mix([]byte{byte(key.kind)})
	mix([]byte(key.name))
	mix(key.peer[:])
	var buf [9]byte
	binary.LittleEndian.PutUint64(buf[:8], counter)
	if deleted {
		buf[8] = 1
	}
	mix(buf[:])
	return h
}

func (s *store) isPrefixConsistent(rec nodeRecord, rd *statev1.PeerDigest) bool {
	h := s.computePeerHash(rec)
	for key, ev := range rec.log {
		if ev.Counter > rd.MaxCounter {
			h ^= s.hashEntry(key, ev.Counter, ev.Deleted)
		}
	}
	return h == rd.StateHash
}

func getAttrKey(ev *statev1.GossipEvent) (attrKey, bool) {
	switch v := ev.Change.(type) {
	case *statev1.GossipEvent_Network:
		return attrKey{kind: attrNetwork}, true
	case *statev1.GossipEvent_ObservedAddress:
		return attrKey{kind: attrObservedAddress}, true
	case *statev1.GossipEvent_CertExpiry:
		return attrKey{kind: attrCertExpiry}, true
	case *statev1.GossipEvent_Service:
		if v.Service.Name == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrService, name: v.Service.Name}, true
	case *statev1.GossipEvent_Reachability:
		pk, err := types.PeerKeyFromString(v.Reachability.PeerId)
		if err != nil {
			return attrKey{}, false
		}
		return attrKey{kind: attrReachability, peer: pk}, true
	case *statev1.GossipEvent_Deny:
		pk := types.PeerKeyFromBytes(v.Deny.PeerPub)
		return attrKey{kind: attrDeny, name: pk.String()}, true
	case *statev1.GossipEvent_PubliclyAccessible:
		return attrKey{kind: attrPubliclyAccessible}, true
	case *statev1.GossipEvent_Vivaldi:
		return attrKey{kind: attrVivaldi}, true
	case *statev1.GossipEvent_NatType:
		return attrKey{kind: attrNatType}, true
	case *statev1.GossipEvent_ResourceTelemetry:
		return attrKey{kind: attrResourceTelemetry}, true
	case *statev1.GossipEvent_WorkloadSpec:
		if v.WorkloadSpec.Hash == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrWorkloadSpec, name: v.WorkloadSpec.Hash}, true
	case *statev1.GossipEvent_WorkloadClaim:
		if v.WorkloadClaim.Hash == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrWorkloadClaim, name: v.WorkloadClaim.Hash}, true
	case *statev1.GossipEvent_TrafficHeatmap:
		return attrKey{kind: attrTrafficHeatmap}, true
	case *statev1.GossipEvent_Heartbeat:
		return attrKey{kind: attrHeartbeat}, true
	case *statev1.GossipEvent_SeedLoad:
		return attrKey{kind: attrSeedLoad}, true
	case *statev1.GossipEvent_AdminCapable:
		return attrKey{kind: attrAdminCapable}, true
	case *statev1.GossipEvent_NodeName:
		return attrKey{kind: attrNodeName}, true
	case *statev1.GossipEvent_SeedDemand:
		return attrKey{kind: attrSeedDemand}, true
	}
	return attrKey{}, false
}
