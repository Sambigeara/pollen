// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/hex"
	"slices"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
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
	attrAdminCapable
	attrNodeName
	attrBlobAvailability
	attrStaticSpec
	attrStaticClaim
	attrBlobSpec
	attrStaticCapable
	attrBackoffTTL
	attrPerSeedCallCounts
	attrDelegationCert
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
	denyOrCertChanged := false

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

		// Drop structurally invalid or impostor delegation certs at apply
		// time. The cert chain is signed end-to-end so this is a free
		// integrity check; without it any admitted peer could spoof
		// another's chain and bypass deny scoping.
		if key.kind == attrDelegationCert && !ev.Deleted && !s.isAcceptableCertEvent(pk, ev) {
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

		if key.kind == attrDeny || key.kind == attrDelegationCert {
			denyOrCertChanged = true
		}
		if key.kind == attrService {
			domainEvents = append(domainEvents, ServiceChanged{Peer: pk, Name: key.name})
		}
		if key.kind == attrReachability || key.kind == attrVivaldi || key.kind == attrNetwork || key.kind == attrObservedAddress {
			domainEvents = append(domainEvents, TopologyChanged{Peer: pk})
		}
		if key.kind == attrNetwork || key.kind == attrObservedAddress {
			domainEvents = append(domainEvents, AddressesChanged{Peer: pk})
		}
		if key.kind == attrWorkloadClaim || key.kind == attrWorkloadSpec {
			domainEvents = append(domainEvents, WorkloadChanged{Hash: key.name})
		}
		if key.kind == attrStaticSpec || key.kind == attrStaticClaim {
			domainEvents = append(domainEvents, StaticChanged{Name: key.name})
		}
	}

	if live && denyOrCertChanged {
		domainEvents = append(domainEvents, s.recomputeDeniedLocked()...)
	}

	return domainEvents, rebroadcast
}

// isAcceptableCertEvent enforces three invariants on incoming cert events:
//   - The cert's subject_pub matches the gossip event's peer_id
//     (basic shape check).
//   - When rootPub is configured, the chain is structurally and
//     cryptographically valid (signatures + root anchor). Expiry is
//     not enforced; past chains stay authoritative for chain-scoped
//     decisions even after their TTL.
//   - When rootPub is configured, the subject_signature is valid
//     under cert.subject_pub. This is the proof-of-possession that
//     prevents a delegated admin from forging a cert for someone
//     else's pub and re-parenting them into the admin's subtree.
func (s *store) isAcceptableCertEvent(pk types.PeerKey, ev *statev1.GossipEvent) bool {
	change := ev.GetDelegationCert()
	cert := change.GetCert()
	if cert == nil {
		return false
	}
	if !bytes.Equal(cert.GetClaims().GetSubjectPub(), pk.Bytes()) {
		return false
	}
	if len(s.rootPub) > 0 {
		if err := auth.VerifyDelegationCertStructure(cert, s.rootPub); err != nil {
			return false
		}
		if err := auth.VerifyDelegationCertSubject(cert, change.GetSubjectSignature()); err != nil {
			return false
		}
	}
	return true
}

// recomputeDeniedLocked rebuilds s.denied from authorised deny events
// plus the gossiped cert graph. Returns PeerDenied domain events for
// peers newly classified as denied. A peer becomes denied iff some
// node in its current delegation chain has an authorised deny against
// it; "authorised" means the deny was issued by the cluster root or
// by an ancestor of the subject in the subject's own cert chain.
//
// When rootPub is unset (test fixtures with no cluster identity), this
// degrades to the legacy semantic: every deny event takes effect
// unconditionally and no cascade is computed. Prod always sets rootPub.
func (s *store) recomputeDeniedLocked() []Event {
	rootKnown := len(s.rootPub) > 0

	revoked := make(map[types.PeerKey]struct{})
	if !rootKnown {
		for _, rec := range s.nodes {
			for key, ev := range rec.log {
				if key.kind == attrDeny && !ev.Deleted {
					revoked[types.PeerKeyFromBytes(ev.GetDeny().PeerPub)] = struct{}{}
				}
			}
		}
		return s.commitDeniedLocked(revoked)
	}

	certs := make(map[types.PeerKey]*admissionv1.DelegationCert)
	for pk, rec := range s.nodes {
		ev, ok := rec.log[attrKey{kind: attrDelegationCert}]
		if !ok || ev.Deleted {
			continue
		}
		if cert := ev.GetDelegationCert().GetCert(); cert != nil {
			certs[pk] = cert
		}
	}

	rootKey := types.PeerKeyFromBytes(s.rootPub)

	for issuerPK, rec := range s.nodes {
		for key, ev := range rec.log {
			if key.kind != attrDeny || ev.Deleted {
				continue
			}
			subject := types.PeerKeyFromBytes(ev.GetDeny().PeerPub)

			// Self-deny is always allowed (peer disowning itself,
			// e.g. on cert expiry sweep). Root-issued denies are
			// authorised regardless of cert availability; root is
			// the universal ancestor.
			if issuerPK == subject || issuerPK == rootKey {
				revoked[subject] = struct{}{}
				continue
			}

			cert, ok := certs[subject]
			if !ok {
				// Subject's chain unknown; deny stays pending until
				// their cert is gossiped.
				continue
			}
			for _, sub := range auth.ChainSubjectPubs(cert) {
				if bytes.Equal(sub, issuerPK.Bytes()) {
					revoked[subject] = struct{}{}
					break
				}
			}
		}
	}

	effective := make(map[types.PeerKey]struct{}, len(revoked))
	for r := range revoked {
		effective[r] = struct{}{}
	}
	for pk, cert := range certs {
		if _, already := effective[pk]; already {
			continue
		}
		for _, sub := range auth.ChainSubjectPubs(cert) {
			if _, ok := revoked[types.PeerKeyFromBytes(sub)]; ok {
				effective[pk] = struct{}{}
				break
			}
		}
	}

	return s.commitDeniedLocked(effective)
}

func (s *store) commitDeniedLocked(effective map[types.PeerKey]struct{}) []Event {
	var events []Event
	for pk := range effective {
		if _, was := s.denied[pk]; !was {
			events = append(events, PeerDenied{Key: pk})
		}
	}
	s.denied = effective
	return events
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
			case attrWorkloadSpec, attrService, attrNetwork, attrCertExpiry, attrDeny, attrNodeName, attrStaticSpec, attrBlobSpec, attrDelegationCert:
				rec.maxCounter++
				rec.log[key] = &statev1.GossipEvent{
					PeerId:  s.localID.String(),
					Counter: rec.maxCounter,
					Change:  ev.Change,
				}
			case attrWorkloadClaim, attrReachability, attrHeartbeat, attrBlobAvailability, attrStaticClaim, attrBackoffTTL, attrPerSeedCallCounts:
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
	return s.encodeDelta(since)
}

func (s *store) EncodeFull() []byte {
	return s.encodeDelta(Digest{proto: &statev1.Digest{}})
}

func (s *store) encodeDelta(since Digest) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	var events []*statev1.GossipEvent
	remote := since.proto.GetPeers()

	for pk, rec := range s.nodes {
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
		{Change: &statev1.GossipEvent_AdminCapable{AdminCapable: &statev1.AdminCapableChange{}}},
		{Change: &statev1.GossipEvent_StaticCapable{StaticCapable: &statev1.StaticCapableChange{}}},
		{Change: &statev1.GossipEvent_BlobAvailability{BlobAvailability: &statev1.BlobAvailabilityChange{}}},
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
	case *statev1.GossipEvent_AdminCapable:
		return attrKey{kind: attrAdminCapable}, true
	case *statev1.GossipEvent_StaticCapable:
		return attrKey{kind: attrStaticCapable}, true
	case *statev1.GossipEvent_NodeName:
		return attrKey{kind: attrNodeName}, true
	case *statev1.GossipEvent_BlobAvailability:
		return attrKey{kind: attrBlobAvailability}, true
	case *statev1.GossipEvent_StaticSpec:
		if v.StaticSpec.GetName() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrStaticSpec, name: v.StaticSpec.GetName()}, true
	case *statev1.GossipEvent_StaticClaim:
		if v.StaticClaim.GetName() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrStaticClaim, name: v.StaticClaim.GetName()}, true
	case *statev1.GossipEvent_BlobSpec:
		digest := v.BlobSpec.GetDigest()
		if len(digest) != sha256Len {
			return attrKey{}, false
		}
		return attrKey{kind: attrBlobSpec, name: hex.EncodeToString(digest)}, true
	case *statev1.GossipEvent_BackoffTtl:
		return attrKey{kind: attrBackoffTTL}, true
	case *statev1.GossipEvent_PerSeedCallCounts:
		return attrKey{kind: attrPerSeedCallCounts}, true
	case *statev1.GossipEvent_DelegationCert:
		return attrKey{kind: attrDelegationCert}, true
	}
	return attrKey{}, false
}
