// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/hex"
	"math"
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
	attrBlobWrapping
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
//
// Deleted spec events are admitted iff the SpecAuth carries `deleted=true`
// signed by the publisher and validate accepts the signature; otherwise a
// peer could unseed any spec by replaying a published SpecAuth wrapped in a
// tombstone envelope.
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
			bumped := s.handleSelfConflictLocked(ev, live)
			if live {
				rebroadcast = append(rebroadcast, bumped...)
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
		// another's chain and bypass deny scoping. Cert tombstones are
		// always rejected: no legitimate code path produces one (rotation
		// overwrites the live event, revocation goes through deny), so
		// admitting them would let any peer wipe another's cert by
		// replaying a captured cert event with the Deleted bit flipped.
		if key.kind == attrDelegationCert {
			if ev.Deleted || !s.isAcceptableCertEvent(pk, ev) {
				continue
			}
		}
		if isSpecKind(key.kind) && !s.acceptableSpecEventLocked(pk, ev) {
			continue
		}
		// Wrapping tombstones never travel over the wire: revocation is a
		// local action (Service.Remove evicts the on-disk envelope and
		// drops the cached DEK), and the wrapping signature alone cannot
		// authenticate the deletion bit because the bit lives on the
		// gossip envelope, not in the signed payload. Allowing tombstones
		// would let any cluster member replay a captured live wrapping
		// with the bit flipped and erase the recipient's only path back
		// to the DEK.
		if key.kind == attrBlobWrapping {
			if ev.Deleted || !s.isAcceptableWrappingEvent(pk, ev) {
				continue
			}
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
//   - The cert's subject_pub matches the gossip event's peer_id (basic
//     shape check).
//   - The chain is structurally and cryptographically valid (signatures
//   - root anchor). Expiry is not enforced; past chains stay
//     authoritative for chain-scoped decisions even after their TTL.
//   - The subject_signature is valid under cert.subject_pub. This is
//     the proof-of-possession that prevents a delegated admin from
//     forging a cert for someone else's pub and re-parenting them into
//     the admin's subtree.
func (s *store) isAcceptableCertEvent(pk types.PeerKey, ev *statev1.GossipEvent) bool {
	change := ev.GetDelegationCert()
	cert := change.GetCert()
	if cert == nil {
		return false
	}
	if !bytes.Equal(cert.GetClaims().GetSubjectPub(), pk.Bytes()) {
		return false
	}
	if err := auth.VerifyDelegationCertStructure(cert, s.rootPub); err != nil {
		return false
	}
	if err := auth.VerifyDelegationCertSubject(cert, change.GetSubjectSignature()); err != nil {
		return false
	}
	return true
}

// isAcceptableWrappingEvent enforces that a live wrapping was actually
// signed by the wrapper claimed in the gossip envelope and that the
// wrapper's cert chains to the cluster root. Tombstones are rejected
// upstream: callers must guard with `ev.Deleted` before invoking this.
func (s *store) isAcceptableWrappingEvent(pk types.PeerKey, ev *statev1.GossipEvent) bool {
	wrapping := ev.GetBlobWrapping()
	if wrapping == nil {
		return false
	}
	wrapper := wrapping.GetWrapper()
	if wrapper == nil {
		return false
	}
	if !bytes.Equal(wrapper.GetClaims().GetSubjectPub(), pk.Bytes()) {
		return false
	}
	if err := auth.VerifyBlobWrapping(wrapping, s.rootPub, s.nowFunc()); err != nil {
		return false
	}
	return true
}

// acceptableSpecEventLocked admits a spec event from a remote peer.
// The publisher pub embedded in SpecAuth must match the gossip's peer
// id (no impersonation), the signed deleted bit must match the gossip
// envelope's Deleted flag (so a published SpecAuth cannot be replayed
// as a tombstone), and the validate hook must accept the change (which
// in production runs gate.Admit and verifies the SpecAuth signature).
func (s *store) acceptableSpecEventLocked(pk types.PeerKey, ev *statev1.GossipEvent) bool {
	sc := ev.GetSpecChange()
	if !specAuthMatchesPeer(pk, sc) {
		return false
	}
	if sc.GetAuth().GetDeleted() != ev.Deleted {
		return false
	}
	if s.validate != nil {
		if err := s.validate(sc); err != nil {
			return false
		}
	}
	return true
}

// acceptableSelfEventLocked enforces the same admission checks on
// gossip events that claim to be from us as we apply to events from
// any other peer. Without these, any peer could plant a SpecChange or
// DelegationCert under our peer-id and have us adopt it as our own
// authoritative state.
//
// The default branch fails closed: any attr not explicitly listed
// here cannot be adopted via self-conflict. attrDeny in particular
// must never be adopted from a peer's claim — legitimate self-deny
// goes through DenyPeer; recovery of a lost self-deny is handled by
// re-issuing the deny rather than trusting a peer's recollection.
func (s *store) acceptableSelfEventLocked(kind attrKind, ev *statev1.GossipEvent) bool {
	switch kind { //nolint:exhaustive
	case attrDelegationCert:
		return s.isAcceptableCertEvent(s.localID, ev)
	case attrWorkloadSpec, attrService, attrStaticSpec, attrBlobSpec:
		return s.acceptableSpecEventLocked(s.localID, ev)
	case attrBlobWrapping:
		return s.isAcceptableWrappingEvent(s.localID, ev)
	case attrNetwork, attrNodeName,
		attrWorkloadClaim, attrReachability, attrHeartbeat, attrBlobAvailability,
		attrStaticClaim, attrBackoffTTL, attrPerSeedCallCounts:
		return true
	}
	return false
}

// recomputeDeniedLocked rebuilds s.denied from authorised deny events
// plus the gossiped cert graph. Returns PeerDenied domain events for
// peers newly classified as denied. A peer becomes denied iff some
// node in its current delegation chain has an authorised deny against
// it; "authorised" means the deny was issued by the cluster root or
// by an ancestor of the subject in the subject's own cert chain.
func (s *store) recomputeDeniedLocked() []Event {
	revoked := make(map[types.PeerKey]struct{})

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

func (s *store) handleSelfConflictLocked(ev *statev1.GossipEvent, live bool) []*statev1.GossipEvent {
	rec := s.nodes[s.localID]

	// Adopt persistent attrs we don't have locally. Ephemeral attrs (claims,
	// reachability) that we don't have locally are tombstoned so the deletion
	// propagates to peers still holding stale state. All adopted/tombstoned
	// entries get a counter immediately so they're visible to EncodeDelta.
	//
	// live=false means we're loading our own previously-persisted state
	// from disk; the admission filter is for live gossip where a peer
	// could plant events under our peer-id.
	key, ok := getAttrKey(ev)
	if ok && !ev.Deleted && (!live || s.acceptableSelfEventLocked(key.kind, ev)) {
		if _, exists := rec.log[key]; !exists {
			switch key.kind { //nolint:exhaustive
			case attrWorkloadSpec, attrService, attrNetwork, attrDeny, attrNodeName, attrStaticSpec, attrBlobSpec, attrDelegationCert, attrBlobWrapping:
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

	// Reject counters that would overflow during the rebroadcast bump.
	// A malicious peer can send ev.Counter near MaxUint64 to push us
	// into wraparound; legitimate peers stay within event-rate-bounded
	// distance. Drop the event and keep our existing counter.
	if uint64(len(rec.log))+1 > math.MaxUint64-ev.Counter {
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
	_, ok := s.nodes[peerID]
	return ok
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
	case *statev1.GossipEvent_StaticClaim:
		if v.StaticClaim.GetName() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrStaticClaim, name: v.StaticClaim.GetName()}, true
	case *statev1.GossipEvent_SpecChange:
		return specAttrKey(v.SpecChange)
	case *statev1.GossipEvent_BackoffTtl:
		return attrKey{kind: attrBackoffTTL}, true
	case *statev1.GossipEvent_PerSeedCallCounts:
		return attrKey{kind: attrPerSeedCallCounts}, true
	case *statev1.GossipEvent_DelegationCert:
		return attrKey{kind: attrDelegationCert}, true
	case *statev1.GossipEvent_BlobWrapping:
		w := v.BlobWrapping
		if len(w.GetBlobHash()) == 0 || len(w.GetRecipientPubkey()) == 0 {
			return attrKey{}, false
		}
		return attrKey{kind: attrBlobWrapping, name: hex.EncodeToString(w.GetBlobHash()), peer: types.PeerKeyFromBytes(w.GetRecipientPubkey())}, true
	}
	return attrKey{}, false
}

func isSpecKind(kind attrKind) bool {
	return kind == attrWorkloadSpec || kind == attrService || kind == attrStaticSpec || kind == attrBlobSpec
}

func specAttrKey(sc *statev1.SpecChange) (attrKey, bool) {
	switch body := sc.GetBody().(type) {
	case *statev1.SpecChange_Workload:
		if body.Workload.GetHash() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrWorkloadSpec, name: body.Workload.GetHash()}, true
	case *statev1.SpecChange_Service:
		if body.Service.GetName() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrService, name: body.Service.GetName()}, true
	case *statev1.SpecChange_Static:
		if body.Static.GetName() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrStaticSpec, name: body.Static.GetName()}, true
	case *statev1.SpecChange_Blob:
		digest := body.Blob.GetDigest()
		if len(digest) != sha256Len {
			return attrKey{}, false
		}
		return attrKey{kind: attrBlobSpec, name: hex.EncodeToString(digest)}, true
	}
	return attrKey{}, false
}

func specAuthMatchesPeer(pk types.PeerKey, sc *statev1.SpecChange) bool {
	return bytes.Equal(sc.GetAuth().GetPublisher().GetClaims().GetSubjectPub(), pk.Bytes())
}
