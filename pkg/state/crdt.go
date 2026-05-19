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

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/identity"
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
	attrGrant
	attrBlobWrapping
	attrControlAddr
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
// Spec events are admitted in a second pass, after every other event in
// the batch has been applied and the denylist and snapshot refreshed.
// Spec admission resolves the authority Grant and the denylist from the
// snapshot, so a Grant and a Fact delivered together (the canonical
// EncodeFull restore blob, which is delivered exactly once) must admit on
// the first delivery rather than being dropped until anti-entropy
// redelivers them, which never happens on the restore path. CRDT
// registers are counter-keyed LWW, so the two-pass ordering changes no
// converged value, and it is skipped when the batch carries no specs.
// This function owns the batch's deny recompute for both paths.
//
// Deleted spec events are admitted iff the Fact carries `deleted=true`
// signed by the publisher and validate accepts the signature; otherwise a
// peer could unseed any spec by replaying a published Fact wrapped in a
// tombstone envelope.
func (s *store) applyBatchLocked(events []*statev1.GossipEvent, live bool) ([]Event, []*statev1.GossipEvent) {
	var domainEvents []Event
	var rebroadcast []*statev1.GossipEvent
	denyOrGrantChanged := false

	applyOne := func(ev *statev1.GossipEvent) {
		pk, err := types.PeerKeyFromString(ev.PeerId)
		if err != nil {
			return
		}

		if pk == s.localID {
			bumped := s.handleSelfConflictLocked(ev, live)
			if live {
				rebroadcast = append(rebroadcast, bumped...)
			}
			return
		}

		key, ok := getAttrKey(ev)
		if !ok {
			return
		}

		// Drop structurally invalid or impostor grants at apply time. The
		// grant chain is signed end-to-end so this is a free integrity
		// check; without it any admitted peer could spoof another's chain
		// and bypass deny scoping. Grant tombstones are always rejected:
		// no legitimate code path produces one (re-mint overwrites the
		// live event, revocation goes through deny), so admitting them
		// would let any peer wipe another's grant by replaying a captured
		// grant event with the Deleted bit flipped.
		if key.kind == attrGrant {
			if ev.Deleted || !s.isAcceptableGrantEvent(pk, ev) {
				return
			}
		}
		if isSpecKind(key.kind) && !s.acceptableSpecEventLocked(ev) {
			return
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
				return
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
			return
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
			return
		}

		if key.kind == attrDeny || key.kind == attrGrant {
			denyOrGrantChanged = true
		}
		if key.kind == attrGrant {
			domainEvents = append(domainEvents, GrantChanged{Peer: pk})
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
		if key.kind == attrWorkloadClaim {
			domainEvents = append(domainEvents, WorkloadChanged{Hash: key.name})
		}
		if key.kind == attrWorkloadSpec {
			domainEvents = append(domainEvents, WorkloadChanged{Hash: ev.GetSpecChange().GetWorkload().GetHash()})
		}
		if key.kind == attrStaticSpec || key.kind == attrStaticClaim {
			domainEvents = append(domainEvents, StaticChanged{Name: key.name})
		}
	}

	var specs []*statev1.GossipEvent
	for _, ev := range events {
		if key, ok := getAttrKey(ev); ok && isSpecKind(key.kind) {
			specs = append(specs, ev)
			continue
		}
		applyOne(ev)
	}

	recompute := func() {
		if !live || denyOrGrantChanged {
			deny := s.recomputeDeniedLocked()
			if live {
				domainEvents = append(domainEvents, deny...)
			}
		}
	}

	if len(specs) == 0 {
		recompute()
		return domainEvents, rebroadcast
	}

	recompute()
	s.updateSnapshotLocked()
	// Spec admission counts budget against the snapshot refreshed once
	// here, not per spec, so a batch carrying several of one authority's
	// facts admits them all against the pre-loop usage. This is
	// intentional and convergent: the publisher's own per-spec publish
	// path enforces the budget at origin, gossip and restore only
	// replay already-admitted facts, and every node applies the same
	// batch to the same count, so no node can be driven to an
	// authority-controlled over-count here.
	for _, ev := range specs {
		applyOne(ev)
	}

	return domainEvents, rebroadcast
}

// isAcceptableGrantEvent enforces three invariants on incoming grant
// events:
//   - The grant's subject_pub matches the gossip event's peer_id (basic
//     shape check).
//   - The chain is structurally and cryptographically valid (signatures
//   - root anchor). The grant deadline is not enforced here; a past
//     grant stays authoritative for chain-scoped decisions.
//   - The subject_signature is valid under grant.subject_pub. This is
//     the proof-of-possession that prevents a delegated admin from
//     forging a grant for someone else's pub and re-parenting them into
//     the admin's subtree.
func (s *store) isAcceptableGrantEvent(pk types.PeerKey, ev *statev1.GossipEvent) bool {
	change := ev.GetGrant()
	grant := change.GetGrant()
	if grant == nil {
		return false
	}
	if !bytes.Equal(grant.GetClaims().GetSubjectPub(), pk.Bytes()) {
		return false
	}
	if err := identity.VerifyGrantStructure(grant, s.rootPub); err != nil {
		return false
	}
	if err := identity.VerifyGrantSubject(grant, change.GetSubjectSignature()); err != nil {
		return false
	}
	return true
}

// isAcceptableWrappingEvent verifies a live wrapping against its named
// authority's grant resolved from gossiped state, holding it to the
// durable-authority rule. Tombstones are rejected upstream: callers
// must guard with `ev.Deleted` before invoking this.
func (s *store) isAcceptableWrappingEvent(_ types.PeerKey, ev *statev1.GossipEvent) bool {
	wrapping := ev.GetBlobWrapping()
	if wrapping == nil {
		return false
	}
	grant := s.grantForPeerLocked(types.PeerKeyFromBytes(wrapping.GetAuthorityPub()))
	if grant == nil {
		return false
	}
	if err := fact.VerifyBlobWrapping(wrapping, grant, s.rootPub, s.nowFunc(), s.deniedCheckerLocked()); err != nil {
		return false
	}
	return true
}

// grantForPeerLocked returns the grant a peer has gossiped, or nil. The
// caller must hold s.mu.
func (s *store) grantForPeerLocked(pk types.PeerKey) *identityv1.Grant {
	rec, ok := s.nodes[pk]
	if !ok {
		return nil
	}
	ev, ok := rec.log[attrKey{kind: attrGrant}]
	if !ok || ev.Deleted {
		return nil
	}
	return ev.GetGrant().GetGrant()
}

// deniedCheckerLocked adapts the locked denied set to identity's
// subject-pub deny predicate for fact verification.
func (s *store) deniedCheckerLocked() identity.DenyChecker {
	return func(subjectPub []byte) bool {
		_, denied := s.denied[types.PeerKeyFromBytes(subjectPub)]
		return denied
	}
}

// acceptableSpecEventLocked admits a spec event from any peer slot.
// Under the signed-event relay model, the gossip-source peer is the
// storing peer for the spec; the Fact signer is the authoritative
// Publisher. They may differ: a daemon storing and gossipping a
// tenant's signed spec is the canonical case.
//
// The signed deleted bit must match the gossip envelope's Deleted flag
// (so a published Fact cannot be replayed as a tombstone), and the
// validate hook must accept the change (which in production runs the
// admission pipeline and verifies the Fact signature).
func (s *store) acceptableSpecEventLocked(ev *statev1.GossipEvent) bool {
	sc := ev.GetSpecChange()
	if sc.GetFact().GetDeleted() != ev.Deleted {
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
// Grant under our peer-id and have us adopt it as our own
// authoritative state.
//
// The default branch fails closed: any attr not explicitly listed
// here cannot be adopted via self-conflict. attrDeny in particular
// must never be adopted from a peer's claim. Legitimate self-deny
// goes through DenyPeer; recovery of a lost self-deny is handled by
// re-issuing the deny rather than trusting a peer's recollection.
func (s *store) acceptableSelfEventLocked(kind attrKind, ev *statev1.GossipEvent) bool {
	switch kind { //nolint:exhaustive
	case attrGrant:
		return s.isAcceptableGrantEvent(s.localID, ev)
	case attrWorkloadSpec, attrService, attrStaticSpec, attrBlobSpec:
		// Reject foreign-signed specs from being adopted into our own
		// slot via gossip impersonation. The relay model still applies
		// to other peers' slots; here we enforce that our slot stays
		// authoritatively ours.
		if !factAuthorityMatchesPeer(s.localID, ev.GetSpecChange()) {
			return false
		}
		return s.acceptableSpecEventLocked(ev)
	case attrBlobWrapping:
		return s.isAcceptableWrappingEvent(s.localID, ev)
	case attrNetwork, attrNodeName, attrControlAddr,
		attrWorkloadClaim, attrReachability, attrHeartbeat, attrBlobAvailability,
		attrStaticClaim, attrBackoffTTL, attrPerSeedCallCounts:
		return true
	}
	return false
}

// recomputeDeniedLocked rebuilds s.denied from authorised deny events
// plus the gossiped grant graph. Returns PeerDenied domain events for
// peers newly classified as denied. A peer becomes denied iff some
// node in its current grant chain has an authorised deny against it;
// "authorised" means the deny was issued by the cluster root or by an
// ancestor of the subject in the subject's own grant chain.
func (s *store) recomputeDeniedLocked() []Event {
	revoked := make(map[types.PeerKey]struct{})

	grants := make(map[types.PeerKey]*identityv1.Grant)
	for pk, rec := range s.nodes {
		ev, ok := rec.log[attrKey{kind: attrGrant}]
		if !ok || ev.Deleted {
			continue
		}
		if grant := ev.GetGrant().GetGrant(); grant != nil {
			grants[pk] = grant
		}
	}

	rootKey := types.PeerKeyFromBytes(s.rootPub)

	for issuerPK, rec := range s.nodes {
		for key, ev := range rec.log {
			if key.kind != attrDeny || ev.Deleted {
				continue
			}
			subject := types.PeerKeyFromBytes(ev.GetDeny().PeerPub)

			// Self-deny is always allowed (peer disowning itself).
			// Root-issued denies are authorised regardless of grant
			// availability; root is the universal ancestor.
			if issuerPK == subject || issuerPK == rootKey {
				revoked[subject] = struct{}{}
				continue
			}

			grant, ok := grants[subject]
			if !ok {
				// Subject's chain unknown; deny stays pending until
				// their grant is gossiped.
				continue
			}
			for _, sub := range identity.ChainSubjectPubs(grant) {
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
	for pk, grant := range grants {
		if _, already := effective[pk]; already {
			continue
		}
		for _, sub := range identity.ChainSubjectPubs(grant) {
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
			case attrWorkloadSpec, attrService, attrNetwork, attrDeny, attrNodeName, attrControlAddr, attrStaticSpec, attrBlobSpec, attrGrant, attrBlobWrapping:
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
	case *statev1.GossipEvent_ControlAddr:
		return attrKey{kind: attrControlAddr}, true
	case *statev1.GossipEvent_BlobAvailability:
		return attrKey{kind: attrBlobAvailability}, true
	case *statev1.GossipEvent_StaticClaim:
		if v.StaticClaim.GetName() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrStaticClaim, name: v.StaticClaim.GetName(), peer: types.PeerKeyFromBytes(v.StaticClaim.GetAuthorityPub())}, true
	case *statev1.GossipEvent_SpecChange:
		return specAttrKey(v.SpecChange)
	case *statev1.GossipEvent_BackoffTtl:
		return attrKey{kind: attrBackoffTTL}, true
	case *statev1.GossipEvent_PerSeedCallCounts:
		return attrKey{kind: attrPerSeedCallCounts}, true
	case *statev1.GossipEvent_Grant:
		return attrKey{kind: attrGrant}, true
	case *statev1.GossipEvent_BlobWrapping:
		w := v.BlobWrapping
		if len(w.GetBlobHash()) == 0 || len(w.GetRecipientPub()) == 0 {
			return attrKey{}, false
		}
		return attrKey{kind: attrBlobWrapping, name: hex.EncodeToString(w.GetBlobHash()), peer: types.PeerKeyFromBytes(w.GetRecipientPub())}, true
	}
	return attrKey{}, false
}

func isSpecKind(kind attrKind) bool {
	return kind == attrWorkloadSpec || kind == attrService || kind == attrStaticSpec || kind == attrBlobSpec
}

// specAttrKey derives a spec event's CRDT register key. Workload,
// static and blob specs are publication identities keyed by (authority
// Principal, logical name): the authority is the Fact signer, the name
// is the publisher-chosen logical name. Two tenants publishing the same
// bytes or the same name therefore occupy distinct registers and never
// collide, which is what makes the cluster multi-tenant. The content
// hash/digest is the artefact identity, carried on the body and keyed
// by the runtime/fetch indices, not here. Services bind per-peer and
// keep their name-only key (no tenant authority dimension).
func specAttrKey(sc *statev1.SpecChange) (attrKey, bool) {
	authority := types.PeerKeyFromBytes(sc.GetFact().GetAuthorityPub())
	switch body := sc.GetBody().(type) {
	case *statev1.SpecChange_Workload:
		if body.Workload.GetName() == "" || body.Workload.GetHash() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrWorkloadSpec, name: body.Workload.GetName(), peer: authority}, true
	case *statev1.SpecChange_Service:
		if body.Service.GetName() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrService, name: body.Service.GetName()}, true
	case *statev1.SpecChange_Static:
		if body.Static.GetName() == "" {
			return attrKey{}, false
		}
		return attrKey{kind: attrStaticSpec, name: body.Static.GetName(), peer: authority}, true
	case *statev1.SpecChange_Blob:
		if body.Blob.GetName() == "" || len(body.Blob.GetDigest()) != sha256Len {
			return attrKey{}, false
		}
		return attrKey{kind: attrBlobSpec, name: body.Blob.GetName(), peer: authority}, true
	}
	return attrKey{}, false
}

func factAuthorityMatchesPeer(pk types.PeerKey, sc *statev1.SpecChange) bool {
	return bytes.Equal(sc.GetFact().GetAuthorityPub(), pk.Bytes())
}
