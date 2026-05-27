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
	attrGatewayDomain
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

// put writes ev into the log under key and lifts maxCounter to
// ev.Counter if higher. The lone chokepoint for log writes; callers
// that need a fresh counter bump it on the record themselves before
// stamping ev.Counter and calling put.
func (r *nodeRecord) put(key attrKey, ev *statev1.GossipEvent) {
	r.log[key] = ev
	if ev.Counter > r.maxCounter {
		r.maxCounter = ev.Counter
	}
}

// liftCounter raises maxCounter to c if higher, without touching the
// log. Used when a stale incoming event is rejected by counter-LWW but
// the slot's bookkeeping still needs to advance so digests stay aligned.
func (r *nodeRecord) liftCounter(c uint64) {
	if c > r.maxCounter {
		r.maxCounter = c
	}
}

// ApplyDelta admits gossip arrived from a peer. See applyDeltaLocked
// for the contract.
func (s *store) ApplyDelta(data []byte) ([]Event, []byte, error) {
	var batch statev1.GossipEventBatch
	if err := batch.UnmarshalVT(data); err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	events, rebroadcast := s.applyDeltaLocked(batch.Events)
	s.updateSnapshotLocked()

	var rbData []byte
	if len(rebroadcast) > 0 {
		rbBatch := &statev1.GossipEventBatch{Events: rebroadcast}
		rbData, _ = rbBatch.MarshalVT()
	}

	return events, rbData, nil
}

// applyDeltaLocked applies a peer-supplied event batch: every event
// runs the admission gates, the slot is stamped, accepted writes are
// queued for rebroadcast, and domain events fire. Spec events are
// admitted in a second pass against a snapshot refreshed with the new
// grants and deny graph, so a Fact and its authority's Grant arriving
// together admit on first delivery (anti-entropy is not guaranteed to
// redeliver the pair). The two-pass ordering changes no converged
// register value because CRDT registers are counter-keyed LWW.
func (s *store) applyDeltaLocked(events []*statev1.GossipEvent) ([]Event, []*statev1.GossipEvent) {
	var domain []Event
	var rebroadcast []*statev1.GossipEvent
	denyOrGrantChanged := false
	now := s.nowFunc()

	apply := func(ev *statev1.GossipEvent) {
		pk, err := types.PeerKeyFromString(ev.PeerId)
		if err != nil {
			return
		}
		if pk == s.localID {
			rebroadcast = append(rebroadcast, s.handleSelfConflictLocked(ev)...)
			return
		}
		key, ok := getAttrKey(ev)
		if !ok {
			return
		}
		written, peerWasNew := s.admitPeerEventLocked(pk, key, ev)
		if peerWasNew {
			domain = append(domain, PeerJoined{Key: pk})
		}
		if !written {
			return
		}
		rec := s.nodes[pk]
		rec.lastEventAt = now
		s.nodes[pk] = rec
		rebroadcast = append(rebroadcast, ev)

		if key.kind == attrDeny || key.kind == attrGrant {
			denyOrGrantChanged = true
		}
		switch key.kind { //nolint:exhaustive
		case attrGrant:
			domain = append(domain, GrantChanged{Peer: pk})
		case attrService:
			domain = append(domain, ServiceChanged{Peer: pk, Name: key.name})
		case attrWorkloadClaim:
			domain = append(domain, WorkloadChanged{Hash: key.name})
		case attrWorkloadSpec:
			domain = append(domain, WorkloadChanged{Hash: ev.GetSpecChange().GetWorkload().GetHash()})
		case attrStaticSpec, attrStaticClaim:
			domain = append(domain, StaticChanged{Name: key.name})
		case attrNetwork, attrObservedAddress:
			domain = append(domain, TopologyChanged{Peer: pk}, AddressesChanged{Peer: pk})
		case attrReachability, attrVivaldi:
			domain = append(domain, TopologyChanged{Peer: pk})
		}
	}

	nonSpecs, specs := partitionSpecs(events)
	for _, ev := range nonSpecs {
		apply(ev)
	}
	if denyOrGrantChanged {
		domain = append(domain, s.recomputeDeniedLocked()...)
	}
	if len(specs) > 0 {
		s.updateSnapshotLocked()
		for _, ev := range specs {
			apply(ev)
		}
	}
	return domain, rebroadcast
}

// restoreFromDiskLocked replays the state.pb blob written by this same
// node on its last shutdown. Self-slot events are trusted on the same
// basis as the bytes that produced them: they were admitted or locally
// signed before they were written. Peer-slot events still pass through
// the admission gates as defence in depth against tampered state.pb.
// No domain events fire and nothing rebroadcasts: this is rehydration,
// not new observation. Wiring this entry to peer-supplied bytes would
// let foreign self-slot events through the admission bypass; use
// applyDeltaLocked / ApplyDelta for peer-sourced batches.
func (s *store) restoreFromDiskLocked(events []*statev1.GossipEvent) {
	apply := func(ev *statev1.GossipEvent) {
		pk, err := types.PeerKeyFromString(ev.PeerId)
		if err != nil {
			return
		}
		if pk == s.localID {
			s.restoreSelfEventLocked(ev)
			return
		}
		key, ok := getAttrKey(ev)
		if !ok {
			return
		}
		s.admitPeerEventLocked(pk, key, ev)
	}

	nonSpecs, specs := partitionSpecs(events)
	for _, ev := range nonSpecs {
		apply(ev)
	}
	s.recomputeDeniedLocked()
	if len(specs) > 0 {
		s.updateSnapshotLocked()
		for _, ev := range specs {
			apply(ev)
		}
	}
}

// admitPeerEventLocked runs the integrity gates on a peer-slot event
// and writes it into the slot under counter-LWW. Returns (a) whether
// the log was actually written (false on rejected admission or a
// counter-stale event) and (b) whether the slot was newly created this
// call, which the live caller turns into PeerJoined. The kind-specific
// tombstone rejections (grant, wrapping) close replay-as-deletion
// attacks: the deletion bit lives on the envelope, not in the signed
// payload.
func (s *store) admitPeerEventLocked(pk types.PeerKey, key attrKey, ev *statev1.GossipEvent) (written, peerWasNew bool) {
	switch key.kind { //nolint:exhaustive
	case attrGrant:
		if ev.Deleted || !s.isAcceptableGrantEvent(pk, ev) {
			return false, false
		}
	case attrBlobWrapping:
		if ev.Deleted || !s.isAcceptableWrappingEvent(ev) {
			return false, false
		}
	}
	if isSpecKind(key.kind) && !s.acceptableSpecEventLocked(ev) {
		return false, false
	}

	rec, exists := s.nodes[pk]
	if !exists {
		rec = newNodeRecord()
		peerWasNew = true
	}
	if old, ok := rec.log[key]; ok && ev.Counter <= old.Counter {
		rec.liftCounter(ev.Counter)
		s.nodes[pk] = rec
		return false, peerWasNew
	}
	rec.put(key, ev)
	s.nodes[pk] = rec
	return true, peerWasNew
}

// partitionSpecs splits a batch into non-spec and spec events so callers
// can apply them in the two-pass order both apply paths share. The
// split is on the proto oneof variant so callers don't have to re-derive
// an attrKey here only to consult its kind.
func partitionSpecs(events []*statev1.GossipEvent) (nonSpecs, specs []*statev1.GossipEvent) {
	for _, ev := range events {
		if _, isSpec := ev.Change.(*statev1.GossipEvent_SpecChange); isSpec {
			specs = append(specs, ev)
			continue
		}
		nonSpecs = append(nonSpecs, ev)
	}
	return nonSpecs, specs
}

// isAcceptableGrantEvent enforces three invariants on incoming grant
// events:
//   - The grant's subject_pub matches the gossip event's peer_id (basic
//     shape check).
//   - The chain is structurally and cryptographically valid (signatures
//     plus the root anchor). The grant deadline is not enforced here;
//     a past grant stays authoritative for chain-scoped decisions.
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
func (s *store) isAcceptableWrappingEvent(ev *statev1.GossipEvent) bool {
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
// The gossip-source peer is the storing peer; the Fact signer is the
// authoritative Publisher; they may differ (a daemon storing a
// tenant's signed spec is the canonical case). The signed deleted bit
// must match the envelope's Deleted flag (so a published Fact cannot
// be replayed as a tombstone) and the validate hook (the admission
// pipeline in production) must accept the change.
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
// authoritative state. Unlisted kinds (including the deny attr) fall
// through to the final `return false`: a legitimate self-deny goes
// through DenyPeer; recovery of a lost self-deny re-issues the deny
// rather than trusting a peer's recollection.
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
		return s.isAcceptableWrappingEvent(ev)
	case attrNetwork, attrNodeName, attrControlAddr, attrGatewayDomain,
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

	var events []Event
	for pk := range effective {
		if _, was := s.denied[pk]; !was {
			events = append(events, PeerDenied{Key: pk})
		}
	}
	s.denied = effective
	return events
}

// restoreSelfEventLocked replays a self-slot event from state.pb at
// its persisted counter and Deleted bit under the contract documented
// on restoreFromDiskLocked.
func (s *store) restoreSelfEventLocked(ev *statev1.GossipEvent) {
	key, ok := getAttrKey(ev)
	if !ok {
		return
	}
	rec := s.nodes[s.localID]
	rec.put(key, ev)
	s.nodes[s.localID] = rec
}

// handleSelfConflictLocked answers a peer-stamped event claiming our
// peer-id: a peer cannot plant a tombstone or a foreign-signed spec
// under our slot via gossip impersonation. The !ev.Deleted gate is
// load-bearing for that; the restore path enters via
// restoreSelfEventLocked instead, where own-disk bytes are trusted.
// We adopt persistent attrs we don't have locally; ephemeral attrs
// (claims, reachability) we don't have locally are tombstoned so the
// deletion propagates to peers still holding stale state. If the peer
// outran our counter we then rebroadcast every entry at a fresh
// counter so the cluster converges on our slot, not theirs.
func (s *store) handleSelfConflictLocked(ev *statev1.GossipEvent) []*statev1.GossipEvent {
	rec := s.nodes[s.localID]
	key, ok := getAttrKey(ev)
	if ok && !ev.Deleted && s.acceptableSelfEventLocked(key.kind, ev) {
		if _, exists := rec.log[key]; !exists {
			switch key.kind { //nolint:exhaustive
			case attrWorkloadSpec, attrService, attrNetwork, attrNodeName, attrControlAddr, attrGatewayDomain, attrStaticSpec, attrBlobSpec, attrGrant, attrBlobWrapping:
				rec.maxCounter++
				rec.put(key, &statev1.GossipEvent{
					PeerId:  s.localID.String(),
					Counter: rec.maxCounter,
					Change:  ev.Change,
				})
			case attrWorkloadClaim, attrReachability, attrHeartbeat, attrBlobAvailability, attrStaticClaim, attrBackoffTTL, attrPerSeedCallCounts:
				rec.maxCounter++
				rec.put(key, &statev1.GossipEvent{
					PeerId:  s.localID.String(),
					Counter: rec.maxCounter,
					Deleted: true,
					Change:  ev.Change,
				})
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
	// distance.
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
		rec.put(key, clone)
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
		rec.put(key, ev)
	}
	for key := range rec.log {
		if key.kind == attrReachability {
			rec.maxCounter++
			ev := &statev1.GossipEvent{PeerId: s.localID.String(), Counter: rec.maxCounter, Deleted: true, Change: &statev1.GossipEvent_Reachability{Reachability: &statev1.ReachabilityChange{PeerId: key.peer.String()}}}
			rec.put(key, ev)
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
	case *statev1.GossipEvent_GatewayDomain:
		return attrKey{kind: attrGatewayDomain}, true
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
