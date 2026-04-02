package state

import (
	"cmp"
	"encoding/binary"
	"maps"
	"slices"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

type applyResult struct {
	rebroadcast    []*statev1.GossipEvent
	newPeers       []types.PeerKey
	deniedPeers    []types.PeerKey
	serviceEvents  []ServiceChanged
	topologyDirty  bool
	workloadsDirty bool
}

func (r applyResult) events() []Event {
	var out []Event
	for _, pk := range r.newPeers {
		out = append(out, PeerJoined{Key: pk})
	}
	for _, pk := range r.deniedPeers {
		out = append(out, PeerDenied{Key: pk})
	}
	if r.topologyDirty {
		out = append(out, TopologyChanged{})
	}
	if r.workloadsDirty {
		out = append(out, WorkloadChanged{})
	}
	for _, sc := range r.serviceEvents {
		out = append(out, sc)
	}
	if len(r.rebroadcast) > 0 {
		out = append(out, GossipApplied{})
	}
	return out
}

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

func computePeerHash(rec nodeRecord) uint64 {
	var digest uint64
	for key, entry := range rec.log {
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
		binary.LittleEndian.PutUint64(buf[:8], entry.Counter)
		if entry.Deleted {
			buf[8] = 1
		}
		mix(buf[:])
		digest ^= h
	}
	return digest
}

func (s *store) digestLocked() *statev1.Digest {
	peers := make(map[string]*statev1.PeerDigest, len(s.nodes))
	for peerID, rec := range s.nodes {
		peers[peerID.String()] = &statev1.PeerDigest{
			MaxCounter: rec.maxCounter,
			StateHash:  computePeerHash(rec),
		}
	}
	return &statev1.Digest{Peers: peers}
}

// missingFor returns gossip events the remote is missing per its digest.
//
// For each peer:
//   - Unknown to remote → full dump
//   - Counter ahead → delta (events above remote's counter)
//   - Counter equal, hash differs → full dump (divergence at same counter)
//   - Counter equal, hash matches → skip (synced)
//
// TODO(saml): the delta path assumes the remote holds a consistent prefix of
// our log, which may not be true if the remote missed an intermediate event
// during a partition. The hash check at equal counters catches this, but under
// sustained event generation counters may never equalize — leaving stale
// attributes until a quiescent period. A future fix could track per-peer
// counter→hash history to verify prefix consistency before sending a delta.
func (s *store) missingFor(digest *statev1.Digest) []*statev1.GossipEvent {
	remotePeers := digest.GetPeers()

	var events []*statev1.GossipEvent
	s.do(func() {
		for peerID, rec := range s.nodes {
			if rec.maxCounter == 0 {
				continue
			}
			rd := remotePeers[peerID.String()]
			if rd == nil {
				events = append(events, buildEventsAbove(peerID, rec, 0)...)
				continue
			}
			if rec.maxCounter > rd.GetMaxCounter() {
				events = append(events, buildEventsAbove(peerID, rec, rd.GetMaxCounter())...)
				continue
			}
			if rec.maxCounter == rd.GetMaxCounter() && computePeerHash(rec) != rd.GetStateHash() {
				events = append(events, buildEventsAbove(peerID, rec, 0)...)
			}
		}
	})

	slices.SortFunc(events, func(a, b *statev1.GossipEvent) int {
		if c := cmp.Compare(a.GetPeerId(), b.GetPeerId()); c != 0 {
			return c
		}
		return cmp.Compare(a.GetCounter(), b.GetCounter())
	})

	return events
}

func (s *store) applyEvents(events []*statev1.GossipEvent) applyResult {
	var (
		result            applyResult
		newlyDenied       []types.PeerKey
		routesDirty       bool
		reachabilityDirty bool
		workloadsDirty    bool
	)

	s.do(func() {
		var selfEvents, otherEvents []*statev1.GossipEvent
		localIDStr := s.localID.String()
		for _, event := range events {
			if event == nil {
				continue
			}
			if event.GetPeerId() == localIDStr {
				selfEvents = append(selfEvents, event)
			} else {
				otherEvents = append(otherEvents, event)
			}
		}

		newlyDenied = make([]types.PeerKey, 0, len(otherEvents))
		seenNewPeers := make(map[types.PeerKey]struct{})

		if len(selfEvents) > 0 {
			r, wlDirty := s.handleSelfConflictLocked(selfEvents)
			if len(r.rebroadcast) > 0 {
				workloadsDirty = workloadsDirty || wlDirty
			}
			result.rebroadcast = append(result.rebroadcast, r.rebroadcast...)
		}

		for _, event := range otherEvents {
			peerID, err := types.PeerKeyFromString(event.GetPeerId())
			isNew := err == nil && s.nodes[peerID].maxCounter == 0

			r, denied := s.applyEventLocked(event)
			if len(r.rebroadcast) > 0 {
				if isNew {
					if _, dup := seenNewPeers[peerID]; !dup {
						seenNewPeers[peerID] = struct{}{}
						result.newPeers = append(result.newPeers, peerID)
					}
				}

				switch v := event.GetChange().(type) {
				case *statev1.GossipEvent_Reachability:
					routesDirty = true
					reachabilityDirty = true
				case *statev1.GossipEvent_Vivaldi:
					routesDirty = true
				case *statev1.GossipEvent_WorkloadSpec, *statev1.GossipEvent_WorkloadClaim:
					workloadsDirty = true
				case *statev1.GossipEvent_Service:
					result.serviceEvents = append(result.serviceEvents, ServiceChanged{
						Peer: peerID,
						Name: v.Service.GetName(),
					})
				}
			}
			result.rebroadcast = append(result.rebroadcast, r.rebroadcast...)
			newlyDenied = append(newlyDenied, denied...)
		}

		result.deniedPeers = newlyDenied
		result.topologyDirty = routesDirty
		result.workloadsDirty = workloadsDirty || reachabilityDirty

		s.updateSnapshot()
	})

	return result
}

func (s *store) handleSelfConflictLocked(selfEvents []*statev1.GossipEvent) (applyResult, bool) {
	local := s.nodes[s.localID]

	var maxIncoming uint64
	for _, ev := range selfEvents {
		if ev.GetCounter() > maxIncoming {
			maxIncoming = ev.GetCounter()
		}
	}

	conflictDetected := maxIncoming > local.maxCounter
	if conflictDetected {
		local.maxCounter = maxIncoming
	}

	for _, ev := range selfEvents {
		v, ok := ev.GetChange().(*statev1.GossipEvent_Deny)
		if !ok || v.Deny == nil || ev.GetDeleted() {
			continue
		}
		subjectKey := types.PeerKeyFromBytes(v.Deny.GetPeerPub())
		s.denied[subjectKey] = struct{}{}
		key := attrKey{kind: attrDeny, name: subjectKey.String()}
		if _, ok := local.log[key]; !ok {
			local.log[key] = logEntry{}
			conflictDetected = true
		}
	}

	bestSpec := make(map[string]*statev1.GossipEvent)
	for _, ev := range selfEvents {
		v, ok := ev.GetChange().(*statev1.GossipEvent_WorkloadSpec)
		if !ok || v.WorkloadSpec == nil || v.WorkloadSpec.GetHash() == "" {
			continue
		}
		hash := v.WorkloadSpec.GetHash()
		if prev, exists := bestSpec[hash]; !exists || ev.GetCounter() > prev.GetCounter() {
			bestSpec[hash] = ev
		}
	}

	adopted := false
	for hash, ev := range bestSpec {
		if ev.GetDeleted() {
			continue
		}
		key := attrKey{kind: attrWorkloadSpec, name: hash}
		if existing, ok := local.log[key]; ok && !existing.Deleted {
			continue
		}
		spec := ev.GetChange().(*statev1.GossipEvent_WorkloadSpec).WorkloadSpec //nolint:forcetypeassert
		m := make(map[string]*statev1.WorkloadSpecChange, len(local.WorkloadSpecs)+1)
		maps.Copy(m, local.WorkloadSpecs)
		m[hash] = spec
		local.WorkloadSpecs = m
		local.log[key] = logEntry{}
		adopted = true
	}

	bestClaim := make(map[string]*statev1.GossipEvent)
	for _, ev := range selfEvents {
		v, ok := ev.GetChange().(*statev1.GossipEvent_WorkloadClaim)
		if !ok || v.WorkloadClaim == nil || v.WorkloadClaim.GetHash() == "" {
			continue
		}
		hash := v.WorkloadClaim.GetHash()
		if prev, exists := bestClaim[hash]; !exists || ev.GetCounter() > prev.GetCounter() {
			bestClaim[hash] = ev
		}
	}

	claimsDeleted := false
	for hash, ev := range bestClaim {
		if ev.GetDeleted() {
			continue
		}
		key := attrKey{kind: attrWorkloadClaim, name: hash}
		if _, ok := local.log[key]; ok {
			continue
		}
		local.log[key] = logEntry{Deleted: true}
		claimsDeleted = true
	}

	reachabilityDeleted := false
	for _, ev := range selfEvents {
		v, ok := ev.GetChange().(*statev1.GossipEvent_Reachability)
		if !ok || v.Reachability == nil || ev.GetDeleted() {
			continue
		}
		pk, err := types.PeerKeyFromString(v.Reachability.GetPeerId())
		if err != nil {
			continue
		}
		key := attrKey{kind: attrReachability, peer: pk}
		if _, ok := local.log[key]; ok {
			continue
		}
		local.log[key] = logEntry{Deleted: true}
		reachabilityDeleted = true
	}

	if !conflictDetected && !adopted && !claimsDeleted && !reachabilityDeleted {
		s.nodes[s.localID] = local
		return applyResult{}, false
	}

	s.nodes[s.localID] = local
	return applyResult{rebroadcast: s.bumpAndBroadcastAllLocked()}, adopted
}

func (s *store) applyEventLocked(event *statev1.GossipEvent) (applyResult, []types.PeerKey) {
	peerID, err := types.PeerKeyFromString(event.GetPeerId())
	if err != nil {
		return applyResult{}, nil
	}

	rec, exists := s.nodes[peerID]
	if !exists {
		rec = newNodeRecord(peerID)
	}

	key, ok := eventAttrKey(event)
	if !ok {
		return applyResult{}, nil
	}

	if peerID == s.localID {
		return applyResult{}, nil
	}

	if existing, ok := rec.log[key]; ok && event.GetCounter() <= existing.Counter {
		if event.GetCounter() > rec.maxCounter {
			rec.maxCounter = event.GetCounter()
			s.nodes[peerID] = rec
		}
		return applyResult{}, nil
	}

	deleted := event.GetDeleted()
	var newlyDenied []types.PeerKey

	switch {
	case key.kind == attrDeny:
		if deleted {
			return applyResult{}, nil
		}
		v := event.GetChange().(*statev1.GossipEvent_Deny) //nolint:forcetypeassert
		subjectKey := types.PeerKeyFromBytes(v.Deny.GetPeerPub())
		if _, ok := s.denied[subjectKey]; !ok {
			s.denied[subjectKey] = struct{}{}
			newlyDenied = append(newlyDenied, subjectKey)
		}
	case deleted:
		applyDeleteLocked(&rec, key)
	default:
		if !applyValueLocked(&rec, event, key) {
			return applyResult{}, nil
		}
	}

	rec.log[key] = logEntry{Counter: event.GetCounter(), Deleted: deleted}
	if event.GetCounter() > rec.maxCounter {
		rec.maxCounter = event.GetCounter()
	}
	s.nodes[peerID] = rec

	rebroadcast := []*statev1.GossipEvent{event}

	if !deleted {
		if v, ok := event.GetChange().(*statev1.GossipEvent_WorkloadSpec); ok && v.WorkloadSpec != nil {
			rebroadcast = append(rebroadcast, s.tombstoneLosingLocalSpec(peerID, v.WorkloadSpec.GetHash())...)
		}
	}

	return applyResult{rebroadcast: rebroadcast}, newlyDenied
}

func (s *store) bumpAndBroadcastAllLocked() []*statev1.GossipEvent {
	local := s.nodes[s.localID]

	for key, entry := range local.log {
		local.maxCounter++
		entry.Counter = local.maxCounter
		local.log[key] = entry
	}

	s.nodes[s.localID] = local
	return buildEventsAbove(s.localID, local, 0)
}

func buildEventsAbove(peerID types.PeerKey, rec nodeRecord, minCounter uint64) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	peerIDStr := peerID.String()

	for key, entry := range rec.log {
		if entry.Counter <= minCounter {
			continue
		}
		events = append(events, buildEventFromLog(peerIDStr, key, entry, rec))
	}

	return events
}
