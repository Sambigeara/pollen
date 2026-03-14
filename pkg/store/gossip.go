package store

import (
	"cmp"
	"maps"
	"slices"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
)

type ApplyResult struct {
	Rebroadcast []*statev1.GossipEvent
}

// EagerSyncClock returns an empty digest when no remote peer has state yet
// (so the responder sends everything), or the real digest otherwise. Both the
// remote-state check and digest construction happen under a single RLock to
// avoid a TOCTOU race with concurrent ApplyEvents calls.
func (s *Store) EagerSyncClock() *statev1.GossipStateDigest {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for peerID, rec := range s.nodes {
		if peerID != s.LocalID && rec.maxCounter > 0 {
			return s.digestLocked()
		}
	}
	return &statev1.GossipStateDigest{}
}

func (s *Store) Clock() *statev1.GossipStateDigest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.digestLocked()
}

func (s *Store) digestLocked() *statev1.GossipStateDigest {
	peers := make(map[string]*statev1.PeerDigest, len(s.nodes))
	for peerID, rec := range s.nodes {
		peers[peerID.String()] = &statev1.PeerDigest{
			MaxCounter: rec.maxCounter,
			StateHash:  computePeerHashLocked(rec),
		}
	}
	return &statev1.GossipStateDigest{Peers: peers}
}

// MissingFor returns the individual events that the remote peer is missing,
// based on the provided state digest.
//
// For each local peer:
//   - unknown to remote          → send all (full dump)
//   - hash mismatch              → send all (full dump)
//   - hash match, counter ahead  → send delta above remote counter
//   - hash match, counter match  → skip (fully synced)
func (s *Store) MissingFor(digest *statev1.GossipStateDigest) []*statev1.GossipEvent {
	remotePeers := digest.GetPeers()

	s.mu.RLock()
	defer s.mu.RUnlock()

	var events []*statev1.GossipEvent
	for peerID, rec := range s.nodes {
		if rec.maxCounter == 0 {
			continue
		}
		rd := remotePeers[peerID.String()]
		if rd == nil {
			events = append(events, s.buildEventsAbove(peerID, rec, 0)...)
			continue
		}
		localHash := computePeerHashLocked(rec)
		if localHash != rd.GetStateHash() {
			events = append(events, s.buildEventsAbove(peerID, rec, 0)...)
			continue
		}
		if rec.maxCounter > rd.GetMaxCounter() {
			events = append(events, s.buildEventsAbove(peerID, rec, rd.GetMaxCounter())...)
		}
	}

	slices.SortFunc(events, func(a, b *statev1.GossipEvent) int {
		if c := cmp.Compare(a.GetPeerId(), b.GetPeerId()); c != 0 {
			return c
		}
		return cmp.Compare(a.GetCounter(), b.GetCounter())
	})

	return events
}

// ApplyEvents applies a batch of incoming gossip events under a single lock
// acquisition. When isPullResponse is true, the batch outcome updates the
// EWMA stale ratio used for health checks; push-rebroadcast events are
// excluded because their inherently high staleness is normal gossip behavior.
// The EWMA is updated once per batch (not per event) so that large batches
// with a few fresh events aren't drowned out by individually-counted stale ones.
func (s *Store) ApplyEvents(events []*statev1.GossipEvent, isPullResponse bool) ApplyResult {
	s.mu.Lock()

	s.metrics.BatchSize.Set(float64(len(events)))
	s.metrics.EventsReceived.Add(int64(len(events)))

	// Partition self-events from remote events so self-conflict resolution
	// can see the full batch of our own state before bumping counters.
	var selfEvents, otherEvents []*statev1.GossipEvent
	localIDStr := s.LocalID.String()
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

	var result ApplyResult
	newlyDenied := make([]types.PeerKey, 0, len(otherEvents))
	anyApplied := false
	routesDirty := false
	reachabilityDirty := false
	workloadsDirty := false
	trafficDirty := false

	// Handle self-events as a batch: re-adopt workload specs before bumping.
	if len(selfEvents) > 0 {
		r, wlDirty := s.handleSelfConflictLocked(selfEvents)
		if len(r.Rebroadcast) > 0 {
			anyApplied = true
			workloadsDirty = workloadsDirty || wlDirty
		}
		result.Rebroadcast = append(result.Rebroadcast, r.Rebroadcast...)
	}

	for _, event := range otherEvents {
		r, denied := s.applyEventLocked(event)
		if len(r.Rebroadcast) > 0 {
			anyApplied = true
			switch event.GetChange().(type) {
			case *statev1.GossipEvent_Reachability:
				routesDirty = true
				reachabilityDirty = true
			case *statev1.GossipEvent_Vivaldi:
				routesDirty = true
			case *statev1.GossipEvent_WorkloadSpec, *statev1.GossipEvent_WorkloadClaim:
				workloadsDirty = true
			case *statev1.GossipEvent_TrafficHeatmap:
				trafficDirty = true
			}
		}
		result.Rebroadcast = append(result.Rebroadcast, r.Rebroadcast...)
		newlyDenied = append(newlyDenied, denied...)
	}

	// Update stale ratio once per pull-response batch: a batch with any
	// fresh events is "useful" (0.0); a fully-stale batch is not (1.0).
	if isPullResponse && len(events) > 0 {
		if anyApplied {
			s.metrics.StaleRatio.Update(0.0)
		} else {
			s.metrics.StaleRatio.Update(1.0)
		}
	}

	onDeny := s.onDeny
	onRouteInvalidate := s.onRouteInvalidate
	onWorkloadChange := s.onWorkloadChange
	onTrafficChange := s.onTrafficChange
	s.mu.Unlock()

	if onDeny != nil {
		for _, pk := range newlyDenied {
			onDeny(pk)
		}
	}

	if routesDirty && onRouteInvalidate != nil {
		onRouteInvalidate()
	}

	// Reachability changes affect workload claim visibility (liveComponentLocked
	// filters claims from unreachable peers), so signal workload reconciliation
	// on reachability changes too — but not on Vivaldi updates, which are
	// frequent and don't affect claim filtering.
	if (workloadsDirty || reachabilityDirty) && onWorkloadChange != nil {
		onWorkloadChange()
	}

	if trafficDirty && onTrafficChange != nil {
		onTrafficChange()
	}

	return result
}

// handleSelfConflictLocked processes a batch of events addressed to the local
// node (received back from peers). It detects self-conflicts (restart recovery)
// and re-adopts workload specs that the local node published in a prior session
// but lost on restart. Returns the rebroadcast events and whether any workload
// state was adopted. Caller must hold s.mu.
func (s *Store) handleSelfConflictLocked(selfEvents []*statev1.GossipEvent) (ApplyResult, bool) {
	local := s.nodes[s.LocalID]
	ensureNodeInit(&local, s.LocalID)

	// Find the max counter across all incoming self-events.
	var maxIncoming uint64
	for _, ev := range selfEvents {
		if ev.GetCounter() > maxIncoming {
			maxIncoming = ev.GetCounter()
		}
	}

	conflictDetected := maxIncoming > local.maxCounter
	if conflictDetected {
		local.maxCounter = maxIncoming
		s.metrics.SelfConflicts.Inc()
	}

	// Collect the highest-counter non-deleted workload spec per hash.
	// These represent user intent from a prior session that must survive.
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

	// Adopt non-deleted specs not already present in the local log.
	adopted := false
	for hash, ev := range bestSpec {
		if ev.GetDeleted() {
			continue
		}
		key := workloadSpecAttrKey(hash)
		if existing, ok := local.log[key]; ok && !existing.Deleted {
			continue
		}
		spec := ev.GetChange().(*statev1.GossipEvent_WorkloadSpec).WorkloadSpec //nolint:forcetypeassert
		m := make(map[string]*statev1.WorkloadSpecChange, len(local.WorkloadSpecs)+1)
		maps.Copy(m, local.WorkloadSpecs)
		m[hash] = spec
		local.WorkloadSpecs = m
		local.log[key] = logEntry{} // placeholder; bumpAndBroadcastAllLocked assigns real counter
		adopted = true
	}

	// Collect the highest-counter claim per hash. Non-deleted claims from a
	// prior session are stale runtime state — we must broadcast deletions so
	// peers drop them.
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
		key := workloadClaimAttrKey(hash)
		if _, ok := local.log[key]; ok {
			continue
		}
		local.log[key] = logEntry{Deleted: true}
		claimsDeleted = true
	}

	if !conflictDetected && !adopted && !claimsDeleted {
		s.nodes[s.LocalID] = local
		return ApplyResult{}, false
	}

	s.nodes[s.LocalID] = local
	return ApplyResult{Rebroadcast: s.bumpAndBroadcastAllLocked()}, adopted
}

// applyEventLocked applies a single event. The second return value contains
// any peer keys that were newly denied (so the caller can fire callbacks
// outside the lock).
func (s *Store) applyEventLocked(event *statev1.GossipEvent) (ApplyResult, []types.PeerKey) {
	peerID, err := types.PeerKeyFromString(event.GetPeerId())
	if err != nil {
		return ApplyResult{}, nil
	}

	rec := s.nodes[peerID]
	ensureNodeInit(&rec, peerID)

	key, ok := eventAttrKey(event)
	if !ok {
		return ApplyResult{}, nil
	}

	// Self-events are handled in batch by handleSelfConflictLocked before
	// this method is called. Reject any that slip through.
	if peerID == s.LocalID {
		return ApplyResult{}, nil
	}

	// Per-key stale check: only accept if this event is newer for this key.
	if existing, ok := rec.log[key]; ok && event.GetCounter() <= existing.Counter {
		s.metrics.EventsStale.Inc()
		if event.GetCounter() > rec.maxCounter {
			rec.maxCounter = event.GetCounter()
			s.nodes[peerID] = rec
		}
		return ApplyResult{}, nil
	}

	deleted := event.GetDeleted()
	var newlyDenied []types.PeerKey

	switch {
	case key.kind == attrDeny:
		if deleted {
			return ApplyResult{}, nil
		}
		v := event.GetChange().(*statev1.GossipEvent_Deny) //nolint:forcetypeassert
		subjectKey := types.PeerKeyFromBytes(v.Deny.GetSubjectPub())
		if _, ok := s.denied[subjectKey]; ok {
			// Already denied; still record the log entry so counters stay consistent.
		} else {
			s.denied[subjectKey] = struct{}{}
			newlyDenied = append(newlyDenied, subjectKey)
		}
	case deleted:
		applyDeleteLocked(&rec, key)
	default:
		if !applyValueLocked(&rec, event, key) {
			return ApplyResult{}, nil
		}
	}

	rec.log[key] = logEntry{Counter: event.GetCounter(), Deleted: deleted}
	if event.GetCounter() > rec.maxCounter {
		rec.maxCounter = event.GetCounter()
	}
	s.nodes[peerID] = rec
	s.metrics.EventsApplied.Inc()

	rebroadcast := []*statev1.GossipEvent{event}

	// When a remote workload spec arrives, check whether the local node holds
	// a losing spec for the same hash. If the remote publisher has a lower
	// PeerKey, tombstone the local spec so it cannot resurface.
	if !deleted {
		if v, ok := event.GetChange().(*statev1.GossipEvent_WorkloadSpec); ok && v.WorkloadSpec != nil {
			rebroadcast = append(rebroadcast, s.tombstoneLosingLocalSpec(peerID, v.WorkloadSpec.GetHash())...)
		}
	}

	return ApplyResult{Rebroadcast: rebroadcast}, newlyDenied
}

func eventAttrKey(event *statev1.GossipEvent) (attrKey, bool) {
	switch v := event.GetChange().(type) {
	case *statev1.GossipEvent_Network:
		return networkAttrKey(), true
	case *statev1.GossipEvent_ExternalPort:
		return externalPortAttrKey(), true
	case *statev1.GossipEvent_ObservedExternalIp:
		return observedExternalIPAttrKey(), true
	case *statev1.GossipEvent_IdentityPub:
		return identityAttrKey(), true
	case *statev1.GossipEvent_Service:
		if v.Service == nil || v.Service.GetName() == "" {
			return attrKey{}, false
		}
		return serviceAttrKey(v.Service.GetName()), true
	case *statev1.GossipEvent_Reachability:
		pk, err := types.PeerKeyFromString(v.Reachability.GetPeerId())
		if err != nil {
			return attrKey{}, false
		}
		return reachabilityAttrKey(pk), true
	case *statev1.GossipEvent_Deny:
		subjectKey := types.PeerKeyFromBytes(v.Deny.GetSubjectPub())
		return denyAttrKey(subjectKey.String()), true
	case *statev1.GossipEvent_PubliclyAccessible:
		return publiclyAccessibleAttrKey(), true
	case *statev1.GossipEvent_Vivaldi:
		return vivaldiAttrKey(), true
	case *statev1.GossipEvent_NatType:
		return natTypeAttrKey(), true
	case *statev1.GossipEvent_ResourceTelemetry:
		return resourceTelemetryAttrKey(), true
	case *statev1.GossipEvent_WorkloadSpec:
		if v.WorkloadSpec == nil || v.WorkloadSpec.GetHash() == "" {
			return attrKey{}, false
		}
		return workloadSpecAttrKey(v.WorkloadSpec.GetHash()), true
	case *statev1.GossipEvent_WorkloadClaim:
		if v.WorkloadClaim == nil || v.WorkloadClaim.GetHash() == "" {
			return attrKey{}, false
		}
		return workloadClaimAttrKey(v.WorkloadClaim.GetHash()), true
	case *statev1.GossipEvent_TrafficHeatmap:
		return trafficHeatmapAttrKey(), true
	default:
		return attrKey{}, false
	}
}

func applyDeleteLocked(rec *nodeRecord, key attrKey) {
	switch key.kind {
	case attrNetwork:
		rec.IPs = nil
		rec.LocalPort = 0
	case attrExternalPort:
		rec.ExternalPort = 0
	case attrObservedExternalIP:
		rec.ObservedExternalIP = ""
	case attrIdentity:
		rec.IdentityPub = nil
		rec.CertExpiry = 0
	case attrService:
		delete(rec.Services, key.name)
	case attrReachability:
		delete(rec.Reachable, key.peer)
	case attrDeny:
		// Deny entries are cluster-wide, not per-node; deletion is a no-op.
	case attrPubliclyAccessible:
		rec.PubliclyAccessible = false
	case attrVivaldi:
		rec.VivaldiCoord = nil
	case attrNatType:
		rec.NatType = nat.Unknown
	case attrResourceTelemetry:
		rec.CPUPercent = 0
		rec.MemPercent = 0
		rec.MemTotalBytes = 0
		rec.NumCPU = 0
	case attrWorkloadSpec:
		delete(rec.WorkloadSpecs, key.name)
	case attrWorkloadClaim:
		delete(rec.WorkloadClaims, key.name)
	case attrTrafficHeatmap:
		rec.TrafficRates = nil
	}
}

// applyValueLocked updates the materialized view from an event's value.
// Returns false if the event payload is malformed and must be rejected.
func applyValueLocked(rec *nodeRecord, event *statev1.GossipEvent, key attrKey) bool {
	switch v := event.GetChange().(type) {
	case *statev1.GossipEvent_Network:
		rec.IPs = append([]string(nil), v.Network.GetIps()...)
		rec.LocalPort = v.Network.GetLocalPort()
	case *statev1.GossipEvent_ExternalPort:
		rec.ExternalPort = v.ExternalPort.GetExternalPort()
	case *statev1.GossipEvent_ObservedExternalIp:
		rec.ObservedExternalIP = v.ObservedExternalIp.GetIp()
	case *statev1.GossipEvent_IdentityPub:
		rec.IdentityPub = append([]byte(nil), v.IdentityPub.GetIdentityPub()...)
		rec.CertExpiry = v.IdentityPub.GetCertExpiryUnix()
	case *statev1.GossipEvent_Service:
		m := make(map[string]*statev1.Service, len(rec.Services)+1)
		maps.Copy(m, rec.Services)
		m[key.name] = &statev1.Service{Name: key.name, Port: v.Service.GetPort()}
		rec.Services = m
	case *statev1.GossipEvent_Reachability:
		rec.Reachable[key.peer] = struct{}{}
	case *statev1.GossipEvent_PubliclyAccessible:
		rec.PubliclyAccessible = true
	case *statev1.GossipEvent_Vivaldi:
		if v.Vivaldi != nil {
			rec.VivaldiCoord = &topology.Coord{
				X:      v.Vivaldi.GetX(),
				Y:      v.Vivaldi.GetY(),
				Height: v.Vivaldi.GetHeight(),
			}
		}
	case *statev1.GossipEvent_NatType:
		rec.NatType = nat.TypeFromUint32(v.NatType.GetNatType())
	case *statev1.GossipEvent_ResourceTelemetry:
		rec.CPUPercent = v.ResourceTelemetry.GetCpuPercent()
		rec.MemPercent = v.ResourceTelemetry.GetMemPercent()
		rec.MemTotalBytes = v.ResourceTelemetry.GetMemTotalBytes()
		rec.NumCPU = v.ResourceTelemetry.GetNumCpu()
	case *statev1.GossipEvent_WorkloadSpec:
		m := make(map[string]*statev1.WorkloadSpecChange, len(rec.WorkloadSpecs)+1)
		maps.Copy(m, rec.WorkloadSpecs)
		m[v.WorkloadSpec.GetHash()] = v.WorkloadSpec
		rec.WorkloadSpecs = m
	case *statev1.GossipEvent_WorkloadClaim:
		m := make(map[string]struct{}, len(rec.WorkloadClaims)+1)
		maps.Copy(m, rec.WorkloadClaims)
		m[v.WorkloadClaim.GetHash()] = struct{}{}
		rec.WorkloadClaims = m
	case *statev1.GossipEvent_TrafficHeatmap:
		if v.TrafficHeatmap != nil {
			m := make(map[types.PeerKey]trafficRate, len(v.TrafficHeatmap.GetRates()))
			for _, r := range v.TrafficHeatmap.GetRates() {
				pk, err := types.PeerKeyFromString(r.GetPeerId())
				if err != nil {
					return false
				}
				m[pk] = trafficRate{BytesIn: r.GetBytesIn(), BytesOut: r.GetBytesOut()}
			}
			rec.TrafficRates = m
		}
	default:
		return true
	}
	return true
}

func ensureNodeInit(rec *nodeRecord, peerID types.PeerKey) {
	if rec.Reachable == nil {
		rec.Reachable = make(map[types.PeerKey]struct{})
	}
	if rec.Services == nil {
		rec.Services = make(map[string]*statev1.Service)
	}
	if rec.WorkloadSpecs == nil {
		rec.WorkloadSpecs = make(map[string]*statev1.WorkloadSpecChange)
	}
	if rec.WorkloadClaims == nil {
		rec.WorkloadClaims = make(map[string]struct{})
	}
	if rec.log == nil {
		rec.log = make(map[attrKey]logEntry)
	}
	if len(rec.IdentityPub) == 0 {
		rec.IdentityPub = append([]byte(nil), peerID[:]...)
	}
}

// LocalEvents returns all current local events without bumping counters.
// Used at startup to broadcast the full local state so that peers receive
// every event and maintain a correct maxCounter (no counter gaps).
func (s *Store) LocalEvents() []*statev1.GossipEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	local := s.nodes[s.LocalID]
	return s.buildEventsAbove(s.LocalID, local, 0)
}

// bumpAndBroadcastAllLocked returns events for ALL current local attributes,
// each with its own incremented counter. Used on self-state conflict (restart
// recovery). Caller must hold s.mu.
func (s *Store) bumpAndBroadcastAllLocked() []*statev1.GossipEvent {
	local := s.nodes[s.LocalID]

	for key, entry := range local.log {
		local.maxCounter++
		entry.Counter = local.maxCounter
		local.log[key] = entry
	}

	s.nodes[s.LocalID] = local
	return s.buildEventsAbove(s.LocalID, local, 0)
}

// buildEventsAbove constructs GossipEvent messages for all log entries with
// counter > minCounter. Caller must hold s.mu (at least RLock).
func (s *Store) buildEventsAbove(peerID types.PeerKey, rec nodeRecord, minCounter uint64) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	peerIDStr := peerID.String()

	for key, entry := range rec.log {
		if entry.Counter <= minCounter {
			continue
		}
		events = append(events, s.buildEventFromLog(peerIDStr, key, entry, rec))
	}

	return events
}

// buildEventFromLog constructs a single GossipEvent from a log entry and the
// materialized view. Caller must hold the store's mu (at least RLock).
func (s *Store) buildEventFromLog(peerIDStr string, key attrKey, entry logEntry, rec nodeRecord) *statev1.GossipEvent {
	event := &statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: entry.Counter,
		Deleted: entry.Deleted,
	}

	switch key.kind {
	case attrNetwork:
		change := &statev1.NetworkChange{}
		if !entry.Deleted {
			change.Ips = append([]string(nil), rec.IPs...)
			change.LocalPort = rec.LocalPort
		}
		event.Change = &statev1.GossipEvent_Network{Network: change}
	case attrExternalPort:
		change := &statev1.ExternalPortChange{}
		if !entry.Deleted {
			change.ExternalPort = rec.ExternalPort
		}
		event.Change = &statev1.GossipEvent_ExternalPort{ExternalPort: change}
	case attrObservedExternalIP:
		change := &statev1.ObservedExternalIPChange{}
		if !entry.Deleted {
			change.Ip = rec.ObservedExternalIP
		}
		event.Change = &statev1.GossipEvent_ObservedExternalIp{ObservedExternalIp: change}
	case attrIdentity:
		change := &statev1.IdentityChange{}
		if !entry.Deleted {
			change.IdentityPub = append([]byte(nil), rec.IdentityPub...)
			change.CertExpiryUnix = rec.CertExpiry
		}
		event.Change = &statev1.GossipEvent_IdentityPub{IdentityPub: change}
	case attrService:
		change := &statev1.ServiceChange{Name: key.name}
		if !entry.Deleted {
			svc, exists := rec.Services[key.name]
			if !exists {
				panic("service log entry missing service payload")
			}
			change.Port = svc.GetPort()
		}
		event.Change = &statev1.GossipEvent_Service{Service: change}
	case attrReachability:
		event.Change = &statev1.GossipEvent_Reachability{
			Reachability: &statev1.ReachabilityChange{PeerId: key.peer.String()},
		}
	case attrPubliclyAccessible:
		event.Change = &statev1.GossipEvent_PubliclyAccessible{
			PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
		}
	case attrVivaldi:
		change := &statev1.VivaldiCoordinateChange{}
		if !entry.Deleted && rec.VivaldiCoord != nil {
			change.X = rec.VivaldiCoord.X
			change.Y = rec.VivaldiCoord.Y
			change.Height = rec.VivaldiCoord.Height
		}
		event.Change = &statev1.GossipEvent_Vivaldi{Vivaldi: change}
	case attrNatType:
		change := &statev1.NatTypeChange{}
		if !entry.Deleted {
			change.NatType = rec.NatType.ToUint32()
		}
		event.Change = &statev1.GossipEvent_NatType{NatType: change}
	case attrResourceTelemetry:
		change := &statev1.ResourceTelemetryChange{}
		if !entry.Deleted {
			change.CpuPercent = rec.CPUPercent
			change.MemPercent = rec.MemPercent
			change.MemTotalBytes = rec.MemTotalBytes
			change.NumCpu = rec.NumCPU
		}
		event.Change = &statev1.GossipEvent_ResourceTelemetry{ResourceTelemetry: change}
	case attrDeny:
		subjectKey, err := types.PeerKeyFromString(key.name)
		if err != nil {
			panic("invalid deny key in log")
		}
		event.Change = &statev1.GossipEvent_Deny{
			Deny: &statev1.DenyChange{SubjectPub: append([]byte(nil), subjectKey[:]...)},
		}
	case attrWorkloadSpec:
		change := &statev1.WorkloadSpecChange{Hash: key.name}
		if !entry.Deleted {
			if spec, ok := rec.WorkloadSpecs[key.name]; ok {
				change.Replicas = spec.GetReplicas()
				change.MemoryPages = spec.GetMemoryPages()
				change.TimeoutMs = spec.GetTimeoutMs()
			}
		}
		event.Change = &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: change}
	case attrWorkloadClaim:
		event.Change = &statev1.GossipEvent_WorkloadClaim{
			WorkloadClaim: &statev1.WorkloadClaimChange{Hash: key.name},
		}
	case attrTrafficHeatmap:
		change := &statev1.TrafficHeatmapChange{}
		if !entry.Deleted {
			for pk, rate := range rec.TrafficRates {
				change.Rates = append(change.Rates, &statev1.TrafficRate{
					PeerId:   pk.String(),
					BytesIn:  rate.BytesIn,
					BytesOut: rate.BytesOut,
				})
			}
		}
		event.Change = &statev1.GossipEvent_TrafficHeatmap{TrafficHeatmap: change}
	}

	return event
}
