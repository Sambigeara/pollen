package store

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
)

var _ auth.InviteConsumer = (*Store)(nil)

// ErrSpecOwnedRemotely is returned by SetLocalWorkloadSpec when a valid remote
// node already publishes a spec for the same hash.
var ErrSpecOwnedRemotely = errors.New("spec published by another node")

const (
	eventBufSize      = 64
	stateSaveInterval = 30 * time.Second
)

type attrKind uint8

const (
	attrNetwork attrKind = iota + 1
	attrExternalPort
	attrObservedExternalIP
	attrIdentity
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
)

type attrKey struct {
	name string
	peer types.PeerKey
	kind attrKind
}

func networkAttrKey() attrKey {
	return attrKey{kind: attrNetwork}
}

func externalPortAttrKey() attrKey {
	return attrKey{kind: attrExternalPort}
}

func observedExternalIPAttrKey() attrKey {
	return attrKey{kind: attrObservedExternalIP}
}

func identityAttrKey() attrKey {
	return attrKey{kind: attrIdentity}
}

func serviceAttrKey(name string) attrKey {
	return attrKey{kind: attrService, name: name}
}

func reachabilityAttrKey(peerID types.PeerKey) attrKey {
	return attrKey{kind: attrReachability, peer: peerID}
}

func denyAttrKey(subjectPubHex string) attrKey {
	return attrKey{kind: attrDeny, name: subjectPubHex}
}

func publiclyAccessibleAttrKey() attrKey {
	return attrKey{kind: attrPubliclyAccessible}
}

func vivaldiAttrKey() attrKey {
	return attrKey{kind: attrVivaldi}
}

func natTypeAttrKey() attrKey {
	return attrKey{kind: attrNatType}
}

func resourceTelemetryAttrKey() attrKey {
	return attrKey{kind: attrResourceTelemetry}
}

func workloadSpecAttrKey(hash string) attrKey {
	return attrKey{kind: attrWorkloadSpec, name: hash}
}

func workloadClaimAttrKey(hash string) attrKey {
	return attrKey{kind: attrWorkloadClaim, name: hash}
}

func trafficHeatmapAttrKey() attrKey {
	return attrKey{kind: attrTrafficHeatmap}
}

type trafficRate struct {
	BytesIn  uint64
	BytesOut uint64
}

func tombstoneStaleAttrs(rec *nodeRecord) {
	for _, key := range []attrKey{publiclyAccessibleAttrKey(), natTypeAttrKey(), observedExternalIPAttrKey(), resourceTelemetryAttrKey(), trafficHeatmapAttrKey()} {
		rec.maxCounter++
		rec.log[key] = logEntry{Counter: rec.maxCounter, Deleted: true}
	}
}

type logEntry struct {
	Counter uint64
	Deleted bool
}

type nodeRecord struct {
	TrafficRates       map[types.PeerKey]trafficRate
	Services           map[string]*statev1.Service
	WorkloadSpecs      map[string]*statev1.WorkloadSpecChange
	WorkloadClaims     map[string]struct{}
	log                map[attrKey]logEntry
	VivaldiCoord       *topology.Coord
	Reachable          map[types.PeerKey]struct{}
	LastAddr           string
	ObservedExternalIP string
	IPs                []string
	IdentityPub        []byte
	maxCounter         uint64
	MemTotalBytes      uint64
	NatType            nat.Type
	CertExpiry         int64
	LocalPort          uint32
	ExternalPort       uint32
	CPUPercent         uint32
	MemPercent         uint32
	NumCPU             uint32
	PubliclyAccessible bool
}

func (r nodeRecord) clone() nodeRecord {
	c := r
	if r.Services != nil {
		c.Services = make(map[string]*statev1.Service, len(r.Services))
		maps.Copy(c.Services, r.Services)
	}
	if r.Reachable != nil {
		c.Reachable = make(map[types.PeerKey]struct{}, len(r.Reachable))
		maps.Copy(c.Reachable, r.Reachable)
	}
	if r.WorkloadSpecs != nil {
		c.WorkloadSpecs = make(map[string]*statev1.WorkloadSpecChange, len(r.WorkloadSpecs))
		maps.Copy(c.WorkloadSpecs, r.WorkloadSpecs)
	}
	if r.WorkloadClaims != nil {
		c.WorkloadClaims = make(map[string]struct{}, len(r.WorkloadClaims))
		maps.Copy(c.WorkloadClaims, r.WorkloadClaims)
	}
	if r.TrafficRates != nil {
		c.TrafficRates = make(map[types.PeerKey]trafficRate, len(r.TrafficRates))
		maps.Copy(c.TrafficRates, r.TrafficRates)
	}
	c.IPs = append([]string(nil), r.IPs...)
	return c
}

type KnownPeer struct {
	VivaldiCoord       *topology.Coord
	LastAddr           string
	ObservedExternalIP string
	IdentityPub        []byte
	IPs                []string
	NatType            nat.Type
	LocalPort          uint32
	ExternalPort       uint32
	PeerID             types.PeerKey
	PubliclyAccessible bool
}

type Connection struct {
	PeerID     types.PeerKey
	RemotePort uint32
	LocalPort  uint32
}

func (c Connection) Key() string {
	return fmt.Sprintf("%s:%d:%d", c.PeerID.String(), c.RemotePort, c.LocalPort)
}

type ApplyResult struct {
	Rebroadcast []*statev1.GossipEvent
}

const consumedExpirySkew = time.Minute

type consumedInviteEntry struct {
	TokenID        string
	ExpiresAtUnix  int64
	ConsumedAtUnix int64
}

type Store struct {
	disk               *disk
	nodes              map[types.PeerKey]nodeRecord
	denied             map[types.PeerKey]struct{}
	consumedInvites    map[string]consumedInviteEntry
	desiredConnections map[string]Connection
	events             chan StoreEvent
	work               chan func()
	ctx                context.Context
	snap               atomic.Pointer[Snapshot]
	metrics            *metrics.GossipMetrics
	LocalID            types.PeerKey
}

func Load(pollenDir string, identityPub []byte) (*Store, error) {
	d, err := openDisk(pollenDir)
	if err != nil {
		return nil, err
	}

	onDisk, err := d.load()
	if err != nil {
		_ = d.close()
		return nil, err
	}

	localID := types.PeerKeyFromBytes(identityPub)

	denied := make(map[types.PeerKey]struct{})
	for _, pub := range onDisk.GetDeniedPeers() {
		denied[types.PeerKeyFromBytes(pub)] = struct{}{}
	}

	s := &Store{
		LocalID: localID,
		disk:    d,
		events:  make(chan StoreEvent, eventBufSize),
		ctx:     context.Background(),
		metrics: metrics.NewGossipMetrics(nil),
		nodes: map[types.PeerKey]nodeRecord{
			localID: {
				maxCounter:     1,
				IdentityPub:    append([]byte(nil), identityPub...),
				Reachable:      make(map[types.PeerKey]struct{}),
				Services:       make(map[string]*statev1.Service),
				WorkloadSpecs:  make(map[string]*statev1.WorkloadSpecChange),
				WorkloadClaims: make(map[string]struct{}),
				log: map[attrKey]logEntry{
					identityAttrKey(): {Counter: 1},
				},
			},
		},
		denied:             denied,
		consumedInvites:    loadConsumedInvites(onDisk.GetConsumedInvites(), time.Now()),
		desiredConnections: make(map[string]Connection),
	}

	// Correct stale state held by peers from a prior session.
	// Vivaldi state is not tombstoned here because node startup immediately
	// publishes the current local coordinate.
	local := s.nodes[localID]
	tombstoneStaleAttrs(&local)

	// Inject disk-loaded denied peers into the local log so that
	// bumpAndBroadcastAllLocked can re-publish them after restart.
	for subjectKey := range s.denied {
		local.maxCounter++
		local.log[denyAttrKey(subjectKey.String())] = logEntry{Counter: local.maxCounter}
	}

	// Inject disk-loaded workload specs into the local log so that
	// bumpAndBroadcastAllLocked can re-publish them after restart.
	for _, spec := range onDisk.GetWorkloadSpecs() {
		hash := spec.GetHash()
		if hash == "" {
			continue
		}
		local.WorkloadSpecs[hash] = spec
		local.maxCounter++
		local.log[workloadSpecAttrKey(hash)] = logEntry{Counter: local.maxCounter}
	}

	s.nodes[localID] = local

	for _, p := range onDisk.GetPeers() {
		peerID := types.PeerKeyFromBytes(p.GetIdentityPub())
		if peerID == localID || peerID == (types.PeerKey{}) {
			continue
		}

		s.nodes[peerID] = nodeRecord{
			IdentityPub:        append([]byte(nil), p.GetIdentityPub()...),
			IPs:                append([]string(nil), p.GetAddresses()...),
			LastAddr:           p.GetLastAddr(),
			LocalPort:          p.GetPort(),
			ExternalPort:       p.GetExternalPort(),
			ObservedExternalIP: p.GetExternalIp(),
			PubliclyAccessible: p.GetPubliclyAccessible(),
			Reachable:          make(map[types.PeerKey]struct{}),
			Services:           make(map[string]*statev1.Service),
			WorkloadSpecs:      make(map[string]*statev1.WorkloadSpecChange),
			WorkloadClaims:     make(map[string]struct{}),
			log:                make(map[attrKey]logEntry),
		}
	}

	s.updateSnapshot()

	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.disk == nil {
		return nil
	}
	return s.disk.close()
}

// do dispatches fn onto the store's single goroutine and blocks until it
// completes. If the work channel is nil (tests that skip Run), fn executes
// inline on the caller's goroutine.
func (s *Store) do(fn func()) {
	if s.work == nil {
		fn()
		return
	}
	done := make(chan struct{})
	select {
	case s.work <- func() {
		fn()
		close(done)
	}:
		<-done
	case <-s.ctx.Done():
	}
}

// updateSnapshot rebuilds the atomic snapshot from current state.
// Must be called on the store's goroutine (or under exclusive access).
func (s *Store) updateSnapshot() {
	snap := s.snapshotLocked()
	s.snap.Store(&snap)
}

// Run drives the store's single-goroutine event loop. All mutations and
// reads of internal state are serialized through the work channel. Run
// also handles periodic state persistence. It blocks until ctx is cancelled,
// then performs a final save.
//
// ready is closed once the loop is accepting work. The caller must wait on
// ready before dispatching mutations via public methods.
func (s *Store) Run(ctx context.Context, ready chan<- struct{}) error {
	s.ctx = ctx
	s.work = make(chan func())
	close(ready)

	saveTicker := time.NewTicker(stateSaveInterval)
	defer saveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return s.saveInternal()
		case fn := <-s.work:
			fn()
		case <-saveTicker.C:
			_ = s.saveInternal()
		}
	}
}

// TryConsume marks an invite token as consumed, returning true if it was
// not previously consumed. Implements auth.InviteConsumer.
func (s *Store) TryConsume(token *admissionv1.InviteToken, now time.Time) (bool, error) {
	claims := token.GetClaims()
	if claims == nil {
		return false, errors.New("invite token missing claims")
	}
	tokenID := claims.GetTokenId()
	if tokenID == "" {
		return false, errors.New("invite token missing token id")
	}

	var (
		consumed bool
		snapshot *statev1.RuntimeState
	)
	s.do(func() {
		s.dropExpiredInvites(now)

		if _, exists := s.consumedInvites[tokenID]; exists {
			return
		}

		s.consumedInvites[tokenID] = consumedInviteEntry{
			TokenID:        tokenID,
			ExpiresAtUnix:  claims.GetExpiresAtUnix(),
			ConsumedAtUnix: now.Unix(),
		}

		snapshot = s.snapshotStateLocked()
		consumed = true
	})

	if !consumed {
		return false, nil
	}

	if err := s.disk.save(snapshot); err != nil {
		return true, fmt.Errorf("persist consumed invite: %w", err)
	}

	return true, nil
}

func (s *Store) dropExpiredInvites(now time.Time) {
	nowUnix := now.Unix()
	for tokenID, entry := range s.consumedInvites {
		if entry.ExpiresAtUnix > 0 && entry.ExpiresAtUnix+int64(consumedExpirySkew/time.Second) < nowUnix {
			delete(s.consumedInvites, tokenID)
		}
	}
}

// Events returns a read-only channel of store events.
func (s *Store) Events() <-chan StoreEvent {
	return s.events
}

func (s *Store) emitEvent(ev StoreEvent) {
	select {
	case s.events <- ev:
	case <-s.ctx.Done():
	}
}

// SetGossipMetrics replaces the no-op metrics with wired instruments.
func (s *Store) SetGossipMetrics(m *metrics.GossipMetrics) {
	s.metrics = m
}

// GossipMetrics returns the store's gossip metrics for direct value reads.
func (s *Store) GossipMetrics() *metrics.GossipMetrics {
	return s.metrics
}

func (s *Store) Save() error {
	var snapshot *statev1.RuntimeState
	s.do(func() {
		snapshot = s.snapshotStateLocked()
	})
	return s.disk.save(snapshot)
}

// saveInternal persists state to disk. Must be called on the store's goroutine.
func (s *Store) saveInternal() error {
	return s.disk.save(s.snapshotStateLocked())
}

// snapshotStateLocked builds a RuntimeState from in-memory data.
// Must be called on the store's goroutine (or during initialization).
func (s *Store) snapshotStateLocked() *statev1.RuntimeState {
	peers := make([]*statev1.PeerState, 0, len(s.nodes))
	for peerID, rec := range s.nodes {
		if peerID == s.LocalID {
			continue
		}
		peers = append(peers, &statev1.PeerState{
			IdentityPub:        append([]byte(nil), peerID[:]...),
			Addresses:          append([]string(nil), rec.IPs...),
			Port:               rec.LocalPort,
			ExternalPort:       rec.ExternalPort,
			ExternalIp:         rec.ObservedExternalIP,
			LastAddr:           rec.LastAddr,
			PubliclyAccessible: rec.PubliclyAccessible,
		})
	}

	slices.SortFunc(peers, func(a, b *statev1.PeerState) int {
		return bytes.Compare(a.GetIdentityPub(), b.GetIdentityPub())
	})

	deniedPeers := make([][]byte, 0, len(s.denied))
	for pk := range s.denied {
		deniedPeers = append(deniedPeers, append([]byte(nil), pk[:]...))
	}
	slices.SortFunc(deniedPeers, bytes.Compare)

	invites := make([]*statev1.ConsumedInvite, 0, len(s.consumedInvites))
	for _, entry := range s.consumedInvites {
		invites = append(invites, &statev1.ConsumedInvite{
			TokenId:      entry.TokenID,
			ExpiryUnix:   entry.ExpiresAtUnix,
			ConsumedUnix: entry.ConsumedAtUnix,
		})
	}
	slices.SortFunc(invites, func(a, b *statev1.ConsumedInvite) int {
		return cmp.Compare(a.GetTokenId(), b.GetTokenId())
	})

	local := s.nodes[s.LocalID]
	specs := make([]*statev1.WorkloadSpecChange, 0, len(local.WorkloadSpecs))
	for _, spec := range local.WorkloadSpecs {
		specs = append(specs, &statev1.WorkloadSpecChange{
			Hash:        spec.GetHash(),
			Replicas:    spec.GetReplicas(),
			MemoryPages: spec.GetMemoryPages(),
			TimeoutMs:   spec.GetTimeoutMs(),
		})
	}
	slices.SortFunc(specs, func(a, b *statev1.WorkloadSpecChange) int {
		return cmp.Compare(a.GetHash(), b.GetHash())
	})

	return &statev1.RuntimeState{
		Peers:           peers,
		DeniedPeers:     deniedPeers,
		ConsumedInvites: invites,
		WorkloadSpecs:   specs,
	}
}

func loadConsumedInvites(protos []*statev1.ConsumedInvite, now time.Time) map[string]consumedInviteEntry {
	nowUnix := now.Unix()
	out := make(map[string]consumedInviteEntry, len(protos))
	for _, p := range protos {
		tokenID := p.GetTokenId()
		if tokenID == "" {
			continue
		}
		entry := consumedInviteEntry{
			TokenID:        tokenID,
			ExpiresAtUnix:  p.GetExpiryUnix(),
			ConsumedAtUnix: p.GetConsumedUnix(),
		}
		if entry.ExpiresAtUnix > 0 && entry.ExpiresAtUnix+int64(consumedExpirySkew/time.Second) < nowUnix {
			continue
		}
		out[tokenID] = entry
	}
	return out
}

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// computePeerHashLocked returns an order-independent XOR of FNV-1a hashes
// over a peer's compacted log. Two peers with identical log contents produce
// identical hashes regardless of iteration order.
func computePeerHashLocked(rec nodeRecord) uint64 {
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

// EagerSyncClock returns an empty digest when no remote peer has state yet
// (so the responder sends everything), or the real digest otherwise.
func (s *Store) EagerSyncClock() *statev1.GossipStateDigest {
	var result *statev1.GossipStateDigest
	s.do(func() {
		for peerID, rec := range s.nodes {
			if peerID != s.LocalID && rec.maxCounter > 0 {
				result = s.digestLocked()
				return
			}
		}
		result = &statev1.GossipStateDigest{}
	})
	return result
}

func (s *Store) Clock() *statev1.GossipStateDigest {
	var result *statev1.GossipStateDigest
	s.do(func() {
		result = s.digestLocked()
	})
	return result
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

	var events []*statev1.GossipEvent
	s.do(func() {
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
	})

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
	var (
		result            ApplyResult
		newlyDenied       []types.PeerKey
		routesDirty       bool
		reachabilityDirty bool
		workloadsDirty    bool
		trafficDirty      bool
	)

	s.do(func() {
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

		newlyDenied = make([]types.PeerKey, 0, len(otherEvents))
		anyApplied := false

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

		s.updateSnapshot()
	})

	for _, pk := range newlyDenied {
		s.emitEvent(DenyApplied{PeerKey: pk})
	}

	if routesDirty {
		s.emitEvent(RouteInvalidated{})
	}

	// Reachability changes affect workload claim visibility (liveComponentLocked
	// filters claims from unreachable peers), so signal workload reconciliation
	// on reachability changes too — but not on Vivaldi updates, which are
	// frequent and don't affect claim filtering.
	if workloadsDirty || reachabilityDirty {
		s.emitEvent(WorkloadChanged{})
	}

	if trafficDirty {
		s.emitEvent(TrafficChanged{})
	}

	if len(result.Rebroadcast) > 0 {
		s.emitEvent(GossipApplied(result))
	}

	return result
}

// handleSelfConflictLocked processes a batch of events addressed to the local
// node (received back from peers). It detects self-conflicts (restart recovery)
// and re-adopts workload specs that the local node published in a prior session
// but lost on restart. Returns the rebroadcast events and whether any workload
// state was adopted. Must be called on the store's goroutine.
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
	default:
		panic("unknown attr kind")
	}
}

// applyValueLocked updates the materialized view from an event's value.
// Returns false if the event payload is malformed and must be rejected.
func applyValueLocked(rec *nodeRecord, event *statev1.GossipEvent, key attrKey) bool {
	switch v := event.GetChange().(type) {
	case *statev1.GossipEvent_Network:
		if v.Network == nil {
			return true
		}
		rec.IPs = append([]string(nil), v.Network.GetIps()...)
		rec.LocalPort = v.Network.GetLocalPort()
	case *statev1.GossipEvent_ExternalPort:
		if v.ExternalPort != nil {
			rec.ExternalPort = v.ExternalPort.GetExternalPort()
		}
	case *statev1.GossipEvent_ObservedExternalIp:
		if v.ObservedExternalIp != nil {
			rec.ObservedExternalIP = v.ObservedExternalIp.GetIp()
		}
	case *statev1.GossipEvent_IdentityPub:
		if v.IdentityPub != nil {
			rec.IdentityPub = append([]byte(nil), v.IdentityPub.GetIdentityPub()...)
			rec.CertExpiry = v.IdentityPub.GetCertExpiryUnix()
		}
	case *statev1.GossipEvent_Service:
		if v.Service != nil {
			m := make(map[string]*statev1.Service, len(rec.Services)+1)
			maps.Copy(m, rec.Services)
			m[key.name] = &statev1.Service{Name: key.name, Port: v.Service.GetPort()}
			rec.Services = m
		}
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
		if v.NatType != nil {
			rec.NatType = nat.TypeFromUint32(v.NatType.GetNatType())
		}
	case *statev1.GossipEvent_ResourceTelemetry:
		if v.ResourceTelemetry != nil {
			rec.CPUPercent = v.ResourceTelemetry.GetCpuPercent()
			rec.MemPercent = v.ResourceTelemetry.GetMemPercent()
			rec.MemTotalBytes = v.ResourceTelemetry.GetMemTotalBytes()
			rec.NumCPU = v.ResourceTelemetry.GetNumCpu()
		}
	case *statev1.GossipEvent_WorkloadSpec:
		if v.WorkloadSpec != nil {
			m := make(map[string]*statev1.WorkloadSpecChange, len(rec.WorkloadSpecs)+1)
			maps.Copy(m, rec.WorkloadSpecs)
			m[v.WorkloadSpec.GetHash()] = v.WorkloadSpec
			rec.WorkloadSpecs = m
		}
	case *statev1.GossipEvent_WorkloadClaim:
		if v.WorkloadClaim != nil {
			m := make(map[string]struct{}, len(rec.WorkloadClaims)+1)
			maps.Copy(m, rec.WorkloadClaims)
			m[v.WorkloadClaim.GetHash()] = struct{}{}
			rec.WorkloadClaims = m
		}
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

// --- Local mutation methods (return events for broadcasting) ---

func (s *Store) SetLocalNetwork(ips []string, port uint32) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]

		if slices.Equal(local.IPs, ips) && local.LocalPort == port {
			return
		}

		local.IPs = append([]string(nil), ips...)
		local.LocalPort = port

		local.maxCounter++
		counter := local.maxCounter
		local.log[networkAttrKey()] = logEntry{Counter: counter}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_Network{
				Network: &statev1.NetworkChange{
					Ips:       append([]string(nil), ips...),
					LocalPort: port,
				},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

func (s *Store) SetExternalPort(port uint32) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if local.ExternalPort == port {
			return
		}

		local.ExternalPort = port

		local.maxCounter++
		counter := local.maxCounter
		local.log[externalPortAttrKey()] = logEntry{Counter: counter}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_ExternalPort{
				ExternalPort: &statev1.ExternalPortChange{ExternalPort: port},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

func (s *Store) SetObservedExternalIP(ip string) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if local.ObservedExternalIP == ip {
			return
		}

		local.ObservedExternalIP = ip

		local.maxCounter++
		counter := local.maxCounter
		deleted := ip == ""
		local.log[observedExternalIPAttrKey()] = logEntry{Counter: counter, Deleted: deleted}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Deleted: deleted,
			Change: &statev1.GossipEvent_ObservedExternalIp{
				ObservedExternalIp: &statev1.ObservedExternalIPChange{Ip: ip},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

func (s *Store) SetLocalConnected(peerID types.PeerKey, connected bool) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]

		_, exists := local.Reachable[peerID]
		if connected == exists {
			return
		}

		if connected {
			local.Reachable[peerID] = struct{}{}
		} else {
			delete(local.Reachable, peerID)
		}

		key := reachabilityAttrKey(peerID)
		local.maxCounter++
		counter := local.maxCounter
		local.log[key] = logEntry{Counter: counter, Deleted: !connected}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Deleted: !connected,
			Change: &statev1.GossipEvent_Reachability{
				Reachability: &statev1.ReachabilityChange{
					PeerId: peerID.String(),
				},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

func (s *Store) SetLocalPubliclyAccessible(accessible bool) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if local.PubliclyAccessible == accessible {
			return
		}

		local.PubliclyAccessible = accessible

		key := publiclyAccessibleAttrKey()
		local.maxCounter++
		counter := local.maxCounter
		local.log[key] = logEntry{Counter: counter, Deleted: !accessible}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Deleted: !accessible,
			Change: &statev1.GossipEvent_PubliclyAccessible{
				PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

func (s *Store) SetLocalNatType(natType nat.Type) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if local.NatType == natType {
			return
		}

		local.NatType = natType

		key := natTypeAttrKey()
		local.maxCounter++
		counter := local.maxCounter
		deleted := natType == nat.Unknown
		local.log[key] = logEntry{Counter: counter, Deleted: deleted}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Deleted: deleted,
			Change: &statev1.GossipEvent_NatType{
				NatType: &statev1.NatTypeChange{NatType: natType.ToUint32()},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

const resourceTelemetryDeadband = 2

func (s *Store) SetLocalResourceTelemetry(cpuPercent, memPercent uint32, memTotalBytes uint64, numCPU uint32) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		cpuDelta := absDiff(local.CPUPercent, cpuPercent)
		memDelta := absDiff(local.MemPercent, memPercent)
		if cpuDelta < resourceTelemetryDeadband && memDelta < resourceTelemetryDeadband && local.MemTotalBytes == memTotalBytes && local.NumCPU == numCPU {
			return
		}

		local.CPUPercent = cpuPercent
		local.MemPercent = memPercent
		local.MemTotalBytes = memTotalBytes
		local.NumCPU = numCPU

		key := resourceTelemetryAttrKey()
		local.maxCounter++
		counter := local.maxCounter
		local.log[key] = logEntry{Counter: counter}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_ResourceTelemetry{
				ResourceTelemetry: &statev1.ResourceTelemetryChange{
					CpuPercent:    cpuPercent,
					MemPercent:    memPercent,
					MemTotalBytes: memTotalBytes,
					NumCpu:        numCPU,
				},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

// SetLocalTrafficHeatmap publishes a traffic heatmap snapshot to gossip.
// Entries with zero bytes are omitted. If the snapshot transitions from
// non-empty to empty, a deletion event is emitted so stale data clears.
func (s *Store) SetLocalTrafficHeatmap(rates map[types.PeerKey]TrafficSnapshot) []*statev1.GossipEvent {
	// Filter zero entries.
	filtered := make(map[types.PeerKey]trafficRate, len(rates))
	for pk, r := range rates {
		if r.BytesIn > 0 || r.BytesOut > 0 {
			filtered[pk] = trafficRate(r)
		}
	}

	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		key := trafficHeatmapAttrKey()

		if len(filtered) == 0 {
			if len(local.TrafficRates) == 0 {
				return
			}
			local.TrafficRates = nil
			local.maxCounter++
			counter := local.maxCounter
			local.log[key] = logEntry{Counter: counter, Deleted: true}
			s.nodes[s.LocalID] = local
			events = []*statev1.GossipEvent{{
				PeerId:  s.LocalID.String(),
				Counter: counter,
				Deleted: true,
				Change: &statev1.GossipEvent_TrafficHeatmap{
					TrafficHeatmap: &statev1.TrafficHeatmapChange{},
				},
			}}
			s.updateSnapshot()
			return
		}

		local.TrafficRates = filtered

		protoRates := make([]*statev1.TrafficRate, 0, len(filtered))
		for pk, r := range filtered {
			protoRates = append(protoRates, &statev1.TrafficRate{
				PeerId:   pk.String(),
				BytesIn:  r.BytesIn,
				BytesOut: r.BytesOut,
			})
		}

		local.maxCounter++
		counter := local.maxCounter
		local.log[key] = logEntry{Counter: counter}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_TrafficHeatmap{
				TrafficHeatmap: &statev1.TrafficHeatmapChange{Rates: protoRates},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

// TrafficSnapshot holds a single peer's accumulated traffic counters.
type TrafficSnapshot struct {
	BytesIn  uint64
	BytesOut uint64
}

// AllTrafficHeatmaps returns nodeID → peerID → TrafficSnapshot for valid nodes.
func (s *Store) AllTrafficHeatmaps() map[types.PeerKey]map[types.PeerKey]TrafficSnapshot {
	return s.Snapshot().Heatmaps
}

// NodePlacementState holds the data the scheduler needs for traffic-aware placement.
type NodePlacementState struct {
	Coord         *topology.Coord
	TrafficTo     map[types.PeerKey]uint64
	MemTotalBytes uint64
	CPUPercent    uint32
	MemPercent    uint32
	NumCPU        uint32
}

// AllNodePlacementStates returns resource, coordinate, and traffic data for
// every valid, live node, suitable for the scheduler's placement scoring
// function. Dead nodes are excluded to stay consistent with AllPeerKeys and
// AllWorkloadClaims.
func (s *Store) AllNodePlacementStates() map[types.PeerKey]NodePlacementState {
	return s.Snapshot().Placements
}

func absDiff(a, b uint32) uint32 {
	if a > b {
		return a - b
	}
	return b - a
}

func (s *Store) NatType(peerID types.PeerKey) nat.Type {
	snap := s.Snapshot()
	nv, ok := snap.Nodes[peerID]
	if !ok {
		return nat.Unknown
	}
	return nv.NatType
}

func (s *Store) SetLocalCertExpiry(expiry int64) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if local.CertExpiry == expiry {
			return
		}

		local.CertExpiry = expiry

		local.maxCounter++
		counter := local.maxCounter
		local.log[identityAttrKey()] = logEntry{Counter: counter}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_IdentityPub{
				IdentityPub: &statev1.IdentityChange{
					IdentityPub:    append([]byte(nil), local.IdentityPub...),
					CertExpiryUnix: expiry,
				},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

func (s *Store) IsPubliclyAccessible(peerID types.PeerKey) bool {
	snap := s.Snapshot()
	nv, ok := snap.Nodes[peerID]
	if !ok {
		return false
	}
	return nv.PubliclyAccessible
}

func (s *Store) SetLocalVivaldiCoord(coord topology.Coord) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		if local.VivaldiCoord != nil && topology.MovementDistance(*local.VivaldiCoord, coord) <= topology.PublishEpsilon {
			return
		}

		local.VivaldiCoord = &coord

		key := vivaldiAttrKey()
		local.maxCounter++
		counter := local.maxCounter
		local.log[key] = logEntry{Counter: counter}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_Vivaldi{
				Vivaldi: &statev1.VivaldiCoordinateChange{
					X:      coord.X,
					Y:      coord.Y,
					Height: coord.Height,
				},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

func (s *Store) PeerVivaldiCoord(peerID types.PeerKey) (*topology.Coord, bool) {
	snap := s.Snapshot()
	nv, ok := snap.Nodes[peerID]
	if !ok {
		return nil, false
	}
	return nv.VivaldiCoord, nv.VivaldiCoord != nil
}

func (s *Store) UpsertLocalService(port uint32, name string) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]

		if existing, ok := local.Services[name]; ok && existing.GetPort() == port {
			return
		}

		local.Services[name] = &statev1.Service{
			Name: name,
			Port: port,
		}

		key := serviceAttrKey(name)
		local.maxCounter++
		counter := local.maxCounter
		local.log[key] = logEntry{Counter: counter}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_Service{
				Service: &statev1.ServiceChange{Name: name, Port: port},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

//nolint:dupl
func (s *Store) RemoveLocalServices(name string) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]

		if _, ok := local.Services[name]; !ok {
			return
		}

		delete(local.Services, name)

		key := serviceAttrKey(name)
		local.maxCounter++
		counter := local.maxCounter
		local.log[key] = logEntry{Counter: counter, Deleted: true}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Deleted: true,
			Change: &statev1.GossipEvent_Service{
				Service: &statev1.ServiceChange{Name: name},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

func (s *Store) LocalServices() map[string]*statev1.Service {
	snap := s.Snapshot()
	nv, ok := snap.Nodes[snap.LocalID]
	if !ok {
		return nil
	}
	return nv.Services
}

// validNodesLocked returns nodes that are neither denied nor expired.
// CertExpiry == 0 (unset/legacy peers) are NOT filtered.
// Must be called on the store's goroutine.
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

func (s *Store) AllNodes() map[types.PeerKey]nodeRecord {
	var result map[types.PeerKey]nodeRecord
	s.do(func() {
		result = s.validNodesLocked()
	})
	return result
}

// AllPeerKeys returns the PeerKey for every valid, live (reachable from the
// local node) peer. Dead nodes are excluded so the scheduler never assigns
// workload slots to peers that can't fulfil them.
func (s *Store) AllPeerKeys() []types.PeerKey {
	return s.Snapshot().PeerKeys
}

func (s *Store) Get(peerID types.PeerKey) (nodeRecord, bool) {
	var rec nodeRecord
	var ok bool
	s.do(func() {
		var r nodeRecord
		r, ok = s.nodes[peerID]
		if ok {
			rec = r.clone()
		}
	})
	return rec, ok
}

func (s *Store) NodeIPs(peerID types.PeerKey) []string {
	snap := s.Snapshot()
	nv, ok := snap.Nodes[peerID]
	if !ok || len(nv.IPs) == 0 {
		return nil
	}
	return nv.IPs
}

func (s *Store) IsConnected(source, target types.PeerKey) bool {
	snap := s.Snapshot()
	nv, ok := snap.Nodes[source]
	if !ok {
		return false
	}
	_, ok = nv.Reachable[target]
	return ok
}

func (s *Store) HasServicePort(peerID types.PeerKey, port uint32) bool {
	snap := s.Snapshot()
	nv, ok := snap.Nodes[peerID]
	if !ok {
		return false
	}
	for _, svc := range nv.Services {
		if svc.GetPort() == port {
			return true
		}
	}
	return false
}

func (s *Store) KnownPeers() []KnownPeer {
	snap := s.Snapshot()
	known := make([]KnownPeer, 0, len(snap.Nodes))
	for peerID, nv := range snap.Nodes {
		if peerID == snap.LocalID {
			continue
		}
		if nv.LastAddr == "" && (len(nv.IPs) == 0 || nv.LocalPort == 0) {
			continue
		}
		known = append(known, KnownPeer{
			PeerID:             peerID,
			LocalPort:          nv.LocalPort,
			ExternalPort:       nv.ExternalPort,
			ObservedExternalIP: nv.ObservedExternalIP,
			NatType:            nv.NatType,
			IdentityPub:        nv.IdentityPub,
			IPs:                nv.IPs,
			LastAddr:           nv.LastAddr,
			PubliclyAccessible: nv.PubliclyAccessible,
			VivaldiCoord:       nv.VivaldiCoord,
		})
	}

	slices.SortFunc(known, func(a, b KnownPeer) int {
		return a.PeerID.Compare(b.PeerID)
	})

	return known
}

func (s *Store) SetLastAddr(peerID types.PeerKey, addr string) {
	s.do(func() {
		rec, ok := s.nodes[peerID]
		if !ok {
			return
		}
		rec.LastAddr = addr
		s.nodes[peerID] = rec
		s.updateSnapshot()
	})
}

func (s *Store) IdentityPub(peerID types.PeerKey) ([]byte, bool) {
	snap := s.Snapshot()
	nv, ok := snap.Nodes[peerID]
	if !ok || len(nv.IdentityPub) == 0 {
		return nil, false
	}
	return nv.IdentityPub, true
}

func (s *Store) AddDesiredConnection(peerID types.PeerKey, remotePort, localPort uint32) {
	s.do(func() {
		c := Connection{PeerID: peerID, RemotePort: remotePort, LocalPort: localPort}
		s.desiredConnections[c.Key()] = c
		s.updateSnapshot()
	})
}

func (s *Store) RemoveDesiredConnection(peerID types.PeerKey, remotePort, localPort uint32) {
	s.do(func() {
		for key, conn := range s.desiredConnections {
			if conn.PeerID != peerID {
				continue
			}
			if remotePort != 0 && conn.RemotePort != remotePort {
				continue
			}
			if localPort != 0 && conn.LocalPort != localPort {
				continue
			}
			delete(s.desiredConnections, key)
		}
		s.updateSnapshot()
	})
}

func (s *Store) DesiredConnections() []Connection {
	return s.Snapshot().Connections
}

func sortConnections(cs []Connection) {
	slices.SortFunc(cs, func(a, b Connection) int {
		if c := a.PeerID.Compare(b.PeerID); c != 0 {
			return c
		}
		if c := cmp.Compare(a.RemotePort, b.RemotePort); c != 0 {
			return c
		}
		return cmp.Compare(a.LocalPort, b.LocalPort)
	})
}

func (s *Store) DenyPeer(subjectPub []byte) []*statev1.GossipEvent {
	subjectKey := types.PeerKeyFromBytes(subjectPub)

	var events []*statev1.GossipEvent
	s.do(func() {
		if _, ok := s.denied[subjectKey]; ok {
			return
		}

		s.denied[subjectKey] = struct{}{}

		local := s.nodes[s.LocalID]
		key := denyAttrKey(subjectKey.String())
		local.maxCounter++
		counter := local.maxCounter
		local.log[key] = logEntry{Counter: counter}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_Deny{
				Deny: &statev1.DenyChange{SubjectPub: append([]byte(nil), subjectPub...)},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

func (s *Store) IsDenied(subjectPub []byte) bool {
	var ok bool
	s.do(func() {
		_, ok = s.denied[types.PeerKeyFromBytes(subjectPub)]
	})
	return ok
}

// isValidOwnerLocked reports whether a peer is a valid scheduling participant
// (not denied, not expired). Must be called on the store's goroutine.
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

func (s *Store) SetLocalWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) ([]*statev1.GossipEvent, error) {
	var (
		events []*statev1.GossipEvent
		err    error
	)
	s.do(func() {
		// Reject if any valid remote node already publishes this hash.
		for pk, rec := range s.nodes {
			if pk == s.LocalID {
				continue
			}
			if _, has := rec.WorkloadSpecs[hash]; !has {
				continue
			}
			if s.isValidOwnerLocked(pk) {
				err = ErrSpecOwnedRemotely
				return
			}
		}

		local := s.nodes[s.LocalID]

		if existing, ok := local.WorkloadSpecs[hash]; ok &&
			existing.GetReplicas() == replicas && existing.GetMemoryPages() == memoryPages &&
			existing.GetTimeoutMs() == timeoutMs {
			return
		}

		m := make(map[string]*statev1.WorkloadSpecChange, len(local.WorkloadSpecs)+1)
		maps.Copy(m, local.WorkloadSpecs)
		m[hash] = &statev1.WorkloadSpecChange{Hash: hash, Replicas: replicas, MemoryPages: memoryPages, TimeoutMs: timeoutMs}
		local.WorkloadSpecs = m

		key := workloadSpecAttrKey(hash)
		local.maxCounter++
		counter := local.maxCounter
		local.log[key] = logEntry{Counter: counter}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_WorkloadSpec{
				WorkloadSpec: &statev1.WorkloadSpecChange{
					Hash:        hash,
					Replicas:    replicas,
					MemoryPages: memoryPages,
					TimeoutMs:   timeoutMs,
				},
			},
		}}
		s.updateSnapshot()
	})
	if err != nil {
		return nil, err
	}
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events, nil
}

// tombstoneLosingLocalSpec checks whether the local node holds a spec for hash
// that loses to the remote publisher (lower PeerKey wins). If so, it deletes
// the local spec and returns the tombstone event for rebroadcast.
// Must be called on the store's goroutine.
func (s *Store) tombstoneLosingLocalSpec(remotePeer types.PeerKey, hash string) []*statev1.GossipEvent {
	if remotePeer == s.LocalID {
		return nil
	}
	// Ignore invalid peers — denied or expired nodes cannot win ownership.
	if !s.isValidOwnerLocked(remotePeer) {
		return nil
	}
	// Only tombstone if remote peer wins (lower PeerKey).
	if s.LocalID.Compare(remotePeer) < 0 {
		return nil
	}
	local := s.nodes[s.LocalID]
	if _, has := local.WorkloadSpecs[hash]; !has {
		return nil
	}

	delete(local.WorkloadSpecs, hash)

	key := workloadSpecAttrKey(hash)
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

//nolint:dupl
func (s *Store) RemoveLocalWorkloadSpec(hash string) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]

		if _, ok := local.WorkloadSpecs[hash]; !ok {
			return
		}

		delete(local.WorkloadSpecs, hash)

		key := workloadSpecAttrKey(hash)
		local.maxCounter++
		counter := local.maxCounter
		local.log[key] = logEntry{Counter: counter, Deleted: true}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Deleted: true,
			Change: &statev1.GossipEvent_WorkloadSpec{
				WorkloadSpec: &statev1.WorkloadSpecChange{Hash: hash},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

func (s *Store) SetLocalWorkloadClaim(hash string, claimed bool) []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]

		_, exists := local.WorkloadClaims[hash]
		if claimed == exists {
			return
		}

		if claimed {
			m := make(map[string]struct{}, len(local.WorkloadClaims)+1)
			maps.Copy(m, local.WorkloadClaims)
			m[hash] = struct{}{}
			local.WorkloadClaims = m
		} else {
			delete(local.WorkloadClaims, hash)
		}

		key := workloadClaimAttrKey(hash)
		local.maxCounter++
		counter := local.maxCounter
		local.log[key] = logEntry{Counter: counter, Deleted: !claimed}
		s.nodes[s.LocalID] = local

		events = []*statev1.GossipEvent{{
			PeerId:  s.LocalID.String(),
			Counter: counter,
			Deleted: !claimed,
			Change: &statev1.GossipEvent_WorkloadClaim{
				WorkloadClaim: &statev1.WorkloadClaimChange{Hash: hash},
			},
		}}
		s.updateSnapshot()
	})
	if len(events) > 0 {
		s.emitEvent(LocalMutationApplied{Events: events})
	}
	return events
}

// WorkloadSpecView is a merged view of a workload spec and its publisher.
type WorkloadSpecView struct {
	Spec      *statev1.WorkloadSpecChange
	Publisher types.PeerKey
}

// AllWorkloadSpecs returns hash → spec merged across valid (non-denied, non-expired) nodes.
// When multiple peers publish a spec for the same hash, the lowest PeerKey
// wins to ensure deterministic conflict resolution across all nodes.
func (s *Store) AllWorkloadSpecs() map[string]WorkloadSpecView {
	return s.Snapshot().Specs
}

// AllWorkloadClaims returns hash → set of claimant PeerKeys from valid nodes.
// Claims from remote peers outside the local node's connected component in
// the reachability graph are excluded so that dead-node claims don't block
// under-replication recovery.
func (s *Store) AllWorkloadClaims() map[string]map[types.PeerKey]struct{} {
	return s.Snapshot().Claims
}

// Snapshot returns an immutable, point-in-time copy of the cluster state.
// Reads from an atomic pointer — lock-free and safe to call from any goroutine.
func (s *Store) Snapshot() Snapshot {
	if p := s.snap.Load(); p != nil {
		return *p
	}
	return Snapshot{}
}

func (s *Store) snapshotLocked() Snapshot {
	valid := s.validNodesLocked()
	live := s.liveComponentLocked(valid)

	// Build NodeView map from valid nodes.
	nodes := make(map[types.PeerKey]NodeView, len(valid))
	for pk, rec := range valid {
		nv := NodeView{
			LastAddr:           rec.LastAddr,
			ObservedExternalIP: rec.ObservedExternalIP,
			MemTotalBytes:      rec.MemTotalBytes,
			NatType:            rec.NatType,
			CertExpiry:         rec.CertExpiry,
			LocalPort:          rec.LocalPort,
			ExternalPort:       rec.ExternalPort,
			CPUPercent:         rec.CPUPercent,
			MemPercent:         rec.MemPercent,
			NumCPU:             rec.NumCPU,
			PubliclyAccessible: rec.PubliclyAccessible,
			IdentityPub:        append([]byte(nil), rec.IdentityPub...),
			IPs:                append([]string(nil), rec.IPs...),
		}
		if rec.VivaldiCoord != nil {
			coord := *rec.VivaldiCoord
			nv.VivaldiCoord = &coord
		}
		if len(rec.Services) > 0 {
			nv.Services = make(map[string]*statev1.Service, len(rec.Services))
			maps.Copy(nv.Services, rec.Services)
		}
		if len(rec.WorkloadSpecs) > 0 {
			nv.WorkloadSpecs = make(map[string]*statev1.WorkloadSpecChange, len(rec.WorkloadSpecs))
			maps.Copy(nv.WorkloadSpecs, rec.WorkloadSpecs)
		}
		if len(rec.WorkloadClaims) > 0 {
			nv.WorkloadClaims = make(map[string]struct{}, len(rec.WorkloadClaims))
			maps.Copy(nv.WorkloadClaims, rec.WorkloadClaims)
		}
		if len(rec.Reachable) > 0 {
			nv.Reachable = make(map[types.PeerKey]struct{}, len(rec.Reachable))
			maps.Copy(nv.Reachable, rec.Reachable)
		}
		if len(rec.TrafficRates) > 0 {
			nv.TrafficRates = make(map[types.PeerKey]TrafficSnapshot, len(rec.TrafficRates))
			for peer, rate := range rec.TrafficRates {
				nv.TrafficRates[peer] = TrafficSnapshot(rate)
			}
		}
		nodes[pk] = nv
	}

	// PeerKeys: valid + live peers.
	peerKeys := make([]types.PeerKey, 0, len(live))
	for pk := range live {
		peerKeys = append(peerKeys, pk)
	}

	// Specs: conflict resolution — lowest PeerKey wins.
	specs := make(map[string]WorkloadSpecView)
	for peerID, rec := range valid {
		for hash, spec := range rec.WorkloadSpecs {
			existing, ok := specs[hash]
			if !ok || peerID.Compare(existing.Publisher) < 0 {
				specs[hash] = WorkloadSpecView{Spec: spec, Publisher: peerID}
			}
		}
	}

	// Claims: only from live-component nodes.
	claims := make(map[string]map[types.PeerKey]struct{})
	for peerID, rec := range valid {
		if _, ok := live[peerID]; !ok {
			continue
		}
		for hash := range rec.WorkloadClaims {
			if claims[hash] == nil {
				claims[hash] = make(map[types.PeerKey]struct{})
			}
			claims[hash][peerID] = struct{}{}
		}
	}

	// Placements: resource + coordinate + traffic for live nodes.
	placements := make(map[types.PeerKey]NodePlacementState, len(live))
	for pk := range live {
		rec := valid[pk]
		nps := NodePlacementState{
			CPUPercent:    rec.CPUPercent,
			MemPercent:    rec.MemPercent,
			MemTotalBytes: rec.MemTotalBytes,
			NumCPU:        rec.NumCPU,
		}
		if rec.VivaldiCoord != nil {
			coord := *rec.VivaldiCoord
			nps.Coord = &coord
		}
		if len(rec.TrafficRates) > 0 {
			nps.TrafficTo = make(map[types.PeerKey]uint64, len(rec.TrafficRates))
			for peer, rate := range rec.TrafficRates {
				nps.TrafficTo[peer] = rate.BytesIn + rate.BytesOut
			}
		}
		placements[pk] = nps
	}

	// Heatmaps: from valid nodes with traffic data.
	heatmaps := make(map[types.PeerKey]map[types.PeerKey]TrafficSnapshot, len(valid))
	for pk, rec := range valid {
		if len(rec.TrafficRates) == 0 {
			continue
		}
		m := make(map[types.PeerKey]TrafficSnapshot, len(rec.TrafficRates))
		for peer, rate := range rec.TrafficRates {
			m[peer] = TrafficSnapshot(rate)
		}
		heatmaps[pk] = m
	}

	// Connections: deep copy.
	connections := make([]Connection, 0, len(s.desiredConnections))
	for _, c := range s.desiredConnections {
		connections = append(connections, c)
	}
	sortConnections(connections)

	return Snapshot{
		LocalID:     s.LocalID,
		Nodes:       nodes,
		PeerKeys:    peerKeys,
		Specs:       specs,
		Claims:      claims,
		Placements:  placements,
		Heatmaps:    heatmaps,
		Connections: connections,
	}
}

// liveComponentLocked returns the set of valid peers reachable from the local
// node via BFS over the reachability graph. This ensures that stale
// reachability entries from dead peers (e.g. rack-level failure) cannot keep
// other dead peers' claims alive, while still trusting multi-hop observations
// through live intermediaries.
func (s *Store) liveComponentLocked(valid map[types.PeerKey]nodeRecord) map[types.PeerKey]struct{} {
	component := map[types.PeerKey]struct{}{s.LocalID: {}}
	queue := []types.PeerKey{s.LocalID}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		rec, ok := valid[cur]
		if !ok {
			continue
		}
		for neighbor := range rec.Reachable {
			if _, seen := component[neighbor]; seen {
				continue
			}
			if _, isValid := valid[neighbor]; !isValid {
				continue
			}
			component[neighbor] = struct{}{}
			queue = append(queue, neighbor)
		}
	}
	return component
}

// ResolveWorkloadPrefix resolves a hash prefix to a full workload hash from
// valid (non-denied, non-expired) nodes' specs. Returns ("", false) if no
// match, or ("", true, false) if multiple specs match the prefix.
func (s *Store) ResolveWorkloadPrefix(prefix string) (hash string, ambiguous, found bool) {
	snap := s.Snapshot()
	var match string
	for _, nv := range snap.Nodes {
		for h := range nv.WorkloadSpecs {
			if len(h) >= len(prefix) && h[:len(prefix)] == prefix {
				if match != "" && match != h {
					return "", true, false
				}
				match = h
			}
		}
	}
	if match == "" {
		return "", false, false
	}
	return match, false, true
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

// --- Internal helpers ---

// LocalEvents returns all current local events without bumping counters.
// Used at startup to broadcast the full local state so that peers receive
// every event and maintain a correct maxCounter (no counter gaps).
func (s *Store) LocalEvents() []*statev1.GossipEvent {
	var events []*statev1.GossipEvent
	s.do(func() {
		local := s.nodes[s.LocalID]
		events = s.buildEventsAbove(s.LocalID, local, 0)
	})
	return events
}

// bumpAndBroadcastAllLocked returns events for ALL current local attributes,
// each with its own incremented counter. Used on self-state conflict (restart
// recovery). Must be called on the store's goroutine.
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
// counter > minCounter. Must be called on the store's goroutine.
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
// materialized view. Must be called on the store's goroutine.
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
	default:
		panic("unknown attr kind in log")
	}

	return event
}
