package scheduler

import (
	"context"
	"sync"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const (
	debounceInterval = 200 * time.Millisecond
	maxDebounceDelay = 2 * time.Second
	evictionCooldown = 30 * time.Second

	trafficDebounceInterval = 10 * time.Second
	maxTrafficDebounceDelay = 30 * time.Second
	minResidencyDuration    = 10 * time.Second
)

// SchedulerStore abstracts the gossip store query methods needed by the reconciler.
type SchedulerStore interface {
	AllWorkloadSpecs() map[string]store.WorkloadSpecView
	AllWorkloadClaims() map[string]map[types.PeerKey]struct{}
	AllPeerKeys() []types.PeerKey
	SetLocalWorkloadClaim(hash string, claimed bool) []*statev1.GossipEvent
	AllNodePlacementStates() map[types.PeerKey]store.NodePlacementState
}

// WorkloadManager abstracts the workload manager.
type WorkloadManager interface {
	SeedFromCAS(hash string) error
	Unseed(hash string) error
	IsRunning(hash string) bool
}

// ArtifactStore checks local artifact availability.
type ArtifactStore interface {
	Has(hash string) bool
}

// ArtifactFetcher fetches WASM artifacts from peers.
type ArtifactFetcher interface {
	Fetch(ctx context.Context, hash string, peers []types.PeerKey) error
}

// GossipPublisher publishes gossip events.
type GossipPublisher func([]*statev1.GossipEvent)

// Reconciler runs the debounced scheduling loop.
type Reconciler struct {
	store            SchedulerStore
	workloads        WorkloadManager
	cas              ArtifactStore
	fetcher          ArtifactFetcher
	trafficTriggerCh chan struct{}
	triggerCh        chan struct{}
	publish          GossipPublisher
	pendingRelease   map[string]time.Time
	claimStartTime   map[string]time.Time
	log              *zap.SugaredLogger
	inFlight         map[string]struct{}
	nowFunc          func() time.Time
	inFlightMu       sync.Mutex
	localID          types.PeerKey
	firstRun         bool
}

// NewReconciler creates a new reconciler.
func NewReconciler(
	localID types.PeerKey,
	store SchedulerStore,
	workloads WorkloadManager,
	cas ArtifactStore,
	fetcher ArtifactFetcher,
	publish GossipPublisher,
	log *zap.SugaredLogger,
) *Reconciler {
	return &Reconciler{
		localID:          localID,
		store:            store,
		workloads:        workloads,
		cas:              cas,
		fetcher:          fetcher,
		publish:          publish,
		triggerCh:        make(chan struct{}, 1),
		trafficTriggerCh: make(chan struct{}, 1),
		pendingRelease:   make(map[string]time.Time),
		claimStartTime:   make(map[string]time.Time),
		inFlight:         make(map[string]struct{}),
		log:              log,
		firstRun:         true,
		nowFunc:          time.Now,
	}
}

// Signal triggers a reconciliation cycle (non-blocking).
func (r *Reconciler) Signal() {
	select {
	case r.triggerCh <- struct{}{}:
	default:
	}
}

// SignalTraffic triggers a traffic-driven reconciliation cycle (non-blocking).
func (r *Reconciler) SignalTraffic() {
	select {
	case r.trafficTriggerCh <- struct{}{}:
	default:
	}
}

// Run is the main reconciliation loop. Blocks until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) {
	var (
		debounce       *time.Timer
		debounceC      <-chan time.Time
		firstTriggerAt time.Time

		trafficDebounce       *time.Timer
		trafficDebounceC      <-chan time.Time
		firstTrafficTriggerAt time.Time
	)
	drainTimer := func(t *time.Timer) {
		if t != nil && !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			drainTimer(debounce)
			drainTimer(trafficDebounce)
			return
		case <-r.triggerCh:
			now := time.Now()
			if firstTriggerAt.IsZero() {
				firstTriggerAt = now
			}
			if now.Sub(firstTriggerAt) >= maxDebounceDelay {
				r.reconcile(ctx)
				drainTimer(debounce)
				debounceC = nil
				firstTriggerAt = time.Time{}
			} else {
				drainTimer(debounce)
				debounce = time.NewTimer(debounceInterval)
				debounceC = debounce.C
			}
		case <-debounceC:
			r.reconcile(ctx)
			debounceC = nil
			firstTriggerAt = time.Time{}
		case <-r.trafficTriggerCh:
			now := time.Now()
			if firstTrafficTriggerAt.IsZero() {
				firstTrafficTriggerAt = now
			}
			if now.Sub(firstTrafficTriggerAt) >= maxTrafficDebounceDelay {
				r.reconcile(ctx)
				drainTimer(trafficDebounce)
				trafficDebounceC = nil
				firstTrafficTriggerAt = time.Time{}
			} else {
				drainTimer(trafficDebounce)
				trafficDebounce = time.NewTimer(trafficDebounceInterval)
				trafficDebounceC = trafficDebounce.C
			}
		case <-trafficDebounceC:
			r.reconcile(ctx)
			trafficDebounceC = nil
			firstTrafficTriggerAt = time.Time{}
		}
	}
}

func (r *Reconciler) buildClusterState() ClusterState {
	nodePlacement := r.store.AllNodePlacementStates()
	cluster := ClusterState{Nodes: make(map[types.PeerKey]NodeState, len(nodePlacement))}
	for pk, nps := range nodePlacement {
		cluster.Nodes[pk] = NodeState{
			CPUPercent:    nps.CPUPercent,
			MemPercent:    nps.MemPercent,
			MemTotalBytes: nps.MemTotalBytes,
			NumCPU:        nps.NumCPU,
			Coord:         nps.Coord,
			TrafficTo:     nps.TrafficTo,
		}
	}
	return cluster
}

func (r *Reconciler) reconcile(ctx context.Context) {
	// Startup cleanup: remove stale claims from prior session.
	if r.firstRun {
		r.firstRun = false
		r.cleanupStaleClaims()
	}

	specViews := r.store.AllWorkloadSpecs()
	claimMap := r.store.AllWorkloadClaims()
	allPeers := r.store.AllPeerKeys()
	cluster := r.buildClusterState()

	specs := make(map[string]Spec, len(specViews))
	for hash, sv := range specViews {
		specs[hash] = Spec{
			Replicas: sv.Spec.GetReplicas(),
		}
	}

	actions := Evaluate(r.localID, allPeers, specs, claimMap, cluster, r.workloads.IsRunning)

	// Track which hashes Evaluate wants released this cycle.
	wantRelease := make(map[string]struct{})

	now := r.now()
	for _, a := range actions {
		switch a.Kind {
		case ActionClaim:
			r.startClaim(ctx, a.Hash, specViews, claimMap)
		case ActionRelease:
			wantRelease[a.Hash] = struct{}{}
			if _, pending := r.pendingRelease[a.Hash]; !pending {
				r.pendingRelease[a.Hash] = now.Add(evictionCooldown)
			}
		}
	}

	// Execute releases whose cooldown has elapsed.
	for hash, deadline := range r.pendingRelease {
		if _, stillWanted := wantRelease[hash]; !stillWanted {
			// Topology stabilized — cancel pending release.
			delete(r.pendingRelease, hash)
			continue
		}
		if now.Before(deadline) {
			continue
		}
		// Residency window: suppress release if claim is too recent.
		// Only applies to migration (exact replica count). Spec deletions
		// and over-replication proceed immediately after cooldown.
		r.inFlightMu.Lock()
		claimTime, hasClaimTime := r.claimStartTime[hash]
		r.inFlightMu.Unlock()
		if hasClaimTime && now.Sub(claimTime) < minResidencyDuration {
			if sv, specExists := specViews[hash]; specExists && uint32(len(claimMap[hash])) <= sv.Spec.GetReplicas() {
				continue
			}
		}
		r.executeRelease(hash)
		delete(r.pendingRelease, hash)
	}
}

// startClaim launches claim execution in a background goroutine so that slow
// artifact fetches don't block unrelated claim/release decisions. If a claim
// for this hash is already in flight, the call is a no-op.
func (r *Reconciler) startClaim(ctx context.Context, hash string, specViews map[string]store.WorkloadSpecView, claims map[string]map[types.PeerKey]struct{}) {
	r.inFlightMu.Lock()
	if _, ok := r.inFlight[hash]; ok {
		r.inFlightMu.Unlock()
		return
	}
	r.inFlight[hash] = struct{}{}
	r.inFlightMu.Unlock()

	// Snapshot the peer list before launching the goroutine.
	var peers []types.PeerKey
	if sv, ok := specViews[hash]; ok {
		peers = append(peers, sv.Publisher)
	}
	for pk := range claims[hash] {
		peers = append(peers, pk)
	}

	go func() {
		defer func() {
			r.inFlightMu.Lock()
			delete(r.inFlight, hash)
			r.inFlightMu.Unlock()
			r.Signal()
		}()
		r.executeClaim(ctx, hash, peers)
	}()
}

func (r *Reconciler) executeClaim(ctx context.Context, hash string, peers []types.PeerKey) {
	// Ensure artifact is available locally.
	if !r.cas.Has(hash) {
		if err := r.fetcher.Fetch(ctx, hash, peers); err != nil {
			r.log.Warnw("fetch artifact failed", "hash", hash, zap.Error(err))
			return
		}
	}

	// Re-check cluster state after fetch — the spec may have been removed or
	// enough other nodes may have claimed while we were fetching.
	specViews := r.store.AllWorkloadSpecs()
	sv, specExists := specViews[hash]
	if !specExists {
		r.log.Infow("spec removed during fetch, skipping claim", "hash", hash)
		return
	}
	claimMap := r.store.AllWorkloadClaims()
	allPeers := r.store.AllPeerKeys()
	claimants := claimMap[hash]
	if _, alreadyClaimed := claimants[r.localID]; !alreadyClaimed {
		cluster := r.buildClusterState()
		if !shouldClaim(r.localID, hash, sv.Spec.GetReplicas(), claimants, allPeers, cluster) {
			r.log.Infow("no longer a winner after fetch, skipping claim", "hash", hash)
			return
		}
	}

	if err := r.workloads.SeedFromCAS(hash); err != nil {
		r.log.Warnw("seed from CAS failed", "hash", hash, zap.Error(err))
		return
	}

	events := r.store.SetLocalWorkloadClaim(hash, true)
	if len(events) > 0 {
		r.publish(events)
	}
	r.inFlightMu.Lock()
	r.claimStartTime[hash] = r.now()
	r.inFlightMu.Unlock()
	r.log.Infow("claimed workload", "hash", hash)
}

func (r *Reconciler) executeRelease(hash string) {
	if err := r.workloads.Unseed(hash); err != nil {
		r.log.Warnw("unseed failed", "hash", hash, zap.Error(err))
	}
	events := r.store.SetLocalWorkloadClaim(hash, false)
	if len(events) > 0 {
		r.publish(events)
	}
	r.inFlightMu.Lock()
	delete(r.claimStartTime, hash)
	r.inFlightMu.Unlock()
	r.log.Infow("released workload", "hash", hash)
}

func (r *Reconciler) cleanupStaleClaims() {
	now := r.now()
	claimMap := r.store.AllWorkloadClaims()
	for hash, claimants := range claimMap {
		if _, mine := claimants[r.localID]; !mine {
			continue
		}
		if !r.workloads.IsRunning(hash) {
			events := r.store.SetLocalWorkloadClaim(hash, false)
			if len(events) > 0 {
				r.publish(events)
			}
			r.log.Infow("cleaned up stale claim", "hash", hash)
		} else {
			// Seed residency window for claims surviving from a prior session
			// so the dwell-time guard persists across restarts.
			r.inFlightMu.Lock()
			r.claimStartTime[hash] = now
			r.inFlightMu.Unlock()
		}
	}
}

func (r *Reconciler) now() time.Time {
	if r.nowFunc != nil {
		return r.nowFunc()
	}
	return time.Now()
}
