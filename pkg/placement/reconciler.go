package placement

import (
	"context"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"go.uber.org/zap"
)

const (
	debounceInterval = 200 * time.Millisecond
	maxDebounceDelay = 2 * time.Second
	evictionCooldown = 30 * time.Second

	minResidencyDuration  = 10 * time.Second
	reconcilePollInterval = 5 * time.Second
)

type workloadManager interface {
	SeedFromCAS(ctx context.Context, hash string, cfg wasm.PluginConfig) error
	Unseed(hash string) error
	IsRunning(hash string) bool
}

type artifactStore interface {
	Has(hash string) bool
}

type artifactFetcher interface {
	Fetch(ctx context.Context, hash string, peers []types.PeerKey) error
}

// reconciler runs the debounced scheduling loop.
type reconciler struct {
	store          WorkloadState
	workloads      workloadManager
	cas            artifactStore
	fetcher        artifactFetcher
	triggerCh      chan struct{}
	pendingRelease map[string]time.Time
	claimStartTime map[string]time.Time
	log            *zap.SugaredLogger
	inFlight       map[string]struct{}
	nowFunc        func() time.Time
	inFlightMu     sync.Mutex
	localID        types.PeerKey
	firstRun       bool
}

func newReconciler(
	localID types.PeerKey,
	store WorkloadState,
	workloads workloadManager,
	cas artifactStore,
	fetcher artifactFetcher,
	log *zap.SugaredLogger,
) *reconciler {
	return &reconciler{
		localID:        localID,
		store:          store,
		workloads:      workloads,
		cas:            cas,
		fetcher:        fetcher,
		triggerCh:      make(chan struct{}, 1),
		pendingRelease: make(map[string]time.Time),
		claimStartTime: make(map[string]time.Time),
		inFlight:       make(map[string]struct{}),
		log:            log,
		firstRun:       true,
		nowFunc:        time.Now,
	}
}

// Signal triggers a reconciliation cycle (non-blocking).
func (r *reconciler) Signal() {
	select {
	case r.triggerCh <- struct{}{}:
	default:
	}
}

// Run is the main reconciliation loop. Blocks until ctx is cancelled.
func (r *reconciler) Run(ctx context.Context) {
	pollTicker := time.NewTicker(reconcilePollInterval)
	defer pollTicker.Stop()

	var (
		debounce       *time.Timer
		debounceC      <-chan time.Time
		firstTriggerAt time.Time
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
			return
		case <-pollTicker.C:
			r.reconcile(ctx)
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
		}
	}
}

func buildClusterState(placements map[types.PeerKey]state.NodePlacementState) clusterState {
	cluster := clusterState{Nodes: make(map[types.PeerKey]nodeState, len(placements))}
	for pk, nps := range placements {
		cluster.Nodes[pk] = nodeState{
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

func (r *reconciler) reconcile(ctx context.Context) {
	snap := r.store.Snapshot()

	if r.firstRun {
		r.firstRun = false
		r.cleanupStaleClaims(snap.Claims)
	}

	cluster := buildClusterState(snap.Placements)

	specs := make(map[string]spec, len(snap.Specs))
	for hash, sv := range snap.Specs {
		specs[hash] = spec{
			Replicas: sv.Spec.GetReplicas(),
		}
	}

	actions := evaluate(r.localID, snap.PeerKeys, specs, snap.Claims, cluster, r.workloads.IsRunning)

	wantRelease := make(map[string]struct{})

	now := r.nowFunc()
	for _, a := range actions {
		switch a.Kind {
		case actionClaim:
			r.startClaim(ctx, a.Hash, snap.Specs, snap.Claims)
		case actionRelease:
			if _, specExists := snap.Specs[a.Hash]; !specExists {
				// Spec deleted — release immediately, no cooldown.
				r.executeRelease(a.Hash)
				delete(r.pendingRelease, a.Hash)
				continue
			}
			wantRelease[a.Hash] = struct{}{}
			if _, pending := r.pendingRelease[a.Hash]; !pending {
				r.pendingRelease[a.Hash] = now.Add(evictionCooldown)
			}
		}
	}

	for hash, deadline := range r.pendingRelease {
		if _, stillWanted := wantRelease[hash]; !stillWanted {
			delete(r.pendingRelease, hash)
			continue
		}
		if now.Before(deadline) {
			continue
		}
		// Residency window: suppress release if claim is too recent.
		// Only applies to migration (exact replica count). Over-replication
		// proceeds immediately after cooldown.
		r.inFlightMu.Lock()
		claimTime, hasClaimTime := r.claimStartTime[hash]
		r.inFlightMu.Unlock()
		if hasClaimTime && now.Sub(claimTime) < minResidencyDuration {
			if sv, specExists := snap.Specs[hash]; specExists && uint32(len(snap.Claims[hash])) <= sv.Spec.GetReplicas() {
				continue
			}
		}
		r.executeRelease(hash)
		delete(r.pendingRelease, hash)
	}
}

// startClaim launches claim execution in a background goroutine so that slow
// artifact fetches don't block unrelated claim/release decisions.
func (r *reconciler) startClaim(ctx context.Context, hash string, specViews map[string]state.WorkloadSpecView, claims map[string]map[types.PeerKey]struct{}) {
	r.inFlightMu.Lock()
	if _, ok := r.inFlight[hash]; ok {
		r.inFlightMu.Unlock()
		return
	}
	r.inFlight[hash] = struct{}{}
	r.inFlightMu.Unlock()

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

func (r *reconciler) executeClaim(ctx context.Context, hash string, peers []types.PeerKey) {
	if !r.cas.Has(hash) {
		if err := r.fetcher.Fetch(ctx, hash, peers); err != nil {
			r.log.Warnw("fetch artifact failed", "hash", hash, "err", err)
			return
		}
	}

	// Re-check cluster state after fetch — the spec may have been removed or
	// enough other nodes may have claimed while we were fetching.
	snap := r.store.Snapshot()
	sv, specExists := snap.Specs[hash]
	if !specExists {
		r.log.Infow("spec removed during fetch, skipping claim", "hash", hash)
		return
	}
	claimants := snap.Claims[hash]
	if _, alreadyClaimed := claimants[r.localID]; !alreadyClaimed {
		cluster := buildClusterState(snap.Placements)
		if !shouldClaim(r.localID, hash, sv.Spec.GetReplicas(), claimants, snap.PeerKeys, cluster) {
			r.log.Infow("no longer a winner after fetch, skipping claim", "hash", hash)
			return
		}
	}

	cfg := wasm.PluginConfig{
		MemoryPages: sv.Spec.GetMemoryPages(),
		Timeout:     time.Duration(sv.Spec.GetTimeoutMs()) * time.Millisecond,
	}
	if err := r.workloads.SeedFromCAS(ctx, hash, cfg); err != nil {
		r.log.Warnw("seed from CAS failed", "hash", hash, "err", err)
		return
	}

	r.store.ClaimWorkload(hash)
	r.inFlightMu.Lock()
	r.claimStartTime[hash] = r.nowFunc()
	r.inFlightMu.Unlock()
	r.log.Infow("claimed workload", "hash", hash)
}

func (r *reconciler) executeRelease(hash string) {
	if err := r.workloads.Unseed(hash); err != nil {
		r.log.Warnw("unseed failed", "hash", hash, "err", err)
	}
	r.store.ReleaseWorkload(hash)
	r.inFlightMu.Lock()
	delete(r.claimStartTime, hash)
	r.inFlightMu.Unlock()
	r.log.Infow("released workload", "hash", hash)
}

func (r *reconciler) cleanupStaleClaims(claims map[string]map[types.PeerKey]struct{}) {
	now := r.nowFunc()
	for hash, claimants := range claims {
		if _, mine := claimants[r.localID]; !mine {
			continue
		}
		if !r.workloads.IsRunning(hash) {
			r.store.ReleaseWorkload(hash)
			r.log.Infow("cleaned up stale claim", "hash", hash)
		} else {
			r.inFlightMu.Lock()
			r.claimStartTime[hash] = now
			r.inFlightMu.Unlock()
		}
	}
}
