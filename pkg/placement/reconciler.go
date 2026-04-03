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
	debounceInterval      = 200 * time.Millisecond
	evictionCooldown      = 30 * time.Second
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

type reconciler struct {
	store          WorkloadState
	workloads      workloadManager
	cas            artifactStore
	fetcher        artifactFetcher
	claimStartTime map[string]time.Time
	pendingRelease map[string]time.Time
	triggerCh      chan struct{}
	log            *zap.SugaredLogger
	inFlight       map[string]struct{}
	nowFunc        func() time.Time
	wg             *sync.WaitGroup
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
	wg *sync.WaitGroup,
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
		wg:             wg,
		firstRun:       true,
		nowFunc:        time.Now,
	}
}

func (r *reconciler) Signal() {
	select {
	case r.triggerCh <- struct{}{}:
	default:
	}
}

func (r *reconciler) Run(ctx context.Context) {
	pollTicker := time.NewTicker(reconcilePollInterval)
	defer pollTicker.Stop()

	var debounce *time.Timer
	var debounceC <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			if debounce != nil {
				debounce.Stop()
			}
			return
		case <-pollTicker.C:
			r.reconcile(ctx)
		case <-r.triggerCh:
			if debounce != nil {
				debounce.Stop()
			}
			debounce = time.NewTimer(debounceInterval)
			debounceC = debounce.C
		case <-debounceC:
			r.reconcile(ctx)
			debounceC = nil
		}
	}
}

func buildClusterState(nodes map[types.PeerKey]state.NodeView, peerKeys []types.PeerKey) clusterState {
	cluster := clusterState{Nodes: make(map[types.PeerKey]nodeState, len(peerKeys))}
	for _, pk := range peerKeys {
		nv := nodes[pk]
		ns := nodeState{
			CPUPercent:    nv.CPUPercent,
			MemPercent:    nv.MemPercent,
			MemTotalBytes: nv.MemTotalBytes,
			NumCPU:        nv.NumCPU,
			Coord:         nv.VivaldiCoord,
		}
		if len(nv.TrafficRates) > 0 {
			ns.TrafficTo = make(map[types.PeerKey]uint64, len(nv.TrafficRates))
			for peer, rate := range nv.TrafficRates {
				ns.TrafficTo[peer] = rate.BytesIn + rate.BytesOut
			}
		}
		cluster.Nodes[pk] = ns
	}
	return cluster
}

func (r *reconciler) reconcile(ctx context.Context) {
	snap := r.store.Snapshot()

	if r.firstRun {
		r.firstRun = false
		r.cleanupStaleClaims(snap.Claims)
	}

	cluster := buildClusterState(snap.Nodes, snap.PeerKeys)

	specs := make(map[string]spec, len(snap.Specs))
	for hash, sv := range snap.Specs {
		specs[hash] = spec{Replicas: sv.Spec.GetReplicas()}
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

	r.wg.Go(func() {
		defer func() {
			r.inFlightMu.Lock()
			delete(r.inFlight, hash)
			r.inFlightMu.Unlock()
			r.Signal()
		}()
		r.executeClaim(ctx, hash, peers)
	})
}

func (r *reconciler) executeClaim(ctx context.Context, hash string, peers []types.PeerKey) {
	if !r.cas.Has(hash) {
		if err := r.fetcher.Fetch(ctx, hash, peers); err != nil {
			r.log.Warnw("fetch artifact failed", "hash", hash, "err", err)
			return
		}
	}

	snap := r.store.Snapshot()
	sv, specExists := snap.Specs[hash]
	if !specExists {
		r.log.Infow("spec removed during fetch, skipping claim", "hash", hash)
		return
	}

	claimants := snap.Claims[hash]
	if _, alreadyClaimed := claimants[r.localID]; !alreadyClaimed {
		cluster := buildClusterState(snap.Nodes, snap.PeerKeys)
		if !shouldClaim(r.localID, hash, sv.Spec.GetReplicas(), claimants, snap.PeerKeys, cluster) {
			r.log.Infow("no longer a winner after fetch, skipping claim", "hash", hash)
			return
		}
	}

	cfg := wasm.NewPluginConfig(sv.Spec.GetMemoryPages(), time.Duration(sv.Spec.GetTimeoutMs())*time.Millisecond)
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
