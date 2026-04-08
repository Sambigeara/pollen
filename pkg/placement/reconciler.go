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
	scaleUpStreakRequired = 3 // consecutive high-pressure cycles before adding a replica
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
	store              WorkloadState
	workloads          workloadManager
	cas                artifactStore
	fetcher            artifactFetcher
	utilisation        *utilisationTracker
	claimStartTime     map[string]time.Time
	pendingRelease     map[string]time.Time
	dynamicTargets     map[string]uint32
	lastPressures      map[string]float64
	highPressureStreak map[string]int
	triggerCh          chan struct{}
	fetchSem           chan struct{}
	log                *zap.SugaredLogger
	inFlight           map[string]struct{}
	nowFunc            func() time.Time
	wg                 *sync.WaitGroup
	inFlightMu         sync.Mutex
	localID            types.PeerKey
	firstRun           bool
}

func newReconciler(
	localID types.PeerKey,
	store WorkloadState,
	workloads workloadManager,
	cas artifactStore,
	fetcher artifactFetcher,
	utilisation *utilisationTracker,
	log *zap.SugaredLogger,
	wg *sync.WaitGroup,
) *reconciler {
	return &reconciler{
		localID:            localID,
		store:              store,
		workloads:          workloads,
		cas:                cas,
		fetcher:            fetcher,
		utilisation:        utilisation,
		triggerCh:          make(chan struct{}, 1),
		fetchSem:           make(chan struct{}, 4), //nolint:mnd
		pendingRelease:     make(map[string]time.Time),
		claimStartTime:     make(map[string]time.Time),
		dynamicTargets:     make(map[string]uint32),
		highPressureStreak: make(map[string]int),
		inFlight:           make(map[string]struct{}),
		log:                log,
		wg:                 wg,
		firstRun:           true,
		nowFunc:            time.Now,
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
	cluster := clusterState{
		Nodes:      make(map[types.PeerKey]nodeState, len(peerKeys)),
		SeedLoad:   make(map[types.PeerKey]map[string]float32, len(peerKeys)),
		SeedDemand: make(map[types.PeerKey]map[string]float32, len(peerKeys)),
	}
	for _, pk := range peerKeys {
		nv := nodes[pk]
		ns := nodeState{
			CPUPercent:       nv.CPUPercent,
			MemPercent:       nv.MemPercent,
			MemTotalBytes:    nv.MemTotalBytes,
			NumCPU:           nv.NumCPU,
			CPUBudgetPercent: nv.CPUBudgetPercent,
			MemBudgetPercent: nv.MemBudgetPercent,
			Coord:            nv.VivaldiCoord,
		}
		if len(nv.TrafficRates) > 0 {
			ns.TrafficTo = make(map[types.PeerKey]uint64, len(nv.TrafficRates))
			for peer, rate := range nv.TrafficRates {
				ns.TrafficTo[peer] = rate.BytesIn + rate.BytesOut
			}
		}
		cluster.Nodes[pk] = ns
		if len(nv.SeedLoad) > 0 {
			cluster.SeedLoad[pk] = nv.SeedLoad
		}
		if len(nv.SeedDemand) > 0 {
			cluster.SeedDemand[pk] = nv.SeedDemand
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

	cluster := buildClusterState(snap.Nodes, snap.PeerKeys)

	// When multiple specs share a name, only schedule the deterministic
	// winner (lowest PeerKey publisher). Unnamed specs always pass through.
	nameWinners := make(map[string]string) // name → winning hash
	for hash, sv := range snap.Specs {
		name := sv.Spec.GetName()
		if name == "" {
			continue
		}
		if existing, ok := nameWinners[name]; !ok || sv.Publisher.Compare(snap.Specs[existing].Publisher) < 0 {
			nameWinners[name] = hash
		}
	}

	specs := make(map[string]spec, len(snap.Specs))
	for hash, sv := range snap.Specs {
		name := sv.Spec.GetName()
		if name != "" && nameWinners[name] != hash {
			continue
		}
		specs[hash] = spec{MinReplicas: sv.Spec.GetMinReplicas(), Spread: sv.Spec.GetSpread()}
	}

	demandRates := make(map[string]float64, len(specs))
	idleDurations := make(map[string]time.Duration, len(specs))
	for hash := range specs {
		demandRates[hash] = r.utilisation.DemandRate(hash)
		idleDurations[hash] = r.utilisation.IdleDuration(hash)
	}

	servedRates := r.utilisation.ServedRates()
	float32Rates := make(map[string]float32, len(servedRates))
	for k, v := range servedRates {
		float32Rates[k] = float32(v)
	}
	r.store.SetSeedLoad(float32Rates)

	demandRatesFull := r.utilisation.DemandRates()
	float32DemandRates := make(map[string]float32, len(demandRatesFull))
	for k, v := range demandRatesFull {
		float32DemandRates[k] = float32(v)
	}
	r.store.SetSeedDemand(float32DemandRates)

	pressures := computePressures(specs, cluster.SeedLoad, cluster.SeedDemand, snap.Claims)
	r.inFlightMu.Lock()
	r.lastPressures = pressures
	r.inFlightMu.Unlock()
	r.stepAdjustTargets(specs, pressures, len(snap.PeerKeys))

	actions := evaluate(evaluateInput{
		localID:        r.localID,
		allPeers:       snap.PeerKeys,
		specs:          specs,
		claims:         snap.Claims,
		cluster:        cluster,
		isRunning:      r.workloads.IsRunning,
		demandRates:    demandRates,
		idleDurations:  idleDurations,
		dynamicTargets: r.dynamicTargets,
	})
	wantRelease := make(map[string]struct{})
	now := r.nowFunc()

	for _, a := range actions {
		switch a.Kind {
		case actionClaim:
			r.startClaim(ctx, a.Hash, a.DynamicTarget, snap.Specs, snap.Claims)
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
			if sv, specExists := snap.Specs[hash]; specExists && uint32(len(snap.Claims[hash])) <= sv.Spec.GetMinReplicas() {
				continue
			}
		}
		r.executeRelease(hash)
		delete(r.pendingRelease, hash)
	}
}

func (r *reconciler) startClaim(ctx context.Context, hash string, dynamicTarget uint32, specViews map[string]state.WorkloadSpecView, claims map[string]map[types.PeerKey]struct{}) {
	r.inFlightMu.Lock()
	if _, ok := r.inFlight[hash]; ok {
		r.inFlightMu.Unlock()
		return
	}
	r.inFlight[hash] = struct{}{}
	r.inFlightMu.Unlock()

	select {
	case r.fetchSem <- struct{}{}:
	default:
		r.inFlightMu.Lock()
		delete(r.inFlight, hash)
		r.inFlightMu.Unlock()
		return
	}

	var peers []types.PeerKey
	if sv, ok := specViews[hash]; ok {
		peers = append(peers, sv.Publisher)
	}
	for pk := range claims[hash] {
		peers = append(peers, pk)
	}

	r.wg.Go(func() {
		defer func() {
			<-r.fetchSem
			r.inFlightMu.Lock()
			delete(r.inFlight, hash)
			r.inFlightMu.Unlock()
			r.Signal()
		}()
		r.executeClaim(ctx, hash, dynamicTarget, peers)
	})
}

func (r *reconciler) executeClaim(ctx context.Context, hash string, dynamicTarget uint32, peers []types.PeerKey) {
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
		target := dynamicTarget
		if target == 0 {
			target = sv.Spec.GetMinReplicas()
		}
		cluster := buildClusterState(snap.Nodes, snap.PeerKeys)
		demandRates := map[string]float64{hash: r.utilisation.DemandRate(hash)}

		claimCount := uint32(len(claimants))
		var stillValid bool
		if claimCount < target {
			stillValid = shouldClaim(r.localID, hash, target, claimants, snap.PeerKeys, cluster, demandRates)
		} else {
			stillValid = shouldChallenge(r.localID, hash, claimants, cluster, demandRates)
		}
		if !stillValid {
			r.log.Debugw("no longer a winner after fetch, skipping claim", "hash", shortHash(hash))
			return
		}
	}

	cfg := wasm.NewPluginConfig(sv.Spec.GetMemoryPages(), time.Duration(sv.Spec.GetTimeoutMs())*time.Millisecond)
	if err := r.workloads.SeedFromCAS(ctx, hash, cfg); err != nil {
		r.log.Warnw("seed from CAS failed", "hash", hash, "err", err)
		return
	}

	r.store.ClaimWorkload(hash)
	r.utilisation.MarkActive(hash)
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
	r.utilisation.Clear(hash)
	r.inFlightMu.Lock()
	delete(r.claimStartTime, hash)
	r.inFlightMu.Unlock()
	r.log.Infow("released workload", "hash", hash)
}

const (
	scaleUpThreshold     = 1.2
	scaleDownThreshold   = 0.8
	coldStartPressureCap = scaleUpThreshold + 0.1
)

func (r *reconciler) stepAdjustTargets(specs map[string]spec, pressures map[string]float64, clusterSize int) {
	r.inFlightMu.Lock()
	defer r.inFlightMu.Unlock()

	for hash, sp := range specs {
		ct := r.dynamicTargets[hash]
		if ct == 0 {
			ct = sp.MinReplicas
		}

		p := pressures[hash]
		switch {
		case p > scaleUpThreshold:
			r.highPressureStreak[hash]++
			if r.highPressureStreak[hash] >= scaleUpStreakRequired {
				ct = min(ct+1, uint32(clusterSize))
				r.highPressureStreak[hash] = 0
			}
		case p < scaleDownThreshold:
			r.highPressureStreak[hash] = 0
			ct = max(sp.MinReplicas, ct-1)
		default:
			r.highPressureStreak[hash] = 0
		}
		r.dynamicTargets[hash] = ct
	}

	for hash := range r.dynamicTargets {
		if _, ok := specs[hash]; !ok {
			delete(r.dynamicTargets, hash)
			delete(r.highPressureStreak, hash)
		}
	}
}

func computePressures(
	specs map[string]spec,
	seedLoad, seedDemand map[types.PeerKey]map[string]float32,
	claims map[string]map[types.PeerKey]struct{},
) map[string]float64 {
	pressures := make(map[string]float64, len(specs))
	for hash := range specs {
		var clusterDemand, clusterServed float64
		for _, rates := range seedDemand {
			clusterDemand += float64(rates[hash])
		}
		for _, rates := range seedLoad {
			clusterServed += float64(rates[hash])
		}
		switch {
		case clusterDemand == 0:
			pressures[hash] = 0
		case clusterServed > 0:
			pressures[hash] = clusterDemand / clusterServed
		case len(claims[hash]) > 0:
			// Claimants exist but served rate hasn't propagated via gossip
			// yet. Hold steady — don't scale up on transient gossip lag.
			pressures[hash] = 0
		default:
			// True cold start: demand exists, no claimants, no served rate.
			// Bootstrap one extra replica per cycle.
			pressures[hash] = coldStartPressureCap
		}
	}
	return pressures
}

func (r *reconciler) allPlacementInfo() map[string][2]float64 {
	r.inFlightMu.Lock()
	defer r.inFlightMu.Unlock()
	out := make(map[string][2]float64, len(r.dynamicTargets))
	for hash, target := range r.dynamicTargets {
		out[hash] = [2]float64{float64(target), r.lastPressures[hash]}
	}
	return out
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
