// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"math"
	"runtime"
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
	reconcilePollInterval = 2 * time.Second
)

type workloadManager interface {
	SeedFromCAS(ctx context.Context, hash string, cfg wasm.PluginConfig) error
	Unseed(hash string) error
	IsRunning(hash string) bool
}

type reconciler struct {
	store           WorkloadState
	workloads       workloadManager
	blobs           blobsAPI
	utilisation     *utilisationTracker
	gates           *gateRegistry
	claimStartTime  map[string]time.Time
	pendingRelease  map[string]time.Time
	desiredReplicas map[string]uint32
	lastPressures   map[string]float64
	triggerCh       chan struct{}
	fetchSem        chan struct{}
	log             *zap.SugaredLogger
	inFlight        map[string]struct{}
	nowFunc         func() time.Time
	wg              *sync.WaitGroup
	inFlightMu      sync.Mutex
	localID         types.PeerKey
}

func newReconciler(
	localID types.PeerKey,
	store WorkloadState,
	workloads workloadManager,
	blobs blobsAPI,
	utilisation *utilisationTracker,
	gates *gateRegistry,
	log *zap.SugaredLogger,
	wg *sync.WaitGroup,
) *reconciler {
	return &reconciler{
		localID:         localID,
		store:           store,
		workloads:       workloads,
		blobs:           blobs,
		utilisation:     utilisation,
		gates:           gates,
		triggerCh:       make(chan struct{}, 1),
		fetchSem:        make(chan struct{}, 4), //nolint:mnd
		pendingRelease:  make(map[string]time.Time),
		claimStartTime:  make(map[string]time.Time),
		desiredReplicas: make(map[string]uint32),
		inFlight:        make(map[string]struct{}),
		log:             log,
		wg:              wg,
		nowFunc:         time.Now,
	}
}

func (r *reconciler) Signal() {
	select {
	case r.triggerCh <- struct{}{}:
	default:
	}
}

func (r *reconciler) Run(ctx context.Context) {
	// Reap stale claims (we crashed before unseeding, the wasm runtime
	// cleared, etc.) before entering the steady-state loop. Running this
	// once at start is enough — every subsequent claim flows through
	// executeClaim, which only sets the CRDT bit after a successful seed.
	r.cleanupStaleClaims(r.store.Snapshot().Claims)

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

func (r *reconciler) reconcile(ctx context.Context) {
	snap := r.store.Snapshot()
	cluster := buildClusterState(snap, r.nowFunc())

	// When multiple specs share a name, only schedule the deterministic
	// winner (lowest PeerKey publisher). Unnamed specs always pass through.
	nameWinners := make(map[string]string) // name → winning hash
	for hash, sv := range snap.Specs {
		name := sv.Spec.Name
		if name == "" {
			continue
		}
		if existing, ok := nameWinners[name]; !ok || sv.Publisher.Compare(snap.Specs[existing].Publisher) < 0 {
			nameWinners[name] = hash
		}
	}

	specs := make(map[string]spec, len(snap.Specs))
	for hash, sv := range snap.Specs {
		name := sv.Spec.Name
		if name != "" && nameWinners[name] != hash {
			continue
		}
		specs[hash] = spec{
			MinReplicas: sv.Spec.MinReplicas,
			MemoryBytes: sv.Spec.MemoryBytes,
			Spread:      sv.Spec.Spread,
		}
	}

	r.store.SetSeedMetrics(buildSeedMetrics(r.utilisation))

	r.adjustGateSizes(specs, snap.Claims)
	r.refreshSLOLookup(snap)

	dashboardPressures := burnRatios(specs, r.utilisation)

	clusterSize := len(snap.PeerKeys)
	targets := make(map[string]uint32, len(specs))
	for hash, sp := range specs {
		targets[hash] = desiredReplicas(sp, hash, cluster, clusterSize)
	}

	r.inFlightMu.Lock()
	r.lastPressures = dashboardPressures
	r.desiredReplicas = targets
	r.inFlightMu.Unlock()

	actions := evaluate(evaluateInput{
		localID:         r.localID,
		allPeers:        snap.PeerKeys,
		specs:           specs,
		claims:          snap.Claims,
		draining:        snap.DrainingClaims,
		cluster:         cluster,
		isRunning:       r.workloads.IsRunning,
		desiredReplicas: targets,
	})
	wantRelease := make(map[string]struct{})
	now := r.nowFunc()

	for _, a := range actions {
		switch a.Kind {
		case actionClaim:
			if _, draining := snap.DrainingClaims[a.Hash][r.localID]; draining {
				// Decision plane has rescinded the drain — backfill is
				// needed or demand recovered. Cancel the cooldown and
				// republish the claim without the drain flag. No fetch
				// or seed: the workload is still running locally.
				delete(r.pendingRelease, a.Hash)
				r.store.ClaimWorkload(a.Hash)
				continue
			}
			r.startClaim(ctx, a.Hash, a.DesiredReplicas, snap.Specs, snap.Claims)
		case actionRelease:
			if _, specExists := snap.Specs[a.Hash]; !specExists {
				r.executeRelease(a.Hash)
				delete(r.pendingRelease, a.Hash)
				continue
			}
			wantRelease[a.Hash] = struct{}{}
			if _, pending := r.pendingRelease[a.Hash]; !pending {
				r.pendingRelease[a.Hash] = now.Add(evictionCooldown)
				// Flag the claim as draining so other peers can issue a
				// replacement during the cooldown — make-before-break.
				r.store.MarkWorkloadDraining(a.Hash)
			}
		}
	}

	for hash, deadline := range r.pendingRelease {
		if _, stillWanted := wantRelease[hash]; !stillWanted {
			// Defensive: the decision plane re-emits actionRelease every
			// tick a drain is in flight, so this branch is unreachable in
			// the steady state. If we land here anyway (e.g. spec deleted
			// out from under us between the action loop and this loop),
			// drop the stale entry so it can't pin draining state.
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
			if sv, specExists := snap.Specs[hash]; specExists && uint32(len(snap.Claims[hash])) <= sv.Spec.MinReplicas {
				continue
			}
		}
		r.executeRelease(hash)
		delete(r.pendingRelease, hash)
	}
}

func (r *reconciler) startClaim(ctx context.Context, hash string, desired uint32, specViews map[string]state.WorkloadSpecView, claims map[string]map[types.PeerKey]struct{}) {
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
		r.executeClaim(ctx, hash, desired, peers)
	})
}

func (r *reconciler) executeClaim(ctx context.Context, hash string, desired uint32, peers []types.PeerKey) {
	if !r.blobs.Has(hash) {
		if err := r.blobs.Fetch(ctx, hash, peers); err != nil {
			r.log.Warnw("fetch blob failed", "hash", hash, "err", err)
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
		target := desired
		if target == 0 {
			target = sv.Spec.MinReplicas
		}
		cluster := buildClusterState(snap, r.nowFunc())
		sp := spec{
			MinReplicas: sv.Spec.MinReplicas,
			MemoryBytes: sv.Spec.MemoryBytes,
			Spread:      sv.Spec.Spread,
		}

		drainSet := snap.DrainingClaims[hash]
		claimCount := activeClaimCount(claimants, drainSet)
		var stillValid bool
		if claimCount < target {
			stillValid = shouldClaim(r.localID, hash, sp, target, claimants, drainSet, snap.PeerKeys, cluster)
		} else {
			stillValid = shouldChallenge(r.localID, hash, sp, target, claimants, drainSet, snap.PeerKeys, cluster)
		}
		if !stillValid {
			r.log.Debugw("no longer a winner after fetch, skipping claim", "hash", types.ShortHash(hash))
			return
		}
	}

	cfg := wasm.NewPluginConfig(sv.Spec.MemoryBytes, sv.Spec.Timeout)
	if err := r.workloads.SeedFromCAS(ctx, hash, cfg); err != nil {
		r.log.Warnw("seed from CAS failed", "name", sv.Spec.Name, "hash", types.ShortHash(hash), "err", err)
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
	r.utilisation.Clear(hash)
	r.gates.Clear(hash)
	r.inFlightMu.Lock()
	delete(r.claimStartTime, hash)
	r.inFlightMu.Unlock()
	r.log.Infow("released workload", "hash", hash)
}

// refreshSLOLookup pushes the latest per-spec latency SLO map into the
// utilisation tracker so RecordSLO can classify completed invocations
// without consulting the snapshot on every call. Specs that haven't set
// a latency SLO get the package default, which the tracker also uses
// before this lookup is first installed.
func (r *reconciler) refreshSLOLookup(snap state.Snapshot) {
	specs := snap.Specs
	slos := make(map[string]time.Duration, len(specs))
	for hash, sv := range specs {
		slo := sv.Spec.LatencySLO
		if slo <= 0 {
			slo = defaultLatencySLO
		}
		slos[hash] = slo
	}
	r.utilisation.SetSLOLookup(func(hash string) time.Duration {
		if slo, ok := slos[hash]; ok {
			return slo
		}
		return defaultLatencySLO
	})
}

// buildSeedMetrics collects all per-seed telemetry on this node into a
// single unified bundle per hash. Hashes are the union of every source;
// a source missing a hash contributes zero for its field. The store's
// SetSeedMetrics drops all-zero entries and applies the dead-band.
func buildSeedMetrics(ut *utilisationTracker) map[string]state.SeedMetrics {
	served := ut.ServedRates()
	origin := ut.OriginRates()
	originFast := ut.OriginRatesFast()
	originSlow := ut.OriginRatesSlow()
	rejects := ut.RejectRates()
	costs := ut.InvocationCosts()
	parked := ut.ParkedTimes()
	satisfied, burned := ut.SLORates()

	out := make(map[string]state.SeedMetrics)
	touch := func(hash string) state.SeedMetrics { return out[hash] }

	for hash, v := range served {
		m := touch(hash)
		m.ServedRate = float32(v)
		out[hash] = m
	}
	for hash, v := range origin {
		m := touch(hash)
		m.OriginRate = float32(v)
		out[hash] = m
	}
	for hash, v := range originFast {
		m := touch(hash)
		m.OriginRateFast = float32(v)
		out[hash] = m
	}
	for hash, v := range originSlow {
		m := touch(hash)
		m.OriginRateSlow = float32(v)
		out[hash] = m
	}
	for hash, v := range rejects {
		m := touch(hash)
		m.RejectRate = float32(v)
		out[hash] = m
	}
	for hash, v := range costs {
		m := touch(hash)
		m.ComputeCostMs = float32(v)
		out[hash] = m
	}
	for hash, v := range parked {
		m := touch(hash)
		m.ParkedMs = float32(v)
		out[hash] = m
	}
	for hash, v := range satisfied {
		m := touch(hash)
		m.SLOSatisfiedRate = float32(v)
		out[hash] = m
	}
	for hash, v := range burned {
		m := touch(hash)
		m.SLOBurnedRate = float32(v)
		out[hash] = m
	}
	return out
}

const (
	// gateInitialMultiplier is the floor cap for a gate relative to host
	// cores. Sized so a cold-started gate admits useful concurrency before
	// any per-seed telemetry has accumulated; without it a non-blocking
	// admission rejects most calls during ramp-up, starving the parked-time
	// signal the local-only adaptive sizer needs to grow the cap further.
	// 4 keeps Little's Law dominant for parked chain workloads
	// (cores/0.1 = 10× cores, > floor) and gives leaf workloads a usable
	// cold cap.
	gateInitialMultiplier = 4
	// gateMinActiveFraction clamps the Little's-Law denominator so a
	// momentary ~100% parked sample doesn't blow the gate size up to
	// thousands. 10% active corresponds to a 10× cap over cores.
	gateMinActiveFraction = 0.1
)

// adjustGateSizes resizes the per-workload concurrency cap on this node
// using LOCAL utilisation samples — never cluster-mean parked time. A
// chain workload that parks ~90% of wall time waiting on downstream
// calls gets a wider cap (cores / 0.1 = 10× cores) so parked slots
// don't starve ready work; a leaf at 0% parked stays at the floor.
// Each peer tunes its own gate from its own observation, so a single
// hot node can grow without dragging cool peers' caps with it.
func (r *reconciler) adjustGateSizes(specs map[string]spec, claims map[string]map[types.PeerKey]struct{}) {
	cores := runtime.NumCPU()
	costs := r.utilisation.InvocationCosts()
	parked := r.utilisation.ParkedTimes()
	for hash := range specs {
		if _, mine := claims[hash][r.localID]; !mine {
			continue
		}
		size := desiredGateSize(cores, costs[hash], parked[hash])
		r.gates.SetHashSize(hash, size)
	}
}

// desiredGateSize returns the per-workload concurrency cap given the
// host's CPU count and locally-observed compute and parked timings.
// Little's Law: active fraction = (compute - parked) / compute, so a
// fully-active gate needs cores / active slots to keep the CPUs busy.
// Missing telemetry falls back to the cores × initial multiplier floor.
func desiredGateSize(cores int, computeCostMs, parkedMs float64) int {
	if computeCostMs <= 0 {
		return cores * gateInitialMultiplier
	}
	activeFraction := (computeCostMs - parkedMs) / computeCostMs
	if activeFraction < gateMinActiveFraction {
		activeFraction = gateMinActiveFraction
	}
	size := max(int(math.Ceil(float64(cores)/activeFraction)), cores*gateInitialMultiplier)
	return size
}

// PlacementInfo summarises a single workload's autoscale state for
// Status() and control-plane consumers. EffectiveTarget is the
// cluster-aggregate desired replica count from desiredReplicas
// (capped by spec.MinReplicas and cluster size); SLOBurnRatio is
// observability only — capacity math (Little's Law on origin/compute)
// is what actually drives placement.
type PlacementInfo struct {
	EffectiveTarget uint32
	SLOBurnRatio    float64
}

func (r *reconciler) allPlacementInfo() map[string]PlacementInfo {
	r.inFlightMu.Lock()
	defer r.inFlightMu.Unlock()
	out := make(map[string]PlacementInfo, len(r.desiredReplicas))
	for hash, target := range r.desiredReplicas {
		out[hash] = PlacementInfo{
			EffectiveTarget: target,
			SLOBurnRatio:    r.lastPressures[hash],
		}
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
