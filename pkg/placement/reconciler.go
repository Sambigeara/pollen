// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/evaluator"
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
	scaleUpSustainTicks   = 2
	// scaleUpMaxStepMultiplier caps per-decision target growth — 2× closes
	// large gaps in a few ticks without overshooting on noisy signals.
	scaleUpMaxStepMultiplier = 2.0
)

type workloadManager interface {
	SeedFromCAS(ctx context.Context, hash string, cfg wasm.PluginConfig) error
	Unseed(hash string) error
	IsRunning(hash string) bool
}

type reconciler struct {
	store          WorkloadState
	workloads      workloadManager
	blobs          blobsAPI
	authz          *evaluator.Router
	utilisation    *utilisationTracker
	gates          *gateRegistry
	claimStartTime map[string]time.Time
	pendingRelease map[string]time.Time
	dynamicTargets map[string]uint32
	lastPressures  map[string]float64
	scaleUpStreak  map[string]int
	triggerCh      chan struct{}
	fetchSem       chan struct{}
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
	blobs blobsAPI,
	utilisation *utilisationTracker,
	gates *gateRegistry,
	authz *evaluator.Router,
	log *zap.SugaredLogger,
	wg *sync.WaitGroup,
) *reconciler {
	return &reconciler{
		localID:        localID,
		store:          store,
		workloads:      workloads,
		blobs:          blobs,
		authz:          authz,
		utilisation:    utilisation,
		gates:          gates,
		triggerCh:      make(chan struct{}, 1),
		fetchSem:       make(chan struct{}, 4), //nolint:mnd
		pendingRelease: make(map[string]time.Time),
		claimStartTime: make(map[string]time.Time),
		dynamicTargets: make(map[string]uint32),
		scaleUpStreak:  make(map[string]int),
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

func buildClusterState(snap state.Snapshot) clusterState {
	cluster := clusterState{
		Nodes:           make(map[types.PeerKey]nodeState, len(snap.PeerKeys)),
		InvocationRates: make(map[string]float64),
		OriginRates:     make(map[string]map[types.PeerKey]float64),
		ComputeCost:     make(map[string]float64),
		ParkedTime:      make(map[string]float64),
	}

	costSamples := make(map[string]int)
	parkedSamples := make(map[string]int)

	for _, pk := range snap.PeerKeys {
		nv := snap.Nodes[pk]
		cluster.Nodes[pk] = nodeState{
			CPUPercent:       nv.CPUPercent,
			MemPercent:       nv.MemPercent,
			MemTotalBytes:    nv.MemTotalBytes,
			NumCPU:           nv.NumCPU,
			CPUBudgetPercent: nv.CPUBudgetPercent,
			MemBudgetPercent: nv.MemBudgetPercent,
			Coord:            nv.VivaldiCoord,
		}
		// Unified seed-metrics bundle: ServedRate sums into cluster-wide
		// InvocationRate; OriginRate is kept per-peer so scoring can weight
		// distances; ComputeCost and ParkedTime contribute to the cluster
		// mean only when nonzero (a present-but-zero entry driven by a
		// sibling field means "no observation", not a real 0 ms sample).
		for hash, m := range nv.SeedMetrics {
			if m.ServedRate > 0 {
				cluster.InvocationRates[hash] += float64(m.ServedRate)
			}
			if m.OriginRate > 0 {
				peers, ok := cluster.OriginRates[hash]
				if !ok {
					peers = make(map[types.PeerKey]float64)
					cluster.OriginRates[hash] = peers
				}
				peers[pk] = float64(m.OriginRate)
			}
			if m.ComputeCostMs > 0 {
				cluster.ComputeCost[hash] += float64(m.ComputeCostMs)
				costSamples[hash]++
			}
			if m.ParkedMs > 0 {
				cluster.ParkedTime[hash] += float64(m.ParkedMs)
				parkedSamples[hash]++
			}
		}
	}

	for hash, n := range costSamples {
		if n > 0 {
			cluster.ComputeCost[hash] /= float64(n)
		}
	}
	for hash, n := range parkedSamples {
		if n > 0 {
			cluster.ParkedTime[hash] /= float64(n)
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

	cluster := buildClusterState(snap)

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

	idleDurations := make(map[string]time.Duration, len(specs))
	for hash := range specs {
		idleDurations[hash] = r.utilisation.IdleDuration(hash)
	}

	r.store.SetSeedMetrics(buildSeedMetrics(r.utilisation, r.gates))

	r.adjustGateSizes(specs, cluster, snap.Claims)
	r.refreshSLOLookup(snap)

	signals := computeAutoscaleSignals(specs, r.utilisation)
	dashboardPressures := make(map[string]float64, len(signals))
	for hash, s := range signals {
		dashboardPressures[hash] = s.burn
	}
	r.inFlightMu.Lock()
	r.lastPressures = dashboardPressures
	r.inFlightMu.Unlock()
	r.stepAdjustTargets(specs, signals, len(snap.PeerKeys))

	actions := evaluate(evaluateInput{
		localID:        r.localID,
		allPeers:       snap.PeerKeys,
		specs:          specs,
		claims:         snap.Claims,
		cluster:        cluster,
		isRunning:      r.workloads.IsRunning,
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
			if sv, specExists := snap.Specs[hash]; specExists && uint32(len(snap.Claims[hash])) <= sv.Spec.MinReplicas {
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
	// Gate placement before any blob fetch — if policy forbids this
	// node from hosting the seed, we shouldn't pay the bandwidth cost
	// of pulling the WASM binary first.
	if r.authz != nil {
		var resourceProps map[string]any
		if sv, ok := r.store.Snapshot().Specs[hash]; ok {
			resourceProps = sv.Spec.Claim.GetProperties()
		}
		req := evaluator.Request{
			Subject:  evaluator.SubjectFromPeerKey(r.localID, nil),
			Action:   evaluator.Action{Name: "place"},
			Resource: evaluator.NewResource(evaluator.ResourceSeed, hash, resourceProps),
		}
		if err := r.authz.Allow(ctx, evaluator.GateSeedPlacement, req); err != nil {
			r.log.Infow("seed_placement denied", "hash", types.ShortHash(hash), "err", err)
			return
		}
	}

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
		target := dynamicTarget
		if target == 0 {
			target = sv.Spec.MinReplicas
		}
		cluster := buildClusterState(snap)
		sp := spec{
			MinReplicas: sv.Spec.MinReplicas,
			MemoryBytes: sv.Spec.MemoryBytes,
			Spread:      sv.Spec.Spread,
		}

		claimCount := uint32(len(claimants))
		var stillValid bool
		if claimCount < target {
			stillValid = shouldClaim(r.localID, hash, sp, target, claimants, snap.PeerKeys, cluster)
		} else {
			stillValid = shouldChallenge(r.localID, hash, sp, target, claimants, snap.PeerKeys, cluster)
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
	r.gates.Clear(hash)
	r.inFlightMu.Lock()
	delete(r.claimStartTime, hash)
	r.inFlightMu.Unlock()
	r.log.Infow("released workload", "hash", hash)
}

const (
	// burnCeiling is the SLO burn ratio above which a seed needs more
	// replicas. 5% — i.e. one in twenty caller-observed invocations
	// exceeded the spec's latency SLO recently.
	burnCeiling = 0.05
	// burnSignalFloor is the minimum sloBurned rate (calls/sec) required
	// to treat the burn ratio as actionable. Below this, the ratio is
	// untrustworthy: the satisfied/burned EWMAs decay at the same rate
	// so the ratio sticks at its last value for ~40 ticks after load
	// stops, and a handful of cold-start probes on a low-volume
	// workload can synthesise a phantom high ratio. Zeroing burn below
	// this floor keeps scale-up from firing on stale signal and lets
	// scale-down via the healthy-under-load branch proceed.
	burnSignalFloor = 1.0
	// scaleDownBurnFloor is the SLO burn ratio below which a seed is
	// comfortably over-provisioned and can shed a replica. Set above
	// zero because network jitter produces an unavoidable trickle of
	// > SLO round-trips; a strict zero-burn predicate would freeze
	// replica counts indefinitely in any real-world cluster.
	scaleDownBurnFloor = 0.005
	// scaleDownTrafficFloor is the served rate below which a seed is
	// considered truly idle for scale-down purposes.
	scaleDownTrafficFloor = rateReportFloor
)

// autoscaleSignals feeds the per-tick scale decision. The
// satisfied/burned rates together detect the truly-idle case where
// there are no observations to drive the burn ratio.
type autoscaleSignals struct {
	satisfied float64
	burned    float64
	burn      float64
}

// stepAdjustTargets updates the reconciler's per-hash dynamicTargets map
// based on this node's local autoscale signals.
//
// Invariant: autoscale decisions are node-local. Each node observes its
// own RecordSLO stream through its own utilisation tracker and runs its
// own reconciliation loop. There is no distributed consensus on target
// replica count — `dynamicTargets` is not gossiped. Cluster-wide
// convergence comes from two places:
//
//  1. Every node receives the same gossiped state (specs, claims, Vivaldi
//     coordinates, compute costs, dial rates), so their candidate pools
//     and latency predictions agree.
//  2. `evaluate()` scores candidates using deterministic tie-breaks (peer
//     key hash blended into the score), so every node would pick the same
//     claimant or eviction target given the same view.
//
// Divergence in per-node `dynamicTargets` is expected and self-correcting
// — the node with the highest computed target is the one whose
// claim/release decision dominates, and all nodes converge on the same
// cluster-wide claim count within a handful of reconcile ticks.
func (r *reconciler) stepAdjustTargets(specs map[string]spec, signals map[string]autoscaleSignals, clusterSize int) {
	r.inFlightMu.Lock()
	defer r.inFlightMu.Unlock()

	for hash, sp := range specs {
		ct := r.dynamicTargets[hash]
		if ct == 0 {
			ct = sp.MinReplicas
		}

		s := signals[hash]
		total := s.satisfied + s.burned
		switch {
		case s.burn > burnCeiling:
			r.scaleUpStreak[hash]++
			if r.scaleUpStreak[hash] >= scaleUpSustainTicks {
				step := math.Min(scaleUpMaxStepMultiplier, 1.0+s.burn)
				ct = min(uint32(math.Ceil(float64(ct)*step)), uint32(clusterSize))
				r.scaleUpStreak[hash] = 0
			}
		case total < scaleDownTrafficFloor && ct > sp.MinReplicas:
			// Truly idle: no observations to drive a burn ratio, and the
			// target is above MinReplicas because we previously scaled up.
			// Shed one replica per tick toward MinReplicas.
			r.scaleUpStreak[hash] = 0
			ct = max(sp.MinReplicas, ct-1)
		case total >= scaleDownTrafficFloor && s.burn < scaleDownBurnFloor && ct > sp.MinReplicas:
			// Steady traffic and near-zero burn: comfortably
			// over-provisioned, shed one replica per tick.
			r.scaleUpStreak[hash] = 0
			ct = max(sp.MinReplicas, ct-1)
		default:
			r.scaleUpStreak[hash] = 0
		}
		r.dynamicTargets[hash] = ct
	}

	for hash := range r.dynamicTargets {
		if _, ok := specs[hash]; !ok {
			delete(r.dynamicTargets, hash)
			delete(r.scaleUpStreak, hash)
		}
	}
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
func buildSeedMetrics(ut *utilisationTracker, gates *gateRegistry) map[string]state.SeedMetrics {
	served := ut.ServedRates()
	origin := ut.OriginRates()
	costs := ut.InvocationCosts()
	parked := ut.ParkedTimes()
	satisfied, burned := ut.SLORates()
	gateWaits := gates.WaitEWMAs()

	out := make(map[string]state.SeedMetrics, len(served)+len(origin)+len(costs)+len(parked)+len(satisfied)+len(burned)+len(gateWaits))
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
	for hash, d := range gateWaits {
		if d <= 0 {
			continue
		}
		m := touch(hash)
		m.GateWaitMs = uint32(d / time.Millisecond)
		out[hash] = m
	}
	return out
}

// adjustGateSizes pushes a per-workload concurrency cap to the gate
// registry for every locally-claimed seed. Size is derived from the
// parked/active split via Little's Law: when invocations spend most of
// their time parked inside pollen_request the cap grows so parked slots
// don't starve CPU work from ready work.
func (r *reconciler) adjustGateSizes(specs map[string]spec, cluster clusterState, claims map[string]map[types.PeerKey]struct{}) {
	cores := runtime.NumCPU()
	for hash := range specs {
		if _, mine := claims[hash][r.localID]; !mine {
			continue
		}
		size := desiredGateSize(cores, cluster.ComputeCost[hash], cluster.ParkedTime[hash])
		r.gates.SetHashSize(hash, size)
	}
}

// desiredGateSize returns the per-workload concurrency cap given the
// host's CPU count and the seed's observed compute and parked timings.
// Little's Law: active fraction = (compute - parked) / compute, so a
// fully-active gate needs cores / active slots to keep the CPUs busy.
// Missing telemetry falls back to a conservative cores × initial
// multiplier.
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

const (
	// gateInitialMultiplier is the floor cap for a gate relative to cores
	// — enough headroom to absorb a brief burst without over-allocating
	// instances on every node before observations land.
	gateInitialMultiplier = 2
	// gateMinActiveFraction clamps the Little's-Law denominator so a
	// momentary ~100% parked sample doesn't blow the gate size up to
	// thousands. 10% active corresponds to a 10× cap over cores — plenty
	// for heavy chain-holders.
	gateMinActiveFraction = 0.1
)

// computeAutoscaleSignals derives per-seed SLO burn ratios from the local
// utilisation tracker. The burn ratio is the share of caller-observed
// invocations exceeding the spec's latency SLO over the recent window;
// it ties scaling decisions directly to user-visible pain rather than
// derived ratios that admission absorption can mask.
func computeAutoscaleSignals(specs map[string]spec, ut *utilisationTracker) map[string]autoscaleSignals {
	out := make(map[string]autoscaleSignals, len(specs))
	for hash := range specs {
		satisfied, burned, burn := ut.SLOBurnRate(hash)
		// Zero out the burn ratio when the absolute burn rate is too
		// low to trust. This kills the stale-EWMA bug where, after
		// load stops, the ratio remains pinned at its last value
		// until both rates decay below rateReportFloor ~40 ticks
		// later — during which scale-up would keep firing on phantom
		// signal. Also suppresses low-volume oscillation where a
		// handful of cold-start probes dominate an otherwise quiet
		// workload's ratio.
		if burned < burnSignalFloor {
			burn = 0
		}
		out[hash] = autoscaleSignals{
			satisfied: satisfied,
			burned:    burned,
			burn:      burn,
		}
	}
	return out
}

// PlacementInfo summarises a single workload's autoscale state for
// Status() and control-plane consumers. EffectiveTarget is the
// per-node autoscale decision (capped by spec.MinReplicas and cluster
// size); SLOBurnRatio is the most recent burn signal feeding scale-up.
type PlacementInfo struct {
	EffectiveTarget uint32
	SLOBurnRatio    float64
}

func (r *reconciler) allPlacementInfo() map[string]PlacementInfo {
	r.inFlightMu.Lock()
	defer r.inFlightMu.Unlock()
	out := make(map[string]PlacementInfo, len(r.dynamicTargets))
	for hash, target := range r.dynamicTargets {
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
