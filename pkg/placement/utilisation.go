// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sambigeara/pollen/pkg/observability/metrics"
)

const (
	utilisationAlpha    = 0.2
	utilisationTickRate = time.Second
	rateReportFloor     = 0.01
	// defaultLatencySLO is the per-invocation caller-perspective latency
	// budget applied when a spec doesn't set one explicitly. Generous
	// enough that most reasonable workloads will satisfy it in steady
	// state; tight enough that sustained pain triggers scale-up.
	defaultLatencySLO = time.Second

	// originAlphaFast and originAlphaSlow flank the canonical OriginRate
	// EWMA (utilisationAlpha) for burst-vs-steady-state demand. Fast
	// reacts in ~1.5 s so a microburst is visible to the autoscaler
	// before the medium EWMA notices; slow integrates over ~10 s so
	// transient noise above the medium reading doesn't yank desired
	// replica counts around.
	originAlphaFast = 0.6
	originAlphaSlow = 0.1
)

// defaultSLOLookup is the fallback used until the reconciler installs
// the spec-aware lookup; ensures observations are classified from the
// very first call.
func defaultSLOLookup(string) time.Duration { return defaultLatencySLO }

// functionMetrics holds the per-(hash, function) counter + EWMA bundle.
// Pointer-backed because atomic.Uint64 is not copyable. Admission,
// latency, and SLO all key by function so one slow function's burn
// doesn't average away a fast function's satisfaction.
type functionMetrics struct {
	servedRate       *metrics.EWMA
	originRate       *metrics.EWMA
	originRateFast   *metrics.EWMA
	originRateSlow   *metrics.EWMA
	rejectRate       *metrics.EWMA
	invocationMsRate *metrics.EWMA
	parkedMsRate     *metrics.EWMA
	sloSatisfiedRate *metrics.EWMA
	sloBurnedRate    *metrics.EWMA
	servedCount      atomic.Uint64
	originCount      atomic.Uint64
	rejectCount      atomic.Uint64
	invocationMs     atomic.Uint64
	parkedMs         atomic.Uint64
	sloSatisfied     atomic.Uint64
	sloBurned        atomic.Uint64
}

func newFunctionMetrics() *functionMetrics {
	return &functionMetrics{
		servedRate:       metrics.NewEWMA(utilisationAlpha, 0),
		originRate:       metrics.NewEWMA(utilisationAlpha, 0),
		originRateFast:   metrics.NewEWMA(originAlphaFast, 0),
		originRateSlow:   metrics.NewEWMA(originAlphaSlow, 0),
		rejectRate:       metrics.NewEWMA(utilisationAlpha, 0),
		invocationMsRate: metrics.NewEWMA(utilisationAlpha, 0),
		parkedMsRate:     metrics.NewEWMA(utilisationAlpha, 0),
		sloSatisfiedRate: metrics.NewEWMA(utilisationAlpha, 0),
		sloBurnedRate:    metrics.NewEWMA(utilisationAlpha, 0),
	}
}

type hashState struct {
	functions map[string]*functionMetrics
}

func newHashState() *hashState {
	return &hashState{functions: make(map[string]*functionMetrics)}
}

type utilisationTracker struct {
	hashes    map[string]*hashState
	sloLookup func(hash string) time.Duration
	nowFunc   func() time.Time
	mu        sync.Mutex
}

func newUtilisationTracker() *utilisationTracker {
	return &utilisationTracker{
		hashes:    make(map[string]*hashState),
		sloLookup: defaultSLOLookup,
		nowFunc:   time.Now,
	}
}

func (u *utilisationTracker) ensureHash(hash string) *hashState {
	hs, ok := u.hashes[hash]
	if !ok {
		hs = newHashState()
		u.hashes[hash] = hs
	}
	return hs
}

func (u *utilisationTracker) ensureFunction(hash, function string) *functionMetrics {
	hs := u.ensureHash(hash)
	fm, ok := hs.functions[function]
	if !ok {
		fm = newFunctionMetrics()
		hs.functions[function] = fm
	}
	return fm
}

// SetSLOLookup replaces the function used to look up a workload's
// caller-perspective latency SLO at observation time. The reconciler
// installs a spec-aware lookup; before that, a package-default fallback
// is in place so RecordSLO never silently drops observations.
func (u *utilisationTracker) SetSLOLookup(fn func(string) time.Duration) {
	if fn == nil {
		fn = defaultSLOLookup
	}
	u.mu.Lock()
	u.sloLookup = fn
	u.mu.Unlock()
}

// RecordSLO classifies a completed invocation of (hash, function) against
// the workload's latency SLO using the caller-perspective elapsed time.
func (u *utilisationTracker) RecordSLO(hash, function string, elapsed time.Duration) {
	u.mu.Lock()
	slo := u.sloLookup(hash)
	if slo <= 0 {
		slo = defaultLatencySLO
	}
	fm := u.ensureFunction(hash, function)
	u.mu.Unlock()

	if elapsed > slo {
		fm.sloBurned.Add(1)
	} else {
		fm.sloSatisfied.Add(1)
	}
}

// RecordSLOMiss records an explicit SLO miss for (hash, function),
// bypassing elapsed-vs-SLO. Used when a call was rejected before
// execution (gate ErrAtCapacity, admission ErrOverloaded) — admission
// shedding is real pain that must feed the autoscaler so it grows
// replicas to absorb the offered rate.
func (u *utilisationTracker) RecordSLOMiss(hash, function string) {
	u.mu.Lock()
	fm := u.ensureFunction(hash, function)
	u.mu.Unlock()
	fm.sloBurned.Add(1)
}

// SLOBurnRate returns the per-module satisfied/burned rates and the
// derived burn ratio, summed across every function of the module. Scale
// decisions are per-module (the binary is the replication unit) so the
// burn signal rolls up here.
func (u *utilisationTracker) SLOBurnRate(hash string) (satisfied, burned, burnRatio float64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	hs, ok := u.hashes[hash]
	if !ok {
		return 0, 0, 0
	}
	for _, fm := range hs.functions {
		satisfied += fm.sloSatisfiedRate.Value()
		burned += fm.sloBurnedRate.Value()
	}
	burnRatio = burned / math.Max(satisfied+burned, rateReportFloor)
	return satisfied, burned, burnRatio
}

// RecordOrigin counts a call that entered the cluster at this node for
// this hash, regardless of whether the node hosts the seed or forwards
// the call onward. The cluster-wide OriginRate distribution is the
// demand signal placement scoring uses to pull a seed toward where its
// traffic enters.
func (u *utilisationTracker) RecordOrigin(hash, function string) {
	u.mu.Lock()
	fm := u.ensureFunction(hash, function)
	u.mu.Unlock()
	fm.originCount.Add(1)
}

// RecordReject counts an admission rejection for (hash, function) — gate
// at-capacity, memory budget, or any other backpressure that returns
// before the workload runs. The rejected calls are also a kind of
// "demand" that wasn't served, gossiped as RejectRate so other peers
// can route away and the autoscaler can grow replicas to absorb the
// offered rate.
func (u *utilisationTracker) RecordReject(hash, function string) {
	u.mu.Lock()
	fm := u.ensureFunction(hash, function)
	u.mu.Unlock()
	fm.rejectCount.Add(1)
}

// RecordServed counts a local execution of (hash, function) — either
// this node's own pickP2C landed local, or a forwardCall arrived here.
// Feeds the gossiped per-(peer, hash) ServedRate that the warm-replacement
// gate consults before draining over-replicated claimants.
func (u *utilisationTracker) RecordServed(hash, function string) {
	u.mu.Lock()
	fm := u.ensureFunction(hash, function)
	u.mu.Unlock()
	fm.servedCount.Add(1)
}

// RecordInvocation accumulates wall-time (ms) into the (hash, function)
// counter that the tick loop converts into a ms/sec rate. Paired with
// the function's served rate this yields its mean cost per invocation,
// decaying to zero when the function idles.
func (u *utilisationTracker) RecordInvocation(hash, function string, elapsed time.Duration) {
	u.mu.Lock()
	fm := u.ensureFunction(hash, function)
	u.mu.Unlock()
	fm.invocationMs.Add(uint64(elapsed / time.Millisecond))
}

// RecordParkedTime accumulates wall-time (ms) that an invocation of
// (hash, function) spent blocked inside pollen_request waiting for
// downstream responses. Per-function so future sizing heuristics can
// distinguish a chain-holding function from a leaf one in the same
// module.
func (u *utilisationTracker) RecordParkedTime(hash, function string, elapsed time.Duration) {
	u.mu.Lock()
	fm := u.ensureFunction(hash, function)
	u.mu.Unlock()
	fm.parkedMs.Add(uint64(elapsed / time.Millisecond))
}

// SLORates returns per-module satisfied and burned rates (calls/sec),
// summed across functions. Suitable for gossip: each node publishes its
// own local view, and the supervisor Prometheus collector aggregates
// cluster-wide.
func (u *utilisationTracker) SLORates() (satisfied, burned map[string]float64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	satisfied = make(map[string]float64)
	burned = make(map[string]float64)
	for hash, hs := range u.hashes {
		var s, b float64
		for _, fm := range hs.functions {
			s += fm.sloSatisfiedRate.Value()
			b += fm.sloBurnedRate.Value()
		}
		if s > rateReportFloor {
			satisfied[hash] = s
		}
		if b > rateReportFloor {
			burned[hash] = b
		}
	}
	return satisfied, burned
}

// ServedRates returns per-module served rates, summed across functions.
func (u *utilisationTracker) ServedRates() map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]float64)
	for hash, hs := range u.hashes {
		var s float64
		for _, fm := range hs.functions {
			s += fm.servedRate.Value()
		}
		if s > rateReportFloor {
			out[hash] = s
		}
	}
	return out
}

// OriginRates returns per-module rates of calls entering the cluster at
// this node for each hash, summed across functions. Gossiped so every
// peer's placement scorer sees where demand originates.
func (u *utilisationTracker) OriginRates() map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]float64)
	for hash, hs := range u.hashes {
		var r float64
		for _, fm := range hs.functions {
			r += fm.originRate.Value()
		}
		if r > rateReportFloor {
			out[hash] = r
		}
	}
	return out
}

// OriginRatesFast and OriginRatesSlow flank OriginRates with short- and
// long-window EWMAs of the same counter stream. The autoscaler reads
// max(slow×headroom, fast) so a microburst that hasn't lifted the
// medium EWMA yet still triggers scale-up. Floor handling matches
// OriginRates so a rate hovering near the noise band doesn't gossip.
func (u *utilisationTracker) OriginRatesFast() map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]float64)
	for hash, hs := range u.hashes {
		var r float64
		for _, fm := range hs.functions {
			r += fm.originRateFast.Value()
		}
		if r > rateReportFloor {
			out[hash] = r
		}
	}
	return out
}

func (u *utilisationTracker) OriginRatesSlow() map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]float64)
	for hash, hs := range u.hashes {
		var r float64
		for _, fm := range hs.functions {
			r += fm.originRateSlow.Value()
		}
		if r > rateReportFloor {
			out[hash] = r
		}
	}
	return out
}

// RejectRates returns per-module admission-rejection rates summed
// across functions. Gossiped so peers route away from saturated
// claimants and the autoscaler reacts immediately to shedding.
func (u *utilisationTracker) RejectRates() map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]float64)
	for hash, hs := range u.hashes {
		var r float64
		for _, fm := range hs.functions {
			r += fm.rejectRate.Value()
		}
		if r > rateReportFloor {
			out[hash] = r
		}
	}
	return out
}

// InvocationCosts returns the per-module mean wall-time (ms) per
// invocation as a traffic-weighted mean across functions: (sum of
// per-function ms/sec) / (sum of per-function calls/sec). Functions
// with no served traffic contribute no weight. Dropped when the
// aggregated served rate falls below rateReportFloor.
func (u *utilisationTracker) InvocationCosts() map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]float64, len(u.hashes))
	for hash, hs := range u.hashes {
		var served, invMs float64
		for _, fm := range hs.functions {
			served += fm.servedRate.Value()
			invMs += fm.invocationMsRate.Value()
		}
		if served <= rateReportFloor {
			continue
		}
		if cost := invMs / served; cost > 0 {
			out[hash] = cost
		}
	}
	return out
}

// ParkedTimes mirrors InvocationCosts for parked-in-pollen_request time —
// a traffic-weighted mean across functions.
func (u *utilisationTracker) ParkedTimes() map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]float64, len(u.hashes))
	for hash, hs := range u.hashes {
		var served, parkedMs float64
		for _, fm := range hs.functions {
			served += fm.servedRate.Value()
			parkedMs += fm.parkedMsRate.Value()
		}
		if served <= rateReportFloor {
			continue
		}
		if parked := parkedMs / served; parked > 0 {
			out[hash] = parked
		}
	}
	return out
}

// Clear removes all tracking state for a module (called on unseed).
func (u *utilisationTracker) Clear(hash string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	delete(u.hashes, hash)
}

// tick converts accumulated call counters to rates and feeds them into
// the EWMAs. Called every utilisationTickRate (1s).
func (u *utilisationTracker) tick(elapsed time.Duration) {
	secs := elapsed.Seconds()
	if secs <= 0 {
		return
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	for _, hs := range u.hashes {
		for _, fm := range hs.functions {
			fm.servedRate.Update(float64(fm.servedCount.Swap(0)) / secs)
			originPerSec := float64(fm.originCount.Swap(0)) / secs
			fm.originRate.Update(originPerSec)
			fm.originRateFast.Update(originPerSec)
			fm.originRateSlow.Update(originPerSec)
			fm.rejectRate.Update(float64(fm.rejectCount.Swap(0)) / secs)
			fm.invocationMsRate.Update(float64(fm.invocationMs.Swap(0)) / secs)
			fm.parkedMsRate.Update(float64(fm.parkedMs.Swap(0)) / secs)
			fm.sloSatisfiedRate.Update(float64(fm.sloSatisfied.Swap(0)) / secs)
			fm.sloBurnedRate.Update(float64(fm.sloBurned.Swap(0)) / secs)
		}
	}
}

// run starts the periodic tick loop. Blocks until ctx is cancelled.
func (u *utilisationTracker) run(ctx context.Context) {
	ticker := time.NewTicker(utilisationTickRate)
	defer ticker.Stop()
	last := u.nowFunc()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := u.nowFunc()
			u.tick(now.Sub(last))
			last = now
		}
	}
}
