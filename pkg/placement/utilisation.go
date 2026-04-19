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
	invocationMsRate *metrics.EWMA
	parkedMsRate     *metrics.EWMA
	sloSatisfiedRate *metrics.EWMA
	sloBurnedRate    *metrics.EWMA
	servedCount      atomic.Uint64
	invocationMs     atomic.Uint64
	parkedMs         atomic.Uint64
	sloSatisfied     atomic.Uint64
	sloBurned        atomic.Uint64
}

func newFunctionMetrics() *functionMetrics {
	return &functionMetrics{
		servedRate:       metrics.NewEWMA(utilisationAlpha, 0),
		invocationMsRate: metrics.NewEWMA(utilisationAlpha, 0),
		parkedMsRate:     metrics.NewEWMA(utilisationAlpha, 0),
		sloSatisfiedRate: metrics.NewEWMA(utilisationAlpha, 0),
		sloBurnedRate:    metrics.NewEWMA(utilisationAlpha, 0),
	}
}

// hashState groups module-level lifecycle (lastActivity) with the
// per-function metric bundles. lastActivity lives at module granularity
// because claim residency and idle-eviction decisions are per-module —
// the binary is the replication unit.
type hashState struct {
	lastActivity time.Time
	functions    map[string]*functionMetrics
}

func newHashState() *hashState {
	return &hashState{functions: make(map[string]*functionMetrics)}
}

type utilisationTracker struct {
	hashes       map[string]*hashState
	dial         map[callKey]map[string]*metrics.EWMA
	dialCounters map[callKey]map[string]*atomic.Uint64
	sloLookup    func(hash string) time.Duration
	nowFunc      func() time.Time
	mu           sync.Mutex
}

func newUtilisationTracker() *utilisationTracker {
	return &utilisationTracker{
		hashes:       make(map[string]*hashState),
		dial:         make(map[callKey]map[string]*metrics.EWMA),
		dialCounters: make(map[callKey]map[string]*atomic.Uint64),
		sloLookup:    defaultSLOLookup,
		nowFunc:      time.Now,
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
	if total := satisfied + burned; total > rateReportFloor {
		burnRatio = burned / total
	}
	return satisfied, burned, burnRatio
}

// MarkActive stamps module-level lastActivity without incrementing any
// counter. Used when a seed is first claimed so the idle-release gate
// starts from claim time rather than treating a fresh claimant as
// infinitely idle.
func (u *utilisationTracker) MarkActive(hash string) {
	u.mu.Lock()
	hs := u.ensureHash(hash)
	hs.lastActivity = u.nowFunc()
	u.mu.Unlock()
}

func (u *utilisationTracker) RecordServed(hash, function string) {
	u.mu.Lock()
	hs := u.ensureHash(hash)
	fm, ok := hs.functions[function]
	if !ok {
		fm = newFunctionMetrics()
		hs.functions[function] = fm
	}
	hs.lastActivity = u.nowFunc()
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

// RecordDial increments the per-(callerHash, callerFn, target) dial
// counter. Dial observations follow the same per-function model as the
// rest of the telemetry; DialRates() aggregates them back to per-hash
// for gossip and placement scoring.
func (u *utilisationTracker) RecordDial(callerHash, callerFn, targetKey string) {
	ck := callKey{Hash: callerHash, Function: callerFn}
	u.mu.Lock()
	targets, ok := u.dialCounters[ck]
	if !ok {
		targets = make(map[string]*atomic.Uint64)
		u.dialCounters[ck] = targets
	}
	c, ok := targets[targetKey]
	if !ok {
		c = &atomic.Uint64{}
		targets[targetKey] = c
	}
	if _, ok := u.dial[ck]; !ok {
		u.dial[ck] = make(map[string]*metrics.EWMA)
	}
	if _, ok := u.dial[ck][targetKey]; !ok {
		u.dial[ck][targetKey] = metrics.NewEWMA(utilisationAlpha, 0)
	}
	u.mu.Unlock()
	c.Add(1)
}

func (u *utilisationTracker) IdleDuration(hash string) time.Duration {
	u.mu.Lock()
	defer u.mu.Unlock()
	if hs, ok := u.hashes[hash]; ok && !hs.lastActivity.IsZero() {
		return u.nowFunc().Sub(hs.lastActivity)
	}
	return time.Duration(math.MaxInt64)
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

// DialRates returns a per-module dial-graph snapshot: caller hash → target
// key → calls/sec, summed across the caller's functions. Placement
// scoring operates at module granularity so the graph is rolled up
// here.
func (u *utilisationTracker) DialRates() map[string]map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]map[string]float64)
	for ck, targets := range u.dial {
		for target, e := range targets {
			v := e.Value()
			if v <= rateReportFloor {
				continue
			}
			dst, ok := out[ck.Hash]
			if !ok {
				dst = make(map[string]float64)
				out[ck.Hash] = dst
			}
			dst[target] += v
		}
	}
	return out
}

// Clear removes all tracking state for a module (called on unseed). Drops
// every function's counters, EWMAs, and outbound dial graph. Inbound
// dials from other seeds are not cleared — they decay to zero through
// the rate filter.
func (u *utilisationTracker) Clear(hash string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	delete(u.hashes, hash)
	for ck := range u.dial {
		if ck.Hash == hash {
			delete(u.dial, ck)
		}
	}
	for ck := range u.dialCounters {
		if ck.Hash == hash {
			delete(u.dialCounters, ck)
		}
	}
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
			fm.invocationMsRate.Update(float64(fm.invocationMs.Swap(0)) / secs)
			fm.parkedMsRate.Update(float64(fm.parkedMs.Swap(0)) / secs)
			fm.sloSatisfiedRate.Update(float64(fm.sloSatisfied.Swap(0)) / secs)
			fm.sloBurnedRate.Update(float64(fm.sloBurned.Swap(0)) / secs)
		}
	}

	for ck, targets := range u.dialCounters {
		for target, c := range targets {
			r := float64(c.Swap(0)) / secs
			u.dial[ck][target].Update(r)
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
