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

// hashMetrics consolidates the per-seed counter + EWMA state that used
// to live in five parallel maps. Pointer-backed everywhere because the
// struct holds atomic.Uint64 fields, which are not copyable. Consolidation
// is structural dedupe only — the tick loop still swaps each counter
// independently and updates each EWMA; there's no cross-counter
// consistency model beyond the coarse one the old design had.
type hashMetrics struct {
	lastActivity     time.Time
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

func newHashMetrics() *hashMetrics {
	return &hashMetrics{
		servedRate:       metrics.NewEWMA(utilisationAlpha, 0),
		invocationMsRate: metrics.NewEWMA(utilisationAlpha, 0),
		parkedMsRate:     metrics.NewEWMA(utilisationAlpha, 0),
		sloSatisfiedRate: metrics.NewEWMA(utilisationAlpha, 0),
		sloBurnedRate:    metrics.NewEWMA(utilisationAlpha, 0),
	}
}

type utilisationTracker struct {
	metrics      map[string]*hashMetrics
	dial         map[string]map[string]*metrics.EWMA
	dialCounters map[string]map[string]*atomic.Uint64
	sloLookup    func(hash string) time.Duration
	nowFunc      func() time.Time
	mu           sync.Mutex
}

func newUtilisationTracker() *utilisationTracker {
	return &utilisationTracker{
		metrics:      make(map[string]*hashMetrics),
		dial:         make(map[string]map[string]*metrics.EWMA),
		dialCounters: make(map[string]map[string]*atomic.Uint64),
		sloLookup:    defaultSLOLookup,
		nowFunc:      time.Now,
	}
}

func (u *utilisationTracker) ensure(hash string) *hashMetrics {
	m, ok := u.metrics[hash]
	if !ok {
		m = newHashMetrics()
		u.metrics[hash] = m
	}
	return m
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

// RecordSLO classifies a completed invocation against the workload's
// latency SLO using the caller-perspective elapsed time.
func (u *utilisationTracker) RecordSLO(hash string, elapsed time.Duration) {
	u.mu.Lock()
	slo := u.sloLookup(hash)
	if slo <= 0 {
		slo = defaultLatencySLO
	}
	m := u.ensure(hash)
	u.mu.Unlock()

	if elapsed > slo {
		m.sloBurned.Add(1)
	} else {
		m.sloSatisfied.Add(1)
	}
}

// SLOBurnRate returns the per-second satisfied/burned rates and the
// derived burn ratio. Burn ratio is undefined when there are no
// observations; callers receive a sentinel below rateReportFloor in
// either rate to detect that case.
func (u *utilisationTracker) SLOBurnRate(hash string) (satisfied, burned, burnRatio float64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	m, ok := u.metrics[hash]
	if !ok {
		return 0, 0, 0
	}
	satisfied = m.sloSatisfiedRate.Value()
	burned = m.sloBurnedRate.Value()
	if total := satisfied + burned; total > rateReportFloor {
		burnRatio = burned / total
	}
	return satisfied, burned, burnRatio
}

// MarkActive stamps lastActivity without incrementing any rate counter.
// Used when a seed is first claimed so the idle-release gate starts from
// claim time rather than treating a fresh claimant as infinitely idle.
func (u *utilisationTracker) MarkActive(hash string) {
	u.mu.Lock()
	m := u.ensure(hash)
	m.lastActivity = u.nowFunc()
	u.mu.Unlock()
}

func (u *utilisationTracker) RecordServed(hash string) {
	u.mu.Lock()
	m := u.ensure(hash)
	m.lastActivity = u.nowFunc()
	u.mu.Unlock()
	m.servedCount.Add(1)
}

// RecordInvocation accumulates wall-time (ms) into a counter that the tick
// loop converts into a ms/sec rate. Paired with the served rate this
// yields a mean cost per invocation, decaying to zero when the seed
// idles.
func (u *utilisationTracker) RecordInvocation(hash string, elapsed time.Duration) {
	u.mu.Lock()
	m := u.ensure(hash)
	u.mu.Unlock()
	m.invocationMs.Add(uint64(elapsed / time.Millisecond))
}

// RecordParkedTime accumulates wall-time (ms) that this invocation of
// hash spent blocked inside pollen_request waiting for downstream
// responses. Summed over the tick window and paired with the served
// rate to produce a mean "parked per invocation" figure the reconciler
// uses for adaptive gate sizing.
func (u *utilisationTracker) RecordParkedTime(hash string, elapsed time.Duration) {
	u.mu.Lock()
	m := u.ensure(hash)
	u.mu.Unlock()
	m.parkedMs.Add(uint64(elapsed / time.Millisecond))
}

// RecordDial increments the per-(caller, target) dial counter. Counters are
// flushed into rate EWMAs each tick.
func (u *utilisationTracker) RecordDial(callerHash, targetKey string) {
	u.mu.Lock()
	targets, ok := u.dialCounters[callerHash]
	if !ok {
		targets = make(map[string]*atomic.Uint64)
		u.dialCounters[callerHash] = targets
	}
	c, ok := targets[targetKey]
	if !ok {
		c = &atomic.Uint64{}
		targets[targetKey] = c
	}
	if _, ok := u.dial[callerHash]; !ok {
		u.dial[callerHash] = make(map[string]*metrics.EWMA)
	}
	if _, ok := u.dial[callerHash][targetKey]; !ok {
		u.dial[callerHash][targetKey] = metrics.NewEWMA(utilisationAlpha, 0)
	}
	u.mu.Unlock()
	c.Add(1)
}

func (u *utilisationTracker) IdleDuration(hash string) time.Duration {
	u.mu.Lock()
	defer u.mu.Unlock()
	if m, ok := u.metrics[hash]; ok && !m.lastActivity.IsZero() {
		return u.nowFunc().Sub(m.lastActivity)
	}
	return time.Duration(math.MaxInt64)
}

// SLORates returns per-hash satisfied and burned rates (calls/sec) for
// every workload with recent caller-perspective observations. Suitable
// for gossip: each node publishes its own local view, and the
// supervisor Prometheus collector aggregates cluster-wide.
func (u *utilisationTracker) SLORates() (satisfied, burned map[string]float64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	satisfied = make(map[string]float64)
	burned = make(map[string]float64)
	for hash, m := range u.metrics {
		if v := m.sloSatisfiedRate.Value(); v > rateReportFloor {
			satisfied[hash] = v
		}
		if v := m.sloBurnedRate.Value(); v > rateReportFloor {
			burned[hash] = v
		}
	}
	return satisfied, burned
}

// ServedRates returns a snapshot of all hashes with non-zero served rates.
func (u *utilisationTracker) ServedRates() map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]float64)
	for hash, m := range u.metrics {
		if v := m.servedRate.Value(); v > rateReportFloor {
			out[hash] = v
		}
	}
	return out
}

// InvocationCosts returns the per-seed mean wall-time (ms) per invocation,
// computed as (ms/sec rate) / (calls/sec rate). Both sides decay to zero
// when the seed goes idle, so the map omits entries whose served rate has
// fallen below rateReportFloor — keeping stale costs out of snapshots.
func (u *utilisationTracker) InvocationCosts() map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]float64, len(u.metrics))
	for hash, m := range u.metrics {
		servedRate := m.servedRate.Value()
		if servedRate <= rateReportFloor {
			continue
		}
		if cost := m.invocationMsRate.Value() / servedRate; cost > 0 {
			out[hash] = cost
		}
	}
	return out
}

// ParkedTimes mirrors InvocationCosts but for the time an invocation
// spends parked inside pollen_request. The reconciler combines this
// with InvocationCosts to derive active (CPU) time per invocation
// and size workload gates adaptively.
func (u *utilisationTracker) ParkedTimes() map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]float64, len(u.metrics))
	for hash, m := range u.metrics {
		servedRate := m.servedRate.Value()
		if servedRate <= rateReportFloor {
			continue
		}
		if parked := m.parkedMsRate.Value() / servedRate; parked > 0 {
			out[hash] = parked
		}
	}
	return out
}

// DialRates returns the per-seed dial-graph snapshot: caller hash → target key
// → calls/sec EWMA.
func (u *utilisationTracker) DialRates() map[string]map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]map[string]float64, len(u.dial))
	for caller, targets := range u.dial {
		dst := make(map[string]float64, len(targets))
		for target, e := range targets {
			v := e.Value()
			if v > rateReportFloor {
				dst[target] = v
			}
		}
		if len(dst) > 0 {
			out[caller] = dst
		}
	}
	return out
}

// Clear removes all tracking state for a hash (called on unseed). Drops
// counters, EWMAs, and the seed's outbound dial graph. Inbound dials from
// other seeds (where this hash appears as a target) are not cleared — they
// will decay to zero through the rate filter.
func (u *utilisationTracker) Clear(hash string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	delete(u.metrics, hash)
	delete(u.dial, hash)
	delete(u.dialCounters, hash)
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

	for _, m := range u.metrics {
		m.servedRate.Update(float64(m.servedCount.Swap(0)) / secs)
		m.invocationMsRate.Update(float64(m.invocationMs.Swap(0)) / secs)
		m.parkedMsRate.Update(float64(m.parkedMs.Swap(0)) / secs)
		m.sloSatisfiedRate.Update(float64(m.sloSatisfied.Swap(0)) / secs)
		m.sloBurnedRate.Update(float64(m.sloBurned.Swap(0)) / secs)
	}

	for caller, targets := range u.dialCounters {
		for target, c := range targets {
			r := float64(c.Swap(0)) / secs
			u.dial[caller][target].Update(r)
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
