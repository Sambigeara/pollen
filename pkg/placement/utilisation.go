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
)

type hashCounters struct {
	demand atomic.Uint64
	served atomic.Uint64
}

type utilisationTracker struct {
	demand       map[string]*metrics.EWMA
	served       map[string]*metrics.EWMA
	counters     map[string]*hashCounters
	lastActivity map[string]time.Time
	nowFunc      func() time.Time
	mu           sync.Mutex
}

func newUtilisationTracker() *utilisationTracker {
	return &utilisationTracker{
		demand:       make(map[string]*metrics.EWMA),
		served:       make(map[string]*metrics.EWMA),
		counters:     make(map[string]*hashCounters),
		lastActivity: make(map[string]time.Time),
		nowFunc:      time.Now,
	}
}

func (u *utilisationTracker) ensureHash(hash string) {
	if _, ok := u.demand[hash]; !ok {
		u.demand[hash] = metrics.NewEWMA(utilisationAlpha, 0)
		u.served[hash] = metrics.NewEWMA(utilisationAlpha, 0)
		u.counters[hash] = &hashCounters{}
	}
}

// MarkActive stamps lastActivity without incrementing any rate counter.
// Used when a seed is first claimed so the idle-release gate starts from
// claim time rather than treating a fresh claimant as infinitely idle.
func (u *utilisationTracker) MarkActive(hash string) {
	u.mu.Lock()
	u.ensureHash(hash)
	u.lastActivity[hash] = u.nowFunc()
	u.mu.Unlock()
}

func (u *utilisationTracker) RecordDemand(hash string) {
	u.mu.Lock()
	u.ensureHash(hash)
	c := u.counters[hash]
	u.lastActivity[hash] = u.nowFunc()
	u.mu.Unlock()
	c.demand.Add(1)
}

func (u *utilisationTracker) RecordServed(hash string) {
	u.mu.Lock()
	u.ensureHash(hash)
	c := u.counters[hash]
	u.lastActivity[hash] = u.nowFunc()
	u.mu.Unlock()
	c.served.Add(1)
}

func (u *utilisationTracker) DemandRate(hash string) float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	if e, ok := u.demand[hash]; ok {
		return e.Value()
	}
	return 0
}

func (u *utilisationTracker) ServedRate(hash string) float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	if e, ok := u.served[hash]; ok {
		return e.Value()
	}
	return 0
}

func (u *utilisationTracker) IdleDuration(hash string) time.Duration {
	u.mu.Lock()
	defer u.mu.Unlock()
	if t, ok := u.lastActivity[hash]; ok {
		return u.nowFunc().Sub(t)
	}
	return time.Duration(math.MaxInt64)
}

// ServedRates returns a snapshot of all hashes with non-zero served rates.
func (u *utilisationTracker) ServedRates() map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]float64)
	for hash, e := range u.served {
		v := e.Value()
		if v > 0.01 { //nolint:mnd
			out[hash] = v
		}
	}
	return out
}

// DemandRates returns a snapshot of all hashes with non-zero demand rates.
func (u *utilisationTracker) DemandRates() map[string]float64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make(map[string]float64)
	for hash, e := range u.demand {
		v := e.Value()
		if v > 0.01 { //nolint:mnd
			out[hash] = v
		}
	}
	return out
}

// Clear removes all tracking state for a hash (called on unseed).
func (u *utilisationTracker) Clear(hash string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	delete(u.demand, hash)
	delete(u.served, hash)
	delete(u.counters, hash)
	delete(u.lastActivity, hash)
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

	for hash, c := range u.counters {
		d := float64(c.demand.Swap(0)) / secs
		s := float64(c.served.Swap(0)) / secs
		u.demand[hash].Update(d)
		u.served[hash].Update(s)
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
