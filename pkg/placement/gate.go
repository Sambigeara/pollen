package placement

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const gateWaitAlpha = 0.2

// callKey identifies an admission unit: a specific exported function within
// a WASM module. The module hash is the replication unit (placement, CAS,
// warm instance); the function is the cost unit (admission, latency, SLO).
// Per-function gating stops a slow function in a module from head-of-line-
// blocking fast functions that share the same binary.
type callKey struct {
	Hash     string
	Function string
}

// workloadGate bounds in-flight invocations for a single (hash, function) on
// this node. Acquire blocks when in-flight count meets the cap; callers
// honour their context deadline rather than a global wait budget. Wait
// time is recorded into an EWMA so placement can observe per-function
// gate wait independently of any other function's behaviour.
//
// The gate is backed by an explicit cap + inflight counter under a
// mutex, with a 1-buffered notify channel waking blocked acquirers when
// capacity changes. SetCap is atomic and immediately reflected — no
// resize coordination, no background drainer goroutines.
type workloadGate struct {
	notify   chan struct{}
	cap      int
	inflight int
	waitNs   atomic.Int64
	mu       sync.Mutex
}

func newWorkloadGate(initial int) *workloadGate {
	if initial < 1 {
		initial = 1
	}
	return &workloadGate{
		notify: make(chan struct{}, 1),
		cap:    initial,
	}
}

// SetCap adjusts the gate's concurrency ceiling. Honoured immediately:
// growing wakes any waiters; shrinking simply means subsequent acquires
// block sooner. In-flight calls are unaffected — releases just bring
// the inflight count down past the new (lower) cap before the next
// acquire admits anyone.
func (g *workloadGate) SetCap(n int) {
	if n < 1 {
		n = 1
	}
	g.mu.Lock()
	g.cap = n
	g.mu.Unlock()
	g.signal()
}

// acquire returns a release function that must be called when the
// invocation completes. If the gate is full, it blocks until either a
// slot frees, the cap grows, or ctx cancels. Both blocked and fast-path
// admits feed the wait EWMA so the signal decays to zero when a
// previously-saturated gate clears — dashboards see "wait cleared" as
// a material nonzero→zero transition rather than a pinned last-
// nonzero sample.
func (g *workloadGate) acquire(ctx context.Context) (release func(), err error) {
	if g.tryAcquire() {
		g.recordWait(0)
		return g.release, nil
	}

	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-g.notify:
			if g.tryAcquire() {
				g.signal()
				g.recordWait(time.Since(start))
				return g.release, nil
			}
		}
	}
}

func (g *workloadGate) tryAcquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inflight < g.cap {
		g.inflight++
		return true
	}
	return false
}

func (g *workloadGate) release() {
	g.mu.Lock()
	g.inflight--
	g.mu.Unlock()
	g.signal()
}

// signal wakes one waiter. The notify channel is 1-buffered so a signal
// while a waiter is mid-cycle isn't lost; acquire's loop also re-signals
// after success so a single capacity-freeing event can cascade through
// multiple waiters when the cap allows it.
func (g *workloadGate) signal() {
	select {
	case g.notify <- struct{}{}:
	default:
	}
}

func (g *workloadGate) recordWait(d time.Duration) {
	sample := float64(d.Nanoseconds())
	for {
		old := g.waitNs.Load()
		next := int64(float64(old)*(1-gateWaitAlpha) + sample*gateWaitAlpha)
		if g.waitNs.CompareAndSwap(old, next) {
			return
		}
	}
}

// WaitEWMA returns the current EWMA of admitted wait times. Zero when the
// gate has never blocked.
func (g *workloadGate) WaitEWMA() time.Duration {
	return time.Duration(g.waitNs.Load())
}

// gateRegistry holds per-(hash, function) workloadGates. Each module hash
// has a single size target — set by the reconciler from cluster-wide
// signals — and every function of that module gets its own gate sized
// to that target. New gates materialise on first acquire, so we don't
// need to pre-enumerate a module's exported functions.
type gateRegistry struct {
	gates       map[callKey]*workloadGate
	hashSizes   map[string]int
	defaultSize int
	mu          sync.Mutex
}

func newGateRegistry(defaultSize int) *gateRegistry {
	if defaultSize < 1 {
		defaultSize = 1
	}
	return &gateRegistry{
		gates:       make(map[callKey]*workloadGate),
		hashSizes:   make(map[string]int),
		defaultSize: defaultSize,
	}
}

func (r *gateRegistry) acquire(ctx context.Context, key callKey) (func(), error) {
	r.mu.Lock()
	g, ok := r.gates[key]
	if !ok {
		size, hasSize := r.hashSizes[key.Hash]
		if !hasSize {
			size = r.defaultSize
		}
		g = newWorkloadGate(size)
		r.gates[key] = g
	}
	r.mu.Unlock()
	return g.acquire(ctx)
}

// SetHashSize records a per-module concurrency target and resizes every
// existing function gate for that module. Functions not yet observed
// inherit the stored size when their gate is first created.
func (r *gateRegistry) SetHashSize(hash string, n int) {
	if n < 1 {
		n = 1
	}
	r.mu.Lock()
	r.hashSizes[hash] = n
	affected := make([]*workloadGate, 0)
	for key, g := range r.gates {
		if key.Hash == hash {
			affected = append(affected, g)
		}
	}
	r.mu.Unlock()
	for _, g := range affected {
		g.SetCap(n)
	}
}

// WaitEWMAs returns a per-hash snapshot of gate-wait EWMAs. Aggregated as
// the max across functions so SeedMetrics surfaces the worst pain any
// function of the module is experiencing.
func (r *gateRegistry) WaitEWMAs() map[string]time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]time.Duration)
	for key, g := range r.gates {
		w := g.WaitEWMA()
		if existing, ok := out[key.Hash]; !ok || w > existing {
			out[key.Hash] = w
		}
	}
	return out
}

// Clear drops every gate belonging to the given module hash (called on
// unseed). Pending acquirers exit via their ctx.
func (r *gateRegistry) Clear(hash string) {
	r.mu.Lock()
	for key := range r.gates {
		if key.Hash == hash {
			delete(r.gates, key)
		}
	}
	delete(r.hashSizes, hash)
	r.mu.Unlock()
}
