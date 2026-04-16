package placement

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const gateWaitAlpha = 0.2

// workloadGate bounds in-flight invocations for a single workload on this
// node. Acquire blocks when in-flight count meets the cap; callers
// honour their context deadline rather than a global wait budget. Wait
// time is recorded into an EWMA so placement can observe per-workload
// gate wait independently of any other workload's behaviour.
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

// gateRegistry holds per-hash workloadGates. New gates are sized via the
// sizer callback (typically a small, conservative default); the reconciler
// then resizes them per-tick from observed dial behaviour via SetSize.
type gateRegistry struct {
	gates map[string]*workloadGate
	sizer func(hash string) int
	mu    sync.Mutex
}

func newGateRegistry(sizer func(string) int) *gateRegistry {
	return &gateRegistry{
		gates: make(map[string]*workloadGate),
		sizer: sizer,
	}
}

func (r *gateRegistry) acquire(ctx context.Context, hash string) (func(), error) {
	r.mu.Lock()
	g, ok := r.gates[hash]
	if !ok {
		g = newWorkloadGate(r.sizer(hash))
		r.gates[hash] = g
	}
	r.mu.Unlock()
	return g.acquire(ctx)
}

// SetSize adjusts a workload's gate cap, creating the gate at the new
// size if it doesn't exist yet.
func (r *gateRegistry) SetSize(hash string, n int) {
	r.mu.Lock()
	g, ok := r.gates[hash]
	if !ok {
		g = newWorkloadGate(n)
		r.gates[hash] = g
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	g.SetCap(n)
}

// WaitEWMAs returns a snapshot of per-hash gate wait times. Used by the
// supervisor's Prometheus collector to emit per-workload signals.
func (r *gateRegistry) WaitEWMAs() map[string]time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]time.Duration, len(r.gates))
	for hash, g := range r.gates {
		out[hash] = g.WaitEWMA()
	}
	return out
}

// Clear drops a workload's gate (called on unseed). Pending acquirers
// will eventually exit via their ctx.
func (r *gateRegistry) Clear(hash string) {
	r.mu.Lock()
	delete(r.gates, hash)
	r.mu.Unlock()
}
