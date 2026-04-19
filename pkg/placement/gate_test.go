// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func key(hash, fn string) callKey { return callKey{Hash: hash, Function: fn} }

func TestWorkloadGate_AcquireAndRelease(t *testing.T) {
	g := newWorkloadGate(2)
	ctx := context.Background()

	r1, err := g.acquire(ctx)
	require.NoError(t, err)
	r2, err := g.acquire(ctx)
	require.NoError(t, err)

	// Third acquire should block until r1 releases.
	done := make(chan struct{})
	go func() {
		r3, err := g.acquire(ctx)
		require.NoError(t, err)
		r3()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("third acquire should have blocked")
	case <-time.After(50 * time.Millisecond):
	}

	r1()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("third acquire should have unblocked after release")
	}
	r2()

	require.Greater(t, g.WaitEWMA(), time.Duration(0), "wait EWMA should reflect blocked acquire")
}

func TestWorkloadGate_RespectsContextCancellation(t *testing.T) {
	g := newWorkloadGate(1)

	r1, err := g.acquire(context.Background())
	require.NoError(t, err)
	defer r1()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err = g.acquire(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestGateRegistry_PerFunctionIsolation is the core starvation-fix
// regression: a slow function saturating its own gate must not block fast
// calls into a different function of the same module.
func TestGateRegistry_PerFunctionIsolation(t *testing.T) {
	const hash = "mod"
	r := newGateRegistry(4)
	r.SetHashSize(hash, 1) // tight cap so the slow fn saturates fast
	ctx := context.Background()

	slowRel, err := r.acquire(ctx, key(hash, "slow"))
	require.NoError(t, err)
	defer slowRel()

	// Slow fn's gate is saturated; a second slow acquire must block.
	saturated := make(chan struct{})
	go func() {
		blockCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		_, err := r.acquire(blockCtx, key(hash, "slow"))
		if err != nil {
			close(saturated)
		}
	}()

	// Fast fn on the same module gets its own gate, also cap=1 but
	// independent. One call in flight, another would queue — we just need
	// to prove the slow saturation doesn't leak into the fast queue.
	fastRel, err := r.acquire(ctx, key(hash, "fast"))
	require.NoError(t, err)
	fastRel()

	select {
	case <-saturated:
	case <-time.After(time.Second):
		t.Fatal("blocked slow-fn acquire should have failed by now")
	}
}

// TestGateRegistry_PerHashIsolation verifies that gates for different
// modules remain independent — a saturated module must not starve
// another module's gates.
func TestGateRegistry_PerHashIsolation(t *testing.T) {
	r := newGateRegistry(4)
	r.SetHashSize("sink", 1)
	ctx := context.Background()

	sinkRel, err := r.acquire(ctx, key("sink", "handle"))
	require.NoError(t, err)
	defer sinkRel()

	saturated := make(chan struct{})
	go func() {
		blockCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		_, err := r.acquire(blockCtx, key("sink", "handle"))
		if err != nil {
			close(saturated)
		}
	}()

	var ingestOK atomic.Int32
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			rel, err := r.acquire(ctx, key("ingest", "handle"))
			if err == nil {
				ingestOK.Add(1)
				rel()
			}
		})
	}
	wg.Wait()

	require.Equal(t, int32(4), ingestOK.Load(), "ingest should not be starved by sink saturation")

	select {
	case <-saturated:
	case <-time.After(time.Second):
		t.Fatal("blocked sink acquire should have failed by now")
	}
}

func TestWorkloadGate_GrowAdmitsMoreCallers(t *testing.T) {
	g := newWorkloadGate(2)
	ctx := context.Background()

	r1, err := g.acquire(ctx)
	require.NoError(t, err)
	r2, err := g.acquire(ctx)
	require.NoError(t, err)
	defer r1()
	defer r2()

	tightCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err = g.acquire(tightCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	g.SetCap(4)
	r3, err := g.acquire(ctx)
	require.NoError(t, err)
	defer r3()
	r4, err := g.acquire(ctx)
	require.NoError(t, err)
	defer r4()
}

func TestWorkloadGate_ShrinkDrainsAfterReleases(t *testing.T) {
	g := newWorkloadGate(4)
	ctx := context.Background()

	r1, err := g.acquire(ctx)
	require.NoError(t, err)
	r2, err := g.acquire(ctx)
	require.NoError(t, err)

	g.SetCap(1)

	r1()
	r2()

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		first, err := g.acquire(ctx)
		if err != nil {
			return false
		}
		defer first()

		ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel2()
		second, err := g.acquire(ctx2)
		if err == nil {
			second()
			return false
		}
		return errors.Is(err, context.DeadlineExceeded)
	}, time.Second, 10*time.Millisecond, "shrink should converge to cap=1 after releases")
}

// TestGateRegistry_SetHashSizeResizesAllFunctions verifies that a new
// per-module size is propagated to every already-created function gate.
func TestGateRegistry_SetHashSizeResizesAllFunctions(t *testing.T) {
	r := newGateRegistry(1)
	ctx := context.Background()

	rel, err := r.acquire(ctx, key("m", "f1"))
	require.NoError(t, err)
	rel()
	rel, err = r.acquire(ctx, key("m", "f2"))
	require.NoError(t, err)
	rel()

	r.SetHashSize("m", 4)

	// Both gates are cap=4 now; four parallel acquires succeed on each
	// without blocking.
	for _, fn := range []string{"f1", "f2"} {
		rels := make([]func(), 0, 4)
		for range 4 {
			rel, err := r.acquire(ctx, key("m", fn))
			require.NoError(t, err)
			rels = append(rels, rel)
		}
		for _, rel := range rels {
			rel()
		}
	}
}

func TestGateRegistry_ClearDropsGate(t *testing.T) {
	r := newGateRegistry(1)
	rel, err := r.acquire(context.Background(), key("abc", "handle"))
	require.NoError(t, err)
	rel()

	require.Contains(t, r.WaitEWMAs(), "abc")
	r.Clear("abc")
	require.NotContains(t, r.WaitEWMAs(), "abc")
}

// TestWorkloadGate_WaitEWMADecaysOnFastPath exercises the regression
// path the clean-room tidy targeted: once traffic stops blocking, the
// EWMA must decay back to zero so dashboards see "gate cleared" as a
// genuine nonzero→zero transition rather than pinning at the last
// blocked sample.
func TestWorkloadGate_WaitEWMADecaysOnFastPath(t *testing.T) {
	g := newWorkloadGate(1)
	ctx := context.Background()

	r1, err := g.acquire(ctx)
	require.NoError(t, err)
	var r2 func()
	done := make(chan struct{})
	go func() {
		var acqErr error
		r2, acqErr = g.acquire(ctx)
		require.NoError(t, acqErr)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	r1()
	<-done
	r2()

	blocked := g.WaitEWMA()
	require.Greater(t, blocked, time.Duration(0), "blocked admit must feed a non-zero EWMA sample")

	for range 50 {
		rel, err := g.acquire(ctx)
		require.NoError(t, err)
		rel()
	}
	require.Less(t, g.WaitEWMA(), blocked/10,
		"fast-path acquires must decay the EWMA after the gate clears")
}
