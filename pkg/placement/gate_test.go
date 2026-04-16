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

func TestGateRegistry_PerHashIsolation(t *testing.T) {
	// Sink-style workload (size 1) saturates without affecting ingest's gate.
	sizer := func(hash string) int {
		if hash == "sink" {
			return 1
		}
		return 4
	}
	r := newGateRegistry(sizer)
	ctx := context.Background()

	// Saturate sink.
	sinkRel, err := r.acquire(ctx, "sink")
	require.NoError(t, err)
	defer sinkRel()

	// Sink is full — second sink acquire should block.
	saturated := make(chan struct{})
	go func() {
		blockCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		_, err := r.acquire(blockCtx, "sink")
		if err != nil {
			close(saturated)
		}
	}()

	// Meanwhile ingest is unaffected — multiple acquires succeed instantly.
	var ingestOK atomic.Int32
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			rel, err := r.acquire(ctx, "ingest")
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

	// Saturate at the original cap.
	r1, err := g.acquire(ctx)
	require.NoError(t, err)
	r2, err := g.acquire(ctx)
	require.NoError(t, err)
	defer r1()
	defer r2()

	// Without growth, a third would block.
	tightCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err = g.acquire(tightCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Grow and the next pair succeeds immediately.
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

	// Shrink while two slots are in flight; the surplus drains as
	// callers release, not before.
	g.SetCap(1)

	// Even after Cap reports 1, releases free their tokens — but the
	// drain goroutine consumes the surplus, so net available stays at 0
	// until the in-flight count drops below the new cap.
	r1()
	r2()

	require.Eventually(t, func() bool {
		// Once drain finishes, exactly one acquire should succeed.
		ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		first, err := g.acquire(ctx)
		if err != nil {
			return false
		}
		defer first()

		// And a second one should block. Release immediately if it
		// somehow succeeds, otherwise we leak an inflight slot and
		// break the accounting for future iterations.
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

func TestGateRegistry_ClearDropsGate(t *testing.T) {
	r := newGateRegistry(func(string) int { return 1 })
	rel, err := r.acquire(context.Background(), "abc")
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

	// First pair: force a blocked admit so the EWMA picks up a
	// non-trivial sample.
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

	// Now run a stream of fast-path acquires (no contention) and
	// verify the EWMA decays toward zero. Each sample pulls the
	// average closer to 0 with alpha=0.2, so a few dozen iterations
	// is plenty.
	for range 50 {
		rel, err := g.acquire(ctx)
		require.NoError(t, err)
		rel()
	}
	require.Less(t, g.WaitEWMA(), blocked/10,
		"fast-path acquires must decay the EWMA after the gate clears")
}
