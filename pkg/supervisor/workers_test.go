package supervisor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerPool_BoundsConcurrency(t *testing.T) {
	pool := newWorkerPool(2)
	ctx := t.Context()

	go pool.run(ctx)

	var running atomic.Int32
	var peak atomic.Int32
	done := make(chan struct{})

	for range 10 {
		pool.submit(ctx, func() {
			cur := running.Add(1)
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			running.Add(-1)
		})
	}

	go func() {
		pool.wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pool to drain")
	}

	require.LessOrEqual(t, int(peak.Load()), 2)
	require.Equal(t, int32(0), running.Load())
}

func TestWorkerPool_ShutdownDrainsWork(t *testing.T) {
	pool := newWorkerPool(2)
	ctx, cancel := context.WithCancel(context.Background())

	go pool.run(ctx)

	var completed atomic.Int32

	for range 5 {
		pool.submit(ctx, func() {
			time.Sleep(10 * time.Millisecond)
			completed.Add(1)
		})
	}

	// Give time for tasks to be submitted and start processing.
	time.Sleep(20 * time.Millisecond)
	cancel()

	pool.wait()
	require.Equal(t, int32(5), completed.Load())
}

func TestWorkerPool_SubmitRespectsContext(t *testing.T) {
	pool := newWorkerPool(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		pool.submit(ctx, func() {
			t.Error("should not execute")
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("submit blocked on cancelled context")
	}
}
