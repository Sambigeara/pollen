// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/stretchr/testify/require"
)

func TestBudget_ReserveAndRelease(t *testing.T) {
	b := newBudget(1000)

	require.True(t, b.Reserve("a", 400))
	require.True(t, b.Reserve("b", 600))
	require.False(t, b.Reserve("c", 1), "third reservation must be refused once total is at capacity")

	b.Release("a")
	require.True(t, b.Reserve("c", 400))
}

func TestBudget_RefusesAtLimit(t *testing.T) {
	b := newBudget(100)
	require.True(t, b.Reserve("a", 60))
	require.False(t, b.Reserve("b", 60))
	require.True(t, b.Reserve("b", 40))
}

func TestBudget_RepeatReserveIsNoop(t *testing.T) {
	b := newBudget(100)
	require.True(t, b.Reserve("a", 60))
	require.True(t, b.Reserve("a", 60), "repeat reservation for same hash must succeed without double-counting")
	require.True(t, b.Reserve("b", 40))
	require.False(t, b.Reserve("c", 1))
}

func TestBudget_ReleaseUnknownHashIsNoop(t *testing.T) {
	b := newBudget(100)
	require.True(t, b.Reserve("a", 50))
	b.Release("ghost")
	require.False(t, b.Reserve("b", 60))
}

func TestBudget_ZeroTotalDisablesGate(t *testing.T) {
	b := newBudget(0)
	require.True(t, b.Reserve("a", 1<<60))
	require.True(t, b.Reserve("b", 1<<60))

	release, ok := b.ReserveCall("a")
	require.True(t, ok, "ReserveCall must admit unconditionally when totalBytes <= 0")
	release()
}

func TestBudget_ConcurrentReserveRespectsLimit(t *testing.T) {
	const total = 1000
	const each = 10
	const goroutines = 200

	b := newBudget(total)
	var wg sync.WaitGroup
	var admitted, refused atomic.Int64

	for i := range goroutines {
		hash := string(rune('A' + i%26))
		hash += string(rune('a' + i/26))
		wg.Go(func() {
			if b.Reserve(hash, each) {
				admitted.Add(1)
			} else {
				refused.Add(1)
			}
		})
	}
	wg.Wait()

	require.Equal(t, int64(goroutines), admitted.Load()+refused.Load())
	require.LessOrEqual(t, admitted.Load(), int64(total/each), "admitted reservations must not exceed budget")
	require.Equal(t, admitted.Load()*each, b.reserved.Load())
}

func TestBudget_ReserveCall_UsesPerSpecCap(t *testing.T) {
	const specCap = int64(64 << 20)
	const callSlots = 4
	b := newBudget(replicaMemoryBytes(uint64(specCap)) + callSlots*specCap)
	require.True(t, b.Reserve("seed", replicaMemoryBytes(uint64(specCap))))

	releases := make([]func(), 0, callSlots)
	for range callSlots {
		release, ok := b.ReserveCall("seed")
		require.True(t, ok)
		releases = append(releases, release)
	}

	_, ok := b.ReserveCall("seed")
	require.False(t, ok, "callSlots+1th in-flight call must be refused once the call budget is exhausted")

	for _, r := range releases {
		r()
	}
}

func TestBudget_ReserveCall_FallsBackToDefault(t *testing.T) {
	b := newBudget(defaultReplicaMemoryBytes * 2)

	release, ok := b.ReserveCall("orphan")
	require.True(t, ok)
	require.Equal(t, defaultReplicaMemoryBytes, b.reserved.Load())
	release()
	require.Zero(t, b.reserved.Load(), "release must drop exactly the bytes the reservation took")
}

func TestBudget_ReserveCall_DoubleReleaseIsNoop(t *testing.T) {
	b := newBudget(defaultReplicaMemoryBytes * 2)
	release, ok := b.ReserveCall("seed")
	require.True(t, ok)
	release()
	release()
	require.Zero(t, b.reserved.Load())
}

func TestBudget_ReserveCall_ConcurrentRespectsLimit(t *testing.T) {
	const calls = 200
	const slots = wasm.IdleCacheSize * 2
	const specCap = int64(50)

	b := newBudget(int64(slots) * specCap)
	require.True(t, b.Reserve("seed", int64(wasm.IdleCacheSize)*specCap))

	var admitted atomic.Int64
	var releases sync.Map
	var wg sync.WaitGroup
	for i := range calls {
		wg.Go(func() {
			release, ok := b.ReserveCall("seed")
			if ok {
				admitted.Add(1)
				releases.Store(i, release)
			}
		})
	}
	wg.Wait()

	require.LessOrEqual(t, admitted.Load(), int64(slots-wasm.IdleCacheSize))
	require.Equal(t, int64(wasm.IdleCacheSize)*specCap+admitted.Load()*specCap, b.reserved.Load())

	releases.Range(func(_, value any) bool {
		value.(func())()
		return true
	})
	require.Equal(t, int64(wasm.IdleCacheSize)*specCap, b.reserved.Load())
}

func TestBudget_ReserveAndCall_ShareCeiling(t *testing.T) {
	const specCap = int64(64 << 20)
	totalBudget := replicaMemoryBytes(uint64(specCap)) + specCap
	b := newBudget(totalBudget)
	require.True(t, b.Reserve("seed", replicaMemoryBytes(uint64(specCap))))

	release, ok := b.ReserveCall("seed")
	require.True(t, ok, "first call must fit in the headroom above the replica reservation")
	_, ok = b.ReserveCall("seed")
	require.False(t, ok, "second call must be refused: replica + in-flight bytes have hit the ceiling")
	release()

	require.True(t, b.Reserve("seed-2", specCap), "freeing call headroom must reopen room for a small replica reservation against the same ceiling")
}

func TestBudget_ReplicaMemoryBytesPreReservesPool(t *testing.T) {
	require.Equal(t, int64(wasm.IdleCacheSize)*defaultReplicaMemoryBytes, replicaMemoryBytes(0))
	const specCap = uint64(128 << 20)
	require.Equal(t, int64(wasm.IdleCacheSize)*int64(specCap), replicaMemoryBytes(specCap))
}
