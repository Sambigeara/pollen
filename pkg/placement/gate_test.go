// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func key(hash, fn string) callKey { return callKey{Hash: hash, Function: fn} }

func TestWorkloadGate_AcquireAdmitsUpToCap(t *testing.T) {
	g := newWorkloadGate(2)

	r1, err := g.acquire()
	require.NoError(t, err)
	r2, err := g.acquire()
	require.NoError(t, err)

	// Third acquire rejects immediately — the gate is at capacity.
	_, err = g.acquire()
	require.ErrorIs(t, err, ErrAtCapacity)

	r1()
	r3, err := g.acquire()
	require.NoError(t, err, "release should free a slot for the next acquire")
	r3()
	r2()
}

// TestGateRegistry_PerFunctionIsolation: a slow function saturating its
// own gate must not reject fast calls into a different function of the
// same module.
func TestGateRegistry_PerFunctionIsolation(t *testing.T) {
	const hash = "mod"
	r := newGateRegistry(4)
	r.SetHashSize(hash, 1)

	slowRel, err := r.acquire(key(hash, "slow"))
	require.NoError(t, err)
	defer slowRel()

	_, err = r.acquire(key(hash, "slow"))
	require.ErrorIs(t, err, ErrAtCapacity)

	// Fast fn on the same module gets its own gate, also cap=1 but
	// independent. The slow saturation does not leak into the fast gate.
	fastRel, err := r.acquire(key(hash, "fast"))
	require.NoError(t, err)
	fastRel()
}

// TestGateRegistry_PerHashIsolation: gates for different modules
// remain independent — saturating one module must not starve another.
func TestGateRegistry_PerHashIsolation(t *testing.T) {
	r := newGateRegistry(4)
	r.SetHashSize("sink", 1)

	sinkRel, err := r.acquire(key("sink", "handle"))
	require.NoError(t, err)
	defer sinkRel()

	_, err = r.acquire(key("sink", "handle"))
	require.ErrorIs(t, err, ErrAtCapacity)

	var ingestOK atomic.Int32
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			rel, err := r.acquire(key("ingest", "handle"))
			if err == nil {
				ingestOK.Add(1)
				rel()
			}
		})
	}
	wg.Wait()
	require.Equal(t, int32(4), ingestOK.Load(), "ingest should not be starved by sink saturation")
}

func TestWorkloadGate_GrowAdmitsMoreCallers(t *testing.T) {
	g := newWorkloadGate(2)

	r1, err := g.acquire()
	require.NoError(t, err)
	r2, err := g.acquire()
	require.NoError(t, err)
	defer r1()
	defer r2()

	_, err = g.acquire()
	require.ErrorIs(t, err, ErrAtCapacity)

	g.SetCap(4)
	r3, err := g.acquire()
	require.NoError(t, err)
	defer r3()
	r4, err := g.acquire()
	require.NoError(t, err)
	defer r4()
}

// TestWorkloadGate_ShrinkRejectsWhenOverNewCap: shrinking the cap below
// current inflight rejects new acquires until releases bring inflight
// under the new ceiling.
func TestWorkloadGate_ShrinkRejectsWhenOverNewCap(t *testing.T) {
	g := newWorkloadGate(4)

	r1, err := g.acquire()
	require.NoError(t, err)
	r2, err := g.acquire()
	require.NoError(t, err)

	g.SetCap(1)

	_, err = g.acquire()
	require.ErrorIs(t, err, ErrAtCapacity)

	r1()
	_, err = g.acquire()
	require.ErrorIs(t, err, ErrAtCapacity)

	r2()
	r3, err := g.acquire()
	require.NoError(t, err)
	r3()
}

// TestGateRegistry_SetHashSizeResizesAllFunctions: a new per-module size
// is propagated to every already-created function gate.
func TestGateRegistry_SetHashSizeResizesAllFunctions(t *testing.T) {
	r := newGateRegistry(1)

	rel, err := r.acquire(key("m", "f1"))
	require.NoError(t, err)
	rel()
	rel, err = r.acquire(key("m", "f2"))
	require.NoError(t, err)
	rel()

	r.SetHashSize("m", 4)

	for _, fn := range []string{"f1", "f2"} {
		rels := make([]func(), 0, 4)
		for range 4 {
			rel, err := r.acquire(key("m", fn))
			require.NoError(t, err)
			rels = append(rels, rel)
		}
		_, err = r.acquire(key("m", fn))
		require.ErrorIs(t, err, ErrAtCapacity)
		for _, rel := range rels {
			rel()
		}
	}
}

func TestGateRegistry_ClearDropsGate(t *testing.T) {
	r := newGateRegistry(1)
	r.SetHashSize("abc", 4)

	rel, err := r.acquire(key("abc", "handle"))
	require.NoError(t, err)
	rel()

	r.mu.Lock()
	_, exists := r.gates[key("abc", "handle")]
	_, sized := r.hashSizes["abc"]
	r.mu.Unlock()
	require.True(t, exists, "gate should exist before Clear")
	require.True(t, sized, "hash size should be recorded before Clear")

	r.Clear("abc")

	r.mu.Lock()
	_, exists = r.gates[key("abc", "handle")]
	_, sized = r.hashSizes["abc"]
	r.mu.Unlock()
	require.False(t, exists, "gate should be dropped after Clear")
	require.False(t, sized, "hash size should be dropped after Clear")
}
