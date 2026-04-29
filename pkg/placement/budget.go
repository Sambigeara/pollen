// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"sync"
	"sync/atomic"

	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/shirou/gopsutil/v4/mem"
)

const (
	// memoryBudgetFraction caps allocated wasm memory at this share of
	// the host's total RAM. Predictive admission: a reservation that
	// would push the running total past this ceiling is refused before
	// the kernel crosses its own limit.
	memoryBudgetFraction = 0.7

	defaultReplicaMemoryBytes int64 = 8 << 20
)

// budget rejects reservations once the running total would exceed
// totalBytes. Two reservation paths share one accumulator:
//
//   - Replica reservations cover the warm-pool worst case
//     (IdleCacheSize × per-spec-cap) and live for the lifetime of the
//     local claim;
//   - Call reservations cover one in-flight invocation each (per-spec-cap)
//     and are released on completion.
//
// totalBytes <= 0 disables the gate.
type budget struct {
	holdings   map[string]int64
	caps       map[string]int64
	mu         sync.Mutex
	totalBytes int64
	reserved   atomic.Int64
}

func newBudget(totalBytes int64) *budget {
	return &budget{
		totalBytes: totalBytes,
		holdings:   make(map[string]int64),
		caps:       make(map[string]int64),
	}
}

// detectMemoryBudget returns the host-derived budget ceiling. Any
// failure to read total memory yields zero, which the caller treats as
// "no budget" — admission gating is then a no-op.
func detectMemoryBudget() int64 {
	vm, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	return int64(float64(vm.Total) * memoryBudgetFraction)
}

// Reserve records a replica's warm-pool footprint against the budget
// and stashes the per-call cap so ReserveCall can size individual
// invocations. The per-call cap is recovered as bytes/IdleCacheSize —
// the inverse of replicaMemoryBytes. A repeat reservation for the same
// hash is a no-op success — replays must not double-count.
func (b *budget) Reserve(hash string, bytes int64) bool {
	if b.totalBytes <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, held := b.holdings[hash]; held {
		return true
	}
	if !b.tryAdd(bytes) {
		return false
	}
	b.holdings[hash] = bytes
	b.caps[hash] = bytes / int64(wasm.IdleCacheSize)
	return true
}

// Release drops the reservation tied to hash. Releasing an unknown
// hash is a no-op so the caller doesn't have to track which seed paths
// were admitted.
func (b *budget) Release(hash string) {
	if b.totalBytes <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	bytes, held := b.holdings[hash]
	if !held {
		return
	}
	delete(b.holdings, hash)
	delete(b.caps, hash)
	b.reserved.Add(-bytes)
}

// ReserveCall reserves a per-call slice of the budget for an in-flight
// invocation of hash. The returned closure releases the slice — calling
// it more than once is safe but only the first call decrements. A
// totalBytes <= 0 budget admits unconditionally with a no-op release.
func (b *budget) ReserveCall(hash string) (func(), bool) {
	if b.totalBytes <= 0 {
		return func() {}, true
	}

	b.mu.Lock()
	bytes, ok := b.caps[hash]
	b.mu.Unlock()
	if !ok {
		bytes = defaultReplicaMemoryBytes
	}

	if !b.tryAdd(bytes) {
		return nil, false
	}

	var released atomic.Bool
	return func() {
		if released.CompareAndSwap(false, true) {
			b.reserved.Add(-bytes)
		}
	}, true
}

// tryAdd CAS-loops the reserved counter, refusing the addition when it
// would breach totalBytes.
func (b *budget) tryAdd(bytes int64) bool {
	for {
		cur := b.reserved.Load()
		next := cur + bytes
		if next > b.totalBytes {
			return false
		}
		if b.reserved.CompareAndSwap(cur, next) {
			return true
		}
	}
}

// replicaMemoryBytes is the warm-pool worst case the replica path
// reserves: IdleCacheSize × per-spec-cap. Pre-reserving the pool
// footprint at claim time lets the call-level path account for one
// in-flight invocation per call without double-counting active vs
// idle instances.
func replicaMemoryBytes(specBytes uint64) int64 {
	specCap := defaultReplicaMemoryBytes
	if specBytes != 0 {
		specCap = int64(specBytes) //nolint:gosec
	}
	return int64(wasm.IdleCacheSize) * specCap
}
