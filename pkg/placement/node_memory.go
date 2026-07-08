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
	// nodeMemoryFraction caps allocated wasm memory at this share of
	// the host's total RAM. Predictive admission: a reservation that
	// would push the running total past this ceiling is refused before
	// the kernel crosses its own limit.
	nodeMemoryFraction = 0.7

	defaultReplicaMemoryBytes int64 = 8 << 20
)

// nodeMemoryGuard is node-level OOM protection: it rejects wasm memory
// reservations once this host's running total would exceed totalBytes
// (a fraction of physical RAM). It is deliberately distinct from the
// per-Principal identity.Budget, which is a cluster-wide fairness quota
// enforced at admission; this guard protects one machine from
// over-commit regardless of which principals own the workloads. Two
// reservation paths share one accumulator:
//
//   - Replica reservations cover the warm-pool worst case
//     (IdleCacheSize × per-spec-cap) and live for the lifetime of the
//     local claim;
//   - Call reservations cover one in-flight invocation each (per-spec-cap)
//     and are released on completion.
//
// totalBytes <= 0 disables the guard.
type nodeMemoryGuard struct {
	holdings   map[string]int64
	caps       map[string]int64
	mu         sync.Mutex
	totalBytes int64
	reserved   atomic.Int64
}

func newNodeMemoryGuard(totalBytes int64) *nodeMemoryGuard {
	return &nodeMemoryGuard{
		totalBytes: totalBytes,
		holdings:   make(map[string]int64),
		caps:       make(map[string]int64),
	}
}

func detectNodeMemoryCeiling() int64 {
	vm, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	return int64(float64(vm.Total) * nodeMemoryFraction)
}

// Reserve is idempotent per hash: replays must not double-count.
func (b *nodeMemoryGuard) Reserve(hash string, bytes int64) bool {
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

func (b *nodeMemoryGuard) Release(hash string) {
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

func (b *nodeMemoryGuard) ReserveCall(hash string) (func(), bool) {
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

func (b *nodeMemoryGuard) tryAdd(bytes int64) bool {
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

// replicaMemoryBytes pre-reserves IdleCacheSize × per-spec-cap at
// claim time so the call path accounts for one in-flight invocation
// without double-counting active vs idle instances.
func replicaMemoryBytes(specBytes uint64) int64 {
	specCap := defaultReplicaMemoryBytes
	if specBytes != 0 {
		specCap = int64(specBytes) //nolint:gosec
	}
	return int64(wasm.IdleCacheSize) * specCap
}
