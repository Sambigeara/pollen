// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"errors"
	"sync/atomic"
)

// ErrOverloaded is returned when admitting another invocation would push
// the node's estimated memory usage past its hard limit. Distinct from
// ErrAlreadyRunning / ErrNotRunning: overload is node-wide and retryable
// on a different claimant, so callers fall through to the next replica
// rather than surfacing the failure to the user.
var ErrOverloaded = errors.New("placement: node memory overloaded")

const (
	// defaultMemBudgetPercent is the inferred ceiling when no operator
	// budget is configured. 80% leaves headroom for the OS, kernel
	// caches, and other tenants on the box.
	defaultMemBudgetPercent uint32 = 80

	// defaultCPUBudgetPercent matches the "no constraint" value every
	// CPU-aware code path already falls back to (scoring.cpuCapacity,
	// scoring.hasCapacity, service.admissionState). Existing as a
	// constant means New() can normalise the gossiped value to the
	// effective one rather than letting raw 0 leak across the wire.
	defaultCPUBudgetPercent uint32 = 100

	// defaultCallMemoryBytes is used when a workload spec doesn't
	// declare MemoryBytes. Optimistic estimate of per-call cost — RSS
	// sampling (every 5s) catches under-estimation, so admission can run
	// looser than the wasm runtime's worst-case ceiling.
	defaultCallMemoryBytes uint64 = 4 << 20
)

// memoryAdmission is a node-global memory ceiling enforced at workload
// admission. The hot path is two atomic loads plus a CAS; sampling
// (process RSS, system total) is amortised into the existing 5-second
// resource ticker, never on the request path.
//
// Disabled (admit always) until the first sample lands, or when a
// caller passes need=0. The pre-sample fail-open window is bounded by
// the immediate sample at Start.
type memoryAdmission struct {
	hardLimit     atomic.Uint64
	rssBytes      atomic.Uint64
	reservedBytes atomic.Uint64
	budgetPercent uint32
}

func newMemoryAdmission(budgetPercent uint32) *memoryAdmission {
	if budgetPercent == 0 {
		budgetPercent = defaultMemBudgetPercent
	}
	if budgetPercent > 100 { //nolint:mnd
		budgetPercent = 100
	}
	return &memoryAdmission{budgetPercent: budgetPercent}
}

// update is called by the resource ticker after each sample. Zero values
// signal a failed sample and are ignored — admission keeps using the
// last good reading rather than fail-open on a transient hiccup.
func (m *memoryAdmission) update(rss, totalSystem uint64) {
	if rss > 0 {
		m.rssBytes.Store(rss)
	}
	if totalSystem > 0 {
		m.hardLimit.Store(totalSystem * uint64(m.budgetPercent) / 100) //nolint:mnd
	}
}

// closed reports whether a default-sized admission would refuse — the
// hard limit is set and committed RSS+reservations alone are already at
// or above it. Used by the local AdmissionState rollup so the gossiped
// signal flips to CLOSED as soon as further new work is doomed to
// reject.
func (m *memoryAdmission) closed() bool {
	limit := m.hardLimit.Load()
	if limit == 0 {
		return false
	}
	return m.rssBytes.Load()+m.reservedBytes.Load() >= limit
}

// tryReserve admits an invocation needing `need` bytes. The release
// function MUST be called when the invocation completes. Compares
// (rss + reserved + need) against the hard limit; on contention the
// CAS retries with the latest reserved snapshot.
func (m *memoryAdmission) tryReserve(need uint64) (release func(), err error) {
	limit := m.hardLimit.Load()
	if limit == 0 || need == 0 {
		return noopRelease, nil
	}
	for {
		reserved := m.reservedBytes.Load()
		rss := m.rssBytes.Load()
		if rss+reserved+need > limit {
			return nil, ErrOverloaded
		}
		if m.reservedBytes.CompareAndSwap(reserved, reserved+need) {
			return func() { m.reservedBytes.Add(^(need - 1)) }, nil
		}
	}
}

func noopRelease() {}
