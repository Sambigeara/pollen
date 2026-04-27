// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryAdmission_AdmitsBelowLimit(t *testing.T) {
	m := newMemoryAdmission(50) // 50% of 1 GiB → 512 MiB hard limit
	m.update(100<<20, 1<<30)    // 100 MiB RSS, 1 GiB total

	rel, err := m.tryReserve(64 << 20)
	require.NoError(t, err)
	require.NotNil(t, rel)
	require.Equal(t, uint64(64<<20), m.reservedBytes.Load())

	rel()
	require.Equal(t, uint64(0), m.reservedBytes.Load())
}

func TestMemoryAdmission_RejectsAtLimit(t *testing.T) {
	m := newMemoryAdmission(50)
	m.update(400<<20, 1<<30) // 400 MiB RSS, 512 MiB cap → 112 MiB headroom

	// First reservation fits (64 MiB ≤ 112 MiB).
	rel, err := m.tryReserve(64 << 20)
	require.NoError(t, err)

	// Second reservation would push us to 528 MiB total — over 512 MiB cap.
	_, err = m.tryReserve(64 << 20)
	require.ErrorIs(t, err, ErrOverloaded)

	// Releasing the first reservation reopens headroom.
	rel()
	rel2, err := m.tryReserve(64 << 20)
	require.NoError(t, err)
	rel2()
}

func TestMemoryAdmission_DisabledUntilSampled(t *testing.T) {
	// No update() call — hard limit is zero, admission fails open so the
	// node is usable during the brief startup window before the first
	// resource sample lands.
	m := newMemoryAdmission(50)
	rel, err := m.tryReserve(1 << 30)
	require.NoError(t, err)
	require.NotNil(t, rel)
	rel()
}

func TestMemoryAdmission_ZeroNeedAdmits(t *testing.T) {
	// Workload with no declared memory cost shouldn't take a slot.
	m := newMemoryAdmission(50)
	m.update(100<<20, 1<<30)

	rel, err := m.tryReserve(0)
	require.NoError(t, err)
	require.Equal(t, uint64(0), m.reservedBytes.Load())
	rel()
}

func TestMemoryAdmission_DefaultBudgetWhenZero(t *testing.T) {
	// Operator left budget unset (0) — admission still enforces an
	// inferred 80% ceiling.
	m := newMemoryAdmission(0)
	m.update(0, 1<<30) // 1 GiB total, 0 RSS so the cap is the only constraint

	require.Equal(t, uint64(1<<30)*uint64(defaultMemBudgetPercent)/100, m.hardLimit.Load())
}

func TestMemoryAdmission_ClampsBudgetAbove100(t *testing.T) {
	// A misconfigured budget > 100 is clamped to 100 so the hard limit
	// can never exceed total system memory.
	m := newMemoryAdmission(250)
	m.update(0, 1<<30)
	require.Equal(t, uint64(1<<30), m.hardLimit.Load())
}

func TestMemoryAdmission_FailedSampleKeepsLastGood(t *testing.T) {
	// Transient sampler failure (rss=0) must not zero out the last good
	// reading — otherwise admission fail-opens for an interval.
	m := newMemoryAdmission(50)
	m.update(400<<20, 1<<30)
	require.Equal(t, uint64(400<<20), m.rssBytes.Load())

	m.update(0, 1<<30) // failed RSS sample
	require.Equal(t, uint64(400<<20), m.rssBytes.Load(), "last good RSS must be preserved")

	// Admission still rejects against the preserved RSS.
	_, err := m.tryReserve(200 << 20) // 400 + 200 = 600 > 512 cap
	require.ErrorIs(t, err, ErrOverloaded)
}

func TestMemoryAdmission_ConcurrentReservations(t *testing.T) {
	// 10 workers race to reserve 100 MiB each against a 500 MiB budget.
	// Exactly 5 must succeed; CAS contention must not let extras through.
	m := newMemoryAdmission(50)
	m.update(0, 1<<30) // 512 MiB cap

	const need = 100 << 20
	const workers = 10

	var admitted atomic.Int32
	var wg sync.WaitGroup
	releases := make(chan func(), workers)

	for range workers {
		wg.Go(func() {
			rel, err := m.tryReserve(need)
			if err == nil {
				admitted.Add(1)
				releases <- rel
			}
		})
	}
	wg.Wait()
	close(releases)

	require.Equal(t, int32(5), admitted.Load(), "exactly 5 reservations of 100 MiB fit in 512 MiB")
	require.Equal(t, uint64(5*need), m.reservedBytes.Load())

	for rel := range releases {
		rel()
	}
	require.Equal(t, uint64(0), m.reservedBytes.Load())
}
