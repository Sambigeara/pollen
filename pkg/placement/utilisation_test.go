// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUtilisationTracker_RecordSLOClassifiesFromConstruction(t *testing.T) {
	// A fresh tracker uses defaultLatencySLO until SetSLOLookup overrides
	// it, so observations from the very first call are classified —
	// previously the nil guard silently dropped them during the
	// reconciler's startup window.
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	for range 5 {
		ut.RecordSLO("abc", "handle", 50*time.Millisecond)
	}
	ut.RecordSLO("abc", "handle", 2*time.Second)

	now = now.Add(time.Second)
	ut.tick(time.Second)

	sat, burned, ratio := ut.SLOBurnRate("abc")
	require.Greater(t, sat, 0.0)
	require.Greater(t, burned, 0.0)
	require.Greater(t, ratio, 0.0)
}

func TestUtilisationTracker_SetSLOLookupNilRestoresDefault(t *testing.T) {
	ut := newUtilisationTracker()
	ut.SetSLOLookup(func(string) time.Duration { return 10 * time.Millisecond })
	ut.SetSLOLookup(nil)

	now := time.Now()
	ut.nowFunc = func() time.Time { return now }
	for range 5 {
		ut.RecordSLO("abc", "handle", 50*time.Millisecond)
	}
	now = now.Add(time.Second)
	ut.tick(time.Second)

	sat, _, _ := ut.SLOBurnRate("abc")
	require.Greater(t, sat, 0.0, "default lookup should classify under the 1s package default")
}

func TestUtilisationTracker_RecordAndRate(t *testing.T) {
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	ut.RecordServed("abc", "handle")

	// Before tick, EWMA is still zero.
	require.NotContains(t, ut.ServedRates(), "abc")

	// Advance 1s and tick.
	now = now.Add(time.Second)
	ut.tick(time.Second)

	require.InDelta(t, 0.2, ut.ServedRates()["abc"], 0.1)
}

func TestUtilisationTracker_Clear(t *testing.T) {
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	ut.RecordServed("abc", "handle")
	now = now.Add(time.Second)
	ut.tick(time.Second)
	require.Contains(t, ut.ServedRates(), "abc")

	ut.Clear("abc")
	require.NotContains(t, ut.ServedRates(), "abc")
}

func TestUtilisationTracker_ServedRates(t *testing.T) {
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	for range 100 {
		ut.RecordServed("hot", "handle")
	}
	ut.RecordServed("cold", "handle")

	now = now.Add(time.Second)
	ut.tick(time.Second)

	rates := ut.ServedRates()
	require.Contains(t, rates, "hot")
	require.InDelta(t, 20.0, rates["hot"], 1.0)
}

func TestUtilisationTracker_OriginRates(t *testing.T) {
	// RecordOrigin fires at Call() entry regardless of whether the seed
	// runs locally — it captures where demand enters the cluster. It
	// produces a per-hash rate analogous to RecordServed.
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	require.Empty(t, ut.OriginRates())

	for range 100 {
		ut.RecordOrigin("hot", "handle")
	}
	now = now.Add(time.Second)
	ut.tick(time.Second)

	rates := ut.OriginRates()
	require.Contains(t, rates, "hot")
	require.InDelta(t, 20.0, rates["hot"], 1.0)

	// ServedRates and OriginRates are independent channels on the same
	// hash — a hash with only origin traffic should not appear in served.
	require.NotContains(t, ut.ServedRates(), "hot")
}

func TestUtilisationTracker_InvocationCost(t *testing.T) {
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	require.Empty(t, ut.InvocationCosts())

	// One second of 10 calls × 50ms each — mean cost is 50ms. The ms-rate
	// and served-rate EWMAs both seed from zero with alpha 0.2, so the
	// first tick lands at (10 × 50 × 0.2) / (10 × 0.2) = 50ms: the ratio
	// is cost per call regardless of EWMA warm-up.
	for range 10 {
		ut.RecordServed("abc", "handle")
		ut.RecordInvocation("abc", "handle", 50*time.Millisecond)
	}
	now = now.Add(time.Second)
	ut.tick(time.Second)
	require.InDelta(t, 50.0, ut.InvocationCosts()["abc"], 1.0)

	// Idle for enough ticks that served rate falls below the floor: cost
	// disappears from snapshots, mirroring ServedRates/DemandRates.
	for range 60 {
		now = now.Add(time.Second)
		ut.tick(time.Second)
	}
	require.NotContains(t, ut.InvocationCosts(), "abc")

	require.NotContains(t, ut.InvocationCosts(), "untouched")
}

func TestUtilisationTracker_ParkedTimes(t *testing.T) {
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	require.Empty(t, ut.ParkedTimes())

	// Ten invocations, each 100ms total, 80ms of which was parked inside
	// pollen_request. Mean parked-per-invocation is 80ms; same EWMA
	// warm-up logic as InvocationCosts — the ratio is invariant of alpha.
	for range 10 {
		ut.RecordServed("abc", "handle")
		ut.RecordInvocation("abc", "handle", 100*time.Millisecond)
		ut.RecordParkedTime("abc", "handle", 80*time.Millisecond)
	}
	now = now.Add(time.Second)
	ut.tick(time.Second)

	require.InDelta(t, 80.0, ut.ParkedTimes()["abc"], 1.0)
	require.InDelta(t, 100.0, ut.InvocationCosts()["abc"], 1.0)

	// Idle long enough that served rate falls below the floor: parked
	// figure disappears from snapshots, matching InvocationCosts.
	for range 60 {
		now = now.Add(time.Second)
		ut.tick(time.Second)
	}
	require.NotContains(t, ut.ParkedTimes(), "abc")
}

func TestUtilisationTracker_ClearDropsParked(t *testing.T) {
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	for range 5 {
		ut.RecordServed("abc", "handle")
		ut.RecordInvocation("abc", "handle", 50*time.Millisecond)
		ut.RecordParkedTime("abc", "handle", 40*time.Millisecond)
	}
	now = now.Add(time.Second)
	ut.tick(time.Second)
	require.Contains(t, ut.ParkedTimes(), "abc")

	ut.Clear("abc")
	require.NotContains(t, ut.ParkedTimes(), "abc")
}

func TestUtilisationTracker_ClearDropsInvocation(t *testing.T) {
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	for range 5 {
		ut.RecordServed("abc", "handle")
		ut.RecordInvocation("abc", "handle", 25*time.Millisecond)
		ut.RecordOrigin("abc", "handle")
	}
	now = now.Add(time.Second)
	ut.tick(time.Second)

	require.Contains(t, ut.InvocationCosts(), "abc")
	require.Contains(t, ut.OriginRates(), "abc")

	ut.Clear("abc")
	require.NotContains(t, ut.InvocationCosts(), "abc")
	require.NotContains(t, ut.OriginRates(), "abc")
}
