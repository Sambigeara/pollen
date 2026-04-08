package placement

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUtilisationTracker_RecordAndRate(t *testing.T) {
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	ut.RecordDemand("abc")
	ut.RecordDemand("abc")
	ut.RecordServed("abc")

	// Before tick, EWMA is still zero.
	require.InDelta(t, 0, ut.DemandRate("abc"), 0.01)

	// Advance 1s and tick.
	now = now.Add(time.Second)
	ut.tick(time.Second)

	require.InDelta(t, 0.4, ut.DemandRate("abc"), 0.1)
	require.InDelta(t, 0.2, ut.ServedRate("abc"), 0.1)
}

func TestUtilisationTracker_IdleDuration(t *testing.T) {
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	require.Equal(t, time.Duration(math.MaxInt64), ut.IdleDuration("abc"))

	ut.RecordServed("abc")
	now = now.Add(3 * time.Minute)
	require.InDelta(t, 3*time.Minute, ut.IdleDuration("abc"), float64(time.Millisecond))
}

func TestUtilisationTracker_Clear(t *testing.T) {
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	ut.RecordDemand("abc")
	now = now.Add(time.Second)
	ut.tick(time.Second)
	require.True(t, ut.DemandRate("abc") > 0)

	ut.Clear("abc")
	require.InDelta(t, 0, ut.DemandRate("abc"), 0.001)
	require.Equal(t, time.Duration(math.MaxInt64), ut.IdleDuration("abc"))
}

func TestUtilisationTracker_ServedRates(t *testing.T) {
	ut := newUtilisationTracker()
	now := time.Now()
	ut.nowFunc = func() time.Time { return now }

	for range 100 {
		ut.RecordServed("hot")
	}
	ut.RecordServed("cold")

	now = now.Add(time.Second)
	ut.tick(time.Second)

	rates := ut.ServedRates()
	require.Contains(t, rates, "hot")
	require.InDelta(t, 20.0, rates["hot"], 1.0)
}
