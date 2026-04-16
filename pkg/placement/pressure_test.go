package placement

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestComputeAutoscaleSignals(t *testing.T) {
	t.Run("burning workload reports the burn ratio", func(t *testing.T) {
		ut := newUtilisationTracker()
		now := time.Now()
		ut.nowFunc = func() time.Time { return now }
		ut.SetSLOLookup(func(string) time.Duration { return 100 * time.Millisecond })

		// 90 satisfied + 10 burned per second → 10% burn.
		// Volume kept above burnSignalFloor so the ratio is actionable.
		for range 90 {
			ut.RecordSLO("h1", 50*time.Millisecond)
		}
		for range 10 {
			ut.RecordSLO("h1", 200*time.Millisecond)
		}
		now = now.Add(time.Second)
		ut.tick(time.Second)

		s := computeAutoscaleSignals(map[string]spec{"h1": {}}, ut)["h1"]
		require.InDelta(t, 0.1, s.burn, 0.05)
		require.Greater(t, s.satisfied, 0.0)
		require.Greater(t, s.burned, burnSignalFloor)
	})

	t.Run("no observations means zero burn ratio", func(t *testing.T) {
		ut := newUtilisationTracker()
		ut.SetSLOLookup(func(string) time.Duration { return 100 * time.Millisecond })

		s := computeAutoscaleSignals(map[string]spec{"h1": {}}, ut)["h1"]
		require.Equal(t, 0.0, s.burn)
		require.Equal(t, 0.0, s.satisfied)
		require.Equal(t, 0.0, s.burned)
	})

	t.Run("stale EWMA below signal floor reports zero burn", func(t *testing.T) {
		// After load drops, sloSatisfied and sloBurned decay at the
		// same alpha, so the ratio would stay pinned at its last
		// value for ~40 ticks. Scale-up must not fire on that stale
		// signal — computeAutoscaleSignals zeroes the ratio when
		// burned rate is below burnSignalFloor.
		ut := newUtilisationTracker()
		now := time.Now()
		ut.nowFunc = func() time.Time { return now }
		ut.SetSLOLookup(func(string) time.Duration { return 100 * time.Millisecond })

		// Seed a high-burn history, then stop observing.
		for range 10 {
			ut.RecordSLO("h1", 50*time.Millisecond)
			ut.RecordSLO("h1", 200*time.Millisecond)
		}
		now = now.Add(time.Second)
		ut.tick(time.Second)

		// Confirm the raw ratio reads as high before the signal floor
		// guard kicks in.
		_, _, rawRatio := ut.SLOBurnRate("h1")
		require.Greater(t, rawRatio, burnCeiling)

		// Advance many ticks with zero traffic. Both rates decay,
		// ratio stays pinned, raw burned eventually drops below the
		// floor.
		for range 20 {
			now = now.Add(time.Second)
			ut.tick(time.Second)
		}
		_, burned, _ := ut.SLOBurnRate("h1")
		require.Less(t, burned, burnSignalFloor, "test pre-condition: burned rate should have decayed below floor")

		// computeAutoscaleSignals must treat this as zero burn.
		s := computeAutoscaleSignals(map[string]spec{"h1": {}}, ut)["h1"]
		require.Equal(t, 0.0, s.burn, "stale burn below signal floor must be suppressed")
	})

	t.Run("workload comfortably under SLO has zero burn", func(t *testing.T) {
		ut := newUtilisationTracker()
		now := time.Now()
		ut.nowFunc = func() time.Time { return now }
		ut.SetSLOLookup(func(string) time.Duration { return 500 * time.Millisecond })

		for range 50 {
			ut.RecordSLO("h1", 30*time.Millisecond)
		}
		now = now.Add(time.Second)
		ut.tick(time.Second)

		s := computeAutoscaleSignals(map[string]spec{"h1": {}}, ut)["h1"]
		require.Equal(t, 0.0, s.burn)
		require.Greater(t, s.satisfied, 0.0)
	})
}

func TestEffectiveTarget(t *testing.T) {
	t.Run("spread mode returns cluster size", func(t *testing.T) {
		require.Equal(t, uint32(4), effectiveTarget(spec{MinReplicas: 2, Spread: 1.0}, 4, 2))
	})

	t.Run("dynamic target above min replicas", func(t *testing.T) {
		require.Equal(t, uint32(3), effectiveTarget(spec{MinReplicas: 2}, 4, 3))
	})

	t.Run("dynamic target zero falls back to min replicas", func(t *testing.T) {
		require.Equal(t, uint32(2), effectiveTarget(spec{MinReplicas: 2}, 4, 0))
	})

	t.Run("dynamic target capped at cluster size", func(t *testing.T) {
		require.Equal(t, uint32(4), effectiveTarget(spec{MinReplicas: 2}, 4, 10))
	})

	t.Run("dynamic target at min replicas", func(t *testing.T) {
		require.Equal(t, uint32(2), effectiveTarget(spec{MinReplicas: 2}, 4, 2))
	})
}

func TestStepAdjustTargets(t *testing.T) {
	newRec := func() *reconciler {
		return &reconciler{
			dynamicTargets: make(map[string]uint32),
			scaleUpStreak:  make(map[string]int),
		}
	}
	specs := map[string]spec{"h1": {MinReplicas: 2}}
	burnSignal := map[string]autoscaleSignals{"h1": {satisfied: 50, burned: 50, burn: 0.5}}
	healthySignal := map[string]autoscaleSignals{"h1": {satisfied: 100, burned: 0, burn: 0}}
	idleSignal := map[string]autoscaleSignals{"h1": {}}

	t.Run("initialises from min replicas", func(t *testing.T) {
		r := newRec()
		r.stepAdjustTargets(specs, idleSignal, 4)
		require.Equal(t, uint32(2), r.dynamicTargets["h1"])
	})

	t.Run("single burn tick does not increment", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 2
		r.stepAdjustTargets(specs, burnSignal, 4)
		require.Equal(t, uint32(2), r.dynamicTargets["h1"])
		require.Equal(t, 1, r.scaleUpStreak["h1"])
	})

	t.Run("sustained burn increments after K_up ticks", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 2
		for range scaleUpSustainTicks - 1 {
			r.stepAdjustTargets(specs, burnSignal, 4)
		}
		require.Equal(t, uint32(2), r.dynamicTargets["h1"])
		r.stepAdjustTargets(specs, burnSignal, 4)
		// burn 0.5 → step = min(2.0, 1.5) = 1.5; target = ceil(2 * 1.5) = 3.
		require.Equal(t, uint32(3), r.dynamicTargets["h1"])
		require.Equal(t, 0, r.scaleUpStreak["h1"])
	})

	t.Run("streak resets on healthy signal", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 2
		r.scaleUpStreak["h1"] = scaleUpSustainTicks - 1
		r.stepAdjustTargets(specs, healthySignal, 4)
		require.Equal(t, 0, r.scaleUpStreak["h1"])
	})

	t.Run("decrements when traffic is healthy and over-provisioned", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 3
		r.stepAdjustTargets(specs, healthySignal, 4)
		require.Equal(t, uint32(2), r.dynamicTargets["h1"])
	})

	t.Run("decrements under residual burn below the scale-down floor", func(t *testing.T) {
		// Real-world signal: steady traffic, tiny residual burn from
		// network jitter (one > SLO call out of ~1000). Must still
		// scale down — otherwise replicas freeze whenever any real
		// traffic flows, which is what happened in the demo at 15:10.
		r := newRec()
		r.dynamicTargets["h1"] = 3
		noisy := map[string]autoscaleSignals{"h1": {satisfied: 200, burned: 0.2, burn: 0.001}}
		r.stepAdjustTargets(specs, noisy, 4)
		require.Equal(t, uint32(2), r.dynamicTargets["h1"])
	})

	t.Run("holds when burn is near but not below the scale-down floor", func(t *testing.T) {
		// Burn is well above scale-down-floor (0.005) but below the
		// burn-ceiling (0.05) — not enough pain to scale up, too much
		// pain to claim steady-state. Hold.
		r := newRec()
		r.dynamicTargets["h1"] = 3
		warm := map[string]autoscaleSignals{"h1": {satisfied: 200, burned: 5, burn: 0.025}}
		r.stepAdjustTargets(specs, warm, 4)
		require.Equal(t, uint32(3), r.dynamicTargets["h1"])
	})

	t.Run("floored at min replicas on healthy signal", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 2
		r.stepAdjustTargets(specs, healthySignal, 4)
		require.Equal(t, uint32(2), r.dynamicTargets["h1"])
	})

	t.Run("cleans up removed specs", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 3
		r.dynamicTargets["h2"] = 2
		r.stepAdjustTargets(specs, healthySignal, 4)
		_, exists := r.dynamicTargets["h2"]
		require.False(t, exists, "should clean up target for removed spec")
	})

	t.Run("idle (no observations) sheds replicas above MinReplicas", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 4
		r.stepAdjustTargets(specs, idleSignal, 4)
		require.Equal(t, uint32(3), r.dynamicTargets["h1"])
		r.stepAdjustTargets(specs, idleSignal, 4)
		require.Equal(t, uint32(2), r.dynamicTargets["h1"])
		r.stepAdjustTargets(specs, idleSignal, 4)
		require.Equal(t, uint32(2), r.dynamicTargets["h1"], "do not shed below MinReplicas")
	})
}
