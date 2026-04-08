package placement

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestComputePressures(t *testing.T) {
	pk1 := peerKey(1)
	pk2 := peerKey(2)

	t.Run("normal: demand exceeds served", func(t *testing.T) {
		specs := map[string]spec{"h1": {MinReplicas: 2}}
		load := map[types.PeerKey]map[string]float32{
			pk1: {"h1": 50},
			pk2: {"h1": 50},
		}
		demand := map[types.PeerKey]map[string]float32{
			pk1: {"h1": 200},
		}
		p := computePressures(specs, load, demand, nil)
		require.InDelta(t, 2.0, p["h1"], 0.01)
	})

	t.Run("zero demand, zero served", func(t *testing.T) {
		specs := map[string]spec{"h1": {MinReplicas: 1}}
		p := computePressures(specs, nil, nil, nil)
		require.Equal(t, 0.0, p["h1"])
	})

	t.Run("zero demand, nonzero served", func(t *testing.T) {
		specs := map[string]spec{"h1": {MinReplicas: 2}}
		load := map[types.PeerKey]map[string]float32{pk1: {"h1": 100}}
		p := computePressures(specs, load, nil, nil)
		require.Equal(t, 0.0, p["h1"])
	})

	t.Run("true cold start: demand, no claimants, no served", func(t *testing.T) {
		specs := map[string]spec{"h1": {MinReplicas: 2}}
		demand := map[types.PeerKey]map[string]float32{pk1: {"h1": 50}}
		p := computePressures(specs, nil, demand, nil)
		require.InDelta(t, coldStartPressureCap, p["h1"], 0.001)
	})

	t.Run("gossip lag: demand, claimants exist, served not propagated", func(t *testing.T) {
		specs := map[string]spec{"h1": {MinReplicas: 2}}
		demand := map[types.PeerKey]map[string]float32{pk1: {"h1": 50}}
		claims := map[string]map[types.PeerKey]struct{}{
			"h1": {pk1: {}, pk2: {}},
		}
		p := computePressures(specs, nil, demand, claims)
		require.Equal(t, 0.0, p["h1"])
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
			dynamicTargets:     make(map[string]uint32),
			highPressureStreak: make(map[string]int),
		}
	}

	t.Run("initialises from min replicas", func(t *testing.T) {
		r := newRec()
		specs := map[string]spec{"h1": {MinReplicas: 2}}
		pressures := map[string]float64{"h1": 1.0}
		r.stepAdjustTargets(specs, pressures, 4)
		require.Equal(t, uint32(2), r.dynamicTargets["h1"])
	})

	t.Run("single high-pressure cycle does not increment", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 2
		specs := map[string]spec{"h1": {MinReplicas: 2}}
		pressures := map[string]float64{"h1": 1.5}
		r.stepAdjustTargets(specs, pressures, 4)
		require.Equal(t, uint32(2), r.dynamicTargets["h1"])
		require.Equal(t, 1, r.highPressureStreak["h1"])
	})

	t.Run("sustained high pressure increments after streak", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 2
		specs := map[string]spec{"h1": {MinReplicas: 2}}
		pressures := map[string]float64{"h1": 1.5}
		for range scaleUpStreakRequired - 1 {
			r.stepAdjustTargets(specs, pressures, 4)
		}
		require.Equal(t, uint32(2), r.dynamicTargets["h1"])
		r.stepAdjustTargets(specs, pressures, 4)
		require.Equal(t, uint32(3), r.dynamicTargets["h1"])
		require.Equal(t, 0, r.highPressureStreak["h1"])
	})

	t.Run("streak resets on normal pressure", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 2
		r.highPressureStreak["h1"] = scaleUpStreakRequired - 1
		specs := map[string]spec{"h1": {MinReplicas: 2}}
		r.stepAdjustTargets(specs, map[string]float64{"h1": 1.0}, 4)
		require.Equal(t, 0, r.highPressureStreak["h1"])
	})

	t.Run("decrements on low pressure", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 3
		specs := map[string]spec{"h1": {MinReplicas: 2}}
		pressures := map[string]float64{"h1": 0.5}
		r.stepAdjustTargets(specs, pressures, 4)
		require.Equal(t, uint32(2), r.dynamicTargets["h1"])
	})

	t.Run("holds between thresholds", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 3
		specs := map[string]spec{"h1": {MinReplicas: 2}}
		pressures := map[string]float64{"h1": 1.0}
		r.stepAdjustTargets(specs, pressures, 4)
		require.Equal(t, uint32(3), r.dynamicTargets["h1"])
	})

	t.Run("floored at min replicas", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 2
		specs := map[string]spec{"h1": {MinReplicas: 2}}
		pressures := map[string]float64{"h1": 0.0}
		r.stepAdjustTargets(specs, pressures, 4)
		require.Equal(t, uint32(2), r.dynamicTargets["h1"])
	})

	t.Run("cleans up removed specs", func(t *testing.T) {
		r := newRec()
		r.dynamicTargets["h1"] = 3
		r.dynamicTargets["h2"] = 2
		specs := map[string]spec{"h1": {MinReplicas: 2}}
		pressures := map[string]float64{"h1": 1.0}
		r.stepAdjustTargets(specs, pressures, 4)
		_, exists := r.dynamicTargets["h2"]
		require.False(t, exists, "should clean up target for removed spec")
	})
}
