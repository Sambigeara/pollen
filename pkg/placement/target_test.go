// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestComputeAutoscaleSignals(t *testing.T) {
	t.Run("burning workload reports the burn ratio", func(t *testing.T) {
		ut := newUtilisationTracker()
		now := time.Now()
		ut.nowFunc = func() time.Time { return now }
		ut.SetSLOLookup(func(string) time.Duration { return 100 * time.Millisecond })

		// 90 satisfied + 10 burned per second → 10% burn.
		for range 90 {
			ut.RecordSLO("h1", "handle", 50*time.Millisecond)
		}
		for range 10 {
			ut.RecordSLO("h1", "handle", 200*time.Millisecond)
		}
		now = now.Add(time.Second)
		ut.tick(time.Second)

		satisfied, burned, burn := ut.SLOBurnRate("h1")
		require.InDelta(t, 0.1, burn, 0.05)
		require.Greater(t, satisfied, 0.0)
		require.Greater(t, burned, 0.0)
	})

	t.Run("no observations means zero burn ratio", func(t *testing.T) {
		ut := newUtilisationTracker()
		ut.SetSLOLookup(func(string) time.Duration { return 100 * time.Millisecond })

		require.Equal(t, map[string]float64{"h1": 0}, burnRatios(map[string]spec{"h1": {}}, ut))
	})

	t.Run("burn ratio decays toward zero after load stops", func(t *testing.T) {
		// SLOBurnRate divides by max(total, rateReportFloor). When
		// observations stop, both sloSatisfied and sloBurned decay at
		// the same alpha — so burned/total stays pinned indefinitely.
		// The floored denominator forces the ratio to track burned's
		// absolute decay once it drops below rateReportFloor, killing
		// the stale-EWMA artefact that used to display a phantom high
		// burn for tens of seconds after traffic stopped.
		ut := newUtilisationTracker()
		now := time.Now()
		ut.nowFunc = func() time.Time { return now }
		ut.SetSLOLookup(func(string) time.Duration { return 100 * time.Millisecond })

		for range 10 {
			ut.RecordSLO("h1", "handle", 50*time.Millisecond)
			ut.RecordSLO("h1", "handle", 200*time.Millisecond)
		}
		now = now.Add(time.Second)
		ut.tick(time.Second)
		_, _, ratioAfterLoad := ut.SLOBurnRate("h1")
		require.Greater(t, ratioAfterLoad, 0.4, "test pre-condition: ratio should reflect 50% burn while loaded")

		for range 50 {
			now = now.Add(time.Second)
			ut.tick(time.Second)
		}
		_, _, ratioAfterDecay := ut.SLOBurnRate("h1")
		require.Less(t, ratioAfterDecay, 0.01, "ratio must decay toward zero once observations stop")
	})

	t.Run("workload comfortably under SLO has zero burn", func(t *testing.T) {
		ut := newUtilisationTracker()
		now := time.Now()
		ut.nowFunc = func() time.Time { return now }
		ut.SetSLOLookup(func(string) time.Duration { return 500 * time.Millisecond })

		for range 50 {
			ut.RecordSLO("h1", "handle", 30*time.Millisecond)
		}
		now = now.Add(time.Second)
		ut.tick(time.Second)

		satisfied, _, burn := ut.SLOBurnRate("h1")
		require.Equal(t, 0.0, burn)
		require.Greater(t, satisfied, 0.0)
	})
}

func TestDesiredReplicas(t *testing.T) {
	const hash = "h1"
	peer := peerKey(1)

	// fourCoreCluster builds a cluster view with three nodes each declaring
	// 4 cores. Cluster mean cores per node = 4.
	fourCoreCluster := func() clusterState {
		nodes := map[types.PeerKey]nodeState{}
		for _, b := range []byte{1, 2, 3} {
			nodes[peerKey(b)] = nodeState{NumCPU: 4, CPUBudgetPercent: 100}
		}
		return clusterState{
			Nodes:          nodes,
			OriginRates:    map[string]map[types.PeerKey]float64{},
			ComputeCost:    map[string]float64{},
			ParkedTime:     map[string]float64{},
			OriginRateFast: map[string]float64{},
			OriginRateSlow: map[string]float64{},
			RejectRate:     map[string]float64{},
		}
	}

	t.Run("zero arrival rate falls back to MinReplicas", func(t *testing.T) {
		c := fourCoreCluster()
		require.Equal(t, uint32(2), desiredReplicas(spec{MinReplicas: 2}, hash, c, 5))
	})

	t.Run("no compute cost observed falls back to MinReplicas", func(t *testing.T) {
		c := fourCoreCluster()
		c.OriginRates[hash] = map[types.PeerKey]float64{peer: 100}
		require.Equal(t, uint32(2), desiredReplicas(spec{MinReplicas: 2}, hash, c, 5))
	})

	t.Run("no CPU telemetry falls back to MinReplicas", func(t *testing.T) {
		c := clusterState{
			Nodes:       map[types.PeerKey]nodeState{peer: {}},
			OriginRates: map[string]map[types.PeerKey]float64{hash: {peer: 50}},
			ComputeCost: map[string]float64{hash: 100},
			ParkedTime:  map[string]float64{},
		}
		require.Equal(t, uint32(2), desiredReplicas(spec{MinReplicas: 2}, hash, c, 5))
	})

	t.Run("all compute parked falls back to MinReplicas", func(t *testing.T) {
		// service_time = compute - parked. When everything is parked
		// the workload is pure I/O wait — there's no CPU work to size
		// against, so capacity math has nothing to say.
		c := fourCoreCluster()
		c.OriginRates[hash] = map[types.PeerKey]float64{peer: 100}
		c.ComputeCost[hash] = 100
		c.ParkedTime[hash] = 100
		require.Equal(t, uint32(2), desiredReplicas(spec{MinReplicas: 2}, hash, c, 5))
	})

	t.Run("light load stays at MinReplicas", func(t *testing.T) {
		// arrival=10/s, service_time=100ms, cores=4
		// raw = 10 × 0.1 × 1.5 / (4 × 0.7) ≈ 0.54 → ceil = 1
		// MinReplicas=2 floors it.
		c := fourCoreCluster()
		c.OriginRates[hash] = map[types.PeerKey]float64{peer: 10}
		c.ComputeCost[hash] = 100
		require.Equal(t, uint32(2), desiredReplicas(spec{MinReplicas: 2}, hash, c, 5))
	})

	t.Run("heavy load grows replica count", func(t *testing.T) {
		// arrival=100/s, service_time=100ms, cores=4
		// raw = 100 × 0.1 × 1.5 / 2.8 ≈ 5.36 → ceil = 6
		c := fourCoreCluster()
		c.OriginRates[hash] = map[types.PeerKey]float64{peer: 100}
		c.ComputeCost[hash] = 100
		require.Equal(t, uint32(6), desiredReplicas(spec{MinReplicas: 1}, hash, c, 10))
	})

	t.Run("clamped to cluster size when arrival exceeds capacity", func(t *testing.T) {
		// arrival=1000/s, service_time=100ms, cores=4 → raw ≈ 53.6
		// clusterSize=5 caps it.
		c := fourCoreCluster()
		c.OriginRates[hash] = map[types.PeerKey]float64{peer: 1000}
		c.ComputeCost[hash] = 100
		require.Equal(t, uint32(5), desiredReplicas(spec{MinReplicas: 1}, hash, c, 5))
	})

	t.Run("parked time is excluded from service time", func(t *testing.T) {
		// arrival=100/s, compute=100ms, parked=80ms → service_time=20ms
		// raw = 100 × 0.02 × 1.5 / 2.8 ≈ 1.07 → ceil = 2
		// A chain workload spending most of its wall time parked needs
		// far fewer replicas than the wall-time-only formula would say.
		c := fourCoreCluster()
		c.OriginRates[hash] = map[types.PeerKey]float64{peer: 100}
		c.ComputeCost[hash] = 100
		c.ParkedTime[hash] = 80
		require.Equal(t, uint32(2), desiredReplicas(spec{MinReplicas: 1}, hash, c, 10))
	})

	t.Run("origin rate sums across peers", func(t *testing.T) {
		// Same total demand (200/s) split across three peers should
		// produce the same desired count as concentrated on one.
		c := fourCoreCluster()
		c.OriginRates[hash] = map[types.PeerKey]float64{
			peerKey(1): 100,
			peerKey(2): 50,
			peerKey(3): 50,
		}
		c.ComputeCost[hash] = 100
		// arrival=200/s → raw ≈ 10.71 → ceil = 11.
		require.Equal(t, uint32(11), desiredReplicas(spec{MinReplicas: 1}, hash, c, 20))
	})

	t.Run("every node computes the same target from the same gossip", func(t *testing.T) {
		// Convergence-by-construction: desiredReplicas is pure, so any
		// peer reading the same snapshot decides the same replica count.
		c := fourCoreCluster()
		c.OriginRates[hash] = map[types.PeerKey]float64{peer: 50}
		c.ComputeCost[hash] = 100
		sp := spec{MinReplicas: 1}
		first := desiredReplicas(sp, hash, c, 10)
		for range 5 {
			require.Equal(t, first, desiredReplicas(sp, hash, c, 10))
		}
	})

	t.Run("CPU budget reduces effective cores and grows replica count", func(t *testing.T) {
		// A node configured with a 25% CPU budget can
		// only deliver a quarter of its raw cores. The replica formula
		// must divide by effective cores (NumCPU × budget), not raw
		// NumCPU — otherwise the autoscaler under-provisions by 1/budget.
		// At 25% budget, effective cores per node = 1, so the same load
		// needs ~4× the replicas.
		nodes := map[types.PeerKey]nodeState{}
		for _, b := range []byte{1, 2, 3} {
			nodes[peerKey(b)] = nodeState{NumCPU: 4, CPUBudgetPercent: 25}
		}
		c := clusterState{
			Nodes:       nodes,
			OriginRates: map[string]map[types.PeerKey]float64{hash: {peer: 100}},
			ComputeCost: map[string]float64{hash: 100},
			ParkedTime:  map[string]float64{},
		}
		// arrival=100/s, service_time=100ms, effective cores=1
		// raw = 100 × 0.1 × 1.5 / (1 × 0.7) ≈ 21.4 → ceil = 22 → clamped to clusterSize=10
		require.Equal(t, uint32(10), desiredReplicas(spec{MinReplicas: 1}, hash, c, 10))
	})

	t.Run("fast EWMA captures a microburst that hasn't lifted the medium reading yet", func(t *testing.T) {
		// Slow OriginRate sum is still 10/s but a fresh burst has lifted
		// OriginRateFast to 100/s. burstAwareArrival picks the higher
		// reading so desiredReplicas grows immediately.
		c := fourCoreCluster()
		c.OriginRates[hash] = map[types.PeerKey]float64{peer: 10}
		c.OriginRateFast[hash] = 100
		c.ComputeCost[hash] = 100
		// arrival=100/s, service_time=100ms, cores=4
		// raw = 100 × 0.1 × 1.5 / 2.8 ≈ 5.36 → ceil = 6
		require.Equal(t, uint32(6), desiredReplicas(spec{MinReplicas: 1}, hash, c, 10))
	})

	t.Run("slow anchors smoothed arrival when slow > medium and fast", func(t *testing.T) {
		// burstAwareArrival picks max(originSum, fast, slow). Headroom
		// is applied exactly once, in desiredReplicas — applying it
		// inside burstAwareArrival as well would double-buffer the
		// slow-dominant branch.
		c := fourCoreCluster()
		c.OriginRates[hash] = map[types.PeerKey]float64{peer: 50}
		c.OriginRateSlow[hash] = 80
		c.ComputeCost[hash] = 100
		// arrival=80/s, service_time=100ms, cores=4, headroom=1.5
		// raw = 80 × 0.1 × 1.5 / 2.8 ≈ 4.29 → ceil = 5
		require.Equal(t, uint32(5), desiredReplicas(spec{MinReplicas: 1}, hash, c, 10))
	})

	t.Run("headroom is applied exactly once across burst-aware paths", func(t *testing.T) {
		// Pin the no-double-headroom invariant: same effective arrival
		// (100/s) reached via originSum, fast EWMA, or slow EWMA must
		// produce the same desiredReplicas.
		const arrival = 100.0
		baseline := func() clusterState {
			c := fourCoreCluster()
			c.ComputeCost[hash] = 100
			return c
		}

		viaOriginSum := baseline()
		viaOriginSum.OriginRates[hash] = map[types.PeerKey]float64{peer: arrival}

		viaFast := baseline()
		viaFast.OriginRateFast[hash] = arrival

		viaSlow := baseline()
		viaSlow.OriginRateSlow[hash] = arrival

		want := desiredReplicas(spec{MinReplicas: 1}, hash, viaOriginSum, 10)
		require.Equal(t, want, desiredReplicas(spec{MinReplicas: 1}, hash, viaFast, 10),
			"fast-EWMA path must produce the same count as the originSum path")
		require.Equal(t, want, desiredReplicas(spec{MinReplicas: 1}, hash, viaSlow, 10),
			"slow-EWMA path must produce the same count — headroom applied once, not twice")
	})

	t.Run("reject pressure forces an extra replica even when capacity math agrees", func(t *testing.T) {
		c := fourCoreCluster()
		c.OriginRates[hash] = map[types.PeerKey]float64{peer: 100}
		c.ComputeCost[hash] = 100
		c.RejectRate[hash] = 5
		// Without rejects: raw≈5.36 → 6. With rejects: +1 → 7.
		require.Equal(t, uint32(7), desiredReplicas(spec{MinReplicas: 1}, hash, c, 10))
	})

	t.Run("reject pressure with no origin signal still nudges from MinReplicas", func(t *testing.T) {
		// Edge case: rejections start before the fast EWMA has lifted,
		// e.g. the very first burst lands on a single replica. Bump
		// desired by one immediately rather than wait for the EWMA.
		c := fourCoreCluster()
		c.RejectRate[hash] = 5
		require.Equal(t, uint32(2), desiredReplicas(spec{MinReplicas: 1}, hash, c, 10))
	})

	t.Run("CPU budget zero (unreported) is treated as full capacity", func(t *testing.T) {
		// Older peers may not publish CPUBudgetPercent (zero on the wire).
		// cpuCapacity treats zero as 100% so we don't penalise them as
		// having no capacity at all.
		nodes := map[types.PeerKey]nodeState{}
		for _, b := range []byte{1, 2, 3} {
			nodes[peerKey(b)] = nodeState{NumCPU: 4, CPUBudgetPercent: 0}
		}
		c := clusterState{
			Nodes:       nodes,
			OriginRates: map[string]map[types.PeerKey]float64{hash: {peer: 100}},
			ComputeCost: map[string]float64{hash: 100},
			ParkedTime:  map[string]float64{},
		}
		// Same as the existing 4-core, full-capacity case: 6 replicas.
		require.Equal(t, uint32(6), desiredReplicas(spec{MinReplicas: 1}, hash, c, 10))
	})
}
