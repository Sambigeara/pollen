// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func node(coord coords.Coord, numCPU uint32) nodeState {
	return nodeState{
		Coord:            &coord,
		NumCPU:           numCPU,
		CPUBudgetPercent: 100,
		MemTotalBytes:    8 << 30, //nolint:mnd
		MemBudgetPercent: 100,
	}
}

func TestPredictedLatency_ColdStart_NoObservations(t *testing.T) {
	// With no ComputeCost yet, two capacity-equivalent candidates tie at
	// zero. Hash tiebreak in rankTopN decides; predictedLatency itself
	// returns 0.
	a := peerKey(1)
	b := peerKey(2)
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			a: node(coords.Coord{X: 0, Y: 0}, 4),
			b: node(coords.Coord{X: 100, Y: 0}, 4),
		},
	}
	require.Equal(t, 0.0, predictedLatency(a, "unseen", cluster))
	require.Equal(t, 0.0, predictedLatency(b, "unseen", cluster))
}

func TestPredictedLatency_ColdStart_FavoursLargerCPU(t *testing.T) {
	// No demand observed yet (InvocationRate = 0) but a compute-cost
	// estimate has propagated from prior runs of this seed. Scoring
	// should rank by compute fit — smaller box scores worse.
	small := peerKey(1)
	big := peerKey(2)
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			small: node(coords.Coord{X: 0, Y: 0}, 1),
			big:   node(coords.Coord{X: 0, Y: 0}, 4),
		},
		ComputeCost: map[string]float64{"seedA": 40.0},
	}

	require.Equal(t, 40.0, predictedLatency(small, "seedA", cluster))
	require.Equal(t, 10.0, predictedLatency(big, "seedA", cluster))
	require.Greater(t, predictedLatency(small, "seedA", cluster), predictedLatency(big, "seedA", cluster))
}

func TestPredictedLatency_ColdStart_NoTelemetry_DoesNotBeatMeasuredPeer(t *testing.T) {
	// A candidate without CPU telemetry must not artificially win over a
	// measured peer. Cold candidate falls back to single-CPU-equivalent
	// capacity so the compute term remains on par with healthy nodes.
	cold := peerKey(1)
	measured := peerKey(2)
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			cold:     {}, // no telemetry
			measured: node(coords.Coord{X: 0, Y: 0}, 1),
		},
		ComputeCost: map[string]float64{"seed": 25.0},
	}
	require.GreaterOrEqual(t, predictedLatency(cold, "seed", cluster),
		predictedLatency(measured, "seed", cluster),
		"cold candidate should not undercut a measured peer of equal capacity")
}

func TestPredictedLatency_SteadyState_DemandCentroidWins(t *testing.T) {
	// Demand originates concentrated at one peer. The candidate colocated
	// with the demand source sits on the centroid (zero weighted RTT);
	// any peer further away pays distance × share.
	near := peerKey(1)
	far := peerKey(2)
	demandSrc := peerKey(3)

	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			near:      node(coords.Coord{X: 0, Y: 0}, 4),
			far:       node(coords.Coord{X: 200, Y: 0}, 4),
			demandSrc: node(coords.Coord{X: 0, Y: 0}, 4),
		},
		InvocationRates: map[string]float64{"hot": 100.0},
		OriginRates: map[string]map[types.PeerKey]float64{
			"hot": {demandSrc: 100.0},
		},
	}

	nearScore := predictedLatency(near, "hot", cluster)
	farScore := predictedLatency(far, "hot", cluster)

	require.Equal(t, 0.0, nearScore, "candidate colocated with demand source scores zero")
	require.Greater(t, farScore, nearScore)
	expectedFar := coords.Distance(coords.Coord{X: 200, Y: 0}, coords.Coord{X: 0, Y: 0})
	require.InDelta(t, expectedFar, farScore, 0.001)
}

func TestPredictedLatency_SteadyState_MultipleDemandSources(t *testing.T) {
	// Two demand sources of equal weight at opposite ends along the X-axis.
	// A candidate on the same X-span scores well; a candidate off-axis sits
	// far from both demand sources and scores worse.
	mid := peerKey(1)
	offAxis := peerKey(2)
	srcA := peerKey(3)
	srcB := peerKey(4)

	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			mid:     node(coords.Coord{X: 50, Y: 0}, 4),
			offAxis: node(coords.Coord{X: 50, Y: 1000}, 4),
			srcA:    node(coords.Coord{X: 0, Y: 0}, 4),
			srcB:    node(coords.Coord{X: 100, Y: 0}, 4),
		},
		InvocationRates: map[string]float64{"seed": 100.0},
		OriginRates: map[string]map[types.PeerKey]float64{
			"seed": {srcA: 50.0, srcB: 50.0},
		},
	}

	midScore := predictedLatency(mid, "seed", cluster)
	offAxisScore := predictedLatency(offAxis, "seed", cluster)

	require.Less(t, midScore, offAxisScore, "on-axis midpoint beats off-axis candidate at same X")
}

func TestPredictedLatency_SteadyState_ShareWeighting(t *testing.T) {
	// Demand is 80% at src1 and 20% at src2. A candidate near src1 should
	// score better than one equidistant from both — share-weighted RTT
	// amplifies proximity to the heavier source.
	nearMajor := peerKey(1)
	nearMinor := peerKey(2)
	src1 := peerKey(3)
	src2 := peerKey(4)

	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			nearMajor: node(coords.Coord{X: 0, Y: 0}, 4),
			nearMinor: node(coords.Coord{X: 100, Y: 0}, 4),
			src1:      node(coords.Coord{X: 0, Y: 0}, 4),
			src2:      node(coords.Coord{X: 100, Y: 0}, 4),
		},
		InvocationRates: map[string]float64{"seed": 100.0},
		OriginRates: map[string]map[types.PeerKey]float64{
			"seed": {src1: 80.0, src2: 20.0},
		},
	}

	require.Less(t, predictedLatency(nearMajor, "seed", cluster),
		predictedLatency(nearMinor, "seed", cluster),
		"candidate near the majority demand source must outrank the minor-side twin")
}

func TestPredictedLatency_SteadyState_ColocatedHostWins(t *testing.T) {
	// The classic "A calls B" scenario. X hosts A and is the sole origin
	// for B's demand. The centroid lives at X — placing B at X is the
	// unambiguous best, beating a far-away twin of identical capacity.
	xHost := peerKey(1)
	farTwin := peerKey(2)

	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			xHost:   node(coords.Coord{X: 0, Y: 0}, 1),
			farTwin: node(coords.Coord{X: 150, Y: 0}, 1),
		},
		InvocationRates: map[string]float64{"b": 1000.0},
		OriginRates: map[string]map[types.PeerKey]float64{
			"b": {xHost: 1000.0},
		},
	}

	require.Equal(t, 0.0, predictedLatency(xHost, "b", cluster),
		"colocated-with-demand-source candidate scores zero")
	require.Greater(t, predictedLatency(farTwin, "b", cluster), 0.0)
}

func TestPredictedLatency_SelfOriginDiscount(t *testing.T) {
	// A peer absorbing demand locally must strictly beat a demand-free
	// peer sitting at the geographic centroid of the demand distribution.
	// Without the self-origin discount, the two tie on weighted-avg
	// distance and a proxy node wins the deterministic hash tiebreak — a
	// hot entry point would never claim despite generating the traffic.
	srcA := peerKey(1)
	srcB := peerKey(2)
	central := peerKey(3)

	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			srcA:    node(coords.Coord{X: 0, Y: 0}, 1),
			srcB:    node(coords.Coord{X: 100, Y: 0}, 1),
			central: node(coords.Coord{X: 50, Y: 0}, 1),
		},
		InvocationRates: map[string]float64{"seed": 100.0},
		OriginRates: map[string]map[types.PeerKey]float64{
			"seed": {srcA: 50.0, srcB: 50.0},
		},
	}

	require.Less(t, predictedLatency(srcA, "seed", cluster),
		predictedLatency(central, "seed", cluster),
		"demand-source candidate must beat centrally-placed demand-free peer")
}

func TestPredictedLatency_ObservedFloor_UsesColdStart(t *testing.T) {
	// InvocationRate below the observed floor means we don't yet trust
	// the demand distribution. Score must fall back to cold-start
	// (compute/capacity), not return centroid noise.
	a := peerKey(1)
	b := peerKey(2)
	src := peerKey(3)

	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			a:   node(coords.Coord{X: 0, Y: 0}, 1),
			b:   node(coords.Coord{X: 0, Y: 0}, 4),
			src: node(coords.Coord{X: 500, Y: 0}, 1),
		},
		ComputeCost:     map[string]float64{"seed": 20.0},
		InvocationRates: map[string]float64{"seed": 0.1}, // below floor
		OriginRates: map[string]map[types.PeerKey]float64{
			"seed": {src: 0.1},
		},
	}
	// Scoring should rank by compute/capacity alone — larger CPU wins.
	require.Greater(t, predictedLatency(a, "seed", cluster), predictedLatency(b, "seed", cluster))
}

func TestHasCapacity_RejectsCpuExhausted(t *testing.T) {
	pk := peerKey(1)
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			pk: {
				NumCPU:           4,
				CPUBudgetPercent: 80,
				CPUPercent:       80,
				MemTotalBytes:    8 << 30, //nolint:mnd
				MemBudgetPercent: 100,
			},
		},
	}
	require.False(t, hasCapacity(pk, 0, cluster))
}

func TestHasCapacity_RejectsMemoryExhausted(t *testing.T) {
	pk := peerKey(1)
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			pk: {
				NumCPU:           4,
				CPUBudgetPercent: 100,
				CPUPercent:       0,
				MemTotalBytes:    1 << 30, //nolint:mnd
				MemBudgetPercent: 100,
				MemPercent:       100,
			},
		},
	}
	require.False(t, hasCapacity(pk, 1<<29, cluster)) //nolint:mnd
}

func TestHasCapacity_AdmitsHealthyNode(t *testing.T) {
	pk := peerKey(1)
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			pk: {
				NumCPU:           4,
				CPUBudgetPercent: 100,
				CPUPercent:       10,
				MemTotalBytes:    8 << 30, //nolint:mnd
				MemBudgetPercent: 100,
				MemPercent:       10,
			},
		},
	}
	require.True(t, hasCapacity(pk, 64<<20, cluster)) //nolint:mnd
}

func TestHasCapacity_NoTelemetry_AllowsPlacement(t *testing.T) {
	pk := peerKey(1)
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{pk: {}},
	}
	require.True(t, hasCapacity(pk, 64<<20, cluster)) //nolint:mnd
}
