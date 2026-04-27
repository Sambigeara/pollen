// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/state"
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
		OriginRates: map[string]map[types.PeerKey]float64{
			"b": {xHost: 1000.0},
		},
	}

	require.Equal(t, 0.0, predictedLatency(xHost, "b", cluster),
		"colocated-with-demand-source candidate scores zero")
	require.Greater(t, predictedLatency(farTwin, "b", cluster), 0.0)
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
		ComputeCost: map[string]float64{"seed": 20.0},
		OriginRates: map[string]map[types.PeerKey]float64{
			"seed": {src: 0.1},
		},
	}
	// Scoring should rank by compute/capacity alone — larger CPU wins.
	require.Greater(t, predictedLatency(a, "seed", cluster), predictedLatency(b, "seed", cluster))
}

func TestPredictedLatency_OverloadKeepsWarmPath(t *testing.T) {
	// Saturation depresses ServedRate (rejections don't serve), but
	// OriginRate still reflects offered load. The warm-path gate
	// reads sum-of-OriginRate so scoring stays warm and centroid-based
	// exactly when the demand signal matters most.
	a := peerKey(1)
	b := peerKey(2)
	src := peerKey(3)

	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			a:   node(coords.Coord{X: 0, Y: 0}, 4),
			b:   node(coords.Coord{X: 1000, Y: 0}, 4),
			src: node(coords.Coord{X: 0, Y: 0}, 1),
		},
		ComputeCost: map[string]float64{"seed": 20.0},
		OriginRates: map[string]map[types.PeerKey]float64{
			"seed": {src: 100.0}, // genuine offered load
		},
	}
	// a is co-located with src (distance 0); b is far. Warm centroid
	// scoring picks a.
	require.Less(t, predictedLatency(a, "seed", cluster), predictedLatency(b, "seed", cluster))
}

func TestPredictedLatency_CoordBootstrap_UnknownDoesNotTieCentroid(t *testing.T) {
	// Bootstrap hole: with only one known coord no pairwise samples
	// exist, MeanCoordDistance stays 0, and distance(unknown, known)
	// would return 0 — falsely tying a missing-coord candidate with a
	// peer sitting on the demand centroid. The unknown candidate
	// switches to cold-start (compute/capacity) so capacity
	// differentiates it from the centroid-colocated peer.
	known := peerKey(1)
	unknown := peerKey(2) // no coord
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			known:   node(coords.Coord{X: 0, Y: 0}, 4),
			unknown: {NumCPU: 4, CPUBudgetPercent: 100}, // no Coord
		},
		ComputeCost: map[string]float64{"seed": 40.0},
		OriginRates: map[string]map[types.PeerKey]float64{
			"seed": {known: 100.0},
		},
		// MeanCoordDistance left zero — only one known coord.
	}
	knownScore := predictedLatency(known, "seed", cluster)
	unknownScore := predictedLatency(unknown, "seed", cluster)
	require.Equal(t, 0.0, knownScore, "known peer is colocated with the demand source")
	require.Greater(t, unknownScore, 0.0,
		"unknown candidate must NOT spuriously tie the centroid at 0; cold-start gives capacity-based score")
}

func TestDistance_MissingCoordReturnsMean(t *testing.T) {
	// A candidate or peer without a published coord must not score
	// distance == 0 — that would falsely reward unknown topology over
	// every measured peer. Substituting the cluster-mean keeps the
	// candidate at average rather than dominant.
	known := peerKey(1)
	unknown := peerKey(2)
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			known:   {Coord: &coords.Coord{X: 0, Y: 0}},
			unknown: {}, // no coord
		},
		MeanCoordDistance: 12.5,
	}
	require.Equal(t, 12.5, distance(known, unknown, cluster))
	require.Equal(t, 12.5, distance(unknown, known, cluster))
	require.Equal(t, 0.0, distance(unknown, unknown, cluster), "self-pair stays 0")
	require.Equal(t, 0.0, distance(known, known, cluster), "self-pair stays 0")
}

func TestRankTopN_ShuffleInvariance(t *testing.T) {
	// rankTopN's output must be a pure function of the candidate set
	// and the cluster state — input ordering must NOT change which
	// peer wins. Without sorted iteration in the FP-summing helpers
	// (originSum, predictedLatency, meanCoordDistance) two peers
	// reading the same gossip can disagree on the winner because
	// float-summation is non-associative. Shuffle the input slice and
	// confirm the same set of winners across N permutations.
	peers := make([]types.PeerKey, 7)
	for i := range peers {
		peers[i] = peerKey(byte(i + 1))
	}
	src := peerKey(99)
	nodes := map[types.PeerKey]nodeState{
		src: node(coords.Coord{X: 0, Y: 0}, 4),
	}
	for i, pk := range peers {
		// Spread peers across coordinate space so distance contributes
		// a non-trivial summed value to the score.
		nodes[pk] = node(coords.Coord{X: float64(i * 25), Y: float64(i * 7)}, 4)
	}
	cluster := clusterState{
		Nodes: nodes,
		OriginRates: map[string]map[types.PeerKey]float64{
			"hot": {
				peers[0]: 17,
				peers[2]: 53,
				peers[4]: 31,
				src:      120,
			},
		},
	}
	cluster.MeanCoordDistance = meanCoordDistance(cluster.Nodes)

	const top = 3
	baseline := topN(peers, "hot", cluster, top)

	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec
	for trial := range 16 {
		shuffled := slices.Clone(peers)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		got := topN(shuffled, "hot", cluster, top)
		require.Equal(t, baseline, got, "trial %d permutation must produce identical top-%d winner set", trial, top)
	}
}

// topN returns the top-n candidates by predictedLatency with the same
// hash tiebreak rankTopN uses, exposing the ordering that rankTopN
// reduces to a single boolean.
func topN(candidates []types.PeerKey, hash string, cluster clusterState, n int) []types.PeerKey {
	scored := make([]scored, 0, len(candidates))
	for _, pk := range candidates {
		scored = append(scored, newScored(pk, hash, cluster))
	}
	slices.SortFunc(scored, compareScored)
	if n > len(scored) {
		n = len(scored)
	}
	out := make([]types.PeerKey, 0, n)
	for _, s := range scored[:n] {
		out = append(out, s.peer)
	}
	return out
}

func TestHasCapacity_RejectsAdmissionClosed(t *testing.T) {
	// Symmetric with routing's Closed exclusion (latency.eligibleRemotes):
	// a peer that won't accept work mustn't pass the placement filter
	// either, otherwise an over-pressured node keeps winning fresh
	// claims while pickP2C correctly routes around it — split-brain.
	pk := peerKey(1)
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			pk: {
				NumCPU:           4,
				MemTotalBytes:    8 << 30, //nolint:mnd
				CPUBudgetPercent: 100,
				MemBudgetPercent: 100,
				CPUPercent:       1,
				MemPercent:       1,
				AdmissionState:   state.AdmissionClosed,
			},
		},
	}
	require.False(t, hasCapacity(pk, 0, cluster), "Closed peers must not pass placement")
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
