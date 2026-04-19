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

func TestPredictedLatency_ColdStart_AllZero(t *testing.T) {
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

func TestPredictedLatency_ComputeOnly_FavoursLargerCPU(t *testing.T) {
	small := peerKey(1)
	big := peerKey(2)
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			small: node(coords.Coord{X: 0, Y: 0}, 1),
			big:   node(coords.Coord{X: 0, Y: 0}, 4),
		},
		ComputeCost:     map[string]float64{"seedA": 40.0},
		InvocationRates: map[string]float64{"seedA": 100.0},
	}

	smallLatency := predictedLatency(small, "seedA", cluster)
	bigLatency := predictedLatency(big, "seedA", cluster)

	require.InDelta(t, 40.0, smallLatency, 0.01)
	require.InDelta(t, 10.0, bigLatency, 0.01)
	require.Greater(t, smallLatency, bigLatency)
}

func TestPredictedLatency_ServiceAffinity_FavoursColocatedCandidate(t *testing.T) {
	near := peerKey(1)
	far := peerKey(2)
	storeHost := peerKey(3)
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			near:      node(coords.Coord{X: 5, Y: 0}, 4),
			far:       node(coords.Coord{X: 200, Y: 0}, 4),
			storeHost: node(coords.Coord{X: 0, Y: 0}, 4),
		},
		ComputeCost:     map[string]float64{"sink": 10.0},
		InvocationRates: map[string]float64{"sink": 100.0},
		DialRates: map[string]map[string]float64{
			"sink": {"service:store": 100.0}, // 1 call per invocation
		},
		ServiceHosts: map[string][]types.PeerKey{
			"store": {storeHost},
		},
	}

	require.Less(t, predictedLatency(near, "sink", cluster), predictedLatency(far, "sink", cluster))
}

func TestPredictedLatency_MultiReplicaTarget_UsesMinRTT(t *testing.T) {
	candidate := peerKey(1)
	closeReplica := peerKey(2)
	farReplica := peerKey(3)

	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			candidate:    node(coords.Coord{X: 0, Y: 0}, 4),
			closeReplica: node(coords.Coord{X: 5, Y: 0}, 4),
			farReplica:   node(coords.Coord{X: 1000, Y: 0}, 4),
		},
		ComputeCost:     map[string]float64{"caller": 0},
		InvocationRates: map[string]float64{"caller": 100.0},
		DialRates: map[string]map[string]float64{
			"caller": {"seed:downstream": 100.0},
		},
		SeedHosts: map[string][]types.PeerKey{
			"downstream": {closeReplica, farReplica},
		},
	}

	got := predictedLatency(candidate, "caller", cluster)
	want := coords.Distance(coords.Coord{X: 0, Y: 0}, coords.Coord{X: 5, Y: 0})
	require.InDelta(t, want, got, 0.001)
}

func TestPredictedLatency_CandidateColocatedWithTarget_ZeroRTT(t *testing.T) {
	candidate := peerKey(1)
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			candidate: node(coords.Coord{X: 0, Y: 0}, 4),
		},
		ComputeCost:     map[string]float64{"sink": 0},
		InvocationRates: map[string]float64{"sink": 100.0},
		DialRates: map[string]map[string]float64{
			"sink": {"service:store": 100.0},
		},
		ServiceHosts: map[string][]types.PeerKey{
			"store": {candidate},
		},
	}
	require.Equal(t, 0.0, predictedLatency(candidate, "sink", cluster))
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

func TestPredictedLatency_NoTelemetry_DoesNotBeatMeasuredPeer(t *testing.T) {
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
		ComputeCost:     map[string]float64{"seed": 25.0},
		InvocationRates: map[string]float64{"seed": 100.0},
	}
	require.GreaterOrEqual(t, predictedLatency(cold, "seed", cluster),
		predictedLatency(measured, "seed", cluster),
		"cold candidate should not undercut a measured peer of equal capacity")
}
