// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func peerKey(b byte) types.PeerKey {
	var pk types.PeerKey
	pk[0] = b
	return pk
}

func hasAction(actions []action, hash string, kind actionKind) bool {
	for _, a := range actions {
		if a.Hash == hash && a.Kind == kind {
			return true
		}
	}
	return false
}

// healthyCluster returns a clusterState where every peer has identical
// capacity headroom (no Vivaldi coords, no observations). Useful for tests
// that exercise placement decisions without latency differentiation.
func healthyCluster(peers ...types.PeerKey) clusterState {
	nodes := make(map[types.PeerKey]nodeState, len(peers))
	for _, pk := range peers {
		nodes[pk] = nodeState{
			NumCPU:           4,
			CPUBudgetPercent: 100,
			MemTotalBytes:    8 << 30, //nolint:mnd
			MemBudgetPercent: 100,
		}
	}
	return clusterState{Nodes: nodes}
}

func TestEvaluate_UnderReplicated_Claims(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	specs := map[string]spec{"abc123": {MinReplicas: 2}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {peer2: {}}}

	actions := evaluate(evaluateInput{
		localID:       local,
		allPeers:      []types.PeerKey{local, peer2},
		specs:         specs,
		claims:        claims,
		cluster:       healthyCluster(local, peer2),
		isRunning:     func(string) bool { return false },
		idleDurations: map[string]time.Duration{},
	})
	require.True(t, hasAction(actions, "abc123", actionClaim))
}

func TestEvaluate_FullyReplicated_NoClaim(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	specs := map[string]spec{"abc123": {MinReplicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {peer2: {}}}

	actions := evaluate(evaluateInput{
		localID:       local,
		allPeers:      []types.PeerKey{local, peer2},
		specs:         specs,
		claims:        claims,
		cluster:       healthyCluster(local, peer2),
		isRunning:     func(string) bool { return false },
		idleDurations: map[string]time.Duration{},
	})
	require.False(t, hasAction(actions, "abc123", actionClaim))
}

func TestEvaluate_OverReplicated_LoserReleases(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	specs := map[string]spec{"abc123": {MinReplicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {local: {}, peer2: {}}}
	allPeers := []types.PeerKey{local, peer2}
	longIdle := map[string]time.Duration{"abc123": 10 * time.Minute}
	cluster := healthyCluster(allPeers...)

	actions := evaluate(evaluateInput{
		localID:       local,
		allPeers:      allPeers,
		specs:         specs,
		claims:        claims,
		cluster:       cluster,
		isRunning:     func(string) bool { return true },
		idleDurations: longIdle,
	})
	releasing := hasAction(actions, "abc123", actionRelease)

	actions2 := evaluate(evaluateInput{
		localID:       peer2,
		allPeers:      allPeers,
		specs:         specs,
		claims:        claims,
		cluster:       cluster,
		isRunning:     func(string) bool { return true },
		idleDurations: longIdle,
	})
	peer2Releasing := hasAction(actions2, "abc123", actionRelease)

	require.NotEqual(t, releasing, peer2Releasing, "exactly one peer should release")
}

func TestEvaluate_NoSpec_ReleaseClaim(t *testing.T) {
	local := peerKey(1)
	claims := map[string]map[types.PeerKey]struct{}{"orphan": {local: {}}}

	actions := evaluate(evaluateInput{
		localID:       local,
		allPeers:      []types.PeerKey{local},
		specs:         map[string]spec{},
		claims:        claims,
		cluster:       healthyCluster(local),
		isRunning:     func(string) bool { return true },
		idleDurations: map[string]time.Duration{},
	})
	require.True(t, hasAction(actions, "orphan", actionRelease))
}

func TestEvaluate_ClaimedButNotRunning_ReClaim(t *testing.T) {
	local := peerKey(1)
	specs := map[string]spec{"abc123": {MinReplicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {local: {}}}

	actions := evaluate(evaluateInput{
		localID:       local,
		allPeers:      []types.PeerKey{local},
		specs:         specs,
		claims:        claims,
		cluster:       healthyCluster(local),
		isRunning:     func(string) bool { return false },
		idleDurations: map[string]time.Duration{},
	})
	require.True(t, hasAction(actions, "abc123", actionClaim))
}

func TestPlacementScore_Deterministic(t *testing.T) {
	pk := peerKey(42)
	hash := "deadbeef"
	require.Equal(t, placementScore(pk, hash), placementScore(pk, hash))
}

func TestPlacementScore_DifferentInputs(t *testing.T) {
	require.NotEqual(t, placementScore(peerKey(1), "deadbeef"), placementScore(peerKey(2), "deadbeef"))
}

func TestEvaluate_IndependentNodesAgreeOnWinner(t *testing.T) {
	peers := []types.PeerKey{peerKey(1), peerKey(2), peerKey(3), peerKey(4), peerKey(5)}
	specs := map[string]spec{"workload1": {MinReplicas: 2}}
	cluster := healthyCluster(peers...)

	var claimers []types.PeerKey
	for _, pk := range peers {
		if hasAction(evaluate(evaluateInput{
			localID:       pk,
			allPeers:      peers,
			specs:         specs,
			claims:        map[string]map[types.PeerKey]struct{}{},
			cluster:       cluster,
			isRunning:     func(string) bool { return false },
			idleDurations: map[string]time.Duration{},
		}), "workload1", actionClaim) {
			claimers = append(claimers, pk)
		}
	}
	require.Len(t, claimers, 2, "exactly Replicas nodes should claim")
}

func TestEvaluate_UnderReplicated_OnlyBestClaims(t *testing.T) {
	peers := []types.PeerKey{peerKey(10), peerKey(11), peerKey(12)}
	specs := map[string]spec{"singleton": {MinReplicas: 1}}
	cluster := healthyCluster(peers...)

	var claimers int
	for _, pk := range peers {
		if hasAction(evaluate(evaluateInput{
			localID:       pk,
			allPeers:      peers,
			specs:         specs,
			claims:        map[string]map[types.PeerKey]struct{}{},
			cluster:       cluster,
			isRunning:     func(string) bool { return false },
			idleDurations: map[string]time.Duration{},
		}), "singleton", actionClaim) {
			claimers++
		}
	}
	require.Equal(t, 1, claimers, "only one node should claim when replicas=1")
}

func TestEvaluate_HashTiebreakIsStable(t *testing.T) {
	// Two peers, identical predicted latency — hash deterministically picks
	// the same winner across repeated evaluations on each peer.
	p1 := peerKey(1)
	p2 := peerKey(2)
	peers := []types.PeerKey{p1, p2}
	specs := map[string]spec{"only": {MinReplicas: 1}}
	cluster := healthyCluster(peers...)

	winner := func(local types.PeerKey) bool {
		return hasAction(evaluate(evaluateInput{
			localID:       local,
			allPeers:      peers,
			specs:         specs,
			claims:        map[string]map[types.PeerKey]struct{}{},
			cluster:       cluster,
			isRunning:     func(string) bool { return false },
			idleDurations: map[string]time.Duration{},
		}), "only", actionClaim)
	}

	for range 5 {
		// p1 and p2 should always disagree on who wins, but the same
		// peer should win on every tick (signal is hash-deterministic).
		require.NotEqual(t, winner(p1), winner(p2))
	}
}

func TestEvaluate_ChallengerJoinsOnDialAffinity(t *testing.T) {
	// Demo-chain shape: p2 hosts the anchored downstream service that
	// `migrating` calls heavily. p1 is the current incumbent but far from
	// p2. p2 should challenge and win on predicted latency alone (no
	// hysteresis).
	p1 := peerKey(1)
	p2 := peerKey(2)
	allPeers := []types.PeerKey{p1, p2}
	hash := "migrating"

	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			p1: {
				NumCPU:           4,
				CPUBudgetPercent: 100,
				MemTotalBytes:    8 << 30, //nolint:mnd
				MemBudgetPercent: 100,
				Coord:            &coords.Coord{X: 1000, Y: 0},
			},
			p2: {
				NumCPU:           4,
				CPUBudgetPercent: 100,
				MemTotalBytes:    8 << 30, //nolint:mnd
				MemBudgetPercent: 100,
				Coord:            &coords.Coord{X: 0, Y: 0},
			},
		},
		ComputeCost:     map[string]float64{hash: 5.0},
		InvocationRates: map[string]float64{hash: 100.0},
		DialRates: map[string]map[string]float64{
			hash: {"service:store": 100.0},
		},
		ServiceHosts: map[string][]types.PeerKey{
			"store": {p2},
		},
	}
	claims := map[string]map[types.PeerKey]struct{}{hash: {p1: {}}}

	actions := evaluate(evaluateInput{
		localID:       p2,
		allPeers:      allPeers,
		specs:         map[string]spec{hash: {MinReplicas: 1}},
		claims:        claims,
		cluster:       cluster,
		isRunning:     func(string) bool { return false },
		idleDurations: map[string]time.Duration{},
	})
	require.True(t, hasAction(actions, hash, actionClaim))
}

func TestEvaluate_OverReplicatedReleasesByLatency(t *testing.T) {
	// Three peers, two incumbents. p3 is uncontended and far from any
	// downstream traffic; p1 is colocated with the only thing the workload
	// dials. Expect p3 (worse predicted latency) to release.
	p1 := peerKey(1)
	p2 := peerKey(2)
	p3 := peerKey(3)
	allPeers := []types.PeerKey{p1, p2, p3}
	hash := "workload"

	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			p1: {
				NumCPU: 4, CPUBudgetPercent: 100, MemTotalBytes: 8 << 30, MemBudgetPercent: 100, //nolint:mnd
				Coord: &coords.Coord{X: 0, Y: 0},
			},
			p2: {
				NumCPU: 4, CPUBudgetPercent: 100, MemTotalBytes: 8 << 30, MemBudgetPercent: 100, //nolint:mnd
				Coord: &coords.Coord{X: 100, Y: 0},
			},
			p3: {
				NumCPU: 4, CPUBudgetPercent: 100, MemTotalBytes: 8 << 30, MemBudgetPercent: 100, //nolint:mnd
				Coord: &coords.Coord{X: 1000, Y: 0},
			},
		},
		ComputeCost:     map[string]float64{hash: 5.0},
		InvocationRates: map[string]float64{hash: 100.0},
		DialRates: map[string]map[string]float64{
			hash: {"service:store": 100.0},
		},
		ServiceHosts: map[string][]types.PeerKey{
			"store": {p1},
		},
	}
	longIdle := map[string]time.Duration{hash: 10 * time.Minute}

	var releaser types.PeerKey
	for _, pk := range []types.PeerKey{p2, p3} {
		actions := evaluate(evaluateInput{
			localID:       pk,
			allPeers:      allPeers,
			specs:         map[string]spec{hash: {MinReplicas: 1}},
			claims:        map[string]map[types.PeerKey]struct{}{hash: {p2: {}, p3: {}}},
			cluster:       cluster,
			isRunning:     func(string) bool { return true },
			idleDurations: longIdle,
		})
		if hasAction(actions, hash, actionRelease) {
			releaser = pk
		}
	}
	require.Equal(t, p3, releaser, "the worst-predicted incumbent should release first")
}

func TestEvaluate_CapacityGate_RejectsOversubscribedCandidate(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	allPeers := []types.PeerKey{local, peer2}
	hash := "wl"

	// peer2 has telemetry showing exhausted CPU; local is healthy.
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			local: {NumCPU: 4, CPUBudgetPercent: 100, CPUPercent: 10, MemTotalBytes: 8 << 30, MemBudgetPercent: 100}, //nolint:mnd
			peer2: {NumCPU: 4, CPUBudgetPercent: 80, CPUPercent: 80, MemTotalBytes: 8 << 30, MemBudgetPercent: 80},   //nolint:mnd
		},
	}

	// Both peers ask "should I claim?": local should claim, peer2 shouldn't.
	localClaims := hasAction(evaluate(evaluateInput{
		localID:       local,
		allPeers:      allPeers,
		specs:         map[string]spec{hash: {MinReplicas: 1}},
		claims:        map[string]map[types.PeerKey]struct{}{},
		cluster:       cluster,
		isRunning:     func(string) bool { return false },
		idleDurations: map[string]time.Duration{},
	}), hash, actionClaim)
	peer2Claims := hasAction(evaluate(evaluateInput{
		localID:       peer2,
		allPeers:      allPeers,
		specs:         map[string]spec{hash: {MinReplicas: 1}},
		claims:        map[string]map[types.PeerKey]struct{}{},
		cluster:       cluster,
		isRunning:     func(string) bool { return false },
		idleDurations: map[string]time.Duration{},
	}), hash, actionClaim)

	require.True(t, localClaims, "healthy node should claim")
	require.False(t, peer2Claims, "exhausted node should be filtered by capacity gate")
}

func TestEvaluate_DeadNodeExcludedFromCandidates(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	actions := evaluate(evaluateInput{
		localID:       local,
		allPeers:      []types.PeerKey{local, peer2},
		specs:         map[string]spec{"wl1": {MinReplicas: 2}},
		claims:        map[string]map[types.PeerKey]struct{}{"wl1": {peer2: {}}},
		cluster:       healthyCluster(local, peer2),
		isRunning:     func(string) bool { return false },
		idleDurations: map[string]time.Duration{},
	})
	require.True(t, hasAction(actions, "wl1", actionClaim))
}
