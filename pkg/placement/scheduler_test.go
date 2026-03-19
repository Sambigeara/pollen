package placement

import (
	"testing"

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

func TestEvaluate_UnderReplicated_Claims(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	specs := map[string]spec{
		"abc123": {Replicas: 2},
	}
	claims := map[string]map[types.PeerKey]struct{}{
		"abc123": {peer2: {}},
	}
	allPeers := []types.PeerKey{local, peer2}
	isRunning := func(string) bool { return false }

	actions := evaluate(local, allPeers, specs, claims, clusterState{}, isRunning)
	require.True(t, hasAction(actions, "abc123", actionClaim), "should claim under-replicated workload")
}

func TestEvaluate_FullyReplicated_NoClaim(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	specs := map[string]spec{
		"abc123": {Replicas: 1},
	}
	claims := map[string]map[types.PeerKey]struct{}{
		"abc123": {peer2: {}},
	}
	allPeers := []types.PeerKey{local, peer2}
	isRunning := func(string) bool { return false }

	actions := evaluate(local, allPeers, specs, claims, clusterState{}, isRunning)
	require.False(t, hasAction(actions, "abc123", actionClaim), "should not claim fully-replicated workload")
}

func TestEvaluate_OverReplicated_LoserReleases(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)

	specs := map[string]spec{
		"abc123": {Replicas: 1},
	}
	claims := map[string]map[types.PeerKey]struct{}{
		"abc123": {local: {}, peer2: {}},
	}
	allPeers := []types.PeerKey{local, peer2}
	isRunning := func(string) bool { return true }

	actions := evaluate(local, allPeers, specs, claims, clusterState{}, isRunning)
	releasing := hasAction(actions, "abc123", actionRelease)

	actions2 := evaluate(peer2, allPeers, specs, claims, clusterState{}, isRunning)
	peer2Releasing := hasAction(actions2, "abc123", actionRelease)

	require.NotEqual(t, releasing, peer2Releasing,
		"exactly one of the two peers should release; local=%v peer2=%v", releasing, peer2Releasing)
}

func TestEvaluate_NoSpec_ReleaseClaim(t *testing.T) {
	local := peerKey(1)
	specs := map[string]spec{}
	claims := map[string]map[types.PeerKey]struct{}{
		"orphan": {local: {}},
	}
	allPeers := []types.PeerKey{local}
	isRunning := func(string) bool { return true }

	actions := evaluate(local, allPeers, specs, claims, clusterState{}, isRunning)
	require.True(t, hasAction(actions, "orphan", actionRelease), "should release claim with no matching spec")
}

func TestEvaluate_ClaimedButNotRunning_ReClaim(t *testing.T) {
	local := peerKey(1)
	specs := map[string]spec{
		"abc123": {Replicas: 1},
	}
	claims := map[string]map[types.PeerKey]struct{}{
		"abc123": {local: {}},
	}
	allPeers := []types.PeerKey{local}
	isRunning := func(string) bool { return false }

	actions := evaluate(local, allPeers, specs, claims, clusterState{}, isRunning)
	require.True(t, hasAction(actions, "abc123", actionClaim), "should re-claim workload that is not running")
}

func TestPlacementScore_Deterministic(t *testing.T) {
	pk := peerKey(42)
	hash := "deadbeef"

	s1 := placementScore(pk, hash)
	s2 := placementScore(pk, hash)
	require.Equal(t, s1, s2)
}

func TestPlacementScore_DifferentInputs(t *testing.T) {
	pk1 := peerKey(1)
	pk2 := peerKey(2)
	hash := "deadbeef"

	s1 := placementScore(pk1, hash)
	s2 := placementScore(pk2, hash)
	require.NotEqual(t, s1, s2)
}

func TestEvaluate_IndependentNodesAgreeOnWinner(t *testing.T) {
	peers := make([]types.PeerKey, 5)
	for i := range peers {
		peers[i] = peerKey(byte(i + 1))
	}

	specs := map[string]spec{
		"workload1": {Replicas: 2},
	}
	claims := map[string]map[types.PeerKey]struct{}{}
	isRunning := func(string) bool { return false }

	var claimers []types.PeerKey
	for _, pk := range peers {
		if hasAction(evaluate(pk, peers, specs, claims, clusterState{}, isRunning), "workload1", actionClaim) {
			claimers = append(claimers, pk)
		}
	}
	require.Len(t, claimers, 2, "exactly Replicas nodes should claim")

	var claimers2 []types.PeerKey
	for _, pk := range peers {
		if hasAction(evaluate(pk, peers, specs, claims, clusterState{}, isRunning), "workload1", actionClaim) {
			claimers2 = append(claimers2, pk)
		}
	}
	require.Equal(t, claimers, claimers2, "repeated evaluation must produce identical winners")
}

func TestEvaluate_UnderReplicated_OnlyBestClaims(t *testing.T) {
	peers := make([]types.PeerKey, 3)
	for i := range peers {
		peers[i] = peerKey(byte(i + 10))
	}

	specs := map[string]spec{
		"singleton": {Replicas: 1},
	}
	claims := map[string]map[types.PeerKey]struct{}{}
	isRunning := func(string) bool { return false }

	var claimers int
	for _, pk := range peers {
		if hasAction(evaluate(pk, peers, specs, claims, clusterState{}, isRunning), "singleton", actionClaim) {
			claimers++
		}
	}
	require.Equal(t, 1, claimers, "only one node should claim when replicas=1")
}

// --- Suitability scoring tests ---

func TestSuitabilityScore_ColdStart(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)

	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 8, CPUPercent: 20, MemTotalBytes: 16 << 30, MemPercent: 30},
		p2: {NumCPU: 2, CPUPercent: 20, MemTotalBytes: 4 << 30, MemPercent: 30},
	}}
	claimants := map[types.PeerKey]struct{}{}

	s1 := suitabilityScore(p1, cluster, claimants, false)
	s2 := suitabilityScore(p2, cluster, claimants, false)
	require.Greater(t, s1, s2, "node with more free resources should score higher")
}

func TestSuitabilityScore_TrafficAffinity(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)
	claimant := peerKey(3)

	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {
			NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50,
			TrafficTo: map[types.PeerKey]uint64{claimant: 10000},
		},
		p2: {
			NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50,
			TrafficTo: map[types.PeerKey]uint64{claimant: 1000},
		},
		claimant: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}
	claimants := map[types.PeerKey]struct{}{claimant: {}}

	s1 := suitabilityScore(p1, cluster, claimants, false)
	s2 := suitabilityScore(p2, cluster, claimants, false)
	require.Greater(t, s1, s2, "node with more traffic to claimants should score higher")
}

func TestSuitabilityScore_IncumbentBonus(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)

	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 60, MemTotalBytes: 8 << 30, MemPercent: 60},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}
	claimants := map[types.PeerKey]struct{}{}

	incumbentScore := suitabilityScore(p1, cluster, claimants, true)
	challengerScore := suitabilityScore(p2, cluster, claimants, false)
	require.Greater(t, incumbentScore, challengerScore,
		"incumbent with slightly lower raw score should still beat challenger due to bonus")
}

func TestSuitabilityScore_ChallengerOvertakesIncumbent(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)

	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 2, CPUPercent: 95, MemTotalBytes: 2 << 30, MemPercent: 95},
		p2: {NumCPU: 16, CPUPercent: 10, MemTotalBytes: 32 << 30, MemPercent: 10},
	}}
	claimants := map[types.PeerKey]struct{}{}

	incumbentScore := suitabilityScore(p1, cluster, claimants, true)
	challengerScore := suitabilityScore(p2, cluster, claimants, false)
	require.Greater(t, challengerScore, incumbentScore,
		"challenger with massively better raw score should overcome incumbent bonus")
}

func TestSuitabilityScore_VivaldiTieBreak(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)
	claimant := peerKey(3)

	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {
			NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50,
			Coord: &coords.Coord{X: 1, Y: 0},
		},
		p2: {
			NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50,
			Coord: &coords.Coord{X: 100, Y: 0},
		},
		claimant: {
			NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50,
			Coord: &coords.Coord{X: 0, Y: 0},
		},
	}}
	claimants := map[types.PeerKey]struct{}{claimant: {}}

	s1 := suitabilityScore(p1, cluster, claimants, false)
	s2 := suitabilityScore(p2, cluster, claimants, false)
	require.Greater(t, s1, s2, "closer node should score higher via Vivaldi")
}

func TestSuitabilityScore_HashFallback(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)

	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}

	specs := map[string]spec{"workload": {Replicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{}
	isRunning := func(string) bool { return false }

	peers := []types.PeerKey{p1, p2}
	var winner types.PeerKey
	for _, pk := range peers {
		if hasAction(evaluate(pk, peers, specs, claims, cluster, isRunning), "workload", actionClaim) {
			winner = pk
		}
	}
	require.NotEqual(t, types.PeerKey{}, winner, "exactly one peer should claim")

	var winner2 types.PeerKey
	for _, pk := range peers {
		if hasAction(evaluate(pk, peers, specs, claims, cluster, isRunning), "workload", actionClaim) {
			winner2 = pk
		}
	}
	require.Equal(t, winner, winner2, "hash tie-break must be deterministic")
}

func TestSuitabilityScore_GracefulDegradation(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)

	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}
	claimants := map[types.PeerKey]struct{}{}

	s1 := suitabilityScore(p1, cluster, claimants, false)
	s2 := suitabilityScore(p2, cluster, claimants, false)
	require.Greater(t, s2, s1, "node with telemetry should beat node without")
	require.Greater(t, s1, 0.0, "node without telemetry should still have a non-zero score")
}

func TestEvaluate_IndependentNodesAgreeWithClusterState(t *testing.T) {
	peers := make([]types.PeerKey, 5)
	nodes := make(map[types.PeerKey]nodeState, 5)
	for i := range peers {
		peers[i] = peerKey(byte(i + 1))
		nodes[peers[i]] = nodeState{
			NumCPU:        uint32(4 + i*2),     //nolint:gosec
			CPUPercent:    uint32(20 + i*10),   //nolint:gosec
			MemTotalBytes: uint64(8+i*4) << 30, //nolint:gosec
			MemPercent:    uint32(30 + i*5),    //nolint:gosec
		}
	}
	cluster := clusterState{Nodes: nodes}

	specs := map[string]spec{"workload1": {Replicas: 2}}
	claims := map[string]map[types.PeerKey]struct{}{}
	isRunning := func(string) bool { return false }

	var claimers []types.PeerKey
	for _, pk := range peers {
		if hasAction(evaluate(pk, peers, specs, claims, cluster, isRunning), "workload1", actionClaim) {
			claimers = append(claimers, pk)
		}
	}
	require.Len(t, claimers, 2, "exactly Replicas nodes should claim")

	var claimers2 []types.PeerKey
	for _, pk := range peers {
		if hasAction(evaluate(pk, peers, specs, claims, cluster, isRunning), "workload1", actionClaim) {
			claimers2 = append(claimers2, pk)
		}
	}
	require.Equal(t, claimers, claimers2, "repeated evaluation must produce identical winners")
}

func TestEvaluate_MigrationOnTrafficShift(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)
	p3 := peerKey(3)
	allPeers := []types.PeerKey{p1, p2, p3}
	hash := "migrating"
	isRunning := func(string) bool { return true }

	cluster1 := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p3: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}
	claims1 := map[string]map[types.PeerKey]struct{}{hash: {p1: {}}}

	actions := evaluate(p1, allPeers, map[string]spec{hash: {Replicas: 1}}, claims1, cluster1, isRunning)
	require.False(t, hasAction(actions, hash, actionRelease),
		"incumbent should NOT release when all nodes are equal (bonus protects)")

	cluster2 := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p2: {
			NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50,
			TrafficTo: map[types.PeerKey]uint64{p1: 100000},
		},
		p3: {
			NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50,
			TrafficTo: map[types.PeerKey]uint64{p1: 100000},
		},
	}}

	actions = evaluate(p1, allPeers, map[string]spec{hash: {Replicas: 1}}, claims1, cluster2, isRunning)
	require.True(t, hasAction(actions, hash, actionRelease),
		"incumbent should release when challenger has significantly better traffic affinity")

	claimsAfterRelease := map[string]map[types.PeerKey]struct{}{}
	actions = evaluate(p2, allPeers, map[string]spec{hash: {Replicas: 1}}, claimsAfterRelease, cluster2, func(string) bool { return false })
	require.True(t, hasAction(actions, hash, actionClaim),
		"challenger should claim after incumbent releases")
}

func TestEvaluate_MigrationInhibitedByIncumbentBonus(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)
	allPeers := []types.PeerKey{p1, p2}
	hash := "stable"
	claims := map[string]map[types.PeerKey]struct{}{hash: {p1: {}}}

	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 55, MemTotalBytes: 8 << 30, MemPercent: 55},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}

	actions := evaluate(p1, allPeers, map[string]spec{hash: {Replicas: 1}}, claims, cluster, func(string) bool { return true })
	require.False(t, hasAction(actions, hash, actionRelease), "small advantage should not overcome incumbent bonus")
}

func TestEvaluate_OverReplicatedWithBetterNonClaimant(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)
	p3 := peerKey(3)
	allPeers := []types.PeerKey{p1, p2, p3}
	hash := "over-rep"

	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 70, MemTotalBytes: 8 << 30, MemPercent: 70},
		p2: {NumCPU: 4, CPUPercent: 60, MemTotalBytes: 8 << 30, MemPercent: 60},
		p3: {NumCPU: 8, CPUPercent: 10, MemTotalBytes: 16 << 30, MemPercent: 10},
	}}
	claims := map[string]map[types.PeerKey]struct{}{hash: {p1: {}, p2: {}}}

	var releasers int
	for _, pk := range allPeers {
		if hasAction(evaluate(pk, allPeers, map[string]spec{hash: {Replicas: 1}}, claims, cluster, func(string) bool { return true }), hash, actionRelease) {
			releasers++
		}
	}
	require.Equal(t, 1, releasers, "exactly one incumbent should release during over-replication, not both")
}

func TestEvaluate_DeadNodeExcludedFromCandidates(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)

	specs := map[string]spec{
		"wl1": {Replicas: 2},
	}
	claims := map[string]map[types.PeerKey]struct{}{
		"wl1": {peer2: {}},
	}
	isRunning := func(string) bool { return false }

	livePeers := []types.PeerKey{local, peer2}
	actions := evaluate(local, livePeers, specs, claims, clusterState{}, isRunning)
	require.True(t, hasAction(actions, "wl1", actionClaim),
		"live node must claim when dead node is excluded from candidates")
}
