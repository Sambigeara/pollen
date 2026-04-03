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
	specs := map[string]spec{"abc123": {Replicas: 2}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {peer2: {}}}

	actions := evaluate(local, []types.PeerKey{local, peer2}, specs, claims, clusterState{}, func(string) bool { return false })
	require.True(t, hasAction(actions, "abc123", actionClaim), "should claim under-replicated workload")
}

func TestEvaluate_FullyReplicated_NoClaim(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	specs := map[string]spec{"abc123": {Replicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {peer2: {}}}

	actions := evaluate(local, []types.PeerKey{local, peer2}, specs, claims, clusterState{}, func(string) bool { return false })
	require.False(t, hasAction(actions, "abc123", actionClaim), "should not claim fully-replicated workload")
}

func TestEvaluate_OverReplicated_LoserReleases(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	specs := map[string]spec{"abc123": {Replicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {local: {}, peer2: {}}}
	allPeers := []types.PeerKey{local, peer2}

	actions := evaluate(local, allPeers, specs, claims, clusterState{}, func(string) bool { return true })
	releasing := hasAction(actions, "abc123", actionRelease)

	actions2 := evaluate(peer2, allPeers, specs, claims, clusterState{}, func(string) bool { return true })
	peer2Releasing := hasAction(actions2, "abc123", actionRelease)

	require.NotEqual(t, releasing, peer2Releasing, "exactly one of the two peers should release")
}

func TestEvaluate_NoSpec_ReleaseClaim(t *testing.T) {
	local := peerKey(1)
	claims := map[string]map[types.PeerKey]struct{}{"orphan": {local: {}}}

	actions := evaluate(local, []types.PeerKey{local}, map[string]spec{}, claims, clusterState{}, func(string) bool { return true })
	require.True(t, hasAction(actions, "orphan", actionRelease), "should release claim with no matching spec")
}

func TestEvaluate_ClaimedButNotRunning_ReClaim(t *testing.T) {
	local := peerKey(1)
	specs := map[string]spec{"abc123": {Replicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {local: {}}}

	actions := evaluate(local, []types.PeerKey{local}, specs, claims, clusterState{}, func(string) bool { return false })
	require.True(t, hasAction(actions, "abc123", actionClaim), "should re-claim workload that is not running")
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
	specs := map[string]spec{"workload1": {Replicas: 2}}
	claims := map[string]map[types.PeerKey]struct{}{}

	var claimers []types.PeerKey
	for _, pk := range peers {
		if hasAction(evaluate(pk, peers, specs, claims, clusterState{}, func(string) bool { return false }), "workload1", actionClaim) {
			claimers = append(claimers, pk)
		}
	}
	require.Len(t, claimers, 2, "exactly Replicas nodes should claim")
}

func TestEvaluate_UnderReplicated_OnlyBestClaims(t *testing.T) {
	peers := []types.PeerKey{peerKey(10), peerKey(11), peerKey(12)}
	specs := map[string]spec{"singleton": {Replicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{}

	var claimers int
	for _, pk := range peers {
		if hasAction(evaluate(pk, peers, specs, claims, clusterState{}, func(string) bool { return false }), "singleton", actionClaim) {
			claimers++
		}
	}
	require.Equal(t, 1, claimers, "only one node should claim when replicas=1")
}

func TestSuitabilityScore_ColdStart(t *testing.T) {
	p1, p2 := peerKey(1), peerKey(2)
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 8, CPUPercent: 20, MemTotalBytes: 16 << 30, MemPercent: 30},
		p2: {NumCPU: 2, CPUPercent: 20, MemTotalBytes: 4 << 30, MemPercent: 30},
	}}

	s1 := suitabilityScore(p1, cluster, map[types.PeerKey]struct{}{}, false)
	s2 := suitabilityScore(p2, cluster, map[types.PeerKey]struct{}{}, false)
	require.Greater(t, s1, s2, "node with more free resources should score higher")
}

func TestSuitabilityScore_TrafficAffinity(t *testing.T) {
	p1, p2, claimant := peerKey(1), peerKey(2), peerKey(3)
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1:       {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, TrafficTo: map[types.PeerKey]uint64{claimant: 10000}},
		p2:       {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, TrafficTo: map[types.PeerKey]uint64{claimant: 1000}},
		claimant: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}

	s1 := suitabilityScore(p1, cluster, map[types.PeerKey]struct{}{claimant: {}}, false)
	s2 := suitabilityScore(p2, cluster, map[types.PeerKey]struct{}{claimant: {}}, false)
	require.Greater(t, s1, s2, "node with more traffic to claimants should score higher")
}

func TestSuitabilityScore_IncumbentBonus(t *testing.T) {
	p1, p2 := peerKey(1), peerKey(2)
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 60, MemTotalBytes: 8 << 30, MemPercent: 60},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}

	incumbentScore := suitabilityScore(p1, cluster, map[types.PeerKey]struct{}{}, true)
	challengerScore := suitabilityScore(p2, cluster, map[types.PeerKey]struct{}{}, false)
	require.Greater(t, incumbentScore, challengerScore, "incumbent with slightly lower raw score should still beat challenger due to bonus")
}

func TestSuitabilityScore_ChallengerOvertakesIncumbent(t *testing.T) {
	p1, p2 := peerKey(1), peerKey(2)
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 2, CPUPercent: 95, MemTotalBytes: 2 << 30, MemPercent: 95},
		p2: {NumCPU: 16, CPUPercent: 10, MemTotalBytes: 32 << 30, MemPercent: 10},
	}}

	incumbentScore := suitabilityScore(p1, cluster, map[types.PeerKey]struct{}{}, true)
	challengerScore := suitabilityScore(p2, cluster, map[types.PeerKey]struct{}{}, false)
	require.Greater(t, challengerScore, incumbentScore, "challenger with massively better raw score should overcome incumbent bonus")
}

func TestSuitabilityScore_VivaldiTieBreak(t *testing.T) {
	p1, p2, claimant := peerKey(1), peerKey(2), peerKey(3)
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1:       {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, Coord: &coords.Coord{X: 1, Y: 0}},
		p2:       {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, Coord: &coords.Coord{X: 100, Y: 0}},
		claimant: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, Coord: &coords.Coord{X: 0, Y: 0}},
	}}

	s1 := suitabilityScore(p1, cluster, map[types.PeerKey]struct{}{claimant: {}}, false)
	s2 := suitabilityScore(p2, cluster, map[types.PeerKey]struct{}{claimant: {}}, false)
	require.Greater(t, s1, s2, "closer node should score higher via Vivaldi")
}

func TestSuitabilityScore_HashFallback(t *testing.T) {
	p1, p2 := peerKey(1), peerKey(2)
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}
	specs := map[string]spec{"workload": {Replicas: 1}}

	peers := []types.PeerKey{p1, p2}
	var winner types.PeerKey
	for _, pk := range peers {
		if hasAction(evaluate(pk, peers, specs, map[string]map[types.PeerKey]struct{}{}, cluster, func(string) bool { return false }), "workload", actionClaim) {
			winner = pk
		}
	}
	require.NotEqual(t, types.PeerKey{}, winner, "exactly one peer should claim")
}

func TestSuitabilityScore_GracefulDegradation(t *testing.T) {
	p1, p2 := peerKey(1), peerKey(2)
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}

	s1 := suitabilityScore(p1, cluster, map[types.PeerKey]struct{}{}, false)
	s2 := suitabilityScore(p2, cluster, map[types.PeerKey]struct{}{}, false)
	require.Greater(t, s2, s1, "node with telemetry should beat node without")
	require.Greater(t, s1, 0.0, "node without telemetry should still have a non-zero score")
}

func TestEvaluate_MigrationOnTrafficShift(t *testing.T) {
	p1, p2, p3 := peerKey(1), peerKey(2), peerKey(3)
	allPeers := []types.PeerKey{p1, p2, p3}
	hash := "migrating"

	cluster1 := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p3: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}
	claims1 := map[string]map[types.PeerKey]struct{}{hash: {p1: {}}}

	actions := evaluate(p1, allPeers, map[string]spec{hash: {Replicas: 1}}, claims1, cluster1, func(string) bool { return true })
	require.False(t, hasAction(actions, hash, actionRelease), "incumbent should NOT release when all nodes are equal")

	cluster2 := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, TrafficTo: map[types.PeerKey]uint64{p1: 100000}},
		p3: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, TrafficTo: map[types.PeerKey]uint64{p1: 100000}},
	}}

	actions = evaluate(p1, allPeers, map[string]spec{hash: {Replicas: 1}}, claims1, cluster2, func(string) bool { return true })
	require.True(t, hasAction(actions, hash, actionRelease), "incumbent should release when challenger has significantly better traffic affinity")
}

func TestEvaluate_MigrationInhibitedByIncumbentBonus(t *testing.T) {
	p1, p2 := peerKey(1), peerKey(2)
	hash := "stable"
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 55, MemTotalBytes: 8 << 30, MemPercent: 55},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}

	actions := evaluate(p1, []types.PeerKey{p1, p2}, map[string]spec{hash: {Replicas: 1}}, map[string]map[types.PeerKey]struct{}{hash: {p1: {}}}, cluster, func(string) bool { return true })
	require.False(t, hasAction(actions, hash, actionRelease), "small advantage should not overcome incumbent bonus")
}

func TestEvaluate_OverReplicatedWithBetterNonClaimant(t *testing.T) {
	p1, p2, p3 := peerKey(1), peerKey(2), peerKey(3)
	allPeers := []types.PeerKey{p1, p2, p3}
	hash := "over-rep"

	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 70, MemTotalBytes: 8 << 30, MemPercent: 70},
		p2: {NumCPU: 4, CPUPercent: 60, MemTotalBytes: 8 << 30, MemPercent: 60},
		p3: {NumCPU: 8, CPUPercent: 10, MemTotalBytes: 16 << 30, MemPercent: 10},
	}}

	var releasers int
	for _, pk := range allPeers {
		if hasAction(evaluate(pk, allPeers, map[string]spec{hash: {Replicas: 1}}, map[string]map[types.PeerKey]struct{}{hash: {p1: {}, p2: {}}}, cluster, func(string) bool { return true }), hash, actionRelease) {
			releasers++
		}
	}
	require.Equal(t, 1, releasers, "exactly one incumbent should release during over-replication, not both")
}

func TestEvaluate_DeadNodeExcludedFromCandidates(t *testing.T) {
	local, peer2 := peerKey(1), peerKey(2)
	actions := evaluate(local, []types.PeerKey{local, peer2}, map[string]spec{"wl1": {Replicas: 2}}, map[string]map[types.PeerKey]struct{}{"wl1": {peer2: {}}}, clusterState{}, func(string) bool { return false })
	require.True(t, hasAction(actions, "wl1", actionClaim), "live node must claim when dead node is excluded from candidates")
}
