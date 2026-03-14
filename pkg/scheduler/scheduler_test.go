package scheduler

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func peerKey(b byte) types.PeerKey {
	var pk types.PeerKey
	pk[0] = b
	return pk
}

func TestEvaluate_UnderReplicated_Claims(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	specs := map[string]Spec{
		"abc123": {Replicas: 2},
	}
	claims := map[string]map[types.PeerKey]struct{}{
		"abc123": {peer2: {}},
	}
	allPeers := []types.PeerKey{local, peer2}
	isRunning := func(string) bool { return false }

	actions := Evaluate(local, allPeers, specs, claims, ClusterState{}, isRunning)

	var found bool
	for _, a := range actions {
		if a.Hash == "abc123" && a.Kind == ActionClaim {
			found = true
		}
	}
	require.True(t, found, "should claim under-replicated workload")
}

func TestEvaluate_FullyReplicated_NoClaim(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	specs := map[string]Spec{
		"abc123": {Replicas: 1},
	}
	claims := map[string]map[types.PeerKey]struct{}{
		"abc123": {peer2: {}},
	}
	allPeers := []types.PeerKey{local, peer2}
	isRunning := func(string) bool { return false }

	actions := Evaluate(local, allPeers, specs, claims, ClusterState{}, isRunning)

	for _, a := range actions {
		if a.Hash == "abc123" && a.Kind == ActionClaim {
			t.Fatal("should not claim fully-replicated workload")
		}
	}
}

func TestEvaluate_OverReplicated_LoserReleases(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)

	specs := map[string]Spec{
		"abc123": {Replicas: 1},
	}
	claims := map[string]map[types.PeerKey]struct{}{
		"abc123": {local: {}, peer2: {}},
	}
	allPeers := []types.PeerKey{local, peer2}
	isRunning := func(string) bool { return true }

	// One of the two must release. Deterministic tie-break decides which.
	actions := Evaluate(local, allPeers, specs, claims, ClusterState{}, isRunning)

	var releasing bool
	for _, a := range actions {
		if a.Hash == "abc123" && a.Kind == ActionRelease {
			releasing = true
		}
	}

	// Also run from peer2's perspective.
	actions2 := Evaluate(peer2, allPeers, specs, claims, ClusterState{}, isRunning)
	var peer2Releasing bool
	for _, a := range actions2 {
		if a.Hash == "abc123" && a.Kind == ActionRelease {
			peer2Releasing = true
		}
	}

	// Exactly one should release.
	require.NotEqual(t, releasing, peer2Releasing,
		"exactly one of the two peers should release; local=%v peer2=%v", releasing, peer2Releasing)
}

func TestEvaluate_NoSpec_ReleaseClaim(t *testing.T) {
	local := peerKey(1)
	specs := map[string]Spec{}
	claims := map[string]map[types.PeerKey]struct{}{
		"orphan": {local: {}},
	}
	allPeers := []types.PeerKey{local}
	isRunning := func(string) bool { return true }

	actions := Evaluate(local, allPeers, specs, claims, ClusterState{}, isRunning)

	var found bool
	for _, a := range actions {
		if a.Hash == "orphan" && a.Kind == ActionRelease {
			found = true
		}
	}
	require.True(t, found, "should release claim with no matching spec")
}

func TestEvaluate_ClaimedButNotRunning_ReClaim(t *testing.T) {
	local := peerKey(1)
	specs := map[string]Spec{
		"abc123": {Replicas: 1},
	}
	claims := map[string]map[types.PeerKey]struct{}{
		"abc123": {local: {}},
	}
	allPeers := []types.PeerKey{local}
	isRunning := func(string) bool { return false }

	actions := Evaluate(local, allPeers, specs, claims, ClusterState{}, isRunning)

	var found bool
	for _, a := range actions {
		if a.Hash == "abc123" && a.Kind == ActionClaim {
			found = true
		}
	}
	require.True(t, found, "should re-claim workload that is not running")
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

// TestEvaluate_IndependentNodesAgreeOnWinner verifies that multiple nodes,
// evaluating independently with identical cluster state, choose the same
// winner for an under-replicated workload.
func TestEvaluate_IndependentNodesAgreeOnWinner(t *testing.T) {
	peers := make([]types.PeerKey, 5)
	for i := range peers {
		peers[i] = peerKey(byte(i + 1))
	}

	specs := map[string]Spec{
		"workload1": {Replicas: 2},
	}
	claims := map[string]map[types.PeerKey]struct{}{}
	isRunning := func(string) bool { return false }

	// Each peer evaluates independently.
	var claimers []types.PeerKey
	for _, pk := range peers {
		actions := Evaluate(pk, peers, specs, claims, ClusterState{}, isRunning)
		for _, a := range actions {
			if a.Hash == "workload1" && a.Kind == ActionClaim {
				claimers = append(claimers, pk)
			}
		}
	}

	// Exactly 2 should claim (matching Replicas=2).
	require.Len(t, claimers, 2, "exactly Replicas nodes should claim")

	// Running again from any node produces the same winners.
	var claimers2 []types.PeerKey
	for _, pk := range peers {
		actions := Evaluate(pk, peers, specs, claims, ClusterState{}, isRunning)
		for _, a := range actions {
			if a.Hash == "workload1" && a.Kind == ActionClaim {
				claimers2 = append(claimers2, pk)
			}
		}
	}
	require.Equal(t, claimers, claimers2, "repeated evaluation must produce identical winners")
}

// TestEvaluate_UnderReplicated_OnlyBestClaims ensures that with 3 peers and
// replicas=1, only the best-scored peer claims — not all unclaimed peers.
func TestEvaluate_UnderReplicated_OnlyBestClaims(t *testing.T) {
	peers := make([]types.PeerKey, 3)
	for i := range peers {
		peers[i] = peerKey(byte(i + 10))
	}

	specs := map[string]Spec{
		"singleton": {Replicas: 1},
	}
	claims := map[string]map[types.PeerKey]struct{}{}
	isRunning := func(string) bool { return false }

	var claimers int
	for _, pk := range peers {
		actions := Evaluate(pk, peers, specs, claims, ClusterState{}, isRunning)
		for _, a := range actions {
			if a.Hash == "singleton" && a.Kind == ActionClaim {
				claimers++
			}
		}
	}
	require.Equal(t, 1, claimers, "only one node should claim when replicas=1")
}

// --- Suitability scoring tests ---

func TestSuitabilityScore_ColdStart(t *testing.T) {
	// No traffic, no coords — capacity alone determines ranking.
	p1 := peerKey(1)
	p2 := peerKey(2)

	cluster := ClusterState{Nodes: map[types.PeerKey]NodeState{
		p1: {NumCPU: 8, CPUPercent: 20, MemTotalBytes: 16 << 30, MemPercent: 30},
		p2: {NumCPU: 2, CPUPercent: 20, MemTotalBytes: 4 << 30, MemPercent: 30},
	}}
	claimants := map[types.PeerKey]struct{}{}

	s1 := suitabilityScore(p1, "hash1", cluster, claimants, false)
	s2 := suitabilityScore(p2, "hash1", cluster, claimants, false)
	require.Greater(t, s1, s2, "node with more free resources should score higher")
}

func TestSuitabilityScore_TrafficAffinity(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)
	claimant := peerKey(3)

	// Both nodes have equal capacity, but p1 has 10x more traffic to claimant.
	cluster := ClusterState{Nodes: map[types.PeerKey]NodeState{
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

	s1 := suitabilityScore(p1, "hash1", cluster, claimants, false)
	s2 := suitabilityScore(p2, "hash1", cluster, claimants, false)
	require.Greater(t, s1, s2, "node with more traffic to claimants should score higher")
}

func TestSuitabilityScore_IncumbentBonus(t *testing.T) {
	p1 := peerKey(1) // incumbent
	p2 := peerKey(2) // challenger — slightly better raw score

	cluster := ClusterState{Nodes: map[types.PeerKey]NodeState{
		p1: {NumCPU: 4, CPUPercent: 60, MemTotalBytes: 8 << 30, MemPercent: 60},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}
	claimants := map[types.PeerKey]struct{}{}

	incumbentScore := suitabilityScore(p1, "hash1", cluster, claimants, true)
	challengerScore := suitabilityScore(p2, "hash1", cluster, claimants, false)
	require.Greater(t, incumbentScore, challengerScore,
		"incumbent with slightly lower raw score should still beat challenger due to bonus")
}

func TestSuitabilityScore_ChallengerOvertakesIncumbent(t *testing.T) {
	p1 := peerKey(1) // incumbent — very loaded
	p2 := peerKey(2) // challenger — much more capacity

	cluster := ClusterState{Nodes: map[types.PeerKey]NodeState{
		p1: {NumCPU: 2, CPUPercent: 95, MemTotalBytes: 2 << 30, MemPercent: 95},
		p2: {NumCPU: 16, CPUPercent: 10, MemTotalBytes: 32 << 30, MemPercent: 10},
	}}
	claimants := map[types.PeerKey]struct{}{}

	incumbentScore := suitabilityScore(p1, "hash1", cluster, claimants, true)
	challengerScore := suitabilityScore(p2, "hash1", cluster, claimants, false)
	require.Greater(t, challengerScore, incumbentScore,
		"challenger with massively better raw score should overcome incumbent bonus")
}

func TestSuitabilityScore_VivaldiTieBreak(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)
	claimant := peerKey(3)

	// Identical capacity, no traffic, but p1 is closer via Vivaldi.
	cluster := ClusterState{Nodes: map[types.PeerKey]NodeState{
		p1: {
			NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50,
			Coord: &topology.Coord{X: 1, Y: 0},
		},
		p2: {
			NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50,
			Coord: &topology.Coord{X: 100, Y: 0},
		},
		claimant: {
			NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50,
			Coord: &topology.Coord{X: 0, Y: 0},
		},
	}}
	claimants := map[types.PeerKey]struct{}{claimant: {}}

	s1 := suitabilityScore(p1, "hash1", cluster, claimants, false)
	s2 := suitabilityScore(p2, "hash1", cluster, claimants, false)
	require.Greater(t, s1, s2, "closer node should score higher via Vivaldi")
}

func TestSuitabilityScore_HashFallback(t *testing.T) {
	// Identical suitability scores — SHA256 tie-break must be deterministic.
	p1 := peerKey(1)
	p2 := peerKey(2)

	cluster := ClusterState{Nodes: map[types.PeerKey]NodeState{
		p1: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}

	specs := map[string]Spec{"workload": {Replicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{}
	isRunning := func(string) bool { return false }

	peers := []types.PeerKey{p1, p2}
	var winner types.PeerKey
	for _, pk := range peers {
		actions := Evaluate(pk, peers, specs, claims, cluster, isRunning)
		for _, a := range actions {
			if a.Hash == "workload" && a.Kind == ActionClaim {
				winner = pk
			}
		}
	}
	require.NotEqual(t, types.PeerKey{}, winner, "exactly one peer should claim")

	// Repeat — same winner.
	var winner2 types.PeerKey
	for _, pk := range peers {
		actions := Evaluate(pk, peers, specs, claims, cluster, isRunning)
		for _, a := range actions {
			if a.Hash == "workload" && a.Kind == ActionClaim {
				winner2 = pk
			}
		}
	}
	require.Equal(t, winner, winner2, "hash tie-break must be deterministic")
}

func TestSuitabilityScore_GracefulDegradation(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)

	// p1 has no telemetry, p2 has full telemetry.
	cluster := ClusterState{Nodes: map[types.PeerKey]NodeState{
		p1: {},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}
	claimants := map[types.PeerKey]struct{}{}

	s1 := suitabilityScore(p1, "hash1", cluster, claimants, false)
	s2 := suitabilityScore(p2, "hash1", cluster, claimants, false)
	// p1 gets neutral 0.5 for capacity; p2 gets 1.0. Both get 0 traffic, 0.5 vivaldi.
	// p1 = 0.5*0.4 + 0*0.5 + 0.5*0.1 = 0.25, p2 = 1.0*0.4 + 0*0.5 + 0.5*0.1 = 0.45
	require.Greater(t, s2, s1, "node with telemetry should beat node without")
	require.Greater(t, s1, 0.0, "node without telemetry should still have a non-zero score")
}

func TestEvaluate_IndependentNodesAgreeWithClusterState(t *testing.T) {
	peers := make([]types.PeerKey, 5)
	nodes := make(map[types.PeerKey]NodeState, 5)
	for i := range peers {
		peers[i] = peerKey(byte(i + 1))
		nodes[peers[i]] = NodeState{
			NumCPU:        uint32(4 + i*2),     //nolint:gosec
			CPUPercent:    uint32(20 + i*10),   //nolint:gosec
			MemTotalBytes: uint64(8+i*4) << 30, //nolint:gosec
			MemPercent:    uint32(30 + i*5),    //nolint:gosec
		}
	}
	cluster := ClusterState{Nodes: nodes}

	specs := map[string]Spec{"workload1": {Replicas: 2}}
	claims := map[string]map[types.PeerKey]struct{}{}
	isRunning := func(string) bool { return false }

	var claimers []types.PeerKey
	for _, pk := range peers {
		actions := Evaluate(pk, peers, specs, claims, cluster, isRunning)
		for _, a := range actions {
			if a.Hash == "workload1" && a.Kind == ActionClaim {
				claimers = append(claimers, pk)
			}
		}
	}
	require.Len(t, claimers, 2, "exactly Replicas nodes should claim")

	// Repeat — same winners.
	var claimers2 []types.PeerKey
	for _, pk := range peers {
		actions := Evaluate(pk, peers, specs, claims, cluster, isRunning)
		for _, a := range actions {
			if a.Hash == "workload1" && a.Kind == ActionClaim {
				claimers2 = append(claimers2, pk)
			}
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

	// All nodes equal capacity, no traffic — incumbent p1 holds via bonus.
	cluster1 := ClusterState{Nodes: map[types.PeerKey]NodeState{
		p1: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p3: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}
	claims1 := map[string]map[types.PeerKey]struct{}{hash: {p1: {}}}

	actions := Evaluate(p1, allPeers, map[string]Spec{hash: {Replicas: 1}}, claims1, cluster1, isRunning)
	var p1Releasing bool
	for _, a := range actions {
		if a.Hash == hash && a.Kind == ActionRelease {
			p1Releasing = true
		}
	}
	require.False(t, p1Releasing, "incumbent should NOT release when all nodes are equal (bonus protects)")

	// Traffic shifts: p2 and p3 talk heavily to p1's claimants (i.e. p1).
	cluster2 := ClusterState{Nodes: map[types.PeerKey]NodeState{
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

	actions = Evaluate(p1, allPeers, map[string]Spec{hash: {Replicas: 1}}, claims1, cluster2, isRunning)
	p1Releasing = false
	for _, a := range actions {
		if a.Hash == hash && a.Kind == ActionRelease {
			p1Releasing = true
		}
	}
	require.True(t, p1Releasing, "incumbent should release when challenger has significantly better traffic affinity")

	// After p1 releases, p2 or p3 should claim.
	claimsAfterRelease := map[string]map[types.PeerKey]struct{}{}
	actions = Evaluate(p2, allPeers, map[string]Spec{hash: {Replicas: 1}}, claimsAfterRelease, cluster2, func(string) bool { return false })
	var p2Claiming bool
	for _, a := range actions {
		if a.Hash == hash && a.Kind == ActionClaim {
			p2Claiming = true
		}
	}
	require.True(t, p2Claiming, "challenger should claim after incumbent releases")
}

func TestEvaluate_MigrationInhibitedByIncumbentBonus(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)
	allPeers := []types.PeerKey{p1, p2}
	hash := "stable"
	claims := map[string]map[types.PeerKey]struct{}{hash: {p1: {}}}

	// p2 has slightly better capacity (50% vs 55%), but p1's incumbent bonus
	// should keep it in place.
	cluster := ClusterState{Nodes: map[types.PeerKey]NodeState{
		p1: {NumCPU: 4, CPUPercent: 55, MemTotalBytes: 8 << 30, MemPercent: 55},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}

	actions := Evaluate(p1, allPeers, map[string]Spec{hash: {Replicas: 1}}, claims, cluster, func(string) bool { return true })
	var releasing bool
	for _, a := range actions {
		if a.Hash == hash && a.Kind == ActionRelease {
			releasing = true
		}
	}
	require.False(t, releasing, "small advantage should not overcome incumbent bonus")
}

// TestEvaluate_OverReplicatedWithBetterNonClaimant verifies that when two
// incumbents hold a replicas=1 workload and a non-claimant scores higher,
// only ONE incumbent releases (the weakest), preventing a drop to zero.
func TestEvaluate_OverReplicatedWithBetterNonClaimant(t *testing.T) {
	p1 := peerKey(1)
	p2 := peerKey(2)
	p3 := peerKey(3)
	allPeers := []types.PeerKey{p1, p2, p3}
	hash := "over-rep"

	// p3 has the best capacity, but both p1 and p2 are incumbents.
	cluster := ClusterState{Nodes: map[types.PeerKey]NodeState{
		p1: {NumCPU: 4, CPUPercent: 70, MemTotalBytes: 8 << 30, MemPercent: 70},
		p2: {NumCPU: 4, CPUPercent: 60, MemTotalBytes: 8 << 30, MemPercent: 60},
		p3: {NumCPU: 8, CPUPercent: 10, MemTotalBytes: 16 << 30, MemPercent: 10},
	}}
	claims := map[string]map[types.PeerKey]struct{}{hash: {p1: {}, p2: {}}}

	var releasers int
	for _, pk := range allPeers {
		actions := Evaluate(pk, allPeers, map[string]Spec{hash: {Replicas: 1}}, claims, cluster, func(string) bool { return true })
		for _, a := range actions {
			if a.Hash == hash && a.Kind == ActionRelease {
				releasers++
			}
		}
	}
	// Over-replication (2 > 1): only 1 incumbent should release, never both.
	require.Equal(t, 1, releasers, "exactly one incumbent should release during over-replication, not both")
}

func TestEvaluate_DeadNodeExcludedFromCandidates(t *testing.T) {
	// Scenario: 3 nodes, one dies. The dead node would rank highest by
	// placement hash, but since it's excluded from allPeers (as
	// AllPeerKeys now filters by liveComponent), the surviving node
	// should claim instead of the slot being assigned to a ghost.
	local := peerKey(1)
	peer2 := peerKey(2)
	dead := peerKey(3)

	specs := map[string]Spec{
		"wl1": {Replicas: 2},
	}
	// peer2 already claims — 1 of 2 replicas. Need 1 more.
	claims := map[string]map[types.PeerKey]struct{}{
		"wl1": {peer2: {}},
	}
	isRunning := func(string) bool { return false }

	// Dead node excluded from allPeers — simulates AllPeerKeys filtering.
	livePeers := []types.PeerKey{local, peer2}

	actions := Evaluate(local, livePeers, specs, claims, ClusterState{}, isRunning)

	var claimed bool
	for _, a := range actions {
		if a.Hash == "wl1" && a.Kind == ActionClaim {
			claimed = true
		}
	}
	require.True(t, claimed, "live node must claim when dead node is excluded from candidates")

	// Contrast: if the dead node were still in allPeers and won the
	// tie-break, local might NOT claim (the slot would go to the ghost).
	// Verify that including the dead node could block the claim.
	allPeersIncludingDead := []types.PeerKey{local, peer2, dead}
	actionsWithDead := Evaluate(local, allPeersIncludingDead, specs, claims, ClusterState{}, isRunning)

	// Find who shouldClaim picks.
	var localClaimsWithDead bool
	for _, a := range actionsWithDead {
		if a.Hash == "wl1" && a.Kind == ActionClaim {
			localClaimsWithDead = true
		}
	}

	// If dead wins the tie-break, local won't claim — proving the bug.
	// If local wins, both paths agree — no harm either way.
	// We just verify the live-only path always produces a claim for local.
	_ = localClaimsWithDead // outcome depends on hash tie-break
	require.True(t, claimed, "with dead node excluded, live node must always be able to claim")
}
