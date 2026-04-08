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
		cluster:       clusterState{},
		isRunning:     func(string) bool { return false },
		demandRates:   map[string]float64{},
		idleDurations: map[string]time.Duration{},
	})
	require.True(t, hasAction(actions, "abc123", actionClaim), "should claim under-replicated workload")
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
		cluster:       clusterState{},
		isRunning:     func(string) bool { return false },
		demandRates:   map[string]float64{},
		idleDurations: map[string]time.Duration{},
	})
	require.False(t, hasAction(actions, "abc123", actionClaim), "should not claim fully-replicated workload")
}

func TestEvaluate_OverReplicated_LoserReleases(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	specs := map[string]spec{"abc123": {MinReplicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {local: {}, peer2: {}}}
	allPeers := []types.PeerKey{local, peer2}
	longIdle := map[string]time.Duration{"abc123": 10 * time.Minute}

	actions := evaluate(evaluateInput{
		localID:       local,
		allPeers:      allPeers,
		specs:         specs,
		claims:        claims,
		cluster:       clusterState{},
		isRunning:     func(string) bool { return true },
		demandRates:   map[string]float64{},
		idleDurations: longIdle,
	})
	releasing := hasAction(actions, "abc123", actionRelease)

	actions2 := evaluate(evaluateInput{
		localID:       peer2,
		allPeers:      allPeers,
		specs:         specs,
		claims:        claims,
		cluster:       clusterState{},
		isRunning:     func(string) bool { return true },
		demandRates:   map[string]float64{},
		idleDurations: longIdle,
	})
	peer2Releasing := hasAction(actions2, "abc123", actionRelease)

	require.NotEqual(t, releasing, peer2Releasing, "exactly one of the two peers should release")
}

func TestEvaluate_NoSpec_ReleaseClaim(t *testing.T) {
	local := peerKey(1)
	claims := map[string]map[types.PeerKey]struct{}{"orphan": {local: {}}}

	actions := evaluate(evaluateInput{
		localID:       local,
		allPeers:      []types.PeerKey{local},
		specs:         map[string]spec{},
		claims:        claims,
		cluster:       clusterState{},
		isRunning:     func(string) bool { return true },
		demandRates:   map[string]float64{},
		idleDurations: map[string]time.Duration{},
	})
	require.True(t, hasAction(actions, "orphan", actionRelease), "should release claim with no matching spec")
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
		cluster:       clusterState{},
		isRunning:     func(string) bool { return false },
		demandRates:   map[string]float64{},
		idleDurations: map[string]time.Duration{},
	})
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
	specs := map[string]spec{"workload1": {MinReplicas: 2}}
	claims := map[string]map[types.PeerKey]struct{}{}

	var claimers []types.PeerKey
	for _, pk := range peers {
		if hasAction(evaluate(evaluateInput{
			localID:       pk,
			allPeers:      peers,
			specs:         specs,
			claims:        claims,
			cluster:       clusterState{},
			isRunning:     func(string) bool { return false },
			demandRates:   map[string]float64{},
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
	claims := map[string]map[types.PeerKey]struct{}{}

	var claimers int
	for _, pk := range peers {
		if hasAction(evaluate(evaluateInput{
			localID:       pk,
			allPeers:      peers,
			specs:         specs,
			claims:        claims,
			cluster:       clusterState{},
			isRunning:     func(string) bool { return false },
			demandRates:   map[string]float64{},
			idleDurations: map[string]time.Duration{},
		}), "singleton", actionClaim) {
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

	s1 := rawSuitabilityScore(p1, cluster, map[types.PeerKey]struct{}{}, nil, "", p1)
	s2 := rawSuitabilityScore(p2, cluster, map[types.PeerKey]struct{}{}, nil, "", p2)
	require.Greater(t, s1, s2, "node with more free resources should score higher")
}

func TestSuitabilityScore_TrafficAffinity(t *testing.T) {
	p1, p2, claimant := peerKey(1), peerKey(2), peerKey(3)
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1:       {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, TrafficTo: map[types.PeerKey]uint64{claimant: 10000}},
		p2:       {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, TrafficTo: map[types.PeerKey]uint64{claimant: 1000}},
		claimant: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}

	s1 := rawSuitabilityScore(p1, cluster, map[types.PeerKey]struct{}{claimant: {}}, nil, "", p1)
	s2 := rawSuitabilityScore(p2, cluster, map[types.PeerKey]struct{}{claimant: {}}, nil, "", p2)
	require.Greater(t, s1, s2, "node with more traffic to claimants should score higher")
}

func TestSuitabilityScore_IncumbentBonus(t *testing.T) {
	p1, p2 := peerKey(1), peerKey(2)
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 60, MemTotalBytes: 8 << 30, MemPercent: 60},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}

	incumbentScore := rawSuitabilityScore(p1, cluster, map[types.PeerKey]struct{}{}, nil, "", p1) * incumbentBonus
	challengerScore := rawSuitabilityScore(p2, cluster, map[types.PeerKey]struct{}{}, nil, "", p2)
	require.Greater(t, incumbentScore, challengerScore, "incumbent with slightly lower raw score should still beat challenger due to bonus")
}

func TestSuitabilityScore_ChallengerOvertakesIncumbent(t *testing.T) {
	p1, p2 := peerKey(1), peerKey(2)
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 2, CPUPercent: 95, MemTotalBytes: 2 << 30, MemPercent: 95},
		p2: {NumCPU: 16, CPUPercent: 10, MemTotalBytes: 32 << 30, MemPercent: 10},
	}}

	incumbentScore := rawSuitabilityScore(p1, cluster, map[types.PeerKey]struct{}{}, nil, "", p1) * incumbentBonus
	challengerScore := rawSuitabilityScore(p2, cluster, map[types.PeerKey]struct{}{}, nil, "", p2)
	require.Greater(t, challengerScore, incumbentScore, "challenger with massively better raw score should overcome incumbent bonus")
}

func TestSuitabilityScore_VivaldiTieBreak(t *testing.T) {
	p1, p2, claimant := peerKey(1), peerKey(2), peerKey(3)
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1:       {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, Coord: &coords.Coord{X: 1, Y: 0}},
		p2:       {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, Coord: &coords.Coord{X: 100, Y: 0}},
		claimant: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, Coord: &coords.Coord{X: 0, Y: 0}},
	}}

	s1 := rawSuitabilityScore(p1, cluster, map[types.PeerKey]struct{}{claimant: {}}, nil, "", p1)
	s2 := rawSuitabilityScore(p2, cluster, map[types.PeerKey]struct{}{claimant: {}}, nil, "", p2)
	require.Greater(t, s1, s2, "closer node should score higher via Vivaldi")
}

func TestSuitabilityScore_HashFallback(t *testing.T) {
	p1, p2 := peerKey(1), peerKey(2)
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}
	specs := map[string]spec{"workload": {MinReplicas: 1}}

	peers := []types.PeerKey{p1, p2}
	var winner types.PeerKey
	for _, pk := range peers {
		if hasAction(evaluate(evaluateInput{
			localID:       pk,
			allPeers:      peers,
			specs:         specs,
			claims:        map[string]map[types.PeerKey]struct{}{},
			cluster:       cluster,
			isRunning:     func(string) bool { return false },
			demandRates:   map[string]float64{},
			idleDurations: map[string]time.Duration{},
		}), "workload", actionClaim) {
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

	s1 := rawSuitabilityScore(p1, cluster, map[types.PeerKey]struct{}{}, nil, "", p1)
	s2 := rawSuitabilityScore(p2, cluster, map[types.PeerKey]struct{}{}, nil, "", p2)
	require.Greater(t, s2, s1, "node with telemetry should beat node without")
	require.Greater(t, s1, 0.0, "node without telemetry should still have a non-zero score")
}

// TestEvaluate_ChallengerJoinsOnTrafficShift verifies that a much-better-suited
// non-claimant uses shouldChallenge to join (temporarily going above target),
// rather than the old at-target replacement path which caused an under-provisioning
// window.
func TestEvaluate_ChallengerJoinsOnTrafficShift(t *testing.T) {
	p1, p2, p3 := peerKey(1), peerKey(2), peerKey(3)
	allPeers := []types.PeerKey{p1, p2, p3}
	hash := "migrating"

	// p2 and p3 have massive traffic affinity to p1 (the claimant).
	// p2 should challenge because its raw score exceeds p1's by >1.2x.
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, TrafficTo: map[types.PeerKey]uint64{p1: 100000}},
		p3: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, TrafficTo: map[types.PeerKey]uint64{p1: 100000}},
	}}
	claims := map[string]map[types.PeerKey]struct{}{hash: {p1: {}}}

	// From p2's perspective: at target (1 claimant, min_replicas=1), so
	// shouldChallenge runs. p2's raw score should exceed p1's raw * 1.2x
	// due to traffic affinity.
	actions := evaluate(evaluateInput{
		localID:       p2,
		allPeers:      allPeers,
		specs:         map[string]spec{hash: {MinReplicas: 1}},
		claims:        claims,
		cluster:       cluster,
		isRunning:     func(string) bool { return false },
		demandRates:   map[string]float64{},
		idleDurations: map[string]time.Duration{},
	})
	require.True(t, hasAction(actions, hash, actionClaim), "better-suited challenger should claim via shouldChallenge")

	// p1 should NOT release — at target, no at-target release path exists.
	// The swap completes when p1 becomes excess (after p2 claims) and goes idle.
	actionsP1 := evaluate(evaluateInput{
		localID:       p1,
		allPeers:      allPeers,
		specs:         map[string]spec{hash: {MinReplicas: 1}},
		claims:        claims,
		cluster:       cluster,
		isRunning:     func(string) bool { return true },
		demandRates:   map[string]float64{},
		idleDurations: map[string]time.Duration{},
	})
	require.False(t, hasAction(actionsP1, hash, actionRelease), "incumbent at target should not release — challenger joins first")
}

func TestEvaluate_ChallengeInhibitedByThreshold(t *testing.T) {
	p1, p2 := peerKey(1), peerKey(2)
	hash := "stable"
	cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
		p1: {NumCPU: 4, CPUPercent: 55, MemTotalBytes: 8 << 30, MemPercent: 55},
		p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
	}}

	actions := evaluate(evaluateInput{
		localID:       p2,
		allPeers:      []types.PeerKey{p1, p2},
		specs:         map[string]spec{hash: {MinReplicas: 1}},
		claims:        map[string]map[types.PeerKey]struct{}{hash: {p1: {}}},
		cluster:       cluster,
		isRunning:     func(string) bool { return false },
		demandRates:   map[string]float64{},
		idleDurations: map[string]time.Duration{},
	})
	require.False(t, hasAction(actions, hash, actionClaim), "small advantage should not overcome 1.2x challenge threshold")
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

	longIdle := map[string]time.Duration{hash: 10 * time.Minute}
	var releasers int
	for _, pk := range allPeers {
		if hasAction(evaluate(evaluateInput{
			localID:       pk,
			allPeers:      allPeers,
			specs:         map[string]spec{hash: {MinReplicas: 1}},
			claims:        map[string]map[types.PeerKey]struct{}{hash: {p1: {}, p2: {}}},
			cluster:       cluster,
			isRunning:     func(string) bool { return true },
			demandRates:   map[string]float64{},
			idleDurations: longIdle,
		}), hash, actionRelease) {
			releasers++
		}
	}
	require.Equal(t, 1, releasers, "exactly one incumbent should release during over-replication, not both")
}

func TestEvaluate_DeadNodeExcludedFromCandidates(t *testing.T) {
	local, peer2 := peerKey(1), peerKey(2)
	actions := evaluate(evaluateInput{
		localID:       local,
		allPeers:      []types.PeerKey{local, peer2},
		specs:         map[string]spec{"wl1": {MinReplicas: 2}},
		claims:        map[string]map[types.PeerKey]struct{}{"wl1": {peer2: {}}},
		cluster:       clusterState{},
		isRunning:     func(string) bool { return false },
		demandRates:   map[string]float64{},
		idleDurations: map[string]time.Duration{},
	})
	require.True(t, hasAction(actions, "wl1", actionClaim), "live node must claim when dead node is excluded from candidates")
}

func TestCapacityScore_BudgetCapped(t *testing.T) {
	p1, p2 := peerKey(1), peerKey(2)

	t.Run("positive headroom within budget", func(t *testing.T) {
		cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
			p1: {NumCPU: 4, CPUPercent: 70, MemTotalBytes: 8 << 30, MemPercent: 70, CPUBudgetPercent: 80, MemBudgetPercent: 80},
		}}
		score := capacityScore(p1, []types.PeerKey{p1}, cluster)
		require.Greater(t, score, 0.0, "node within budget should have positive score")
	})

	t.Run("negative headroom over budget", func(t *testing.T) {
		// Two nodes: p2 has positive headroom (provides the maxFree reference),
		// p1 is over budget so its negative headroom produces a negative score.
		cluster := clusterState{Nodes: map[types.PeerKey]nodeState{
			p1: {NumCPU: 4, CPUPercent: 90, MemTotalBytes: 8 << 30, MemPercent: 90, CPUBudgetPercent: 80, MemBudgetPercent: 80},
			p2: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		}}
		peers := []types.PeerKey{p1, p2}
		score := capacityScore(p1, peers, cluster)
		require.Less(t, score, 0.0, "node over budget should have negative score when peers have positive headroom")
	})

	t.Run("zero budget means no limit", func(t *testing.T) {
		clusterNoBudget := clusterState{Nodes: map[types.PeerKey]nodeState{
			p1: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50},
		}}
		clusterFullBudget := clusterState{Nodes: map[types.PeerKey]nodeState{
			p1: {NumCPU: 4, CPUPercent: 50, MemTotalBytes: 8 << 30, MemPercent: 50, CPUBudgetPercent: 100, MemBudgetPercent: 100},
		}}
		scoreNoBudget := capacityScore(p1, []types.PeerKey{p1}, clusterNoBudget)
		scoreFullBudget := capacityScore(p1, []types.PeerKey{p1}, clusterFullBudget)
		require.InDelta(t, scoreFullBudget, scoreNoBudget, 0.001, "zero budget should behave like 100%%")
	})
}
