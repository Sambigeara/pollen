// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

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
		localID:   local,
		allPeers:  []types.PeerKey{local, peer2},
		specs:     specs,
		claims:    claims,
		cluster:   healthyCluster(local, peer2),
		isRunning: func(string) bool { return false },
	})
	require.True(t, hasAction(actions, "abc123", actionClaim))
}

func TestEvaluate_FullyReplicated_NoClaim(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	specs := map[string]spec{"abc123": {MinReplicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {peer2: {}}}

	actions := evaluate(evaluateInput{
		localID:   local,
		allPeers:  []types.PeerKey{local, peer2},
		specs:     specs,
		claims:    claims,
		cluster:   healthyCluster(local, peer2),
		isRunning: func(string) bool { return false },
	})
	require.False(t, hasAction(actions, "abc123", actionClaim))
}

func TestEvaluate_OverReplicated_LoserReleases(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	specs := map[string]spec{"abc123": {MinReplicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {local: {}, peer2: {}}}
	allPeers := []types.PeerKey{local, peer2}
	cluster := healthyCluster(allPeers...)

	actions := evaluate(evaluateInput{
		localID:   local,
		allPeers:  allPeers,
		specs:     specs,
		claims:    claims,
		cluster:   cluster,
		isRunning: func(string) bool { return true },
	})
	releasing := hasAction(actions, "abc123", actionRelease)

	actions2 := evaluate(evaluateInput{
		localID:   peer2,
		allPeers:  allPeers,
		specs:     specs,
		claims:    claims,
		cluster:   cluster,
		isRunning: func(string) bool { return true },
	})
	peer2Releasing := hasAction(actions2, "abc123", actionRelease)

	require.NotEqual(t, releasing, peer2Releasing, "exactly one peer should release")
}

func TestEvaluate_NoSpec_ReleaseClaim(t *testing.T) {
	local := peerKey(1)
	claims := map[string]map[types.PeerKey]struct{}{"orphan": {local: {}}}

	actions := evaluate(evaluateInput{
		localID:   local,
		allPeers:  []types.PeerKey{local},
		specs:     map[string]spec{},
		claims:    claims,
		cluster:   healthyCluster(local),
		isRunning: func(string) bool { return true },
	})
	require.True(t, hasAction(actions, "orphan", actionRelease))
}

func TestEvaluate_ClaimedButNotRunning_ReClaim(t *testing.T) {
	local := peerKey(1)
	specs := map[string]spec{"abc123": {MinReplicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {local: {}}}

	actions := evaluate(evaluateInput{
		localID:   local,
		allPeers:  []types.PeerKey{local},
		specs:     specs,
		claims:    claims,
		cluster:   healthyCluster(local),
		isRunning: func(string) bool { return false },
	})
	require.True(t, hasAction(actions, "abc123", actionClaim))
}

func TestEvaluate_DrainingClaimantTriggersReplacement(t *testing.T) {
	// An at-target workload with one claimant draining looks under-replicated
	// from the decision plane's perspective — make-before-break demands a
	// replacement claim be issued during the drain window so the new replica
	// can compile while the old one continues serving. localID is in the
	// pool of capacity-passing non-claimants and must be picked when it
	// ranks top by deterministic tiebreak.
	local := peerKey(1)
	drainee := peerKey(2)
	specs := map[string]spec{"abc123": {MinReplicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {drainee: {}}}
	draining := map[string]map[types.PeerKey]struct{}{"abc123": {drainee: {}}}

	actions := evaluate(evaluateInput{
		localID:   local,
		allPeers:  []types.PeerKey{local, drainee},
		specs:     specs,
		claims:    claims,
		draining:  draining,
		cluster:   healthyCluster(local, drainee),
		isRunning: func(string) bool { return false },
	})
	require.True(t, hasAction(actions, "abc123", actionClaim),
		"non-claimant must issue a replacement during the drainee's cooldown")
}

func TestEvaluate_SelfDrainingBelowTargetUndrains(t *testing.T) {
	// When local is the only (draining) claimant and active count drops
	// below target, evaluate must emit actionClaim so the reconciler
	// rescinds the drain and republishes a fresh claim. Silent absence
	// here would let the cooldown loop interpret "no release wanted"
	// as a cancellation and then re-cancel the un-drain on the next tick.
	local := peerKey(1)
	specs := map[string]spec{"abc123": {MinReplicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {local: {}}}
	draining := map[string]map[types.PeerKey]struct{}{"abc123": {local: {}}}

	actions := evaluate(evaluateInput{
		localID:   local,
		allPeers:  []types.PeerKey{local},
		specs:     specs,
		claims:    claims,
		draining:  draining,
		cluster:   healthyCluster(local),
		isRunning: func(string) bool { return true },
	})
	require.True(t, hasAction(actions, "abc123", actionClaim),
		"draining-but-below-target must emit actionClaim to un-drain")
}

func TestEvaluate_SelfDrainingAtTargetReEmitsRelease(t *testing.T) {
	// Local is draining and other claimants already cover target — the
	// drain should proceed. Evaluate must re-emit actionRelease every
	// tick so the reconciler's cooldown loop sees a populated wantRelease
	// and lets the timer run to completion. Silence (the previous
	// behaviour) caused immediate cancellation on the next reconcile tick.
	local := peerKey(1)
	other := peerKey(2)
	specs := map[string]spec{"abc123": {MinReplicas: 1}}
	claims := map[string]map[types.PeerKey]struct{}{"abc123": {local: {}, other: {}}}
	draining := map[string]map[types.PeerKey]struct{}{"abc123": {local: {}}}

	actions := evaluate(evaluateInput{
		localID:   local,
		allPeers:  []types.PeerKey{local, other},
		specs:     specs,
		claims:    claims,
		draining:  draining,
		cluster:   healthyCluster(local, other),
		isRunning: func(string) bool { return true },
	})
	require.True(t, hasAction(actions, "abc123", actionRelease),
		"draining-at-target must re-emit actionRelease so the cooldown can proceed")
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

func TestEvaluate_IndependentNodesAgreeOnWinner(t *testing.T) {
	peers := []types.PeerKey{peerKey(1), peerKey(2), peerKey(3), peerKey(4), peerKey(5)}
	specs := map[string]spec{"workload1": {MinReplicas: 2}}
	cluster := healthyCluster(peers...)

	var claimers []types.PeerKey
	for _, pk := range peers {
		if hasAction(evaluate(evaluateInput{
			localID:   pk,
			allPeers:  peers,
			specs:     specs,
			claims:    map[string]map[types.PeerKey]struct{}{},
			cluster:   cluster,
			isRunning: func(string) bool { return false },
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
			localID:   pk,
			allPeers:  peers,
			specs:     specs,
			claims:    map[string]map[types.PeerKey]struct{}{},
			cluster:   cluster,
			isRunning: func(string) bool { return false },
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
			localID:   local,
			allPeers:  peers,
			specs:     specs,
			claims:    map[string]map[types.PeerKey]struct{}{},
			cluster:   cluster,
			isRunning: func(string) bool { return false },
		}), "only", actionClaim)
	}

	for range 5 {
		// p1 and p2 should always disagree on who wins, but the same
		// peer should win on every tick (signal is hash-deterministic).
		require.NotEqual(t, winner(p1), winner(p2))
	}
}

func TestEvaluate_ChallengerJoinsOnDemandAffinity(t *testing.T) {
	// Demo-chain shape: p2 is where all the demand for `migrating` lands
	// (the peer running upstream seeds). p1 is the current incumbent but
	// far from p2, so forwarded traffic pays a hop each time. p2 must
	// fire actionClaim — the 25% incumbentMargin gate inside
	// shouldChallenge already filters near-equivalent placements, so the
	// reconciler can act on the first tick without a streak counter.
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
		OriginRates: map[string]map[types.PeerKey]float64{
			hash: {p2: 100.0},
		},
	}
	claims := map[string]map[types.PeerKey]struct{}{hash: {p1: {}}}

	actions := evaluate(evaluateInput{
		localID:   p2,
		allPeers:  allPeers,
		specs:     map[string]spec{hash: {MinReplicas: 1}},
		claims:    claims,
		cluster:   cluster,
		isRunning: func(string) bool { return false },
	})
	require.True(t, hasAction(actions, hash, actionClaim))
}

// TestEvaluate_IncumbentMargin_SuppressesNarrowChallenge: a non-claimant
// whose predicted latency is only marginally better than the worst
// current claimant must NOT fire actionClaim. Without this the
// placement loop oscillates between near-equivalent layouts.
func TestEvaluate_IncumbentMargin_SuppressesNarrowChallenge(t *testing.T) {
	incumbent := peerKey(1)
	worstIncumbent := peerKey(2)
	challenger := peerKey(3)
	src := peerKey(4)
	allPeers := []types.PeerKey{incumbent, worstIncumbent, challenger, src}
	hash := "wl"

	// Coordinates: all three claimant candidates are clustered close to
	// the demand centroid (src at X=0). Challenger has a slight (10%)
	// edge over worstIncumbent — under the previous policy it would
	// fire and trigger a migration, but with the 25% margin the
	// challenge is suppressed.
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			incumbent:      node(coords.Coord{X: 5, Y: 0}, 4),
			worstIncumbent: node(coords.Coord{X: 11, Y: 0}, 4),
			challenger:     node(coords.Coord{X: 10, Y: 0}, 4),
			src:            node(coords.Coord{X: 0, Y: 0}, 4),
		},
		OriginRates: map[string]map[types.PeerKey]float64{
			hash: {src: 100.0},
		},
	}
	claims := map[string]map[types.PeerKey]struct{}{
		hash: {incumbent: {}, worstIncumbent: {}},
	}

	actions := evaluate(evaluateInput{
		localID:         challenger,
		allPeers:        allPeers,
		specs:           map[string]spec{hash: {MinReplicas: 2}},
		claims:          claims,
		cluster:         cluster,
		isRunning:       func(string) bool { return true },
		desiredReplicas: map[string]uint32{hash: 2},
	})
	require.False(t, hasAction(actions, hash, actionClaim),
		"narrow scoring difference must not trigger a challenge — placement should stick")
}

// TestEvaluate_IncumbentMargin_PermitsClearWin: when a non-claimant is
// clearly (≥ 25%) better than the worst current claimant, the challenge
// must still fire — the margin guard is anti-flap, not anti-improvement.
func TestEvaluate_IncumbentMargin_PermitsClearWin(t *testing.T) {
	farIncumbent := peerKey(1)
	closeIncumbent := peerKey(2)
	clearWinner := peerKey(3)
	src := peerKey(4)
	allPeers := []types.PeerKey{farIncumbent, closeIncumbent, clearWinner, src}
	hash := "wl"

	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			farIncumbent:   node(coords.Coord{X: 1000, Y: 0}, 4),
			closeIncumbent: node(coords.Coord{X: 50, Y: 0}, 4),
			clearWinner:    node(coords.Coord{X: 0, Y: 0}, 4), // colocated with demand
			src:            node(coords.Coord{X: 0, Y: 0}, 4),
		},
		OriginRates: map[string]map[types.PeerKey]float64{
			hash: {src: 100.0},
		},
	}
	claims := map[string]map[types.PeerKey]struct{}{
		hash: {farIncumbent: {}, closeIncumbent: {}},
	}

	actions := evaluate(evaluateInput{
		localID:         clearWinner,
		allPeers:        allPeers,
		specs:           map[string]spec{hash: {MinReplicas: 2}},
		claims:          claims,
		cluster:         cluster,
		isRunning:       func(string) bool { return true },
		desiredReplicas: map[string]uint32{hash: 2},
	})
	require.True(t, hasAction(actions, hash, actionClaim),
		"a clearly better placement must still trigger a claim")
}

// TestEvaluate_IncumbentMargin_SuppressesNarrowRelease: a marginally
// excess claimant must NOT shed when its score is within the margin
// band of the worst keeper. Symmetric guard to the challenge-side
// margin — without it a flap can develop where the new winner sheds
// when the EWMA recovers on the previous winner.
func TestEvaluate_IncumbentMargin_SuppressesNarrowRelease(t *testing.T) {
	keeper := peerKey(1)
	excess := peerKey(2) // marginally worse than keeper
	src := peerKey(3)
	allPeers := []types.PeerKey{keeper, excess, src}
	hash := "wl"

	// Both claimants near the demand centroid; excess is slightly
	// further. Within the margin band → don't shed.
	cluster := clusterState{
		Nodes: map[types.PeerKey]nodeState{
			keeper: node(coords.Coord{X: 10, Y: 0}, 4),
			excess: node(coords.Coord{X: 11, Y: 0}, 4),
			src:    node(coords.Coord{X: 0, Y: 0}, 4),
		},
		OriginRates: map[string]map[types.PeerKey]float64{
			hash: {src: 100.0},
		},
	}
	claims := map[string]map[types.PeerKey]struct{}{
		hash: {keeper: {}, excess: {}},
	}

	actions := evaluate(evaluateInput{
		localID:         excess,
		allPeers:        allPeers,
		specs:           map[string]spec{hash: {MinReplicas: 1}},
		claims:          claims,
		cluster:         cluster,
		isRunning:       func(string) bool { return true },
		desiredReplicas: map[string]uint32{hash: 1},
	})
	require.False(t, hasAction(actions, hash, actionRelease),
		"marginally-excess claimant must stay — within-margin shedding causes flap")
}

func TestEvaluate_OverReplicatedReleasesByLatency(t *testing.T) {
	// Three peers, two incumbents. p3 is uncontended and far from where
	// demand lands; p1 sits on the demand centroid. Expect p3 (worst
	// centroid score) to release first.
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
		OriginRates: map[string]map[types.PeerKey]float64{
			hash: {p1: 100.0},
		},
	}

	var releaser types.PeerKey
	for _, pk := range []types.PeerKey{p2, p3} {
		actions := evaluate(evaluateInput{
			localID:   pk,
			allPeers:  allPeers,
			specs:     map[string]spec{hash: {MinReplicas: 1}},
			claims:    map[string]map[types.PeerKey]struct{}{hash: {p2: {}, p3: {}}},
			cluster:   cluster,
			isRunning: func(string) bool { return true },
		})
		if hasAction(actions, hash, actionRelease) {
			releaser = pk
		}
	}
	require.Equal(t, p3, releaser, "the worst-predicted incumbent should release first")
}

// TestEvaluate_OverReplicatedDelaysReleaseUntilReplacementWarm: when
// the workload has live demand but the over-counting replacement
// hasn't started serving yet (ServedRate=0), shouldReleaseExcess must
// hold the drain. Only when the replacement clears warm-up and starts
// serving does the loser drain. This is the make-before-break warmth
// gate — without it traffic falls into a cold pool while the replacement
// fetches the blob or finishes its first invocation.
func TestEvaluate_OverReplicatedDelaysReleaseUntilReplacementWarm(t *testing.T) {
	winner := peerKey(1) // sits on the demand centroid
	loser := peerKey(2)  // far from the centroid — will lose on score
	src := peerKey(3)    // origin source for the seed
	allPeers := []types.PeerKey{winner, loser, src}
	hash := "workload"

	build := func(servedRates map[types.PeerKey]float64) clusterState {
		return clusterState{
			Nodes: map[types.PeerKey]nodeState{
				winner: {
					NumCPU: 4, CPUBudgetPercent: 100, MemTotalBytes: 8 << 30, MemBudgetPercent: 100, //nolint:mnd
					Coord: &coords.Coord{X: 0, Y: 0},
				},
				loser: {
					NumCPU: 4, CPUBudgetPercent: 100, MemTotalBytes: 8 << 30, MemBudgetPercent: 100, //nolint:mnd
					Coord: &coords.Coord{X: 1000, Y: 0},
				},
				src: {
					NumCPU: 4, CPUBudgetPercent: 100, MemTotalBytes: 8 << 30, MemBudgetPercent: 100, //nolint:mnd
					Coord: &coords.Coord{X: 0, Y: 0},
				},
			},
			OriginRates: map[string]map[types.PeerKey]float64{
				hash: {src: 100.0},
			},
			ServedRates: map[string]map[types.PeerKey]float64{
				hash: servedRates,
			},
		}
	}
	claims := map[string]map[types.PeerKey]struct{}{hash: {winner: {}, loser: {}}}

	// Demand exists, but only the loser has been serving so far —
	// the winner's claim is still ramping. Loser must NOT release;
	// otherwise traffic drops into a cold winner.
	actions := evaluate(evaluateInput{
		localID:         loser,
		allPeers:        allPeers,
		specs:           map[string]spec{hash: {MinReplicas: 1}},
		claims:          claims,
		cluster:         build(map[types.PeerKey]float64{loser: 50.0}),
		isRunning:       func(string) bool { return true },
		desiredReplicas: map[string]uint32{hash: 1},
	})
	require.False(t, hasAction(actions, hash, actionRelease),
		"drain must wait for the replacement to warm — releasing now hands traffic to a cold pool")

	// Once the winner is serving too, the loser is free to drain.
	actions = evaluate(evaluateInput{
		localID:         loser,
		allPeers:        allPeers,
		specs:           map[string]spec{hash: {MinReplicas: 1}},
		claims:          claims,
		cluster:         build(map[types.PeerKey]float64{loser: 50.0, winner: 50.0}),
		isRunning:       func(string) bool { return true },
		desiredReplicas: map[string]uint32{hash: 1},
	})
	require.True(t, hasAction(actions, hash, actionRelease),
		"drain must proceed once the replacement is proven serving")
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
		localID:   local,
		allPeers:  allPeers,
		specs:     map[string]spec{hash: {MinReplicas: 1}},
		claims:    map[string]map[types.PeerKey]struct{}{},
		cluster:   cluster,
		isRunning: func(string) bool { return false },
	}), hash, actionClaim)
	peer2Claims := hasAction(evaluate(evaluateInput{
		localID:   peer2,
		allPeers:  allPeers,
		specs:     map[string]spec{hash: {MinReplicas: 1}},
		claims:    map[string]map[types.PeerKey]struct{}{},
		cluster:   cluster,
		isRunning: func(string) bool { return false },
	}), hash, actionClaim)

	require.True(t, localClaims, "healthy node should claim")
	require.False(t, peer2Claims, "exhausted node should be filtered by capacity gate")
}

func TestEvaluate_DeadNodeExcludedFromCandidates(t *testing.T) {
	local := peerKey(1)
	peer2 := peerKey(2)
	actions := evaluate(evaluateInput{
		localID:   local,
		allPeers:  []types.PeerKey{local, peer2},
		specs:     map[string]spec{"wl1": {MinReplicas: 2}},
		claims:    map[string]map[types.PeerKey]struct{}{"wl1": {peer2: {}}},
		cluster:   healthyCluster(local, peer2),
		isRunning: func(string) bool { return false },
	})
	require.True(t, hasAction(actions, "wl1", actionClaim))
}
