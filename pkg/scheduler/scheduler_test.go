package scheduler

import (
	"testing"

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

	actions := Evaluate(local, allPeers, specs, claims, isRunning)

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

	actions := Evaluate(local, allPeers, specs, claims, isRunning)

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
	actions := Evaluate(local, allPeers, specs, claims, isRunning)

	var releasing bool
	for _, a := range actions {
		if a.Hash == "abc123" && a.Kind == ActionRelease {
			releasing = true
		}
	}

	// Also run from peer2's perspective.
	actions2 := Evaluate(peer2, allPeers, specs, claims, isRunning)
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

	actions := Evaluate(local, allPeers, specs, claims, isRunning)

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

	actions := Evaluate(local, allPeers, specs, claims, isRunning)

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
		actions := Evaluate(pk, peers, specs, claims, isRunning)
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
		actions := Evaluate(pk, peers, specs, claims, isRunning)
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
		actions := Evaluate(pk, peers, specs, claims, isRunning)
		for _, a := range actions {
			if a.Hash == "singleton" && a.Kind == ActionClaim {
				claimers++
			}
		}
	}
	require.Equal(t, 1, claimers, "only one node should claim when replicas=1")
}
