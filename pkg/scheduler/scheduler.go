package scheduler

import (
	"crypto/sha256"
	"slices"

	"github.com/sambigeara/pollen/pkg/types"
)

// ActionKind describes what the local node should do for a workload.
type ActionKind int

const (
	ActionClaim   ActionKind = iota // start running this workload
	ActionRelease                   // stop running this workload
)

// Action is a single scheduling decision for this node.
type Action struct {
	Hash string
	Kind ActionKind
}

// Spec describes desired workload state.
type Spec struct {
	Replicas uint32
}

// Evaluate determines what this node should do given cluster state.
// Pure function — no side effects, fully deterministic.
//
// allPeers is the set of all known valid peers in the cluster (including localID).
// This is needed so that all nodes agree on who the eligible candidates are
// for under-replicated workloads.
func Evaluate(
	localID types.PeerKey,
	allPeers []types.PeerKey,
	specs map[string]Spec,
	claims map[string]map[types.PeerKey]struct{},
	isRunning func(string) bool,
) []Action {
	var actions []Action

	for hash, spec := range specs {
		claimants := claims[hash]
		claimCount := uint32(len(claimants))
		_, iClaimed := claimants[localID]

		if claimCount < spec.Replicas && !iClaimed {
			if shouldClaim(localID, hash, spec.Replicas, claimants, allPeers) {
				actions = append(actions, Action{Hash: hash, Kind: ActionClaim})
			}
		} else if claimCount > spec.Replicas && iClaimed {
			if shouldRelease(localID, hash, spec.Replicas, claimants) {
				actions = append(actions, Action{Hash: hash, Kind: ActionRelease})
			}
		}
	}

	// Release claims with no matching spec.
	for hash, claimants := range claims {
		if _, iClaimed := claimants[localID]; !iClaimed {
			continue
		}
		if _, hasSpec := specs[hash]; !hasSpec {
			actions = append(actions, Action{Hash: hash, Kind: ActionRelease})
		}
	}

	// Re-claim for specs where I've claimed but the workload isn't running.
	for hash := range specs {
		claimants := claims[hash]
		if _, iClaimed := claimants[localID]; !iClaimed {
			continue
		}
		if !isRunning(hash) {
			actions = append(actions, Action{Hash: hash, Kind: ActionClaim})
		}
	}

	return actions
}

// shouldClaim returns true if localID is among the top `needed` unclaimed
// nodes when all eligible peers are ranked by placement score.
func shouldClaim(localID types.PeerKey, hash string, desired uint32, claimants map[types.PeerKey]struct{}, allPeers []types.PeerKey) bool {
	needed := int(desired) - len(claimants)
	if needed <= 0 {
		return false
	}

	// Build list of unclaimed peers, sorted by placement score.
	unclaimed := make([]scored, 0, len(allPeers))
	for _, pk := range allPeers {
		if _, already := claimants[pk]; already {
			continue
		}
		unclaimed = append(unclaimed, scored{peer: pk, score: placementScore(pk, hash)})
	}

	slices.SortFunc(unclaimed, func(a, b scored) int {
		return compareScores(a.score, b.score)
	})

	// localID should claim if it's in the top `needed` slots.
	if needed > len(unclaimed) {
		needed = len(unclaimed)
	}
	for _, s := range unclaimed[:needed] {
		if s.peer == localID {
			return true
		}
	}
	return false
}

type scored struct {
	peer  types.PeerKey
	score [32]byte
}

// shouldRelease returns true if localID loses the tie-break and should
// release its claim to bring the replica count back to desired.
func shouldRelease(localID types.PeerKey, hash string, desired uint32, claimants map[types.PeerKey]struct{}) bool {
	all := make([]scored, 0, len(claimants))
	for pk := range claimants {
		all = append(all, scored{peer: pk, score: placementScore(pk, hash)})
	}

	slices.SortFunc(all, func(a, b scored) int {
		return compareScores(a.score, b.score)
	})

	// Keep top `desired` — if I'm not in that set, I should release.
	keep := min(int(desired), len(all))
	for _, s := range all[:keep] {
		if s.peer == localID {
			return false
		}
	}
	return true
}

// placementScore produces a deterministic hash for tie-breaking.
// Lowest score wins placement.
func placementScore(nodeID types.PeerKey, hash string) [32]byte {
	h := sha256.New()
	h.Write(nodeID[:])
	h.Write([]byte(hash))
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func compareScores(a, b [32]byte) int {
	for i := range a {
		if a[i] < b[i] { //nolint:gosec
			return -1
		}
		if a[i] > b[i] { //nolint:gosec
			return 1
		}
	}
	return 0
}
