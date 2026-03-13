package scheduler

import (
	"cmp"
	"crypto/sha256"
	"math"
	"slices"

	"github.com/sambigeara/pollen/pkg/topology"
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

// NodeState holds per-node data used for placement scoring.
type NodeState struct {
	Coord         *topology.Coord
	TrafficTo     map[types.PeerKey]uint64
	MemTotalBytes uint64
	CPUPercent    uint32
	MemPercent    uint32
	NumCPU        uint32
}

// ClusterState provides cluster-wide data for traffic-aware placement.
type ClusterState struct {
	Nodes map[types.PeerKey]NodeState
}

const (
	weightCapacity = 0.4
	weightTraffic  = 0.5
	weightVivaldi  = 0.1
	incumbentBonus = 1.3
	neutralScore   = 0.5
	percentMax     = 100

	// memScaleFactor normalises memory bytes to comparable units with CPU
	// "free cores". A machine with 1 free GiB of RAM contributes ~1 unit,
	// matching 1 free CPU core. This keeps the two dimensions balanced in
	// the combined capacity score.
	memScaleFactor = 1 << 30
)

type scored struct {
	peer        types.PeerKey
	suitability float64
	tieBreak    [32]byte
}

func compareScored(a, b scored) int {
	// Higher suitability wins (descending).
	if a.suitability != b.suitability {
		return cmp.Compare(b.suitability, a.suitability)
	}
	// Lowest hash wins (ascending).
	return compareScores(a.tieBreak, b.tieBreak)
}

// Evaluate determines what this node should do given cluster state.
// Pure function — no side effects, fully deterministic.
//
// allPeers is the set of all known valid peers in the cluster (including localID).
func Evaluate(
	localID types.PeerKey,
	allPeers []types.PeerKey,
	specs map[string]Spec,
	claims map[string]map[types.PeerKey]struct{},
	cluster ClusterState,
	isRunning func(string) bool,
) []Action {
	var actions []Action

	for hash, spec := range specs {
		claimants := claims[hash]
		claimCount := uint32(len(claimants))
		_, iClaimed := claimants[localID]

		if claimCount < spec.Replicas && !iClaimed {
			if shouldClaim(localID, hash, spec.Replicas, claimants, allPeers, cluster) {
				actions = append(actions, Action{Hash: hash, Kind: ActionClaim})
			}
		} else if claimCount >= spec.Replicas && iClaimed {
			// Covers both over-replication (claimCount > desired) and
			// migration at exact count: if a non-claimant would rank
			// higher than localID, release so it can claim.
			if shouldRelease(localID, hash, spec.Replicas, claimants, allPeers, cluster) {
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
// nodes when all eligible peers are ranked by suitability then tie-break.
func shouldClaim(localID types.PeerKey, hash string, desired uint32, claimants map[types.PeerKey]struct{}, allPeers []types.PeerKey, cluster ClusterState) bool {
	needed := int(desired) - len(claimants)
	if needed <= 0 {
		return false
	}

	unclaimed := make([]scored, 0, len(allPeers))
	for _, pk := range allPeers {
		if _, already := claimants[pk]; already {
			continue
		}
		unclaimed = append(unclaimed, scored{
			peer:        pk,
			suitability: suitabilityScore(pk, hash, cluster, claimants, false),
			tieBreak:    placementScore(pk, hash),
		})
	}

	slices.SortFunc(unclaimed, compareScored)

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

// shouldRelease returns true if localID should give up its claim.
//
// Two distinct cases:
//  1. Over-replication (len(claimants) > desired): rank incumbents only,
//     release the weakest until we reach desired. Non-claimants are ignored
//     so we never drop below desired replicas.
//  2. Migration at exact count (len(claimants) == desired): compare the
//     weakest incumbent against the best non-claimant. If a challenger
//     outranks the weakest incumbent (after incumbent bonus), that
//     incumbent releases so the challenger can claim.
func shouldRelease(localID types.PeerKey, hash string, desired uint32, claimants map[types.PeerKey]struct{}, allPeers []types.PeerKey, cluster ClusterState) bool {
	claimCount := len(claimants)

	if claimCount > int(desired) {
		// Over-replication: rank incumbents only, keep the best `desired`.
		incumbents := make([]scored, 0, claimCount)
		for pk := range claimants {
			incumbents = append(incumbents, scored{
				peer:        pk,
				suitability: suitabilityScore(pk, hash, cluster, claimants, true),
				tieBreak:    placementScore(pk, hash),
			})
		}
		slices.SortFunc(incumbents, compareScored)

		keep := min(int(desired), len(incumbents))
		for _, s := range incumbents[:keep] {
			if s.peer == localID {
				return false
			}
		}
		return true
	}

	// Exact count: check whether any non-claimant outranks the weakest
	// incumbent. Only the weakest incumbent releases.
	incumbents := make([]scored, 0, claimCount)
	for pk := range claimants {
		incumbents = append(incumbents, scored{
			peer:        pk,
			suitability: suitabilityScore(pk, hash, cluster, claimants, true),
			tieBreak:    placementScore(pk, hash),
		})
	}
	slices.SortFunc(incumbents, compareScored)

	weakest := incumbents[len(incumbents)-1]
	if weakest.peer != localID {
		return false // only the weakest incumbent considers releasing
	}

	// Find best non-claimant challenger.
	var bestChallenger *scored
	for _, pk := range allPeers {
		if _, incumbent := claimants[pk]; incumbent {
			continue
		}
		s := scored{
			peer:        pk,
			suitability: suitabilityScore(pk, hash, cluster, claimants, false),
			tieBreak:    placementScore(pk, hash),
		}
		if bestChallenger == nil || compareScored(s, *bestChallenger) < 0 {
			bestChallenger = &s
		}
	}
	if bestChallenger == nil {
		return false // no challengers
	}

	// Release only if the challenger strictly outranks the weakest incumbent.
	return compareScored(*bestChallenger, weakest) < 0
}

// suitabilityScore computes a composite placement score for a candidate node.
// Returns 0.0–1.0+ (incumbent bonus can push above 1.0). Higher is better.
func suitabilityScore(candidate types.PeerKey, _ string, cluster ClusterState, claimants map[types.PeerKey]struct{}, isIncumbent bool) float64 {
	if len(cluster.Nodes) == 0 {
		return 0.0
	}

	// Collect all candidate peers for normalisation context.
	peers := make([]types.PeerKey, 0, len(cluster.Nodes))
	for pk := range cluster.Nodes {
		peers = append(peers, pk)
	}

	capacity := capacityScore(candidate, peers, cluster)
	traffic := trafficAffinityScore(candidate, peers, claimants, cluster)
	vivaldi := vivaldiScore(candidate, peers, claimants, cluster)

	raw := capacity*weightCapacity + traffic*weightTraffic + vivaldi*weightVivaldi

	if isIncumbent {
		raw *= incumbentBonus
	}

	return raw
}

// capacityScore returns 0–1 for the candidate's free resources relative to
// all peers. No telemetry → 0.5 (neutral).
func capacityScore(candidate types.PeerKey, peers []types.PeerKey, cluster ClusterState) float64 {
	freeCapacity := func(pk types.PeerKey) float64 {
		ns := cluster.Nodes[pk]
		if ns.NumCPU == 0 && ns.MemTotalBytes == 0 {
			return -1 // sentinel: no telemetry
		}
		freeCPU := float64(ns.NumCPU) * float64(percentMax-ns.CPUPercent) / percentMax
		freeMem := float64(ns.MemTotalBytes) * float64(percentMax-ns.MemPercent) / percentMax
		return freeCPU + freeMem/memScaleFactor
	}

	candidateFree := freeCapacity(candidate)
	if candidateFree < 0 {
		return neutralScore
	}

	var maxFree float64
	for _, pk := range peers {
		f := freeCapacity(pk)
		if f > maxFree {
			maxFree = f
		}
	}
	if maxFree == 0 {
		return neutralScore
	}
	return candidateFree / maxFree
}

// trafficAffinityScore returns 0–1 for how much bidirectional traffic the
// candidate exchanges with existing claimants. No traffic data → 0.0.
func trafficAffinityScore(candidate types.PeerKey, peers []types.PeerKey, claimants map[types.PeerKey]struct{}, cluster ClusterState) float64 {
	if len(claimants) == 0 {
		return 0.0
	}

	trafficSum := func(pk types.PeerKey) float64 {
		var total uint64
		ns := cluster.Nodes[pk]
		for cl := range claimants {
			if ns.TrafficTo != nil {
				total += ns.TrafficTo[cl]
			}
			if clNS, ok := cluster.Nodes[cl]; ok && clNS.TrafficTo != nil {
				total += clNS.TrafficTo[pk]
			}
		}
		return float64(total)
	}

	candidateTraffic := trafficSum(candidate)

	var maxTraffic float64
	for _, pk := range peers {
		t := trafficSum(pk)
		if t > maxTraffic {
			maxTraffic = t
		}
	}
	if maxTraffic == 0 {
		return 0.0
	}
	return candidateTraffic / maxTraffic
}

// vivaldiScore returns 0–1 for proximity to claimants. Closest = 1.0.
// Nil coords → 0.5 (neutral).
func vivaldiScore(candidate types.PeerKey, peers []types.PeerKey, claimants map[types.PeerKey]struct{}, cluster ClusterState) float64 {
	if len(claimants) == 0 {
		return neutralScore
	}

	candidateCoord := cluster.Nodes[candidate].Coord
	if candidateCoord == nil {
		return neutralScore
	}

	meanDist := func(pk types.PeerKey) float64 {
		coord := cluster.Nodes[pk].Coord
		if coord == nil {
			return -1 // sentinel
		}
		var sum float64
		var count int
		for cl := range claimants {
			clCoord := cluster.Nodes[cl].Coord
			if clCoord == nil {
				continue
			}
			sum += topology.Distance(*coord, *clCoord)
			count++
		}
		if count == 0 {
			return -1
		}
		return sum / float64(count)
	}

	candidateDist := meanDist(candidate)
	if candidateDist < 0 {
		return neutralScore
	}

	var maxDist float64
	for _, pk := range peers {
		d := meanDist(pk)
		if d > maxDist {
			maxDist = d
		}
	}
	if maxDist == 0 {
		return 1.0
	}
	// Invert: closest = 1.0, farthest = 0.0.
	return math.Max(0, 1.0-candidateDist/maxDist)
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
