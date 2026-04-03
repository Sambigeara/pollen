package placement

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"maps"
	"math"
	"slices"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/types"
)

type actionKind int

const (
	actionClaim   actionKind = iota // start running this workload
	actionRelease                   // stop running this workload
)

type action struct {
	Hash string
	Kind actionKind
}

type spec struct {
	Replicas uint32
}

type nodeState struct {
	Coord         *coords.Coord
	TrafficTo     map[types.PeerKey]uint64
	MemTotalBytes uint64
	CPUPercent    uint32
	MemPercent    uint32
	NumCPU        uint32
}

type clusterState struct {
	Nodes map[types.PeerKey]nodeState
}

const (
	weightCapacity = 0.4
	weightTraffic  = 0.5
	weightVivaldi  = 0.1
	incumbentBonus = 1.3
	neutralScore   = 0.5
	percentMax     = 100
	memScaleFactor = 1 << 30
)

type scored struct {
	peer        types.PeerKey
	suitability float64
	tieBreak    [32]byte
}

func compareScored(a, b scored) int {
	if a.suitability != b.suitability {
		return cmp.Compare(b.suitability, a.suitability)
	}
	return bytes.Compare(a.tieBreak[:], b.tieBreak[:])
}

func evaluate(
	localID types.PeerKey,
	allPeers []types.PeerKey,
	specs map[string]spec,
	claims map[string]map[types.PeerKey]struct{},
	cluster clusterState,
	isRunning func(string) bool,
) []action {
	var actions []action

	for hash, spec := range specs {
		claimants := claims[hash]
		claimCount := uint32(len(claimants))
		_, iClaimed := claimants[localID]

		if iClaimed && !isRunning(hash) {
			actions = append(actions, action{Hash: hash, Kind: actionClaim})
			continue // Recover before assessing release status
		}

		if !iClaimed && claimCount < spec.Replicas {
			if shouldClaim(localID, hash, spec.Replicas, claimants, allPeers, cluster) {
				actions = append(actions, action{Hash: hash, Kind: actionClaim})
			}
		} else if iClaimed && claimCount >= spec.Replicas {
			if shouldRelease(localID, hash, spec.Replicas, claimants, allPeers, cluster) {
				actions = append(actions, action{Hash: hash, Kind: actionRelease})
			}
		}
	}

	for hash, claimants := range claims {
		if _, iClaimed := claimants[localID]; !iClaimed {
			continue
		}
		if _, hasSpec := specs[hash]; !hasSpec {
			actions = append(actions, action{Hash: hash, Kind: actionRelease})
		}
	}

	return actions
}

func shouldClaim(localID types.PeerKey, hash string, desired uint32, claimants map[types.PeerKey]struct{}, allPeers []types.PeerKey, cluster clusterState) bool {
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
			suitability: suitabilityScore(pk, cluster, claimants, false),
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

func shouldRelease(localID types.PeerKey, hash string, desired uint32, claimants map[types.PeerKey]struct{}, allPeers []types.PeerKey, cluster clusterState) bool {
	claimCount := len(claimants)

	incumbents := make([]scored, 0, claimCount)
	for pk := range claimants {
		incumbents = append(incumbents, scored{
			peer:        pk,
			suitability: suitabilityScore(pk, cluster, claimants, true),
			tieBreak:    placementScore(pk, hash),
		})
	}
	slices.SortFunc(incumbents, compareScored)

	if claimCount > int(desired) {
		keep := min(int(desired), len(incumbents))
		for _, s := range incumbents[:keep] {
			if s.peer == localID {
				return false
			}
		}
		return true
	}

	weakest := incumbents[len(incumbents)-1]
	if weakest.peer != localID {
		return false
	}

	var bestChallenger *scored
	for _, pk := range allPeers {
		if _, incumbent := claimants[pk]; incumbent {
			continue
		}
		s := scored{
			peer:        pk,
			suitability: suitabilityScore(pk, cluster, claimants, false),
			tieBreak:    placementScore(pk, hash),
		}
		if bestChallenger == nil || compareScored(s, *bestChallenger) < 0 {
			bestChallenger = &s
		}
	}
	if bestChallenger == nil {
		return false
	}

	return compareScored(*bestChallenger, weakest) < 0
}

func suitabilityScore(candidate types.PeerKey, cluster clusterState, claimants map[types.PeerKey]struct{}, isIncumbent bool) float64 {
	peers := slices.Collect(maps.Keys(cluster.Nodes))

	raw := capacityScore(candidate, peers, cluster)*weightCapacity +
		trafficAffinityScore(candidate, peers, claimants, cluster)*weightTraffic +
		vivaldiScore(candidate, peers, claimants, cluster)*weightVivaldi

	if isIncumbent {
		raw *= incumbentBonus
	}

	return raw
}

func capacityScore(candidate types.PeerKey, peers []types.PeerKey, cluster clusterState) float64 {
	freeCapacity := func(pk types.PeerKey) float64 {
		ns := cluster.Nodes[pk]
		if ns.NumCPU == 0 && ns.MemTotalBytes == 0 {
			return -1
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

func trafficAffinityScore(candidate types.PeerKey, peers []types.PeerKey, claimants map[types.PeerKey]struct{}, cluster clusterState) float64 {
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

func vivaldiScore(candidate types.PeerKey, peers []types.PeerKey, claimants map[types.PeerKey]struct{}, cluster clusterState) float64 {
	if len(claimants) == 0 || cluster.Nodes[candidate].Coord == nil {
		return neutralScore
	}

	meanDist := func(pk types.PeerKey) float64 {
		coord := cluster.Nodes[pk].Coord
		if coord == nil {
			return -1
		}
		var sum float64
		var count int
		for cl := range claimants {
			clCoord := cluster.Nodes[cl].Coord
			if clCoord == nil {
				continue
			}
			sum += coords.Distance(*coord, *clCoord)
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
	return math.Max(0, 1.0-candidateDist/maxDist)
}

func placementScore(nodeID types.PeerKey, hash string) [32]byte {
	buf := make([]byte, len(nodeID)+len(hash))
	copy(buf, nodeID[:])
	copy(buf[len(nodeID):], hash)
	return sha256.Sum256(buf)
}
