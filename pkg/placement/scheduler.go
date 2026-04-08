package placement

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/types"
)

type actionKind int

const (
	actionClaim   actionKind = iota // start running this workload
	actionRelease                   // stop running this workload
)

type action struct {
	Hash          string
	Kind          actionKind
	DynamicTarget uint32
}

type spec struct {
	MinReplicas uint32
	Spread      float32
}

type nodeState struct {
	Coord            *coords.Coord
	TrafficTo        map[types.PeerKey]uint64
	MemTotalBytes    uint64
	CPUPercent       uint32
	MemPercent       uint32
	NumCPU           uint32
	CPUBudgetPercent uint32
	MemBudgetPercent uint32
}

type clusterState struct {
	Nodes      map[types.PeerKey]nodeState
	SeedLoad   map[types.PeerKey]map[string]float32 // per-node seed served rates (from gossip)
	SeedDemand map[types.PeerKey]map[string]float32 // per-node seed demand rates (from gossip)
}

const (
	weightCapacity = 0.30
	weightDemand   = 0.30
	weightTraffic  = 0.25
	weightVivaldi  = 0.15

	incumbentBonus     = 1.3
	challengeThreshold = 1.2
	neutralScore       = 0.5
	percentMax         = 100
	memScaleFactor     = 1 << 30

	releaseIdleBase   = 2 * time.Minute
	releaseIdleJitter = 30 * time.Second
)

type scored struct {
	peer        types.PeerKey
	suitability float64
	rawScore    float64
	tieBreak    [32]byte
}

func compareScored(a, b scored) int {
	if a.suitability != b.suitability {
		return cmp.Compare(b.suitability, a.suitability)
	}
	return bytes.Compare(a.tieBreak[:], b.tieBreak[:])
}

func effectiveTarget(s spec, clusterSize int, dynamicTarget uint32) uint32 {
	if s.Spread >= 1.0 {
		return uint32(clusterSize)
	}
	return max(s.MinReplicas, min(dynamicTarget, uint32(clusterSize)))
}

type evaluateInput struct {
	specs          map[string]spec
	claims         map[string]map[types.PeerKey]struct{}
	cluster        clusterState
	isRunning      func(string) bool
	demandRates    map[string]float64
	idleDurations  map[string]time.Duration
	dynamicTargets map[string]uint32
	allPeers       []types.PeerKey
	localID        types.PeerKey
}

func evaluate(in evaluateInput) []action {
	var actions []action

	for hash, sp := range in.specs {
		claimants := in.claims[hash]
		claimCount := uint32(len(claimants))
		_, iClaimed := claimants[in.localID]
		target := effectiveTarget(sp, len(in.allPeers), in.dynamicTargets[hash])

		if iClaimed && !in.isRunning(hash) {
			actions = append(actions, action{Hash: hash, Kind: actionClaim, DynamicTarget: target})
			continue
		}

		switch {
		case !iClaimed && claimCount < target:
			if shouldClaim(in.localID, hash, target, claimants, in.allPeers, in.cluster, in.demandRates) {
				actions = append(actions, action{Hash: hash, Kind: actionClaim, DynamicTarget: target})
			}
		case !iClaimed && claimCount >= target:
			if shouldChallenge(in.localID, hash, claimants, in.cluster, in.demandRates) {
				actions = append(actions, action{Hash: hash, Kind: actionClaim, DynamicTarget: target})
			}
		case iClaimed && claimCount > target:
			if shouldReleaseExcess(in.localID, hash, target, claimants, in.cluster, in.demandRates, in.idleDurations) {
				actions = append(actions, action{Hash: hash, Kind: actionRelease})
			}
		}
	}

	for hash, claimants := range in.claims {
		if _, iClaimed := claimants[in.localID]; !iClaimed {
			continue
		}
		if _, hasSpec := in.specs[hash]; !hasSpec {
			actions = append(actions, action{Hash: hash, Kind: actionRelease})
		}
	}

	return actions
}

func shouldClaim(localID types.PeerKey, hash string, desired uint32, claimants map[types.PeerKey]struct{}, allPeers []types.PeerKey, cluster clusterState, demandRates map[string]float64) bool {
	needed := int(desired) - len(claimants)
	unclaimed := make([]scored, 0, len(allPeers))
	for _, pk := range allPeers {
		if _, already := claimants[pk]; already {
			continue
		}
		raw := rawSuitabilityScore(pk, cluster, claimants, demandRates, hash, localID)
		unclaimed = append(unclaimed, scored{
			peer:        pk,
			suitability: raw,
			rawScore:    raw,
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

// shouldChallenge decides whether localID should claim a hash that is already
// at or above effectiveTarget. Requires the local raw score to exceed the
// weakest incumbent's raw score by challengeThreshold.
func shouldChallenge(localID types.PeerKey, hash string, claimants map[types.PeerKey]struct{}, cluster clusterState, demandRates map[string]float64) bool {
	myRaw := rawSuitabilityScore(localID, cluster, claimants, demandRates, hash, localID)

	var weakestRaw float64
	first := true
	for pk := range claimants {
		raw := rawSuitabilityScore(pk, cluster, claimants, demandRates, hash, localID)
		if first || raw < weakestRaw {
			weakestRaw = raw
			first = false
		}
	}

	return myRaw > weakestRaw*challengeThreshold
}

// shouldReleaseExcess handles release for excess claimants (above effectiveTarget).
// Requires the seed to be idle for releaseIdleBase + jitter, and the local node
// to be the lowest-scoring excess claimant.
func shouldReleaseExcess(localID types.PeerKey, hash string, target uint32, claimants map[types.PeerKey]struct{}, cluster clusterState, demandRates map[string]float64, idleDurations map[string]time.Duration) bool {
	idle := idleDurations[hash]
	//nolint:gosec
	jitter := time.Duration(rand.Int64N(int64(releaseIdleJitter)))
	if idle < releaseIdleBase+jitter {
		return false
	}

	incumbents := make([]scored, 0, len(claimants))
	for pk := range claimants {
		raw := rawSuitabilityScore(pk, cluster, claimants, demandRates, hash, localID)
		incumbents = append(incumbents, scored{
			peer:        pk,
			suitability: raw * incumbentBonus,
			rawScore:    raw,
			tieBreak:    placementScore(pk, hash),
		})
	}
	slices.SortFunc(incumbents, compareScored)

	keep := min(int(target), len(incumbents))
	for _, s := range incumbents[:keep] {
		if s.peer == localID {
			return false
		}
	}
	return true
}

func rawSuitabilityScore(candidate types.PeerKey, cluster clusterState, claimants map[types.PeerKey]struct{}, demandRates map[string]float64, hash string, localID types.PeerKey) float64 {
	peers := slices.Collect(maps.Keys(cluster.Nodes))

	return capacityScore(candidate, peers, cluster)*weightCapacity +
		localDemandScore(candidate, localID, demandRates, hash)*weightDemand +
		trafficAffinityScore(candidate, peers, claimants, cluster)*weightTraffic +
		vivaldiScore(candidate, peers, claimants, cluster)*weightVivaldi
}

func localDemandScore(candidate, localID types.PeerKey, demandRates map[string]float64, hash string) float64 {
	if candidate != localID {
		return 0
	}
	rate := demandRates[hash]
	if rate <= 0 {
		return 0
	}
	// Normalise to [0,1]. The 100 calls/sec saturation point is a placeholder —
	// ideally this would normalise against peers (like capacityScore does), but
	// demand rates are purely local so we have no cluster-wide max to compare
	// against. Tune once real workload data is available.
	return math.Min(1.0, rate/100.0) //nolint:mnd
}

func capacityScore(candidate types.PeerKey, peers []types.PeerKey, cluster clusterState) float64 {
	freeCapacity := func(pk types.PeerKey) (float64, bool) {
		ns := cluster.Nodes[pk]
		if ns.NumCPU == 0 && ns.MemTotalBytes == 0 {
			return 0, false
		}
		cpuBudget := uint32(percentMax)
		if ns.CPUBudgetPercent > 0 {
			cpuBudget = ns.CPUBudgetPercent
		}
		memBudget := uint32(percentMax)
		if ns.MemBudgetPercent > 0 {
			memBudget = ns.MemBudgetPercent
		}
		freeCPU := float64(ns.NumCPU) * float64(int32(cpuBudget)-int32(ns.CPUPercent)) / percentMax
		freeMem := float64(ns.MemTotalBytes) * float64(int32(memBudget)-int32(ns.MemPercent)) / percentMax
		return freeCPU + freeMem/memScaleFactor, true
	}

	candidateFree, ok := freeCapacity(candidate)
	if !ok {
		return neutralScore
	}

	var maxFree float64
	for _, pk := range peers {
		f, ok := freeCapacity(pk)
		if ok && f > maxFree {
			maxFree = f
		}
	}
	if maxFree <= 0 {
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
