package placement

import (
	"bytes"
	"cmp"
	"crypto/sha256"
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
	MemoryBytes uint64
	MinReplicas uint32
	Spread      float32
}

type nodeState struct {
	Coord            *coords.Coord
	MemTotalBytes    uint64
	CPUPercent       uint32
	MemPercent       uint32
	NumCPU           uint32
	CPUBudgetPercent uint32
	MemBudgetPercent uint32
}

type clusterState struct {
	Nodes map[types.PeerKey]nodeState

	// Per-seed observations aggregated across hosts. Compute cost is the mean
	// wall-time per invocation; dial rates are the cluster-wide sum (aligned
	// with the cluster-wide invocation rate so calls_per_invocation = sum/sum
	// is well-defined). InvocationRates is the cluster-wide sum of per-node
	// ServedRate contributions from the SeedMetrics bundle. ParkedTime is
	// the cluster-wide mean of per-node parked-time observations (ms spent
	// inside pollen_request per invocation); the gate sizer uses it to
	// derive active CPU time as ComputeCost - ParkedTime.
	ComputeCost     map[string]float64
	ParkedTime      map[string]float64
	DialRates       map[string]map[string]float64
	InvocationRates map[string]float64

	// Target → host lookups for dial-graph resolution.
	SeedHosts    map[string][]types.PeerKey
	ServiceHosts map[string][]types.PeerKey
}

const (
	percentMax = 100

	releaseIdleBase   = 30 * time.Second
	releaseIdleJitter = 10 * time.Second
)

type scored struct {
	peer     types.PeerKey
	latency  float64
	tieBreak [32]byte
}

// compareScored ranks lowest predicted latency first, breaking exact ties
// by hash so all nodes converge on the same ordering each tick.
func compareScored(a, b scored) int {
	if a.latency != b.latency {
		return cmp.Compare(a.latency, b.latency)
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
			if shouldClaim(in.localID, hash, sp, target, claimants, in.allPeers, in.cluster) {
				actions = append(actions, action{Hash: hash, Kind: actionClaim, DynamicTarget: target})
			}
		case !iClaimed && claimCount >= target:
			if shouldChallenge(in.localID, hash, sp, target, claimants, in.cluster) {
				actions = append(actions, action{Hash: hash, Kind: actionClaim, DynamicTarget: target})
			}
		case iClaimed && claimCount > target:
			if shouldReleaseExcess(in.localID, hash, target, claimants, in.cluster, in.idleDurations) {
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

// rankTopN scores each candidate by predicted latency (with a
// deterministic hash tiebreak), sorts, and returns whether localID is
// in the first n entries. Used by the three decision functions below —
// they only differ in which peers go into the candidate pool and what
// n is.
func rankTopN(candidates []types.PeerKey, hash string, cluster clusterState, n int, localID types.PeerKey) bool {
	if n <= 0 || len(candidates) == 0 {
		return false
	}
	scored := make([]scored, 0, len(candidates))
	for _, pk := range candidates {
		scored = append(scored, newScored(pk, hash, cluster))
	}
	slices.SortFunc(scored, compareScored)

	if n > len(scored) {
		n = len(scored)
	}
	for _, s := range scored[:n] {
		if s.peer == localID {
			return true
		}
	}
	return false
}

func newScored(pk types.PeerKey, hash string, cluster clusterState) scored {
	return scored{
		peer:     pk,
		latency:  predictedLatency(pk, hash, cluster),
		tieBreak: placementScore(pk, hash),
	}
}

// shouldClaim decides whether localID should join the claimant set for an
// under-replicated workload. Local wins if it is one of the `needed`
// best-predicted candidates among capacity-passing unclaimed peers.
func shouldClaim(localID types.PeerKey, hash string, sp spec, desired uint32, claimants map[types.PeerKey]struct{}, allPeers []types.PeerKey, cluster clusterState) bool {
	needed := int(desired) - len(claimants)
	if needed <= 0 {
		return false
	}
	unclaimed := make([]types.PeerKey, 0, len(allPeers))
	for _, pk := range allPeers {
		if _, already := claimants[pk]; already {
			continue
		}
		if !hasCapacity(pk, sp.MemoryBytes, cluster) {
			continue
		}
		unclaimed = append(unclaimed, pk)
	}
	return rankTopN(unclaimed, hash, cluster, needed, localID)
}

// shouldChallenge decides whether localID should join an at-or-above-target
// claimant set. Local wins if, when added to the contender pool, it ranks in
// the top-`target` by predicted latency (with hash tiebreak on equality).
func shouldChallenge(localID types.PeerKey, hash string, sp spec, target uint32, claimants map[types.PeerKey]struct{}, cluster clusterState) bool {
	if !hasCapacity(localID, sp.MemoryBytes, cluster) {
		return false
	}
	contenders := make([]types.PeerKey, 0, len(claimants)+1)
	for pk := range claimants {
		contenders = append(contenders, pk)
	}
	contenders = append(contenders, localID)
	return rankTopN(contenders, hash, cluster, int(target), localID)
}

// shouldReleaseExcess fires when localID is among the excess claimants for a
// workload (claimCount > target). Releases when the seed has been idle long
// enough and localID is not one of the top-`target` claimants by predicted
// latency. The idle window prevents thrashing during transient rebalances;
// deterministic latency+hash ranking ensures at most one node chooses to
// release per tick.
func shouldReleaseExcess(localID types.PeerKey, hash string, target uint32, claimants map[types.PeerKey]struct{}, cluster clusterState, idleDurations map[string]time.Duration) bool {
	idle := idleDurations[hash]
	//nolint:gosec
	jitter := time.Duration(rand.Int64N(int64(releaseIdleJitter)))
	if idle < releaseIdleBase+jitter {
		return false
	}

	incumbents := make([]types.PeerKey, 0, len(claimants))
	for pk := range claimants {
		incumbents = append(incumbents, pk)
	}
	// Top-target are safe; local is excess if not in the top-target.
	return !rankTopN(incumbents, hash, cluster, int(target), localID)
}

func placementScore(nodeID types.PeerKey, hash string) [32]byte {
	buf := make([]byte, len(nodeID)+len(hash))
	copy(buf, nodeID[:])
	copy(buf[len(nodeID):], hash)
	return sha256.Sum256(buf)
}

// hostsForTarget resolves a dial-graph target key ("seed:<hash>" or
// "service:<name>") to the set of peers currently hosting it.
func hostsForTarget(targetKey string, cluster clusterState) []types.PeerKey {
	prefix, name, ok := splitTargetKey(targetKey)
	if !ok {
		return nil
	}
	switch prefix {
	case "seed":
		return cluster.SeedHosts[name]
	case "service":
		return cluster.ServiceHosts[name]
	}
	return nil
}

func splitTargetKey(target string) (prefix, name string, ok bool) {
	for i := 0; i < len(target); i++ {
		if target[i] == ':' {
			return target[:i], target[i+1:], true
		}
	}
	return "", "", false
}

// minRTT returns the minimum Vivaldi distance from candidate to any node in
// hosts. Returns 0 if candidate itself appears in hosts (co-located target).
// Returns 0 if no usable coordinate pair exists — treats unknown distances
// as zero so missing topology data doesn't artificially inflate latency.
func minRTT(candidate types.PeerKey, hosts []types.PeerKey, cluster clusterState) float64 {
	if len(hosts) == 0 {
		return 0
	}
	candidateCoord := cluster.Nodes[candidate].Coord
	best := math.Inf(1)
	found := false
	for _, h := range hosts {
		if h == candidate {
			return 0
		}
		hostCoord := cluster.Nodes[h].Coord
		if candidateCoord == nil || hostCoord == nil {
			continue
		}
		d := coords.Distance(*candidateCoord, *hostCoord)
		if d < best {
			best = d
			found = true
		}
	}
	if !found {
		return 0
	}
	return best
}

// cpuCapacity returns the candidate's CPU allocation expressed as
// CPU-seconds per wall-second (numCPU × budget fraction). Returns 0 when
// node telemetry has not yet been published.
func cpuCapacity(ns nodeState) float64 {
	if ns.NumCPU == 0 {
		return 0
	}
	budget := float64(percentMax)
	if ns.CPUBudgetPercent > 0 {
		budget = float64(ns.CPUBudgetPercent)
	}
	return float64(ns.NumCPU) * budget / float64(percentMax)
}

// predictedLatency estimates the per-invocation wall-time for hosting seed
// hash on candidate. Combines the seed's measured compute cost (scaled to
// the candidate's CPU capacity) with the network-traversal cost of each
// downstream dial, weighted by calls-per-invocation.
func predictedLatency(candidate types.PeerKey, hash string, cluster clusterState) float64 {
	ns := cluster.Nodes[candidate]
	capacity := cpuCapacity(ns)

	var compute float64
	if cost, ok := cluster.ComputeCost[hash]; ok {
		// Cold candidates (no CPU telemetry yet) fall back to a single-CPU
		// equivalent — pessimistic enough that they don't unfairly outrank
		// measured peers, lenient enough that they remain placeable.
		effectiveCapacity := capacity
		if effectiveCapacity <= 0 {
			effectiveCapacity = 1.0
		}
		compute = cost / effectiveCapacity
	}

	var network float64
	if dials, ok := cluster.DialRates[hash]; ok {
		invocationRate := cluster.InvocationRates[hash]
		if invocationRate > 0 {
			for target, rate := range dials {
				callsPerInvocation := rate / invocationRate
				hosts := hostsForTarget(target, cluster)
				network += callsPerInvocation * minRTT(candidate, hosts, cluster)
			}
		}
	}

	return compute + network
}

// hasCapacity is the hard placement gate: rejects candidates whose
// remaining CPU/memory headroom can't host the seed. Missing telemetry
// (no entry in cluster.Nodes, or empty NodeState) is treated as
// "unknown — assume fits" so cold nodes can be considered.
func hasCapacity(candidate types.PeerKey, memoryBytes uint64, cluster clusterState) bool {
	ns := cluster.Nodes[candidate]
	if ns.NumCPU == 0 && ns.MemTotalBytes == 0 {
		return true
	}

	cpuBudget := uint32(percentMax)
	if ns.CPUBudgetPercent > 0 {
		cpuBudget = ns.CPUBudgetPercent
	}
	if int32(ns.CPUPercent) >= int32(cpuBudget) {
		return false
	}

	if memoryBytes > 0 && ns.MemTotalBytes > 0 {
		memBudget := uint32(percentMax)
		if ns.MemBudgetPercent > 0 {
			memBudget = ns.MemBudgetPercent
		}
		freeMem := float64(ns.MemTotalBytes) * float64(int32(memBudget)-int32(ns.MemPercent)) / float64(percentMax)
		if freeMem < float64(memoryBytes) {
			return false
		}
	}
	return true
}
