// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"bytes"
	"cmp"
	"crypto/sha256"
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

// clusterState is the per-tick input to placement scoring, built by
// buildClusterState from the gossiped state snapshot. Node resources
// live in Nodes; per-seed observations live in the two rate maps below.
type clusterState struct {
	Nodes map[types.PeerKey]nodeState

	// InvocationRates[hash] is the cluster-wide sum of ServedRates — the
	// total execution rate the cluster is carrying for each seed.
	InvocationRates map[string]float64

	// OriginRates[hash][peer] is the rate of calls entering the cluster
	// at peer for hash. Demand signal — where traffic for the seed wants
	// to land. Gossiped per-peer so every node sees the same distribution.
	OriginRates map[string]map[types.PeerKey]float64

	// ComputeCost[hash] is the cluster-mean wall-time (ms) per invocation
	// observed across hosts. Used for cold-start scoring (scaling by CPU
	// capacity) and for Little's-Law gate sizing (active = compute -
	// parked).
	ComputeCost map[string]float64

	// ParkedTime[hash] is the cluster-mean time (ms) per invocation that
	// seeds spent blocked inside pollen_request waiting for downstream
	// responses. Feeds adaptive gate sizing alongside ComputeCost.
	ParkedTime map[string]float64
}

const (
	percentMax = 100

	releaseIdleBase   = 30 * time.Second
	releaseIdleJitter = 10 * time.Second

	// observedDemandFloor is the cluster-wide InvocationRate below which
	// the centroid score has too little signal and we fall back to
	// cold-start scoring (CPU-fit plus hash tiebreak).
	observedDemandFloor = 1.0
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
			if shouldChallenge(in.localID, hash, sp, target, claimants, in.allPeers, in.cluster) {
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
// claimant set. Contenders are the current claimants plus every
// capacity-passing non-claimant — local wins only when it ranks in the
// top-`target` across that full pool. Ranking the full pool (rather than
// just claimants+local) guarantees at most `target` winners cluster-wide;
// otherwise every non-claimant that beats a single claimant would fire
// actionClaim independently and herd-over-replicate.
func shouldChallenge(localID types.PeerKey, hash string, sp spec, target uint32, claimants map[types.PeerKey]struct{}, allPeers []types.PeerKey, cluster clusterState) bool {
	contenders := make([]types.PeerKey, 0, len(allPeers))
	for _, pk := range allPeers {
		if _, isClaimant := claimants[pk]; !isClaimant && !hasCapacity(pk, sp.MemoryBytes, cluster) {
			continue
		}
		contenders = append(contenders, pk)
	}
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

// distance returns the Vivaldi distance between two peers' coordinates.
// Returns 0 when either side is missing coordinates or the peers are the
// same — treats unknown distances as zero so missing topology data
// doesn't artificially inflate the centroid score.
func distance(a, b types.PeerKey, cluster clusterState) float64 {
	if a == b {
		return 0
	}
	ca := cluster.Nodes[a].Coord
	cb := cluster.Nodes[b].Coord
	if ca == nil || cb == nil {
		return 0
	}
	return coords.Distance(*ca, *cb)
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

// predictedLatency scores a candidate to host a seed. The lower the
// better; deterministic sha256(peerKey, hash) tiebreak handles exact
// equalities.
//
// Under steady state (cluster-wide InvocationRate ≥ observedDemandFloor)
// the score is the demand-weighted Vivaldi distance from candidate to
// every peer currently observing traffic for the seed. Hosting here
// minimises the expected first-hop cost for the cluster as a whole; a
// peer sitting on the demand centroid scores 0, a peer far from all
// traffic scores high. Chain affinity (A's hosts call B → those peers
// carry OriginRate[B]) and external-entry affinity (edge peers see the
// arriving pln call → they carry OriginRate[A]) both fall out of this
// single signal.
//
// Before traffic flows (cold start), we have no demand distribution.
// Rank by raw compute fit instead: expected wall-time = cluster-mean
// ComputeCost / this peer's CPU capacity. With no observations at all,
// every candidate scores zero and the hash tiebreak picks.
func predictedLatency(candidate types.PeerKey, hash string, cluster clusterState) float64 {
	inv := cluster.InvocationRates[hash]
	if inv < observedDemandFloor {
		if cost, ok := cluster.ComputeCost[hash]; ok {
			capacity := cpuCapacity(cluster.Nodes[candidate])
			if capacity <= 0 {
				// Cold candidate: pessimistic single-CPU default so
				// nodes without telemetry don't unfairly outrank
				// measured peers.
				capacity = 1.0
			}
			return cost / capacity
		}
		return 0
	}

	var total float64
	for peer, originRate := range cluster.OriginRates[hash] {
		total += originRate * distance(candidate, peer, cluster)
	}
	// Self-origin discount: a peer absorbing its own share of demand
	// strictly beats a centrally-placed demand-free peer of the same
	// total distance profile. Without this, two candidates with
	// identical weighted-avg distance to all origins tie — and a
	// proxy sitting at the Vivaldi centroid of the demand set can
	// outrank a hot entry node. The (1 - ownShare) factor breaks that
	// tie in favour of the candidate actually generating traffic.
	ownShare := cluster.OriginRates[hash][candidate] / inv
	return total * (1 - ownShare) / inv
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
