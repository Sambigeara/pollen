// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"maps"
	"slices"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	percentMax = 100

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

// scoredPeers ranks `candidates` by predictedLatency with a deterministic
// hash tiebreak. Lower scores rank first. Pure function of cluster state —
// every node sees the same order.
func scoredPeers(candidates []types.PeerKey, hash string, cluster clusterState) []scored {
	out := make([]scored, 0, len(candidates))
	for _, pk := range candidates {
		out = append(out, newScored(pk, hash, cluster))
	}
	slices.SortFunc(out, compareScored)
	return out
}

// rankTopN reports whether localID is in the first n entries after the
// deterministic ranking. Thin wrapper kept for the binary "in/out" use case
// where callers don't need the worst-keeper score.
func rankTopN(candidates []types.PeerKey, hash string, cluster clusterState, n int, localID types.PeerKey) bool {
	if n <= 0 || len(candidates) == 0 {
		return false
	}
	sorted := scoredPeers(candidates, hash, cluster)
	if n > len(sorted) {
		n = len(sorted)
	}
	for _, s := range sorted[:n] {
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

func placementScore(nodeID types.PeerKey, hash string) [32]byte {
	buf := make([]byte, len(nodeID)+len(hash))
	copy(buf, nodeID[:])
	copy(buf[len(nodeID):], hash)
	return sha256.Sum256(buf)
}

// distance returns the Vivaldi distance between two peers' coordinates.
// Returns 0 for self-pairs. When either side is missing coordinates it
// returns the cluster-mean pairwise distance — a neutral penalty —
// rather than 0, otherwise an unknown-topology candidate would always
// score better than every measured peer. Pre-computed in
// buildClusterState so this stays O(1).
func distance(a, b types.PeerKey, cluster clusterState) float64 {
	if a == b {
		return 0
	}
	ca := cluster.Nodes[a].Coord
	cb := cluster.Nodes[b].Coord
	if ca == nil || cb == nil {
		return cluster.MeanCoordDistance
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
// Under steady state (cluster-wide OriginRate sum ≥ observedDemandFloor)
// the score is the demand-weighted Vivaldi distance from candidate to
// every peer currently observing traffic for the seed. Hosting here
// minimises the expected first-hop cost for the cluster as a whole; a
// peer sitting on the demand centroid scores 0, a peer far from all
// traffic scores high. Chain affinity (A's hosts call B → those peers
// carry OriginRate[B]) and external-entry affinity (edge peers see the
// arriving pln call → they carry OriginRate[A]) both fall out of this
// single signal.
//
// We gate and divide by sum-of-OriginRate, not sum-of-ServedRate.
// During saturation ServedRate lags OriginRate (rejections don't serve)
// — using ServedRate as the gate would flip scoring back to cold-start
// at exactly the moment the demand signal is most informative.
//
// Before traffic flows (cold start), or when topology hasn't converged
// enough to tell a missing-coord candidate apart from a centroid-
// colocated one, rank by raw compute fit instead: expected wall-time =
// cluster-mean ComputeCost / this peer's CPU capacity. With no
// observations at all, every candidate scores zero and the hash
// tiebreak picks.
func predictedLatency(candidate types.PeerKey, hash string, cluster clusterState) float64 {
	demand := originSum(hash, cluster)
	if demand < observedDemandFloor || coordTopologyAmbiguous(candidate, hash, cluster) {
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

	rates := cluster.OriginRates[hash]
	var total float64
	for _, peer := range slices.SortedFunc(maps.Keys(rates), types.PeerKey.Compare) {
		total += rates[peer] * distance(candidate, peer, cluster)
	}
	return total / demand
}

// coordTopologyAmbiguous reports whether the cluster's Vivaldi data is
// too thin to score the candidate fairly. distance() falls back to
// MeanCoordDistance whenever a coord is missing — but with fewer than
// two known coords MeanCoordDistance is 0, indistinguishable from a
// candidate sitting exactly on the demand centroid. Detect that combo
// and route the call to cold-start instead. With ≥2 known coords (mean
// strictly positive) the substitution is meaningful, and with all
// coords known the answer is "false" regardless of the mean.
func coordTopologyAmbiguous(candidate types.PeerKey, hash string, cluster clusterState) bool {
	if cluster.MeanCoordDistance > 0 {
		return false
	}
	if cluster.Nodes[candidate].Coord == nil {
		return true
	}
	rates := cluster.OriginRates[hash]
	for _, peer := range slices.SortedFunc(maps.Keys(rates), types.PeerKey.Compare) {
		if cluster.Nodes[peer].Coord == nil {
			return true
		}
	}
	return false
}

// hasCapacity is the hard placement gate: rejects candidates whose
// remaining CPU/memory headroom can't host the seed, or whose gossiped
// AdmissionState is Closed. Missing telemetry (no entry in cluster.Nodes,
// or empty NodeState) is treated as "unknown — assume fits" so cold
// nodes can be considered. AdmissionClosed exclusion is symmetric with
// the routing-side filter in pickP2C — a peer that won't accept work
// shouldn't be picked to host work either.
func hasCapacity(candidate types.PeerKey, memoryBytes uint64, cluster clusterState) bool {
	ns := cluster.Nodes[candidate]
	if ns.AdmissionState == state.AdmissionClosed {
		return false
	}
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
