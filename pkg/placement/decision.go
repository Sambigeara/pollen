// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"github.com/sambigeara/pollen/pkg/types"
)

type actionKind int

const (
	actionClaim   actionKind = iota // start running this workload
	actionRelease                   // stop running this workload
)

type action struct {
	Hash            string
	Kind            actionKind
	DesiredReplicas uint32
}

type spec struct {
	MemoryBytes uint64
	MinReplicas uint32
	Spread      float32
}

func effectiveTarget(s spec, clusterSize int, desired uint32) uint32 {
	if s.Spread >= 1.0 {
		return uint32(clusterSize)
	}
	return max(s.MinReplicas, min(desired, uint32(clusterSize)))
}

type evaluateInput struct {
	specs           map[string]spec
	claims          map[string]map[types.PeerKey]struct{}
	draining        map[string]map[types.PeerKey]struct{}
	cluster         clusterState
	isRunning       func(string) bool
	desiredReplicas map[string]uint32
	allPeers        []types.PeerKey
	localID         types.PeerKey
}

// activeClaimCount returns the number of claimants whose claim is *not*
// flagged as draining — the count that drives "do we need more replicas".
// Draining peers stay in the full claimants set (callers still route to
// them until release) but don't count against the desired-replica target;
// this is the make-before-break overlap mechanism.
func activeClaimCount(claimants, drainSet map[types.PeerKey]struct{}) uint32 {
	if len(drainSet) == 0 {
		return uint32(len(claimants))
	}
	var n uint32
	for pk := range claimants {
		if _, drain := drainSet[pk]; !drain {
			n++
		}
	}
	return n
}

func evaluate(in evaluateInput) []action {
	var actions []action

	for hash, sp := range in.specs {
		claimants := in.claims[hash]
		drainSet := in.draining[hash]
		_, iClaimed := claimants[in.localID]
		_, iDraining := drainSet[in.localID]
		target := effectiveTarget(sp, len(in.allPeers), in.desiredReplicas[hash])
		claimCount := activeClaimCount(claimants, drainSet)

		// Local is mid-drain. The reconciler's cooldown owns the actual
		// release; this layer must still emit an explicit signal so the
		// cooldown can distinguish "still excess" (release proceeds) from
		// "demand recovered, un-drain". Silence here lets the cooldown
		// cancel itself on the very next tick.
		if iDraining {
			if claimCount < target {
				actions = append(actions, action{Hash: hash, Kind: actionClaim, DesiredReplicas: target})
			} else {
				actions = append(actions, action{Hash: hash, Kind: actionRelease})
			}
			continue
		}

		if iClaimed && !in.isRunning(hash) {
			actions = append(actions, action{Hash: hash, Kind: actionClaim, DesiredReplicas: target})
			continue
		}

		switch {
		case !iClaimed && claimCount < target:
			if shouldClaim(in.localID, hash, sp, target, claimants, drainSet, in.allPeers, in.cluster) {
				actions = append(actions, action{Hash: hash, Kind: actionClaim, DesiredReplicas: target})
			}
		case !iClaimed && claimCount >= target:
			if shouldChallenge(in.localID, hash, sp, target, claimants, drainSet, in.allPeers, in.cluster) {
				// At-or-above-target challenge: claim immediately. The
				// 25% incumbentMargin gate inside shouldChallenge is the
				// anti-flap mechanism — only sustained, materially better
				// placements clear it, so action-side hysteresis isn't
				// needed.
				actions = append(actions, action{Hash: hash, Kind: actionClaim, DesiredReplicas: target})
			}
		case iClaimed && claimCount > target:
			if shouldReleaseExcess(in.localID, hash, target, claimants, drainSet, in.cluster) {
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

// shouldClaim decides whether localID should join the claimant set for an
// under-replicated workload. `desired - active_claimants` slots are open;
// local wins if it is one of the best-predicted capacity-passing
// non-claimants. Draining claimants do not block their own replacement —
// they're leaving — but they aren't candidates either, since they're
// already publishing intent to release.
func shouldClaim(localID types.PeerKey, hash string, sp spec, desired uint32, claimants, drainSet map[types.PeerKey]struct{}, allPeers []types.PeerKey, cluster clusterState) bool {
	needed := int(desired) - int(activeClaimCount(claimants, drainSet))
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

// incumbentMargin is the minimum scoring improvement (predictedLatency
// ratio) a non-claimant must demonstrate over the worst current claimant
// before challenging — and symmetrically the minimum improvement an
// over-replicated claimant must observe in the keeper pool before draining.
// 1.25 = 25% better. Below this band the migration cost (wasm compile +
// blob fetch + gate ramp + warm-up traffic split) outweighs the placement
// improvement, and the cluster oscillates between near-equivalent layouts.
const incumbentMargin = 1.25

// shouldChallenge decides whether localID should join an at-or-above-target
// claimant set. Contenders are the current active claimants plus every
// capacity-passing non-claimant — local wins only when it ranks in the
// top-`target` across that full pool AND its predicted latency is at
// least incumbentMargin × better than the worst current claimant.
// Ranking the full pool (rather than just claimants+local) guarantees at
// most `target` winners cluster-wide; otherwise every non-claimant that
// beats a single claimant would fire actionClaim independently and
// herd-over-replicate. The margin gate prevents flap when scores are
// within noise of each other. Draining claimants are excluded from
// contenders — they're leaving and shouldn't compete for slots they're
// vacating.
func shouldChallenge(localID types.PeerKey, hash string, sp spec, target uint32, claimants, drainSet map[types.PeerKey]struct{}, allPeers []types.PeerKey, cluster clusterState) bool {
	contenders := make([]types.PeerKey, 0, len(allPeers))
	for _, pk := range allPeers {
		if _, isClaimant := claimants[pk]; isClaimant {
			if _, drain := drainSet[pk]; drain {
				continue
			}
			contenders = append(contenders, pk)
			continue
		}
		if !hasCapacity(pk, sp.MemoryBytes, cluster) {
			continue
		}
		contenders = append(contenders, pk)
	}
	if !rankTopN(contenders, hash, cluster, int(target), localID) {
		return false
	}
	return beatsWorstClaimantByMargin(localID, hash, claimants, drainSet, cluster)
}

// beatsWorstClaimantByMargin returns true when localID's predicted score
// is at least incumbentMargin × better than the worst current (active,
// non-draining) claimant. With no claimants or zero-score signal the
// margin gate yields to the rankTopN decision (no informative comparison
// available) so cold-start placement still proceeds.
func beatsWorstClaimantByMargin(localID types.PeerKey, hash string, claimants, drainSet map[types.PeerKey]struct{}, cluster clusterState) bool {
	var worstClaimantScore float64
	for pk := range claimants {
		if _, drain := drainSet[pk]; drain {
			continue
		}
		s := predictedLatency(pk, hash, cluster)
		if s > worstClaimantScore {
			worstClaimantScore = s
		}
	}
	if worstClaimantScore <= 0 {
		return true
	}
	localScore := predictedLatency(localID, hash, cluster)
	return localScore*incumbentMargin <= worstClaimantScore
}

// shouldReleaseExcess fires when localID is among the excess active claimants
// for a workload. Releases when local is not one of the top-`target` active
// claimants by predicted latency. Draining claimants are skipped from the
// incumbents pool — they are already leaving via the cooldown path; counting
// them here would double-shed.
//
// Make-before-break gate: when the workload has live demand, refuse to
// drain unless `target` non-local non-draining claimants are *already
// serving* (ServedRate > 0). Otherwise we'd hand traffic off to a pool
// that hasn't ramped — fresh claimants may still be fetching the blob,
// or compiled but never invoked. Idle workloads have no demand to lose,
// so the warm-replacement check is skipped and shedding proceeds.
//
// The cooldown + drain handshake in the reconciler already provides the
// 30s overlap window — no idle-duration gate is needed here.
func shouldReleaseExcess(localID types.PeerKey, hash string, target uint32, claimants, drainSet map[types.PeerKey]struct{}, cluster clusterState) bool {
	served := cluster.ServedRates[hash]
	if hasLiveDemand(served) {
		var warm uint32
		for pk := range claimants {
			if pk == localID {
				continue
			}
			if _, drain := drainSet[pk]; drain {
				continue
			}
			if served[pk] > 0 {
				warm++
			}
		}
		if warm < target {
			return false
		}
	}

	incumbents := make([]types.PeerKey, 0, len(claimants))
	for pk := range claimants {
		if _, drain := drainSet[pk]; drain {
			continue
		}
		incumbents = append(incumbents, pk)
	}
	sorted := scoredPeers(incumbents, hash, cluster)
	keep := min(int(target), len(sorted))
	for _, s := range sorted[:keep] {
		if s.peer == localID {
			return false
		}
	}
	// Local is excess. Margin gate: only shed if local is at least
	// incumbentMargin × worse than the worst keeper. Within the band
	// the placements are roughly equivalent and the migration cost
	// outweighs the convergence benefit.
	worstKeeperScore := sorted[keep-1].latency
	if worstKeeperScore <= 0 {
		return true
	}
	localScore := predictedLatency(localID, hash, cluster)
	return localScore > worstKeeperScore*incumbentMargin
}

// hasLiveDemand reports whether any peer is currently serving the
// workload. False means the workload is idle from the cluster's
// perspective and the make-before-break warm-replacement gate doesn't
// apply (nothing to drop).
func hasLiveDemand(servedRates map[types.PeerKey]float64) bool {
	for _, r := range servedRates {
		if r > 0 {
			return true
		}
	}
	return false
}
