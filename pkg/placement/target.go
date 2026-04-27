// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import "math"

const (
	// targetHeadroom multiplies the steady-state Little's-Law replica
	// count to absorb short bursts above the smoothed arrival rate.
	// 1.5 sets aside 50% spare capacity above what current demand
	// strictly requires.
	targetHeadroom = 1.5
	// targetUtilisation is the steady-state CPU utilisation each
	// replica is sized to run at. Below 1.0 because queueing wait
	// grows hyperbolically as utilisation approaches saturation;
	// 0.7 keeps tail latency stable without paying for unused capacity.
	targetUtilisation = 0.7
)

// burnRatios derives per-seed SLO burn ratios from the local utilisation
// tracker. Returned values feed PlacementInfo.SLOBurnRatio so operators
// can see when a workload is in pain even when capacity math alone is
// keeping up. After the Little's-Law rework burn is observability only;
// capacity math drives placement.
func burnRatios(specs map[string]spec, ut *utilisationTracker) map[string]float64 {
	out := make(map[string]float64, len(specs))
	for hash := range specs {
		_, _, burn := ut.SLOBurnRate(hash)
		out[hash] = burn
	}
	return out
}

// desiredReplicas computes the cluster-aggregate desired replica count for
// a workload using Little's Law on gossiped demand and capacity signals.
// Every node sees the same gossip and computes the same target, so cluster
// convergence is by construction — no per-node target drift, no burn-driven
// scale loops fighting each other.
//
// The formula is:
//
//	desired = ceil( arrival_rate × service_time × headroom /
//	                (cores_per_replica × utilisation) )
//
// where arrival_rate is the cluster sum of OriginRate (offered load including
// rejections — we want to provision for what callers want, not what we're
// already serving), service_time is active CPU time per call (compute minus
// parked, since parked time doesn't consume CPU), and cores_per_replica is
// the cluster-mean NumCPU. Result is clamped to [MinReplicas, ClusterSize].
//
// Returns MinReplicas when telemetry is incomplete (no demand, no compute
// cost observed, no node CPU samples) — cold-start is not the place to
// spin up replicas based on guesses.
func desiredReplicas(sp spec, hash string, demand clusterState, clusterSize int) uint32 {
	arrivalRate := burstAwareArrival(hash, demand)
	rejectActive := demand.RejectRate[hash] > 0

	if arrivalRate <= 0 {
		// Reject pressure but no measurable origin yet (fast EWMAs may
		// still be warming): nudge from MinReplicas by one immediately
		// rather than wait for the EWMA to lift.
		base := sp.MinReplicas
		if rejectActive {
			base++
		}
		return clampReplicas(base, sp.MinReplicas, clusterSize)
	}

	serviceMs := demand.ComputeCost[hash] - demand.ParkedTime[hash]
	if serviceMs <= 0 {
		return clampReplicas(sp.MinReplicas, sp.MinReplicas, clusterSize)
	}

	cores := meanEffectiveCoresPerNode(demand)
	if cores <= 0 {
		return clampReplicas(sp.MinReplicas, sp.MinReplicas, clusterSize)
	}

	const msPerSec = 1000.0
	raw := arrivalRate * (serviceMs / msPerSec) * targetHeadroom / (cores * targetUtilisation)
	desired := uint32(math.Ceil(raw))
	if rejectActive && int(desired) < clusterSize {
		// Reject-driven scale-up: any rejection means the cluster is
		// already shedding load. Bumping desired by one short-circuits
		// the EWMA's lag and lets the next reconcile claim a fresh
		// replica before the autoscaler would otherwise see the burst.
		desired++
	}
	return clampReplicas(desired, sp.MinReplicas, clusterSize)
}

// burstAwareArrival returns the per-tick arrival rate that drives
// desiredReplicas. We take the max of three signals:
//
//   - the canonical OriginRate sum (medium-window EWMA, alpha 0.2) — the
//     legacy reading, conservatively kept as a lower bound;
//   - OriginRateFast (cluster sum of short-window EWMAs) — visible during
//     a microburst before the medium EWMA reacts;
//   - OriginRateSlow (cluster sum of long-window EWMAs) — anchors the
//     steady-state arrival when traffic is smooth.
//
// max() across the three picks whichever signal currently dominates.
// Headroom is applied once, in desiredReplicas — multiplying slow by
// targetHeadroom here as well would compound to 2.25× the intended
// buffer in the slow-dominant branch.
func burstAwareArrival(hash string, demand clusterState) float64 {
	arrival := originSum(hash, demand)
	if fast := demand.OriginRateFast[hash]; fast > arrival {
		arrival = fast
	}
	if slow := demand.OriginRateSlow[hash]; slow > arrival {
		arrival = slow
	}
	return arrival
}

// meanEffectiveCoresPerNode returns the cluster mean of usable CPU per
// node — NumCPU multiplied by the node's CPUBudgetPercent fraction. A
// node configured with a 25% budget contributes a quarter of its raw
// cores. This is the right divisor for Little's-Law replica sizing:
// a per-replica concurrency cap of "all cores" overstates capacity by
// 1/budget, leaving the autoscaler under-provisioning the workload.
// Nodes that haven't published telemetry (NumCPU == 0) are skipped.
func meanEffectiveCoresPerNode(demand clusterState) float64 {
	var total, samples float64
	for _, n := range demand.Nodes {
		c := cpuCapacity(n)
		if c > 0 {
			total += c
			samples++
		}
	}
	if samples == 0 {
		return 0
	}
	return total / samples
}

func clampReplicas(desired, minReplicas uint32, clusterSize int) uint32 {
	if desired < minReplicas {
		desired = minReplicas
	}
	if clusterSize > 0 && int(desired) > clusterSize {
		desired = uint32(clusterSize)
	}
	return desired
}
