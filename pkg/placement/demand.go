// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"maps"
	"slices"
	"time"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

// telemetryMaxAge is the freshness window for a peer's gossiped
// telemetry. Beyond this, the peer's SeedMetrics are excluded from
// cluster aggregation and its node entry is dropped from the per-tick
// view — a partitioned-but-still-valid peer's pre-partition origin
// rates would otherwise inflate desiredReplicas and over-provision.
// Sized to match state's reachability freshness window (30s) so the
// two notions of "stale peer" agree.
const telemetryMaxAge = 30 * time.Second

type nodeState struct {
	Coord            *coords.Coord
	MemTotalBytes    uint64
	CPUPercent       uint32
	MemPercent       uint32
	NumCPU           uint32
	CPUBudgetPercent uint32
	MemBudgetPercent uint32
	AdmissionState   state.AdmissionState
}

// clusterState is the per-tick input to placement scoring, built by
// buildClusterState from the gossiped state snapshot. Node resources
// live in Nodes; per-seed observations live in the rate maps below.
// ServedRates is per-peer because the make-before-break gate consults
// "is the replacement actually serving traffic" before draining a warm
// incumbent — a per-hash sum can't answer that.
type clusterState struct {
	Nodes             map[types.PeerKey]nodeState
	ServedRates       map[string]map[types.PeerKey]float64
	OriginRates       map[string]map[types.PeerKey]float64
	ComputeCost       map[string]float64
	ParkedTime        map[string]float64
	OriginRateFast    map[string]float64
	OriginRateSlow    map[string]float64
	RejectRate        map[string]float64
	MeanCoordDistance float64
}

// originSum totals the per-peer origin rate for a hash — the
// cluster-wide offered load. Used as the warm-path gate and divisor in
// predictedLatency (so saturation depressing ServedRate doesn't flip
// scoring back to cold-start) and as the arrival rate input to
// Little's Law in desiredReplicas. Iteration is sorted so every node
// computes the same float-summation order — without this, FP
// non-associativity makes a placement target peer-key-dependent.
func originSum(hash string, cluster clusterState) float64 {
	rates, ok := cluster.OriginRates[hash]
	if !ok {
		return 0
	}
	var total float64
	for _, pk := range slices.SortedFunc(maps.Keys(rates), types.PeerKey.Compare) {
		total += rates[pk]
	}
	return total
}

// buildClusterState aggregates node telemetry and per-seed metrics from a
// gossip snapshot into the per-tick view consumed by scoring and target
// math. Peers whose last gossip event is older than telemetryMaxAge are
// excluded entirely — their stale resource and rate data must not feed
// placement decisions. Pure: snapshot + clock in, view out.
func buildClusterState(snap state.Snapshot, now time.Time) clusterState {
	cluster := clusterState{
		Nodes:          make(map[types.PeerKey]nodeState, len(snap.PeerKeys)),
		ServedRates:    make(map[string]map[types.PeerKey]float64),
		OriginRates:    make(map[string]map[types.PeerKey]float64),
		ComputeCost:    make(map[string]float64),
		ParkedTime:     make(map[string]float64),
		OriginRateFast: make(map[string]float64),
		OriginRateSlow: make(map[string]float64),
		RejectRate:     make(map[string]float64),
	}

	// Weighted mean denominator. Same set of (peer, hash) samples
	// contributes to compute and parked so the difference in
	// desiredReplicas is over a consistent denominator.
	costWeight := make(map[string]float64)

	for _, pk := range snap.PeerKeys {
		nv := snap.Nodes[pk]
		if pk != snap.LocalID && now.Sub(nv.LastEventAt) > telemetryMaxAge {
			continue
		}
		cluster.Nodes[pk] = nodeState{
			CPUPercent:       nv.CPUPercent,
			MemPercent:       nv.MemPercent,
			MemTotalBytes:    nv.MemTotalBytes,
			NumCPU:           nv.NumCPU,
			CPUBudgetPercent: nv.CPUBudgetPercent,
			MemBudgetPercent: nv.MemBudgetPercent,
			AdmissionState:   nv.AdmissionState,
			Coord:            nv.VivaldiCoord,
		}
		// Unified seed-metrics bundle:
		//   - OriginRate is kept per-peer so scoring can weight by
		//     where demand originates.
		//   - ComputeCostMs is already a per-peer traffic-weighted mean
		//     (invocation_ms_rate / served_rate). The cluster aggregate
		//     must therefore weight by each peer's ServedRate, not
		//     average node means — otherwise a low-traffic slow node
		//     dominates a hot fast node, and the Little's-Law target
		//     scales by the wrong order of magnitude.
		//   - ParkedMs is similarly served-rate-weighted on the source
		//     side; reuse ServedRate as the cluster weight so cost and
		//     parked subtract consistently.
		// A peer with ServedRate > 0 but ComputeCostMs == 0 means the
		// source omitted the cost sample (utilisation drops cost == 0
		// entries before gossip), so we skip cost AND parked together to
		// keep the denominator aligned. ParkedMs == 0 with
		// ComputeCostMs > 0 is a genuine "no parking" sample and
		// contributes 0 to the weighted parked mean — correct.
		for hash, m := range nv.SeedMetrics {
			if m.ServedRate > 0 {
				peers, ok := cluster.ServedRates[hash]
				if !ok {
					peers = make(map[types.PeerKey]float64)
					cluster.ServedRates[hash] = peers
				}
				peers[pk] = float64(m.ServedRate)
			}
			if m.OriginRate > 0 {
				peers, ok := cluster.OriginRates[hash]
				if !ok {
					peers = make(map[types.PeerKey]float64)
					cluster.OriginRates[hash] = peers
				}
				peers[pk] = float64(m.OriginRate)
			}
			if m.OriginRateFast > 0 {
				cluster.OriginRateFast[hash] += float64(m.OriginRateFast)
			}
			if m.OriginRateSlow > 0 {
				cluster.OriginRateSlow[hash] += float64(m.OriginRateSlow)
			}
			if m.RejectRate > 0 {
				cluster.RejectRate[hash] += float64(m.RejectRate)
			}
			if m.ServedRate > 0 && m.ComputeCostMs > 0 {
				w := float64(m.ServedRate)
				cluster.ComputeCost[hash] += float64(m.ComputeCostMs) * w
				cluster.ParkedTime[hash] += float64(m.ParkedMs) * w
				costWeight[hash] += w
			}
		}
	}

	for hash, w := range costWeight {
		if w > 0 {
			cluster.ComputeCost[hash] /= w
			cluster.ParkedTime[hash] /= w
		}
	}

	cluster.MeanCoordDistance = meanCoordDistance(cluster.Nodes)

	return cluster
}

// meanCoordDistance returns the mean pairwise Vivaldi distance across
// peers with known coords. Returns 0 when fewer than two coords are
// known (no pairs available) or when every known peer is colocated;
// predictedLatency treats either as "topology not yet established" and
// falls back to cold-start scoring. Pre-computed once per tick so
// distance() can substitute a neutral value in O(1). Iteration is
// sorted so the float sum is identical across nodes — otherwise
// MeanCoordDistance differs between peers and rankings drift.
func meanCoordDistance(nodes map[types.PeerKey]nodeState) float64 {
	keys := slices.SortedFunc(maps.Keys(nodes), types.PeerKey.Compare)
	var sum, count float64
	for _, ka := range keys {
		na := nodes[ka]
		if na.Coord == nil {
			continue
		}
		for _, kb := range keys {
			if ka == kb {
				continue
			}
			nb := nodes[kb]
			if nb.Coord == nil {
				continue
			}
			sum += coords.Distance(*na.Coord, *nb.Coord)
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return sum / count
}
