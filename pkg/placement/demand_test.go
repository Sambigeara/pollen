// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBuildClusterState_AggregatesFreshPeers(t *testing.T) {
	now := time.Date(2026, time.April, 27, 12, 0, 0, 0, time.UTC)
	local := peerKey(1)
	peer2 := peerKey(2)

	snap := state.Snapshot{
		LocalID:  local,
		PeerKeys: []types.PeerKey{local, peer2},
		Nodes: map[types.PeerKey]state.NodeView{
			local: {
				NumCPU:      4,
				LastEventAt: now,
				SeedMetrics: map[string]state.SeedMetrics{
					"h1": {ServedRate: 50, OriginRate: 30, ComputeCostMs: 100},
				},
			},
			peer2: {
				NumCPU:      4,
				LastEventAt: now.Add(-5 * time.Second),
				SeedMetrics: map[string]state.SeedMetrics{
					"h1": {ServedRate: 50, OriginRate: 70, ComputeCostMs: 100},
				},
			},
		},
	}

	c := buildClusterState(snap, now)
	require.InDelta(t, 30.0, c.OriginRates["h1"][local], 0.001)
	require.InDelta(t, 70.0, c.OriginRates["h1"][peer2], 0.001)
	require.Contains(t, c.Nodes, local)
	require.Contains(t, c.Nodes, peer2)
}

func TestBuildClusterState_DropsStalePeerTelemetry(t *testing.T) {
	// A peer whose last gossip event is older than telemetryMaxAge is
	// excluded entirely — its pre-partition origin/served rates would
	// otherwise inflate cluster aggregates and over-provision replicas.
	now := time.Date(2026, time.April, 27, 12, 0, 0, 0, time.UTC)
	local := peerKey(1)
	stale := peerKey(2)

	snap := state.Snapshot{
		LocalID:  local,
		PeerKeys: []types.PeerKey{local, stale},
		Nodes: map[types.PeerKey]state.NodeView{
			local: {
				NumCPU:      4,
				LastEventAt: now,
				SeedMetrics: map[string]state.SeedMetrics{
					"h1": {ServedRate: 10, OriginRate: 10, ComputeCostMs: 100},
				},
			},
			stale: {
				NumCPU:      4,
				LastEventAt: now.Add(-2 * telemetryMaxAge),
				SeedMetrics: map[string]state.SeedMetrics{
					"h1": {ServedRate: 1000, OriginRate: 1000, ComputeCostMs: 100},
				},
			},
		},
	}

	c := buildClusterState(snap, now)
	require.NotContains(t, c.Nodes, stale, "stale peer must drop out of the cluster view")
	require.NotContains(t, c.OriginRates["h1"], stale, "stale peer's origin rate must not contribute")
	require.Equal(t, 10.0, c.OriginRates["h1"][local])
}

func TestBuildClusterState_ComputeCostWeightedByServedRate(t *testing.T) {
	// A hot fast peer must dominate the cluster mean over a cold slow
	// peer: 99 rps @ 10ms + 1 rps @ 1000ms must aggregate to ≈19.9ms
	// (the served-rate-weighted mean), not 505ms (the unweighted node
	// mean — off by ~25×).
	now := time.Date(2026, time.April, 27, 12, 0, 0, 0, time.UTC)
	local := peerKey(1)
	peer2 := peerKey(2)

	snap := state.Snapshot{
		LocalID:  local,
		PeerKeys: []types.PeerKey{local, peer2},
		Nodes: map[types.PeerKey]state.NodeView{
			local: {
				NumCPU:      4,
				LastEventAt: now,
				SeedMetrics: map[string]state.SeedMetrics{
					"h1": {ServedRate: 99, ComputeCostMs: 10},
				},
			},
			peer2: {
				NumCPU:      4,
				LastEventAt: now,
				SeedMetrics: map[string]state.SeedMetrics{
					"h1": {ServedRate: 1, ComputeCostMs: 1000},
				},
			},
		},
	}

	c := buildClusterState(snap, now)
	require.InDelta(t, 19.9, c.ComputeCost["h1"], 0.05,
		"compute cost must be weighted by ServedRate, not unweighted node-mean")
}

func TestBuildClusterState_ParkedTimeWeightedByServedRate(t *testing.T) {
	// Parked time must use the same weighting as compute cost so
	// desiredReplicas's `compute - parked` subtraction operates on aligned
	// denominators. A peer with cost reported but no parking contributes a
	// 0 parked sample at its serve weight, not no sample.
	now := time.Date(2026, time.April, 27, 12, 0, 0, 0, time.UTC)
	local := peerKey(1)
	peer2 := peerKey(2)

	snap := state.Snapshot{
		LocalID:  local,
		PeerKeys: []types.PeerKey{local, peer2},
		Nodes: map[types.PeerKey]state.NodeView{
			local: {
				NumCPU:      4,
				LastEventAt: now,
				SeedMetrics: map[string]state.SeedMetrics{
					"h1": {ServedRate: 99, ComputeCostMs: 100, ParkedMs: 0},
				},
			},
			peer2: {
				NumCPU:      4,
				LastEventAt: now,
				SeedMetrics: map[string]state.SeedMetrics{
					"h1": {ServedRate: 1, ComputeCostMs: 100, ParkedMs: 100},
				},
			},
		},
	}

	c := buildClusterState(snap, now)
	// Weighted parked = (99×0 + 1×100) / (99+1) = 1.0
	require.InDelta(t, 1.0, c.ParkedTime["h1"], 0.05,
		"parked time must be weighted by ServedRate; absent ParkedMs counts as 0 alongside the same ServedRate weight")
}

func TestBuildClusterState_LocalNeverFiltered(t *testing.T) {
	// Local always reports its own telemetry even if LastEventAt looks
	// stale (we generate the events ourselves; the freshness check is
	// for remote peers whose gossip might have stopped flowing).
	now := time.Date(2026, time.April, 27, 12, 0, 0, 0, time.UTC)
	local := peerKey(1)

	snap := state.Snapshot{
		LocalID:  local,
		PeerKeys: []types.PeerKey{local},
		Nodes: map[types.PeerKey]state.NodeView{
			local: {
				NumCPU:      4,
				LastEventAt: time.Time{}, // zero — would fail the staleness check
				SeedMetrics: map[string]state.SeedMetrics{
					"h1": {ServedRate: 100, OriginRate: 100},
				},
			},
		},
	}

	c := buildClusterState(snap, now)
	require.Contains(t, c.Nodes, local)
	require.Equal(t, 100.0, c.OriginRates["h1"][local])
}
