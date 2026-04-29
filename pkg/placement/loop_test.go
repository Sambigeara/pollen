// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func defaultPlacementCfg() placementConfig {
	return placementConfig{
		tick:             time.Hour,
		migrateThreshold: 50,
		minDwell:         time.Second,
	}
}

func newPlacementHarness(t *testing.T, self types.PeerKey, cfg placementConfig, store *mockStore) (*placementLoop, *mockClock) {
	t.Helper()
	clock := &mockClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	p := newPlacementLoop(self, cfg, store)
	p.now = clock.Now
	return p, clock
}

func nodeWithCalls(x, y float64, counts map[string]uint64) state.NodeView {
	nv := state.NodeView{VivaldiCoord: &coords.Coord{X: x, Y: y}, CallCounts: counts}
	return nv
}

func TestPlacementLoop_RelinquishesAfterDwellWhenCentroidFar(t *testing.T) {
	self, caller := peerKey(1), peerKey(2)
	store := &mockStore{
		claims: map[string]map[types.PeerKey]struct{}{"seed-x": {self: {}}},
		nodes: map[types.PeerKey]state.NodeView{
			self:   nodeAt(0, 0),
			caller: nodeWithCalls(100, 0, map[string]uint64{"seed-x": 100}),
		},
		localID: self,
	}
	p, clock := newPlacementHarness(t, self, defaultPlacementCfg(), store)

	p.tick()
	require.False(t, store.draining["seed-x"], "must not relinquish before dwell")

	clock.advance(2 * time.Second)
	p.tick()

	require.True(t, store.draining["seed-x"], "should drain after dwell when centroid is far")
}

func TestPlacementLoop_DoesNotRelinquishWithinDwell(t *testing.T) {
	self, caller := peerKey(1), peerKey(2)
	store := &mockStore{
		claims: map[string]map[types.PeerKey]struct{}{"seed-x": {self: {}}},
		nodes: map[types.PeerKey]state.NodeView{
			self:   nodeAt(0, 0),
			caller: nodeWithCalls(100, 0, map[string]uint64{"seed-x": 100}),
		},
		localID: self,
	}
	p, clock := newPlacementHarness(t, self, defaultPlacementCfg(), store)

	p.tick()
	clock.advance(500 * time.Millisecond)
	p.tick()

	require.False(t, store.draining["seed-x"])
}

func TestPlacementLoop_DoesNotRelinquishWhenCentroidClose(t *testing.T) {
	self, caller := peerKey(1), peerKey(2)
	store := &mockStore{
		claims: map[string]map[types.PeerKey]struct{}{"seed-x": {self: {}}},
		nodes: map[types.PeerKey]state.NodeView{
			self:   nodeAt(0, 0),
			caller: nodeWithCalls(1, 0, map[string]uint64{"seed-x": 100}),
		},
		localID: self,
	}
	p, clock := newPlacementHarness(t, self, defaultPlacementCfg(), store)

	p.tick()
	clock.advance(2 * time.Second)
	p.tick()

	require.False(t, store.draining["seed-x"])
}

func TestPlacementLoop_DoesNotRelinquishWithoutCallers(t *testing.T) {
	self := peerKey(1)
	store := &mockStore{
		claims: map[string]map[types.PeerKey]struct{}{"seed-x": {self: {}}},
		nodes: map[types.PeerKey]state.NodeView{
			self: nodeAt(0, 0),
		},
		localID: self,
	}
	p, clock := newPlacementHarness(t, self, defaultPlacementCfg(), store)

	p.tick()
	clock.advance(2 * time.Second)
	p.tick()

	require.False(t, store.draining["seed-x"])
}

func TestCentroidOf_Weighted(t *testing.T) {
	a, b := peerKey(1), peerKey(2)
	snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
		a: nodeAt(0, 0),
		b: nodeAt(100, 100),
	}}
	sources := []sourceCount{
		{caller: a, count: 3},
		{caller: b, count: 1},
	}
	centroid, ok := centroidOf(snap, sources)
	require.True(t, ok)
	require.InDelta(t, 25.0, centroid.X, 0.01)
	require.InDelta(t, 25.0, centroid.Y, 0.01)
}

func TestCentroidOf_NoCallers(t *testing.T) {
	snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{}}
	_, ok := centroidOf(snap, nil)
	require.False(t, ok)
}

func TestCentroidOf_DropsCallersWithoutCoord(t *testing.T) {
	a, b := peerKey(1), peerKey(2)
	snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
		a: {VivaldiCoord: &coords.Coord{X: 10, Y: 20}},
		b: {},
	}}
	sources := []sourceCount{
		{caller: a, count: 1},
		{caller: b, count: 100},
	}
	centroid, ok := centroidOf(snap, sources)
	require.True(t, ok)
	require.InDelta(t, 10.0, centroid.X, 0.01)
	require.InDelta(t, 20.0, centroid.Y, 0.01)
}
