// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package route_test

import (
	"math"
	"testing"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/route"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func pk(b byte) types.PeerKey {
	var k types.PeerKey
	k[0] = b
	return k
}

// snapAt places self at the origin and each peer in at at (x,0), so a
// peer's distance is just its x. Peers listed in noCoord have no
// coordinate at all (infinitely far).
func snapAt(self types.PeerKey, at map[types.PeerKey]float64, noCoord ...types.PeerKey) state.Snapshot {
	nodes := map[types.PeerKey]state.NodeView{
		self: {VivaldiCoord: &coords.Coord{}},
	}
	for peer, x := range at {
		nodes[peer] = state.NodeView{VivaldiCoord: &coords.Coord{X: x}}
	}
	for _, peer := range noCoord {
		nodes[peer] = state.NodeView{}
	}
	return state.Snapshot{Nodes: nodes}
}

func TestDistance(t *testing.T) {
	self, near, far, noCoord, absent := pk(1), pk(2), pk(3), pk(4), pk(9)
	snap := snapAt(self, map[types.PeerKey]float64{near: 3, far: 50}, noCoord)

	require.InDelta(t, 3, route.Distance(snap, self, near), 1e-9)
	require.InDelta(t, 50, route.Distance(snap, self, far), 1e-9)
	require.True(t, math.IsInf(route.Distance(snap, self, noCoord), 1), "no coordinate is infinitely far")
	require.True(t, math.IsInf(route.Distance(snap, self, absent), 1), "absent peer is infinitely far")
}

func TestByLocalityAndNearest(t *testing.T) {
	self, a, b, c := pk(1), pk(2), pk(3), pk(4)
	snap := snapAt(self, map[types.PeerKey]float64{a: 30, b: 5, c: 80})

	require.Equal(t, []types.PeerKey{b, a, c}, route.ByLocality(snap, self, []types.PeerKey{a, b, c}))

	got, ok := route.Nearest(snap, self, []types.PeerKey{a, b, c})
	require.True(t, ok)
	require.Equal(t, b, got)

	_, ok = route.Nearest(snap, self, nil)
	require.False(t, ok)
}

func TestByLocalityDeterministicTieBreak(t *testing.T) {
	self, a, b, c := pk(1), pk(2), pk(3), pk(4)
	// No peer has a coordinate: all equidistant (+Inf). Order must fall
	// back to PeerKey so a caller keeps hitting the same source.
	snap := snapAt(self, nil, a, b, c)

	want := []types.PeerKey{a, b, c} // pk(2) < pk(3) < pk(4)
	require.Equal(t, want, route.ByLocality(snap, self, []types.PeerKey{c, a, b}))
	require.Equal(t, want, route.ByLocality(snap, self, []types.PeerKey{b, c, a}))
}

func TestPowerOfTwo(t *testing.T) {
	self, a, b, c, d := pk(1), pk(2), pk(3), pk(4), pk(5)
	snap := snapAt(self, map[types.PeerKey]float64{a: 10, b: 20, c: 30, d: 40})
	first := func(int) int { return 0 }

	t.Run("empty is not ok", func(t *testing.T) {
		_, ok := route.PowerOfTwo(snap, self, nil, 2, nil, first)
		require.False(t, ok)
	})

	t.Run("narrows to the nearest k then picks", func(t *testing.T) {
		// Nearest 2 of {a,b,c,d} are a,b; first picks a.
		got, ok := route.PowerOfTwo(snap, self, []types.PeerKey{d, c, b, a}, 2, nil, first)
		require.True(t, ok)
		require.Equal(t, a, got)
	})

	t.Run("skip drops a candidate", func(t *testing.T) {
		// Nearest 2 are a,b; a is skipped, so b survives.
		skipA := func(p types.PeerKey) bool { return p == a }
		got, ok := route.PowerOfTwo(snap, self, []types.PeerKey{a, b, c, d}, 2, skipA, first)
		require.True(t, ok)
		require.Equal(t, b, got)
	})

	t.Run("all skipped falls back to the nearest-k pool", func(t *testing.T) {
		skipAll := func(types.PeerKey) bool { return true }
		got, ok := route.PowerOfTwo(snap, self, []types.PeerKey{a, b, c, d}, 2, skipAll, first)
		require.True(t, ok)
		require.Equal(t, a, got, "fallback keeps the nearest-k set, not the whole candidate list")
	})

	t.Run("k >= len keeps every candidate (no distance sort)", func(t *testing.T) {
		// Only b is not backed off; it must be returned regardless of
		// its distance rank because no truncation happens.
		skipNotB := func(p types.PeerKey) bool { return p != b }
		got, ok := route.PowerOfTwo(snap, self, []types.PeerKey{a, b}, 2, skipNotB, first)
		require.True(t, ok)
		require.Equal(t, b, got)
	})
}
