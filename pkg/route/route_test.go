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

// costMap answers Cost from a fixed table; peers absent from it fall
// back to straight-line Vivaldi distance.
type costMap map[types.PeerKey]float64

func (c costMap) Cost(dest types.PeerKey) (float64, bool) {
	v, ok := c[dest]
	return v, ok
}

func TestDistance(t *testing.T) {
	self, near, far, noCoord, absent := pk(1), pk(2), pk(3), pk(4), pk(9)
	snap := snapAt(self, map[types.PeerKey]float64{near: 3, far: 50}, noCoord)
	sel := route.NewSelector(snap, self, nil)

	require.InDelta(t, 3, sel.Distance(near), 1e-9)
	require.InDelta(t, 50, sel.Distance(far), 1e-9)
	require.True(t, math.IsInf(sel.Distance(noCoord), 1), "no coordinate is infinitely far")
	require.True(t, math.IsInf(sel.Distance(absent), 1), "absent peer is infinitely far")
}

// TestDistancePrefersRoutingCost shows the routing path cost overriding
// the straight-line distance: a holder that is near in coordinate space
// but only reachable via a costly relay path is priced at the higher
// path cost, while a peer the router has no entry for keeps its
// straight-line distance.
func TestDistancePrefersRoutingCost(t *testing.T) {
	self, relayed, direct := pk(1), pk(2), pk(3)
	snap := snapAt(self, map[types.PeerKey]float64{relayed: 5, direct: 20})
	sel := route.NewSelector(snap, self, costMap{relayed: 40})

	require.InDelta(t, 40, sel.Distance(relayed), 1e-9, "relay path cost overrides the straight-line 5")
	require.InDelta(t, 20, sel.Distance(direct), 1e-9, "router-absent peer keeps its straight-line distance")

	got, ok := sel.Nearest([]types.PeerKey{relayed, direct})
	require.True(t, ok)
	require.Equal(t, direct, got, "the directly-reachable holder wins once relay cost is priced in")
}

func TestByLocalityAndNearest(t *testing.T) {
	self, a, b, c := pk(1), pk(2), pk(3), pk(4)
	snap := snapAt(self, map[types.PeerKey]float64{a: 30, b: 5, c: 80})
	sel := route.NewSelector(snap, self, nil)

	require.Equal(t, []types.PeerKey{b, a, c}, sel.ByLocality([]types.PeerKey{a, b, c}))

	got, ok := sel.Nearest([]types.PeerKey{a, b, c})
	require.True(t, ok)
	require.Equal(t, b, got)

	_, ok = sel.Nearest(nil)
	require.False(t, ok)
}

func TestByLocalityDeterministicTieBreak(t *testing.T) {
	self, a, b, c := pk(1), pk(2), pk(3), pk(4)
	// No peer has a coordinate: all equidistant (+Inf). Order must fall
	// back to PeerKey so a caller keeps hitting the same source.
	snap := snapAt(self, nil, a, b, c)
	sel := route.NewSelector(snap, self, nil)

	want := []types.PeerKey{a, b, c} // pk(2) < pk(3) < pk(4)
	require.Equal(t, want, sel.ByLocality([]types.PeerKey{c, a, b}))
	require.Equal(t, want, sel.ByLocality([]types.PeerKey{b, c, a}))
}

func TestPowerOfTwo(t *testing.T) {
	self, a, b, c, d := pk(1), pk(2), pk(3), pk(4), pk(5)
	snap := snapAt(self, map[types.PeerKey]float64{a: 10, b: 20, c: 30, d: 40})
	sel := route.NewSelector(snap, self, nil)
	first := func(int) int { return 0 }

	t.Run("empty is not ok", func(t *testing.T) {
		_, ok := sel.PowerOfTwo(nil, 2, nil, first)
		require.False(t, ok)
	})

	t.Run("narrows to the nearest k then picks", func(t *testing.T) {
		// Nearest 2 of {a,b,c,d} are a,b; first picks a.
		got, ok := sel.PowerOfTwo([]types.PeerKey{d, c, b, a}, 2, nil, first)
		require.True(t, ok)
		require.Equal(t, a, got)
	})

	t.Run("skip drops a candidate", func(t *testing.T) {
		// Nearest 2 are a,b; a is skipped, so b survives.
		skipA := func(p types.PeerKey) bool { return p == a }
		got, ok := sel.PowerOfTwo([]types.PeerKey{a, b, c, d}, 2, skipA, first)
		require.True(t, ok)
		require.Equal(t, b, got)
	})

	t.Run("all skipped falls back to the nearest-k pool", func(t *testing.T) {
		skipAll := func(types.PeerKey) bool { return true }
		got, ok := sel.PowerOfTwo([]types.PeerKey{a, b, c, d}, 2, skipAll, first)
		require.True(t, ok)
		require.Equal(t, a, got, "fallback keeps the nearest-k set, not the whole candidate list")
	})

	t.Run("k >= len keeps every candidate (no distance sort)", func(t *testing.T) {
		// Only b is not backed off; it must be returned regardless of
		// its distance rank because no truncation happens.
		skipNotB := func(p types.PeerKey) bool { return p != b }
		got, ok := sel.PowerOfTwo([]types.PeerKey{a, b}, 2, skipNotB, first)
		require.True(t, ok)
		require.Equal(t, b, got)
	})
}
