package route

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func pk(b byte) types.PeerKey {
	var k types.PeerKey
	k[0] = b
	return k
}

func coord(x, y float64) *topology.Coord {
	return &topology.Coord{X: x, Y: y}
}

// A—B—C—D linear chain. A is local, A↔B direct.
// Routes from A: C via B (2 hops), D via B (3 hops).
func TestRecompute_LinearChain(t *testing.T) {
	A, B, C, D := pk(1), pk(2), pk(3), pk(4)

	nodes := map[types.PeerKey]NodeInfo{
		A: {Reachable: map[types.PeerKey]struct{}{B: {}}, Coord: coord(0, 0)},
		B: {Reachable: map[types.PeerKey]struct{}{A: {}, C: {}}, Coord: coord(10, 0)},
		C: {Reachable: map[types.PeerKey]struct{}{B: {}, D: {}}, Coord: coord(20, 0)},
		D: {Reachable: map[types.PeerKey]struct{}{C: {}}, Coord: coord(30, 0)},
	}
	direct := map[types.PeerKey]struct{}{B: {}}

	routes := Recompute(A, coord(0, 0), direct, nodes)

	// B is direct → excluded from routes.
	_, hasB := routes[B]
	require.False(t, hasB, "direct peer B should not be in routes")

	rC, ok := routes[C]
	require.True(t, ok, "should have route to C")
	require.Equal(t, B, rC.NextHop)
	require.Equal(t, 2, rC.HopCount)

	rD, ok := routes[D]
	require.True(t, ok, "should have route to D")
	require.Equal(t, B, rD.NextHop)
	require.Equal(t, 3, rD.HopCount)
}

// Star: hub H connected to spokes S1, S2, S3.
// Local = S1, direct to H. S2 and S3 routed via H.
func TestRecompute_Star(t *testing.T) {
	H, S1, S2, S3 := pk(1), pk(2), pk(3), pk(4)

	nodes := map[types.PeerKey]NodeInfo{
		H:  {Reachable: map[types.PeerKey]struct{}{S1: {}, S2: {}, S3: {}}, Coord: coord(0, 0)},
		S1: {Reachable: map[types.PeerKey]struct{}{H: {}}, Coord: coord(-10, 0)},
		S2: {Reachable: map[types.PeerKey]struct{}{H: {}}, Coord: coord(10, 0)},
		S3: {Reachable: map[types.PeerKey]struct{}{H: {}}, Coord: coord(0, 10)},
	}
	direct := map[types.PeerKey]struct{}{H: {}}

	routes := Recompute(S1, coord(-10, 0), direct, nodes)

	require.Len(t, routes, 2)
	require.Equal(t, H, routes[S2].NextHop)
	require.Equal(t, 2, routes[S2].HopCount)
	require.Equal(t, H, routes[S3].NextHop)
	require.Equal(t, 2, routes[S3].HopCount)
}

// Disconnected component: A—B, C—D (no path from A to C or D).
func TestRecompute_Disconnected(t *testing.T) {
	A, B, C, D := pk(1), pk(2), pk(3), pk(4)

	nodes := map[types.PeerKey]NodeInfo{
		A: {Reachable: map[types.PeerKey]struct{}{B: {}}, Coord: coord(0, 0)},
		B: {Reachable: map[types.PeerKey]struct{}{A: {}}, Coord: coord(10, 0)},
		C: {Reachable: map[types.PeerKey]struct{}{D: {}}, Coord: coord(100, 0)},
		D: {Reachable: map[types.PeerKey]struct{}{C: {}}, Coord: coord(110, 0)},
	}
	direct := map[types.PeerKey]struct{}{B: {}}

	routes := Recompute(A, coord(0, 0), direct, nodes)

	_, hasC := routes[C]
	_, hasD := routes[D]
	require.False(t, hasC, "C is in a disconnected component")
	require.False(t, hasD, "D is in a disconnected component")
}

// Nil coordinates: nodes with nil Coord get penalized (high weight) but are
// still reachable.
func TestRecompute_NilCoords(t *testing.T) {
	A, B, C := pk(1), pk(2), pk(3)

	nodes := map[types.PeerKey]NodeInfo{
		A: {Reachable: map[types.PeerKey]struct{}{B: {}}, Coord: coord(0, 0)},
		B: {Reachable: map[types.PeerKey]struct{}{A: {}, C: {}}, Coord: nil}, // nil coord
		C: {Reachable: map[types.PeerKey]struct{}{B: {}}, Coord: coord(20, 0)},
	}
	direct := map[types.PeerKey]struct{}{B: {}}

	routes := Recompute(A, coord(0, 0), direct, nodes)

	rC, ok := routes[C]
	require.True(t, ok, "should still route to C through nil-coord B")
	require.Equal(t, B, rC.NextHop)
	require.InDelta(t, 2*defaultNilCoordWeight, rC.Distance, 1.0)
}

// Direct peers are excluded from routes.
func TestRecompute_DirectPeerExclusion(t *testing.T) {
	A, B, C := pk(1), pk(2), pk(3)

	nodes := map[types.PeerKey]NodeInfo{
		A: {Reachable: map[types.PeerKey]struct{}{B: {}, C: {}}, Coord: coord(0, 0)},
		B: {Reachable: map[types.PeerKey]struct{}{A: {}}, Coord: coord(10, 0)},
		C: {Reachable: map[types.PeerKey]struct{}{A: {}}, Coord: coord(20, 0)},
	}
	direct := map[types.PeerKey]struct{}{B: {}, C: {}}

	routes := Recompute(A, coord(0, 0), direct, nodes)
	require.Empty(t, routes, "all peers are direct → no routes")
}

// Recompute is idempotent: running twice with same inputs produces same output.
func TestRecompute_Idempotent(t *testing.T) {
	A, B, C := pk(1), pk(2), pk(3)

	nodes := map[types.PeerKey]NodeInfo{
		A: {Reachable: map[types.PeerKey]struct{}{B: {}}, Coord: coord(0, 0)},
		B: {Reachable: map[types.PeerKey]struct{}{A: {}, C: {}}, Coord: coord(10, 0)},
		C: {Reachable: map[types.PeerKey]struct{}{B: {}}, Coord: coord(20, 0)},
	}
	direct := map[types.PeerKey]struct{}{B: {}}
	localCoord := coord(0, 0)

	r1 := Recompute(A, localCoord, direct, nodes)
	r2 := Recompute(A, localCoord, direct, nodes)

	require.Equal(t, r1, r2)
}

// Shortest path: A—B—D vs A—C—D, choose based on Vivaldi distance.
func TestRecompute_ShortestPath(t *testing.T) {
	A, B, C, D := pk(1), pk(2), pk(3), pk(4)

	// A—B—D path: distance ~10+10 = ~20
	// A—C—D path: distance ~100+100 = ~200
	// B is the shorter path's first hop.
	nodes := map[types.PeerKey]NodeInfo{
		A: {Reachable: map[types.PeerKey]struct{}{B: {}, C: {}}, Coord: coord(0, 0)},
		B: {Reachable: map[types.PeerKey]struct{}{A: {}, D: {}}, Coord: coord(10, 0)},
		C: {Reachable: map[types.PeerKey]struct{}{A: {}, D: {}}, Coord: coord(0, 100)},
		D: {Reachable: map[types.PeerKey]struct{}{B: {}, C: {}}, Coord: coord(20, 0)},
	}
	direct := map[types.PeerKey]struct{}{B: {}, C: {}}

	routes := Recompute(A, coord(0, 0), direct, nodes)

	rD, ok := routes[D]
	require.True(t, ok)
	require.Equal(t, B, rD.NextHop, "should route through B (shorter path)")
}

func TestTable_LookupAndUpdate(t *testing.T) {
	A, B, C := pk(1), pk(2), pk(3)
	tbl := New(A)

	_, ok := tbl.Lookup(C)
	require.False(t, ok, "empty table has no routes")

	tbl.Update(map[types.PeerKey]Route{
		C: {NextHop: B, Distance: 20, HopCount: 2},
	})

	nextHop, ok := tbl.Lookup(C)
	require.True(t, ok)
	require.Equal(t, B, nextHop)
	require.Equal(t, 1, tbl.Len())

	// Replacing with empty clears all routes.
	tbl.Update(map[types.PeerKey]Route{})
	require.Equal(t, 0, tbl.Len())
}

// Empty graph: no nodes besides local → no routes.
func TestRecompute_EmptyGraph(t *testing.T) {
	A := pk(1)
	routes := Recompute(A, coord(0, 0), nil, nil)
	require.Empty(t, routes)
}
