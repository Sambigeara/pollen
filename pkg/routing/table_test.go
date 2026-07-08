// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package routing

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func pk(b byte) types.PeerKey {
	var k types.PeerKey
	k[0] = b
	return k
}

func coord(x, y float64) *coords.Coord {
	return &coords.Coord{X: x, Y: y}
}

func topo(key types.PeerKey, c *coords.Coord, reachable ...types.PeerKey) PeerTopology {
	r := make(map[types.PeerKey]struct{}, len(reachable))
	for _, p := range reachable {
		r[p] = struct{}{}
	}
	return PeerTopology{Key: key, Reachable: r, Coord: c}
}

func TestBuild(t *testing.T) {
	A, B, C, D := pk(1), pk(2), pk(3), pk(4)

	tests := []struct {
		name      string
		self      types.PeerKey
		topology  []PeerTopology
		connected []types.PeerKey
		expected  map[types.PeerKey]types.PeerKey
		noRoute   []types.PeerKey
	}{
		{
			name:     "empty graph",
			self:     A,
			expected: nil,
			noRoute:  []types.PeerKey{A, B},
		},
		{
			name: "single peer, directly connected",
			self: A,
			topology: []PeerTopology{
				topo(A, coord(0, 0), B),
				topo(B, coord(10, 0), A),
			},
			connected: []types.PeerKey{B},
			noRoute:   []types.PeerKey{B},
		},
		{
			name: "single peer, not connected",
			self: A,
			topology: []PeerTopology{
				topo(A, coord(0, 0), B),
				topo(B, coord(10, 0), A),
			},
			expected: map[types.PeerKey]types.PeerKey{B: B},
		},
		{
			name: "linear chain A-B-C-D, A↔B direct",
			self: A,
			topology: []PeerTopology{
				topo(A, coord(0, 0), B),
				topo(B, coord(10, 0), A, C),
				topo(C, coord(20, 0), B, D),
				topo(D, coord(30, 0), C),
			},
			connected: []types.PeerKey{B},
			expected: map[types.PeerKey]types.PeerKey{
				C: B,
				D: B,
			},
			noRoute: []types.PeerKey{B},
		},
		{
			name: "star topology, local is spoke",
			self: B,
			topology: []PeerTopology{
				topo(A, coord(0, 0), B, C, D),
				topo(B, coord(-10, 0), A),
				topo(C, coord(10, 0), A),
				topo(D, coord(0, 10), A),
			},
			connected: []types.PeerKey{A},
			expected: map[types.PeerKey]types.PeerKey{
				C: A,
				D: A,
			},
			noRoute: []types.PeerKey{A},
		},
		{
			name: "disconnected components",
			self: A,
			topology: []PeerTopology{
				topo(A, coord(0, 0), B),
				topo(B, coord(10, 0), A),
				topo(C, coord(100, 0), D),
				topo(D, coord(110, 0), C),
			},
			connected: []types.PeerKey{B},
			noRoute:   []types.PeerKey{C, D},
		},
		{
			name: "nil coordinates penalized but reachable",
			self: A,
			topology: []PeerTopology{
				topo(A, coord(0, 0), B),
				topo(B, nil, A, C),
				topo(C, coord(20, 0), B),
			},
			connected: []types.PeerKey{B},
			expected:  map[types.PeerKey]types.PeerKey{C: B},
		},
		{
			name: "all nil coordinates",
			self: A,
			topology: []PeerTopology{
				topo(A, nil, B),
				topo(B, nil, A, C),
				topo(C, nil, B),
			},
			connected: []types.PeerKey{B},
			expected:  map[types.PeerKey]types.PeerKey{C: B},
		},
		{
			name: "all peers directly connected means no routes",
			self: A,
			topology: []PeerTopology{
				topo(A, coord(0, 0), B, C),
				topo(B, coord(10, 0), A),
				topo(C, coord(20, 0), A),
			},
			connected: []types.PeerKey{B, C},
			noRoute:   []types.PeerKey{B, C},
		},
		{
			name: "shortest path selection",
			self: A,
			topology: []PeerTopology{
				topo(A, coord(0, 0), B, C),
				topo(B, coord(10, 0), A, D),
				topo(C, coord(0, 100), A, D),
				topo(D, coord(20, 0), B, C),
			},
			connected: []types.PeerKey{B, C},
			expected:  map[types.PeerKey]types.PeerKey{D: B},
		},
		{
			name: "cycle handling (triangle)",
			self: A,
			topology: []PeerTopology{
				topo(A, coord(0, 0), B, C),
				topo(B, coord(10, 0), A, C),
				topo(C, coord(5, 8), A, B),
			},
			connected: []types.PeerKey{B},
			expected:  map[types.PeerKey]types.PeerKey{C: C},
		},
		{
			name: "self never in routes",
			self: A,
			topology: []PeerTopology{
				topo(A, coord(0, 0), B),
				topo(B, coord(10, 0), A),
			},
			noRoute: []types.PeerKey{A},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := Build(tt.self, tt.topology, tt.connected, nil)

			for dest, wantNext := range tt.expected {
				next, ok := table.NextHop(dest)
				require.True(t, ok, "expected route to %v", dest)
				require.Equal(t, wantNext, next, "wrong next hop for %v", dest)
			}

			for _, dest := range tt.noRoute {
				_, ok := table.NextHop(dest)
				require.False(t, ok, "expected no route to %v", dest)
			}
		})
	}
}

func TestBuild_Cost(t *testing.T) {
	A, B, C, absent := pk(1), pk(2), pk(3), pk(9)

	// A reaches C only through B. C is nearer to A in a straight line
	// than the A-B-C relay path is long, so C's path cost must exceed the
	// straight-line distance between their coordinates.
	ca, cb, cc := coord(0, 0), coord(0, 10), coord(8, 0)
	table := Build(A, []PeerTopology{
		topo(A, ca, B),
		topo(B, cb, A, C),
		topo(C, cc, B),
	}, []types.PeerKey{B}, nil)

	selfCost, ok := table.Cost(A)
	require.True(t, ok)
	require.Zero(t, selfCost)

	directCost, ok := table.Cost(B)
	require.True(t, ok)
	require.InDelta(t, coords.Distance(*ca, *cb), directCost, 1e-9,
		"a direct neighbour's cost is its straight-line edge weight")

	relayCost, ok := table.Cost(C)
	require.True(t, ok)
	require.InDelta(t, coords.Distance(*ca, *cb)+coords.Distance(*cb, *cc), relayCost, 1e-9,
		"a relay-only peer's cost sums the path edges")
	require.Greater(t, relayCost, coords.Distance(*ca, *cc),
		"the relay path costs more than the straight line")

	_, ok = table.Cost(absent)
	require.False(t, ok, "no cost for an unreachable peer")
}

func TestBuild_Idempotent(t *testing.T) {
	A, B, C := pk(1), pk(2), pk(3)

	topology := []PeerTopology{
		topo(A, coord(0, 0), B),
		topo(B, coord(10, 0), A, C),
		topo(C, coord(20, 0), B),
	}
	connected := []types.PeerKey{B}

	t1 := Build(A, topology, connected, nil)
	t2 := Build(A, topology, connected, nil)

	require.Equal(t, t1.routes, t2.routes)
}
