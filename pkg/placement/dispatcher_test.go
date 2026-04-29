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

func nodeAt(x, y float64) state.NodeView {
	return state.NodeView{VivaldiCoord: &coords.Coord{X: x, Y: y}}
}

func TestDispatcher_NoReplicas(t *testing.T) {
	store := &mockStore{}
	d := newDispatcher(store, peerKey(1))
	_, err := d.Pick("seed")
	require.ErrorIs(t, err, ErrNoReplicas)
}

func TestDispatcher_SingleReplica(t *testing.T) {
	self, a := peerKey(1), peerKey(2)
	store := &mockStore{
		claims: map[string]map[types.PeerKey]struct{}{"seed": {a: {}}},
		nodes: map[types.PeerKey]state.NodeView{
			self: nodeAt(0, 0),
			a:    nodeAt(1, 0),
		},
	}
	d := newDispatcher(store, self)
	got, err := d.Pick("seed")
	require.NoError(t, err)
	require.Equal(t, a, got)
}

func TestDispatcher_AvoidsBackedOff(t *testing.T) {
	self, a, b := peerKey(1), peerKey(2), peerKey(3)
	now := time.Now()
	aNode := nodeAt(1, 0)
	aNode.BackoffExpiry = now.Add(time.Hour)
	store := &mockStore{
		claims: map[string]map[types.PeerKey]struct{}{"seed": {a: {}, b: {}}},
		nodes: map[types.PeerKey]state.NodeView{
			self: nodeAt(0, 0),
			a:    aNode,
			b:    nodeAt(2, 0),
		},
	}
	d := newDispatcher(store, self)
	d.now = func() time.Time { return now }

	for range 10 {
		got, err := d.Pick("seed")
		require.NoError(t, err)
		require.Equal(t, b, got)
	}
}

func TestDispatcher_FallsBackWhenAllBackedOff(t *testing.T) {
	self, a, b := peerKey(1), peerKey(2), peerKey(3)
	now := time.Now()
	expired := now.Add(time.Hour)
	aNode := nodeAt(1, 0)
	aNode.BackoffExpiry = expired
	bNode := nodeAt(2, 0)
	bNode.BackoffExpiry = expired
	store := &mockStore{
		claims: map[string]map[types.PeerKey]struct{}{"seed": {a: {}, b: {}}},
		nodes: map[types.PeerKey]state.NodeView{
			self: nodeAt(0, 0),
			a:    aNode,
			b:    bNode,
		},
	}
	d := newDispatcher(store, self)
	d.now = func() time.Time { return now }

	got, err := d.Pick("seed")
	require.NoError(t, err)
	require.Contains(t, []types.PeerKey{a, b}, got)
}

func TestDispatcher_OnlyKNearest(t *testing.T) {
	self := peerKey(1)
	near1, near2 := peerKey(2), peerKey(3)
	far1, far2, far3 := peerKey(4), peerKey(5), peerKey(6)
	store := &mockStore{
		claims: map[string]map[types.PeerKey]struct{}{
			"seed": {near1: {}, near2: {}, far1: {}, far2: {}, far3: {}},
		},
		nodes: map[types.PeerKey]state.NodeView{
			self:  nodeAt(0, 0),
			near1: nodeAt(1, 0),
			near2: nodeAt(2, 0),
			far1:  nodeAt(10, 0),
			far2:  nodeAt(11, 0),
			far3:  nodeAt(12, 0),
		},
	}
	d := newDispatcher(store, self)

	seen := map[types.PeerKey]bool{}
	for range 100 {
		got, err := d.Pick("seed")
		require.NoError(t, err)
		seen[got] = true
	}
	require.True(t, seen[near1], "expected nearest to be picked")
	require.True(t, seen[near2], "expected second-nearest to be picked")
	for _, f := range []types.PeerKey{far1, far2, far3} {
		require.False(t, seen[f], "should never pick far replica %x", f[0])
	}
}
