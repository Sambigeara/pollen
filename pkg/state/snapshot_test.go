// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestStaticServingPeers pins the wrapping-fanout recipient set: every
// peer that gossiped StaticCapable belongs in it (so an offline serving
// node still has a wrapping waiting on rejoin), peers without the bit
// are dropped, and the order is deterministic so callers can diff
// results across snapshots.
func TestStaticServingPeers(t *testing.T) {
	pk := func(b byte) types.PeerKey {
		raw := make([]byte, 32)
		raw[0] = b
		return types.PeerKeyFromBytes(raw)
	}

	t.Run("empty snapshot yields no peers", func(t *testing.T) {
		require.Empty(t, state.Snapshot{}.StaticServingPeers())
	})

	t.Run("single capable peer is returned", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			pk(7): {CanServeStatic: true},
		}}
		require.Equal(t, []types.PeerKey{pk(7)}, snap.StaticServingPeers())
	})

	t.Run("only CanServeStatic peers are returned, sorted", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			pk(3): {CanServeStatic: true},
			pk(1): {CanServeStatic: true},
			pk(2): {CanServeStatic: false},
			pk(4): {CanServeStatic: true},
		}}
		require.Equal(t, []types.PeerKey{pk(1), pk(3), pk(4)}, snap.StaticServingPeers())
	})

	t.Run("no capable peers yields no peers", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			pk(1): {CanServeStatic: false},
			pk(2): {CanServeStatic: false},
		}}
		require.Empty(t, snap.StaticServingPeers())
	})
}
