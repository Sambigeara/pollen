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

// dispStore satisfies WorkloadState for the dispatch path, which only
// reads Snapshot().
type dispStore struct {
	WorkloadState
	snap state.Snapshot
}

func (d dispStore) Snapshot() state.Snapshot { return d.snap }

func dpk(b byte) types.PeerKey {
	var k types.PeerKey
	k[0] = b
	return k
}

// These pin the dispatch wiring: replicasOf feeds the candidate set,
// isBackedOff feeds the skip predicate, and an empty replica set still
// maps to ErrNoReplicas.
func TestDispatcherPick(t *testing.T) {
	self, a, b := dpk(1), dpk(2), dpk(3)
	now := time.Unix(1_000, 0)

	base := func(aBackoff time.Time) state.Snapshot {
		return state.Snapshot{
			Claims: map[string]map[types.PeerKey]struct{}{"seed": {a: {}, b: {}}},
			Nodes: map[types.PeerKey]state.NodeView{
				self: {VivaldiCoord: &coords.Coord{}},
				a:    {VivaldiCoord: &coords.Coord{X: 1}, BackoffExpiry: aBackoff},
				b:    {VivaldiCoord: &coords.Coord{X: 2}},
			},
		}
	}

	t.Run("no replicas yields ErrNoReplicas", func(t *testing.T) {
		d := &dispatcher{store: dispStore{snap: state.Snapshot{}}, self: self, now: func() time.Time { return now }}
		_, err := d.Pick("seed")
		require.ErrorIs(t, err, ErrNoReplicas)
	})

	t.Run("returns a live replica", func(t *testing.T) {
		d := &dispatcher{store: dispStore{snap: base(time.Time{})}, self: self, now: func() time.Time { return now }}
		got, err := d.Pick("seed")
		require.NoError(t, err)
		require.Contains(t, []types.PeerKey{a, b}, got)
	})

	t.Run("backed-off replica is skipped when an alternative is live", func(t *testing.T) {
		d := &dispatcher{store: dispStore{snap: base(now.Add(time.Minute))}, self: self, now: func() time.Time { return now }}
		got, err := d.Pick("seed")
		require.NoError(t, err)
		require.Equal(t, b, got, "a is backed off, so only b survives the skip filter")
	})
}
