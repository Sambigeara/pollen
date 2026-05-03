// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"cmp"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

var ErrNoReplicas = errors.New("placement: no replicas for seed")

// dispatchK is the cap on the candidate set after Vivaldi-distance
// filtering. Two replicas is the smallest set where P2C still
// statistically smooths variance without serialising on the closest peer.
const dispatchK = 2

type dispatcher struct {
	store WorkloadState
	now   func() time.Time
	self  types.PeerKey
}

func newDispatcher(store WorkloadState, self types.PeerKey) *dispatcher {
	return &dispatcher{store: store, self: self, now: time.Now}
}

func (d *dispatcher) Pick(seed string) (types.PeerKey, error) {
	snap := d.store.Snapshot()
	replicas := replicasOf(snap, seed)
	if len(replicas) == 0 {
		return types.PeerKey{}, ErrNoReplicas
	}

	if len(replicas) > dispatchK {
		distances := func(peer types.PeerKey) float64 { return distanceFromSelf(snap, d.self, peer) }
		slices.SortFunc(replicas, func(a, b types.PeerKey) int {
			return cmp.Compare(distances(a), distances(b))
		})
		replicas = replicas[:dispatchK]
	}

	now := d.now()
	pool := make([]types.PeerKey, 0, len(replicas))
	for _, r := range replicas {
		if !isBackedOff(snap, r, now) {
			pool = append(pool, r)
		}
	}
	if len(pool) == 0 {
		pool = replicas
	}
	return pool[rand.IntN(len(pool))], nil //nolint:gosec
}

func distanceFromSelf(snap state.Snapshot, self, peer types.PeerKey) float64 {
	selfNV, sok := snap.Nodes[self]
	peerNV, pok := snap.Nodes[peer]
	if !sok || !pok || selfNV.VivaldiCoord == nil || peerNV.VivaldiCoord == nil {
		return math.Inf(1)
	}
	return coords.Distance(*selfNV.VivaldiCoord, *peerNV.VivaldiCoord)
}
