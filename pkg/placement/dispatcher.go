// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"errors"
	"math/rand/v2"
	"time"

	"github.com/sambigeara/pollen/pkg/route"
	"github.com/sambigeara/pollen/pkg/types"
)

var ErrNoReplicas = errors.New("placement: no replicas for seed")

// dispatchK is the cap on the candidate set after locality
// filtering. Two replicas is the smallest set where P2C still
// statistically smooths variance without serialising on the closest peer.
const dispatchK = 2

type dispatcher struct {
	store WorkloadState
	now   func() time.Time
	costs route.Costs
	self  types.PeerKey
}

func newDispatcher(store WorkloadState, self types.PeerKey, costs route.Costs) *dispatcher {
	return &dispatcher{store: store, self: self, costs: costs, now: time.Now}
}

func (d *dispatcher) Pick(seed string) (types.PeerKey, error) {
	snap := d.store.Snapshot()
	replicas := replicasOf(snap, seed)
	now := d.now()
	pick, ok := route.NewSelector(snap, d.self, d.costs).PowerOfTwo(replicas, dispatchK,
		func(p types.PeerKey) bool { return isBackedOff(snap, p, now) },
		rand.IntN, //nolint:gosec
	)
	if !ok {
		return types.PeerKey{}, ErrNoReplicas
	}
	return pick, nil
}
