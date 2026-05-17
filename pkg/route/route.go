// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package route

import (
	"cmp"
	"math"
	"slices"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

// Distance is the locality metric between two peers: the distance
// between their Vivaldi coordinates. A peer that is absent from the
// snapshot or whose coordinate has not converged is infinitely far, so
// it sorts last and is chosen only when nothing nearer is available.
// This is the metric lifted verbatim from placement dispatch so blob
// fetch and workload routing agree on what "nearest" means.
func Distance(snap state.Snapshot, self, peer types.PeerKey) float64 {
	selfNV, sok := snap.Nodes[self]
	peerNV, pok := snap.Nodes[peer]
	if !sok || !pok || selfNV.VivaldiCoord == nil || peerNV.VivaldiCoord == nil {
		return math.Inf(1)
	}
	return coords.Distance(*selfNV.VivaldiCoord, *peerNV.VivaldiCoord)
}

// orderByDistance sorts a copy of candidates nearest-first by Distance
// alone. This is the placement-dispatch ordering lifted verbatim
// (distance-only, no tie-break) so the workload path's behaviour is
// unchanged by the move into this package.
func orderByDistance(snap state.Snapshot, self types.PeerKey, candidates []types.PeerKey) []types.PeerKey {
	out := slices.Clone(candidates)
	slices.SortFunc(out, func(a, b types.PeerKey) int {
		return cmp.Compare(Distance(snap, self, a), Distance(snap, self, b))
	})
	return out
}

// ByLocality orders candidates nearest-first with a deterministic
// PeerKey tie-break, so equal-distance or coordinate-less holders are
// chosen consistently across calls. A caller that fetches in this order
// keeps hitting the same source, which keeps that source's cache warm.
func ByLocality(snap state.Snapshot, self types.PeerKey, candidates []types.PeerKey) []types.PeerKey {
	out := slices.Clone(candidates)
	slices.SortFunc(out, func(a, b types.PeerKey) int {
		return cmp.Or(
			cmp.Compare(Distance(snap, self, a), Distance(snap, self, b)),
			a.Compare(b),
		)
	})
	return out
}

// Nearest returns the candidate closest to self, with a deterministic
// PeerKey tie-break. ok is false only when candidates is empty.
func Nearest(snap state.Snapshot, self types.PeerKey, candidates []types.PeerKey) (types.PeerKey, bool) {
	if len(candidates) == 0 {
		return types.PeerKey{}, false
	}
	return ByLocality(snap, self, candidates)[0], true
}

// PowerOfTwo narrows candidates to the nearest k by Distance, drops any
// the skip predicate rejects, and returns a uniform random pick among
// the survivors (the full nearest-k when every one is skipped). skip
// may be nil; rng must return a value in [0,n). ok is false only when
// candidates is empty. This is the placement dispatch policy lifted
// verbatim so the same locality-aware selection backs every holder set.
func PowerOfTwo(snap state.Snapshot, self types.PeerKey, candidates []types.PeerKey, k int, skip func(types.PeerKey) bool, rng func(n int) int) (types.PeerKey, bool) {
	if len(candidates) == 0 {
		return types.PeerKey{}, false
	}
	pool := candidates
	if len(pool) > k {
		pool = orderByDistance(snap, self, candidates)[:k]
	}
	survivors := make([]types.PeerKey, 0, len(pool))
	for _, p := range pool {
		if skip == nil || !skip(p) {
			survivors = append(survivors, p)
		}
	}
	if len(survivors) == 0 {
		survivors = pool
	}
	return survivors[rng(len(survivors))], true
}
