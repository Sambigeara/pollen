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

// Costs reports the routing-layer cost to reach a peer from the local
// node, summed over the relay path so a holder behind relays is priced
// at its true reach cost rather than its straight-line distance. ok is
// false when the router has no path to dest, in which case selection
// falls back to the straight-line Vivaldi distance. A nil Costs disables
// the path-cost preference entirely.
type Costs interface {
	Cost(dest types.PeerKey) (float64, bool)
}

// Selector ranks candidate holders by locality from one node's vantage.
// It prefers the routing-layer path cost, which accounts for relay
// detours, and falls back to the straight-line distance between Vivaldi
// coordinates for peers the router has no path to.
type Selector struct {
	costs Costs
	snap  state.Snapshot
	self  types.PeerKey
}

func NewSelector(snap state.Snapshot, self types.PeerKey, costs Costs) Selector {
	return Selector{snap: snap, self: self, costs: costs}
}

// Distance is the locality metric to peer: the routing-layer path cost
// when the router has a route, else the straight-line Vivaldi-coordinate
// distance. A peer absent from the snapshot or whose coordinate has not
// converged is infinitely far, so it sorts last and is chosen only when
// nothing nearer is available.
func (s Selector) Distance(peer types.PeerKey) float64 {
	if s.costs != nil {
		if c, ok := s.costs.Cost(peer); ok {
			return c
		}
	}
	selfNV, sok := s.snap.Nodes[s.self]
	peerNV, pok := s.snap.Nodes[peer]
	if !sok || !pok || selfNV.VivaldiCoord == nil || peerNV.VivaldiCoord == nil {
		return math.Inf(1)
	}
	return coords.Distance(*selfNV.VivaldiCoord, *peerNV.VivaldiCoord)
}

// orderByDistance sorts a copy of candidates nearest-first by Distance
// alone, with no tie-break.
func (s Selector) orderByDistance(candidates []types.PeerKey) []types.PeerKey {
	out := slices.Clone(candidates)
	slices.SortFunc(out, func(a, b types.PeerKey) int {
		return cmp.Compare(s.Distance(a), s.Distance(b))
	})
	return out
}

// ByLocality orders candidates nearest-first with a deterministic
// PeerKey tie-break, so equal-cost or coordinate-less holders are chosen
// consistently across calls. A caller that fetches in this order keeps
// hitting the same source, which keeps that source's cache warm.
func (s Selector) ByLocality(candidates []types.PeerKey) []types.PeerKey {
	out := slices.Clone(candidates)
	slices.SortFunc(out, func(a, b types.PeerKey) int {
		return cmp.Or(
			cmp.Compare(s.Distance(a), s.Distance(b)),
			a.Compare(b),
		)
	})
	return out
}

// Nearest returns the candidate closest to self, with a deterministic
// PeerKey tie-break. ok is false only when candidates is empty.
func (s Selector) Nearest(candidates []types.PeerKey) (types.PeerKey, bool) {
	if len(candidates) == 0 {
		return types.PeerKey{}, false
	}
	return s.ByLocality(candidates)[0], true
}

// PowerOfTwo narrows candidates to the nearest k by Distance, drops any
// the skip predicate rejects, and returns a uniform random pick among
// the survivors (the full nearest-k when every one is skipped). skip
// may be nil; rng must return a value in [0,n). ok is false only when
// candidates is empty.
func (s Selector) PowerOfTwo(candidates []types.PeerKey, k int, skip func(types.PeerKey) bool, rng func(n int) int) (types.PeerKey, bool) {
	if len(candidates) == 0 {
		return types.PeerKey{}, false
	}
	pool := candidates
	if len(pool) > k {
		pool = s.orderByDistance(candidates)[:k]
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
