// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"math/rand/v2"
	"sync"

	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	latencyAlpha = 0.3
	// degradedPenalty multiplies the EWMA latency for a peer in
	// AdmissionDegraded so P2C deprioritises it without removing it
	// entirely. 1.5× lines up with the headroom multiplier in target.go;
	// a degraded peer has to be 33% faster than an open peer to win the
	// pick, which is roughly the boundary between "useful but pinched"
	// and "actively saturating".
	degradedPenalty = 1.5
)

// routingView bundles the routing-relevant signals from the snapshot —
// claim membership, drain flags, and per-peer admission state — so
// pickP2C and pickP2CFallback can apply make-before-break and Open/
// Degraded/Closed filtering without dragging the whole snapshot through
// their signatures. Drain and AdmissionStates may be nil; both default
// to "open, no drain" then. Claim membership IS the source of truth for
// "can self serve" — the local wasm manager state is intentionally not
// consulted, so we never route to ourselves on the back of stale local
// runtime that the CRDT no longer claims.
type routingView struct {
	Claimants       map[types.PeerKey]struct{}
	Draining        map[types.PeerKey]struct{}
	AdmissionStates map[types.PeerKey]state.AdmissionState
	// RejectShares is the per-peer rejected-share for this hash:
	// rejects / (served + rejects) over the same EWMA window. A peer
	// hovering at 50% reject share gets a 1.5× latency penalty in
	// pickP2C (1 + share = expected attempts to land a success);
	// missing or zero entries impose no penalty. Computed once per
	// Call from gossiped SeedMetrics so the routing layer doesn't
	// have to thread the snapshot through every helper.
	RejectShares map[types.PeerKey]float64
}

// admission returns the peer's last-known admission state, defaulting
// to AdmissionOpen when the peer hasn't been observed yet — old peers
// that don't publish AdmissionState mustn't be filtered out as Closed.
func (v routingView) admission(pk types.PeerKey) state.AdmissionState {
	s, ok := v.AdmissionStates[pk]
	if !ok || s == state.AdmissionUnspecified {
		return state.AdmissionOpen
	}
	return s
}

func (v routingView) isDraining(pk types.PeerKey) bool {
	_, drain := v.Draining[pk]
	return drain
}

// isClaimant reports whether pk is in the gossiped claim set for the
// hash this view is built for.
func (v routingView) isClaimant(pk types.PeerKey) bool {
	_, ok := v.Claimants[pk]
	return ok
}

// adjustedLatency penalises a peer's EWMA based on backpressure signals
// so P2C deprioritises it without excluding it. -1 (unknown) passes
// through unmodified — the bootstrap branches in pickP2C still need
// that signal. Two penalties compose multiplicatively:
//
//   - AdmissionDegraded: 1.5× (33% faster than open to win the pick)
//   - RejectShare > 0:   (1 + share)× (expected attempts per success)
//
// Stacking is intentional: a degraded peer that's also shedding 50%
// of demand pays 1.5 × 1.5 = 2.25×, which is what its expected wall
// time per successful invocation actually looks like.
func (v routingView) adjustedLatency(pk types.PeerKey, raw float64) float64 {
	if raw < 0 {
		return raw
	}
	adj := raw
	if v.admission(pk) == state.AdmissionDegraded {
		adj *= degradedPenalty
	}
	if share := v.RejectShares[pk]; share > 0 {
		adj *= 1 + share
	}
	return adj
}

type latencyKey struct {
	hash     string
	function string
	peer     types.PeerKey
}

type latencyTracker struct {
	ewma map[latencyKey]*metrics.EWMA
	mu   sync.Mutex
}

func newLatencyTracker() *latencyTracker {
	return &latencyTracker{ewma: make(map[latencyKey]*metrics.EWMA)}
}

func (lt *latencyTracker) Record(peer types.PeerKey, hash, function string, ms float64) {
	k := latencyKey{peer: peer, hash: hash, function: function}
	lt.mu.Lock()
	e, ok := lt.ewma[k]
	if !ok {
		e = metrics.NewEWMA(latencyAlpha, ms)
		lt.ewma[k] = e
	}
	lt.mu.Unlock()
	e.Update(ms)
}

func (lt *latencyTracker) Get(peer types.PeerKey, hash, function string) float64 {
	k := latencyKey{peer: peer, hash: hash, function: function}
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if e, ok := lt.ewma[k]; ok {
		return e.Value()
	}
	return -1
}

// pickP2C selects a target from claimants using power-of-two-choices for
// the given (hash, function). Per-function latency lets a fast function
// route to one peer while a slow function on the same module routes
// elsewhere, rather than averaging both into a single hash-keyed EWMA.
//
// Routing filters drive make-before-break and admission backpressure:
//   - AdmissionClosed peers are excluded entirely — sending to a closed
//     peer is wasted budget.
//   - Draining peers are excluded so long as a non-draining alternative
//     exists, so a soon-to-leave claimant carries the minimum residual
//     traffic during the cooldown.
//   - AdmissionDegraded peers stay in the pool but their EWMA is
//     multiplied by degradedPenalty so P2C deprioritises them.
//
// If self is a claimant and other (eligible) claimants exist, it
// compares self against one random eligible remote. Otherwise picks 2
// random eligible remotes and prefers the lower (penalised) latency.
//
//nolint:gosec,nestif
func pickP2C(
	self types.PeerKey,
	view routingView,
	lt *latencyTracker,
	hash, function string,
) (target types.PeerKey, isLocal, ok bool) {
	nonDrainingRemotes, drainingRemotes := eligibleRemotes(self, view)

	selfIsClaimant := view.isClaimant(self)
	selfClosed := view.admission(self) == state.AdmissionClosed
	selfDraining := selfIsClaimant && view.isDraining(self)
	// Self enters the pool when the gossiped claim set names us and the
	// local admission state isn't Closed. Drain state demotes self the
	// same way it demotes a remote: only picked when no non-draining
	// alternative exists.
	selfEligible := selfIsClaimant && !selfClosed
	preferSelf := selfEligible && !selfDraining

	others := nonDrainingRemotes
	if len(others) == 0 {
		others = drainingRemotes
	}

	if preferSelf && len(others) > 0 {
		remote := others[rand.IntN(len(others))]
		selfLat := view.adjustedLatency(self, lt.Get(self, hash, function))
		remoteLat := view.adjustedLatency(remote, lt.Get(remote, hash, function))
		switch {
		case selfLat < 0:
			// Self unknown — serve locally to bootstrap our own EWMA
			// (covers the both-unknown case too).
			return self, true, true
		case remoteLat < 0:
			return remote, false, true
		case selfLat < remoteLat:
			return self, true, true
		case remoteLat < selfLat:
			return remote, false, true
		case rand.IntN(2) == 0: //nolint:mnd
			return self, true, true
		default:
			return remote, false, true
		}
	}

	if preferSelf {
		// No remote alternative; serve locally.
		return self, true, true
	}

	if len(others) == 0 {
		// Self is draining or otherwise ineligible AND no remote
		// alternative exists; signal failure so the caller surfaces a
		// structured overload instead of dialing the zero peer.
		if selfEligible {
			return self, true, true
		}
		return types.PeerKey{}, false, false
	}
	if len(others) == 1 {
		return others[0], others[0] == self, true
	}

	a := others[rand.IntN(len(others))]
	b := others[rand.IntN(len(others))]
	for b == a && len(others) > 1 {
		b = others[rand.IntN(len(others))]
	}

	latA := view.adjustedLatency(a, lt.Get(a, hash, function))
	latB := view.adjustedLatency(b, lt.Get(b, hash, function))
	if latA >= 0 && (latB < 0 || latA <= latB) {
		return a, a == self, true
	}
	return b, b == self, true
}

// eligibleRemotes splits the peers in view.Claimants other than self
// into non-draining and draining buckets, dropping AdmissionClosed
// peers entirely. Callers prefer non-draining and fall back to draining
// when no alternative exists; closed peers are never resurrected.
func eligibleRemotes(self types.PeerKey, view routingView) (nonDraining, draining []types.PeerKey) {
	for pk := range view.Claimants {
		if pk == self {
			continue
		}
		if view.admission(pk) == state.AdmissionClosed {
			continue
		}
		if view.isDraining(pk) {
			draining = append(draining, pk)
			continue
		}
		nonDraining = append(nonDraining, pk)
	}
	return nonDraining, draining
}

// pickP2CFallback runs power-of-two-choices across the claimant pool
// minus `primary` for retry purposes — the second attempt after the
// first claimant fails transiently. Self is included in the pool only
// when the gossiped claim set names us, so a saturated local that
// callLocal already rejected isn't picked again. Closed/Draining peers
// are filtered the same way pickP2C filters them. Returns ok=false
// when no fallback candidate exists.
//
//nolint:gosec
func pickP2CFallback(
	self types.PeerKey,
	view routingView,
	primary types.PeerKey,
	lt *latencyTracker,
	hash, function string,
) (target types.PeerKey, isLocal, ok bool) {
	selfEligible := view.isClaimant(self) && view.admission(self) != state.AdmissionClosed

	var nonDraining, draining []types.PeerKey
	add := func(pk types.PeerKey) {
		if view.isDraining(pk) {
			draining = append(draining, pk)
		} else {
			nonDraining = append(nonDraining, pk)
		}
	}
	if selfEligible && self != primary {
		add(self)
	}
	for pk := range view.Claimants {
		if pk == primary || pk == self {
			continue
		}
		if view.admission(pk) == state.AdmissionClosed {
			continue
		}
		add(pk)
	}
	pool := nonDraining
	if len(pool) == 0 {
		pool = draining
	}

	if len(pool) == 0 {
		return types.PeerKey{}, false, false
	}
	if len(pool) == 1 {
		c := pool[0]
		return c, c == self, true
	}

	a := pool[rand.IntN(len(pool))]
	b := pool[rand.IntN(len(pool))]
	for b == a {
		b = pool[rand.IntN(len(pool))]
	}
	latA := view.adjustedLatency(a, lt.Get(a, hash, function))
	latB := view.adjustedLatency(b, lt.Get(b, hash, function))
	var winner types.PeerKey
	switch {
	case latA < 0 && a == self:
		winner = a // bootstrap local EWMA
	case latB < 0 && b == self:
		winner = b
	case latA < 0:
		winner = b
	case latB < 0:
		winner = a
	case latA <= latB:
		winner = a
	default:
		winner = b
	}
	return winner, winner == self, true
}
