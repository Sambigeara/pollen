// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestPickP2C(t *testing.T) {
	self := types.PeerKey{0x01}
	peerA := types.PeerKey{0x02}
	peerB := types.PeerKey{0x03}
	hash := "deadbeef"

	claimants := func(pks ...types.PeerKey) map[types.PeerKey]struct{} {
		m := make(map[types.PeerKey]struct{}, len(pks))
		for _, pk := range pks {
			m[pk] = struct{}{}
		}
		return m
	}
	view := func(claims map[types.PeerKey]struct{}) routingView {
		return routingView{Claimants: claims}
	}

	t.Run("bootstrap: self unknown, remote known prefers local", func(t *testing.T) {
		lt := newLatencyTracker()
		lt.Record(peerA, hash, "handle", 10)

		target, isLocal, _ := pickP2C(self, view(claimants(self, peerA)), lt, hash, "handle")
		require.Equal(t, self, target)
		require.True(t, isLocal)
	})

	t.Run("both known, self faster prefers local", func(t *testing.T) {
		lt := newLatencyTracker()
		lt.Record(self, hash, "handle", 5)
		lt.Record(peerA, hash, "handle", 10)

		target, isLocal, _ := pickP2C(self, view(claimants(self, peerA)), lt, hash, "handle")
		require.Equal(t, self, target)
		require.True(t, isLocal)
	})

	t.Run("both known, remote faster prefers remote", func(t *testing.T) {
		lt := newLatencyTracker()
		lt.Record(self, hash, "handle", 50)
		lt.Record(peerA, hash, "handle", 5)
		lt.Record(peerB, hash, "handle", 5)

		target, isLocal, _ := pickP2C(self, view(claimants(self, peerA)), lt, hash, "handle")
		require.Equal(t, peerA, target)
		require.False(t, isLocal)
	})

	t.Run("both unknown prefers local", func(t *testing.T) {
		lt := newLatencyTracker()

		target, isLocal, _ := pickP2C(self, view(claimants(self, peerA)), lt, hash, "handle")
		require.Equal(t, self, target)
		require.True(t, isLocal)
	})

	t.Run("self known, remote unknown probes remote", func(t *testing.T) {
		lt := newLatencyTracker()
		lt.Record(self, hash, "handle", 10)

		target, isLocal, _ := pickP2C(self, view(claimants(self, peerA)), lt, hash, "handle")
		require.Equal(t, peerA, target)
		require.False(t, isLocal)
	})

	t.Run("self not in claim set routes to remote", func(t *testing.T) {
		lt := newLatencyTracker()

		target, isLocal, _ := pickP2C(self, view(claimants(peerA)), lt, hash, "handle")
		require.Equal(t, peerA, target)
		require.False(t, isLocal)
	})

	t.Run("self in claim set with no others prefers local", func(t *testing.T) {
		lt := newLatencyTracker()

		target, isLocal, _ := pickP2C(self, view(claimants(self)), lt, hash, "handle")
		require.Equal(t, self, target)
		require.True(t, isLocal)
	})

	t.Run("excludes AdmissionClosed remote", func(t *testing.T) {
		lt := newLatencyTracker()
		lt.Record(peerA, hash, "handle", 5) // closed remote — would otherwise win
		lt.Record(peerB, hash, "handle", 50)
		v := routingView{
			Claimants: claimants(peerA, peerB),
			AdmissionStates: map[types.PeerKey]state.AdmissionState{
				peerA: state.AdmissionClosed,
			},
		}
		target, isLocal, _ := pickP2C(self, v, lt, hash, "handle")
		require.Equal(t, peerB, target, "closed peers must be excluded entirely")
		require.False(t, isLocal)
	})

	t.Run("prefers non-draining over draining when alternative exists", func(t *testing.T) {
		lt := newLatencyTracker()
		lt.Record(peerA, hash, "handle", 5) // draining peerA — would otherwise win
		lt.Record(peerB, hash, "handle", 50)
		v := routingView{
			Claimants: claimants(peerA, peerB),
			Draining:  claimants(peerA),
		}
		target, _, _ := pickP2C(self, v, lt, hash, "handle")
		require.Equal(t, peerB, target, "draining peers must be deprioritised when a non-draining alternative exists")
	})

	t.Run("falls back to draining when every other claimant is closed", func(t *testing.T) {
		lt := newLatencyTracker()
		lt.Record(peerA, hash, "handle", 5)
		v := routingView{
			Claimants: claimants(peerA),
			Draining:  claimants(peerA),
		}
		target, _, _ := pickP2C(self, v, lt, hash, "handle")
		require.Equal(t, peerA, target, "draining is preferable to no candidate at all")
	})

	t.Run("Degraded peer is penalised but still picked when sufficiently faster", func(t *testing.T) {
		lt := newLatencyTracker()
		lt.Record(peerA, hash, "handle", 1)  // degraded but very fast: 1 × 1.5 = 1.5
		lt.Record(peerB, hash, "handle", 10) // open, slower
		v := routingView{
			Claimants: claimants(peerA, peerB),
			AdmissionStates: map[types.PeerKey]state.AdmissionState{
				peerA: state.AdmissionDegraded,
			},
		}
		target, _, _ := pickP2C(self, v, lt, hash, "handle")
		require.Equal(t, peerA, target, "degraded penalty (1.5×) doesn't exclude — the degraded peer wins when it's far faster")
	})

	t.Run("high reject share penalises a fast peer", func(t *testing.T) {
		// peerA is faster on raw EWMA but rejecting half its offered
		// load. peerB is slower but absorbing everything. Routing
		// must prefer peerB — sending to peerA would round-trip
		// through the bounce.
		//   adjusted(peerA) = 5 × (1 + 0.5) = 7.5
		//   adjusted(peerB) = 6 × 1         = 6.0
		lt := newLatencyTracker()
		lt.Record(peerA, hash, "handle", 5)
		lt.Record(peerB, hash, "handle", 6)
		v := routingView{
			Claimants: claimants(peerA, peerB),
			RejectShares: map[types.PeerKey]float64{
				peerA: 0.5,
			},
		}
		target, _, _ := pickP2C(self, v, lt, hash, "handle")
		require.Equal(t, peerB, target, "rejection-aware routing must avoid the bouncing claimant")
	})

	t.Run("self-draining defers to a non-draining remote", func(t *testing.T) {
		// Local hosts the seed but is itself draining. The local-bias
		// branch must NOT preserve self in this case — make-before-break
		// at the placement layer means the routing layer must also bleed
		// traffic away from the draining claimant when an alternative
		// exists.
		lt := newLatencyTracker()
		lt.Record(self, hash, "handle", 1)   // self is fast — would win without drain
		lt.Record(peerA, hash, "handle", 50) // remote is slow but not draining
		v := routingView{
			Claimants: claimants(self, peerA),
			Draining:  claimants(self),
		}
		target, isLocal, ok := pickP2C(self, v, lt, hash, "handle")
		require.True(t, ok)
		require.Equal(t, peerA, target, "self draining must yield to a non-draining alternative")
		require.False(t, isLocal)
	})

	t.Run("self-draining keeps self when no alternative exists", func(t *testing.T) {
		// If self is the only eligible candidate and self is draining,
		// keeping self beats dialing nothing — the call-side gate will
		// reject if local is genuinely saturated.
		lt := newLatencyTracker()
		v := routingView{
			Claimants: claimants(self, peerA),
			Draining:  claimants(self),
			AdmissionStates: map[types.PeerKey]state.AdmissionState{
				peerA: state.AdmissionClosed,
			},
		}
		target, isLocal, ok := pickP2C(self, v, lt, hash, "handle")
		require.True(t, ok)
		require.Equal(t, self, target)
		require.True(t, isLocal)
	})

	t.Run("all claimants closed and self ineligible signals failure", func(t *testing.T) {
		// Saturated topology: every claimant is Closed and self isn't
		// running. pickP2C must signal failure so the caller surfaces a
		// structured OverloadError instead of dialing the zero peer.
		lt := newLatencyTracker()
		v := routingView{
			Claimants: claimants(peerA, peerB),
			AdmissionStates: map[types.PeerKey]state.AdmissionState{
				peerA: state.AdmissionClosed,
				peerB: state.AdmissionClosed,
			},
		}
		_, _, ok := pickP2C(self, v, lt, hash, "handle")
		require.False(t, ok, "every claimant Closed with no eligible self must signal failure")
	})
}

func TestPickP2CFallback(t *testing.T) {
	self := types.PeerKey{0x01}
	peerA := types.PeerKey{0x02}
	peerB := types.PeerKey{0x03}
	hash := "deadbeef"

	claimants := func(pks ...types.PeerKey) map[types.PeerKey]struct{} {
		m := make(map[types.PeerKey]struct{}, len(pks))
		for _, pk := range pks {
			m[pk] = struct{}{}
		}
		return m
	}
	view := func(claims map[types.PeerKey]struct{}) routingView {
		return routingView{Claimants: claims}
	}

	t.Run("excludes the primary", func(t *testing.T) {
		lt := newLatencyTracker()
		target, isLocal, ok := pickP2CFallback(self, view(claimants(peerA, peerB)), peerA, lt, hash, "handle")
		require.True(t, ok)
		require.Equal(t, peerB, target)
		require.False(t, isLocal)
	})

	t.Run("returns ok=false when only the primary is available", func(t *testing.T) {
		lt := newLatencyTracker()
		_, _, ok := pickP2CFallback(self, view(claimants(peerA)), peerA, lt, hash, "handle")
		require.False(t, ok, "single-claimant pool has no fallback")
	})

	t.Run("falls back to self when self in claim set and primary was remote", func(t *testing.T) {
		lt := newLatencyTracker()
		target, isLocal, ok := pickP2CFallback(self, view(claimants(self, peerA)), peerA, lt, hash, "handle")
		require.True(t, ok)
		require.Equal(t, self, target)
		require.True(t, isLocal)
	})

	t.Run("excludes self when not in the claim set", func(t *testing.T) {
		lt := newLatencyTracker()
		_, _, ok := pickP2CFallback(self, view(claimants(peerA)), peerA, lt, hash, "handle")
		require.False(t, ok, "self must drop out of the pool when the CRDT doesn't claim it")
	})

	t.Run("skips draining peer when non-draining alternative exists", func(t *testing.T) {
		lt := newLatencyTracker()
		// peerA is the primary that just failed; peerB is draining; only
		// self (claimant, non-draining) is the eligible fallback.
		v := routingView{
			Claimants: claimants(self, peerA, peerB),
			Draining:  claimants(peerB),
		}
		target, isLocal, ok := pickP2CFallback(self, v, peerA, lt, hash, "handle")
		require.True(t, ok)
		require.Equal(t, self, target)
		require.True(t, isLocal)
	})
}

func TestCanRetry(t *testing.T) {
	t.Run("non-overload error is retryable", func(t *testing.T) {
		require.True(t, canRetry(t.Context(), io.ErrUnexpectedEOF))
	})

	t.Run("overload without retry-after is retryable", func(t *testing.T) {
		err := &OverloadError{Sentinel: ErrAtCapacity}
		require.True(t, canRetry(t.Context(), err))
	})

	t.Run("deadline still has time for retry-after", func(t *testing.T) {
		err := &OverloadError{Sentinel: ErrOverloaded, RetryAfter: 50 * time.Millisecond}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		t.Cleanup(cancel)
		require.True(t, canRetry(ctx, err))
	})

	t.Run("deadline shorter than retry-after refuses retry", func(t *testing.T) {
		err := &OverloadError{Sentinel: ErrOverloaded, RetryAfter: 500 * time.Millisecond}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		t.Cleanup(cancel)
		require.False(t, canRetry(ctx, err), "no point retrying when remaining time can't honour retry-after")
	})
}

func TestPreferStructured(t *testing.T) {
	overload := &OverloadError{Sentinel: ErrOverloaded, Reason: "node memory budget"}
	transport := io.ErrClosedPipe

	require.Equal(t, error(overload), preferStructured(overload, transport))
	require.Equal(t, error(overload), preferStructured(transport, overload))
	require.Equal(t, transport, preferStructured(transport, io.EOF), "two unstructured errors return the first")
}
