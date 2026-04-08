package placement

import (
	"math/rand/v2"
	"sync"

	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/types"
)

const latencyAlpha = 0.3

type latencyKey struct {
	hash string
	peer types.PeerKey
}

type latencyTracker struct {
	ewma map[latencyKey]*metrics.EWMA
	mu   sync.Mutex
}

func newLatencyTracker() *latencyTracker {
	return &latencyTracker{ewma: make(map[latencyKey]*metrics.EWMA)}
}

func (lt *latencyTracker) Record(peer types.PeerKey, hash string, ms float64) {
	k := latencyKey{peer: peer, hash: hash}
	lt.mu.Lock()
	e, ok := lt.ewma[k]
	if !ok {
		e = metrics.NewEWMA(latencyAlpha, ms)
		lt.ewma[k] = e
	}
	lt.mu.Unlock()
	e.Update(ms)
}

func (lt *latencyTracker) Get(peer types.PeerKey, hash string) float64 {
	k := latencyKey{peer: peer, hash: hash}
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if e, ok := lt.ewma[k]; ok {
		return e.Value()
	}
	return -1
}

// pickP2C selects a target from claimants using power-of-two-choices.
// If self is a claimant and other claimants exist, it compares self against
// one random remote (local-biased variant). Otherwise picks 2 random remotes.
//
//nolint:gosec
func pickP2C(
	self types.PeerKey,
	locallyRunning bool,
	claimants map[types.PeerKey]struct{},
	lt *latencyTracker,
	hash string,
) (target types.PeerKey, isLocal bool) {
	others := make([]types.PeerKey, 0, len(claimants))
	for pk := range claimants {
		if pk != self {
			others = append(others, pk)
		}
	}

	if locallyRunning && len(others) > 0 {
		remote := others[rand.IntN(len(others))]
		selfLat := lt.Get(self, hash)
		remoteLat := lt.Get(remote, hash)
		if selfLat >= 0 && remoteLat >= 0 {
			if selfLat <= remoteLat {
				return self, true
			}
			return remote, false
		}
		if selfLat >= 0 {
			// Self known, remote unknown — probe to bootstrap remote latency.
			return remote, false
		}
		// Self unknown — serve locally to bootstrap self-latency and avoid
		// a deadlock where a fresh claimant always defers to remotes with
		// known latency.
		return self, true
	}

	if locallyRunning {
		return self, true
	}

	if len(others) == 0 {
		return types.PeerKey{}, false
	}

	if len(others) == 1 {
		return others[0], false
	}

	a := others[rand.IntN(len(others))]
	b := others[rand.IntN(len(others))]
	for b == a && len(others) > 1 {
		b = others[rand.IntN(len(others))]
	}

	latA := lt.Get(a, hash)
	latB := lt.Get(b, hash)
	if latA >= 0 && (latB < 0 || latA <= latB) {
		return a, false
	}
	return b, false
}

// shuffledClaimants returns all claimants except `exclude` in random order.
//
//nolint:gosec
func shuffledClaimants(claimants map[types.PeerKey]struct{}, exclude types.PeerKey) []types.PeerKey {
	out := make([]types.PeerKey, 0, len(claimants))
	for pk := range claimants {
		if pk != exclude {
			out = append(out, pk)
		}
	}
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}
