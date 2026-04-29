// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"bytes"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

type replicaCountConfig struct {
	tick               time.Duration
	scaleUpThreshold   float64
	scaleDownThreshold float64
	scaleDownGrace     time.Duration
}

// replicaCountLoop holds replica count above the spec's MinReplicas
// floor (lex-min election among non-replicas) and scales further on
// saturation = |claims ∩ backed_off| / |claims|: scale up at the
// heaviest unserved source, scale down on sustained low saturation
// with the lex-min replica electing to relinquish.
type replicaCountLoop struct {
	store    WorkloadState
	calls    *callTracker
	backoff  *backoff
	now      func() time.Time
	lowSince map[string]time.Time
	cfg      replicaCountConfig
	mu       sync.Mutex
	self     types.PeerKey
}

func newReplicaCountLoop(self types.PeerKey, cfg replicaCountConfig, store WorkloadState, calls *callTracker, b *backoff) *replicaCountLoop {
	return &replicaCountLoop{
		self:     self,
		cfg:      cfg,
		store:    store,
		calls:    calls,
		backoff:  b,
		now:      time.Now,
		lowSince: make(map[string]time.Time),
	}
}

func (r *replicaCountLoop) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick()
		}
	}
}

func (r *replicaCountLoop) tick() {
	snap := r.store.Snapshot()
	now := r.now()
	seeds := allSeeds(snap)

	seedSet := make(map[string]struct{}, len(seeds))
	for _, s := range seeds {
		seedSet[s] = struct{}{}
	}
	r.mu.Lock()
	for s := range r.lowSince {
		if _, ok := seedSet[s]; !ok {
			delete(r.lowSince, s)
		}
	}
	r.mu.Unlock()

	backed := backedOffSet(snap, now)
	if r.backoff.IsLocallyOverloaded() {
		backed[r.self] = struct{}{}
	}

	for _, seed := range seeds {
		r.evaluate(snap, seed, backed, now)
	}
}

func (r *replicaCountLoop) evaluate(snap state.Snapshot, seed string, backed map[types.PeerKey]struct{}, now time.Time) {
	replicas := replicasOf(snap, seed)
	target := targetFor(snap, seed)

	if uint32(len(replicas)) < target {
		r.tryFillFloor(snap, seed, replicas, backed)
		r.clearLowSince(seed)
		return
	}

	saturation := saturationOf(replicas, backed)
	if saturation >= r.cfg.scaleUpThreshold {
		r.tryScaleUp(snap, seed, replicas, backed)
		r.clearLowSince(seed)
		return
	}

	if saturation < r.cfg.scaleDownThreshold {
		r.maybeScaleDown(seed, target, replicas, now)
		return
	}

	r.clearLowSince(seed)
}

// tryFillFloor elects the lex-min live peer not already claiming the
// seed and not currently in backoff. One claim per tick — successive
// ticks fill the gap.
func (r *replicaCountLoop) tryFillFloor(snap state.Snapshot, seed string, replicas []types.PeerKey, backed map[types.PeerKey]struct{}) {
	replicaSet := make(map[types.PeerKey]struct{}, len(replicas))
	for _, p := range replicas {
		replicaSet[p] = struct{}{}
	}
	var lexMin types.PeerKey
	found := false
	for _, p := range snap.PeerKeys {
		if _, isReplica := replicaSet[p]; isReplica {
			continue
		}
		if _, off := backed[p]; off {
			continue
		}
		if !found || bytes.Compare(p[:], lexMin[:]) < 0 {
			lexMin = p
			found = true
		}
	}
	if !found {
		return
	}
	if lexMin == r.self {
		r.store.ClaimWorkload(seed)
	}
}

// tryScaleUp picks the heaviest non-replica caller as the new
// claimant when one exists (load-led growth toward demand). When every
// caller is already a replica — the common case for chain heads under
// direct load — saturation still demands capacity, so fall through to
// the lex-min non-replica election. Without the fallback, autoscale
// freezes at MinReplicas whenever traffic enters at the replicas
// themselves.
func (r *replicaCountLoop) tryScaleUp(snap state.Snapshot, seed string, replicas []types.PeerKey, backed map[types.PeerKey]struct{}) {
	if heaviest, ok := heaviestUnservedSource(sourcesOf(snap, seed), r.calls.localCount(seed), r.self, replicas, backed); ok {
		if heaviest == r.self {
			r.store.ClaimWorkload(seed)
		}
		return
	}
	r.tryFillFloor(snap, seed, replicas, backed)
}

// targetFor reads the spec's effective replica target: MinReplicas
// (capped by cluster size), or full cluster when Spread >= 1.
func targetFor(snap state.Snapshot, seed string) uint32 {
	sv, ok := snap.Specs[seed]
	if !ok {
		return 0
	}
	return effectiveTarget(sv.Spec.MinReplicas, sv.Spec.Spread, len(snap.PeerKeys))
}

func (r *replicaCountLoop) maybeScaleDown(seed string, target uint32, replicas []types.PeerKey, now time.Time) {
	if uint32(len(replicas)) <= target {
		r.clearLowSince(seed)
		return
	}

	r.mu.Lock()
	since, ok := r.lowSince[seed]
	if !ok {
		r.lowSince[seed] = now
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	if now.Sub(since) < r.cfg.scaleDownGrace {
		return
	}

	lexMin := slices.MinFunc(replicas, func(a, b types.PeerKey) int {
		return bytes.Compare(a[:], b[:])
	})
	if lexMin == r.self {
		r.store.MarkWorkloadDraining(seed)
		r.clearLowSince(seed)
	}
}

func (r *replicaCountLoop) clearLowSince(seed string) {
	r.mu.Lock()
	delete(r.lowSince, seed)
	r.mu.Unlock()
}

func saturationOf(replicas []types.PeerKey, backed map[types.PeerKey]struct{}) float64 {
	if len(replicas) == 0 {
		return 0
	}
	count := 0
	for _, r := range replicas {
		if _, ok := backed[r]; ok {
			count++
		}
	}
	return float64(count) / float64(len(replicas))
}

// heaviestUnservedSource returns the peer that issued the most calls to
// a seed it isn't already claiming. Self counts via localCount because
// the in-flight window hasn't gossiped yet. Lex-min on PeerKey breaks
// ties so every node converges on the same candidate. Peers in backoff
// are excluded so saturated nodes can't be elected to take more load.
func heaviestUnservedSource(sources []sourceCount, localCount uint64, self types.PeerKey, replicas []types.PeerKey, backed map[types.PeerKey]struct{}) (types.PeerKey, bool) {
	replicaSet := make(map[types.PeerKey]struct{}, len(replicas))
	for _, r := range replicas {
		replicaSet[r] = struct{}{}
	}

	skip := func(p types.PeerKey) bool {
		if _, hasReplica := replicaSet[p]; hasReplica {
			return true
		}
		_, off := backed[p]
		return off
	}

	candidates := make([]sourceCount, 0, len(sources)+1)
	if !skip(self) && localCount > 0 {
		candidates = append(candidates, sourceCount{caller: self, count: localCount})
	}
	for _, src := range sources {
		if skip(src.caller) {
			continue
		}
		if src.count > 0 {
			candidates = append(candidates, src)
		}
	}
	if len(candidates) == 0 {
		return types.PeerKey{}, false
	}

	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.count > best.count {
			best = c
			continue
		}
		if c.count == best.count && bytes.Compare(c.caller[:], best.caller[:]) < 0 {
			best = c
		}
	}
	return best.caller, true
}
