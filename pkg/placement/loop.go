// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

type placementConfig struct {
	tick             time.Duration
	migrateThreshold float64
	minDwell         time.Duration
}

// placementLoop is the owner-initiated centroid migration: a peer that
// owns a seed relinquishes its claim once the call-weighted Vivaldi
// centroid of remote callers drifts past migrateThreshold and the seed
// has resided locally for at least minDwell. Scaling the count back up
// is replicaCountLoop's job; this loop only picks where the seed should
// not be.
type placementLoop struct {
	store     WorkloadState
	now       func() time.Time
	firstSeen map[string]time.Time
	cfg       placementConfig
	mu        sync.Mutex
	self      types.PeerKey
}

func newPlacementLoop(self types.PeerKey, cfg placementConfig, store WorkloadState) *placementLoop {
	return &placementLoop{
		self:      self,
		cfg:       cfg,
		store:     store,
		now:       time.Now,
		firstSeen: make(map[string]time.Time),
	}
}

func (p *placementLoop) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tick()
		}
	}
}

func (p *placementLoop) tick() {
	snap := p.store.Snapshot()
	owned := ownedBy(snap, p.self)
	now := p.now()

	p.mu.Lock()
	ownedSet := make(map[string]struct{}, len(owned))
	for _, seed := range owned {
		ownedSet[seed] = struct{}{}
		if _, ok := p.firstSeen[seed]; !ok {
			p.firstSeen[seed] = now
		}
	}
	for seed := range p.firstSeen {
		if _, ok := ownedSet[seed]; !ok {
			delete(p.firstSeen, seed)
		}
	}
	p.mu.Unlock()

	selfNV, ok := snap.Nodes[p.self]
	if !ok || selfNV.VivaldiCoord == nil {
		return
	}

	for _, seed := range owned {
		p.evaluate(snap, seed, *selfNV.VivaldiCoord, now)
	}
}

func (p *placementLoop) evaluate(snap state.Snapshot, seed string, selfCoord coords.Coord, now time.Time) {
	p.mu.Lock()
	started := p.firstSeen[seed]
	p.mu.Unlock()

	if now.Sub(started) < p.cfg.minDwell {
		return
	}

	sources := sourcesOf(snap, seed)
	if len(sources) == 0 {
		return
	}

	centroid, ok := centroidOf(snap, sources)
	if !ok {
		return
	}

	if coords.Distance(selfCoord, centroid) < p.cfg.migrateThreshold {
		return
	}

	// TODO(saml): if self is the sole replica, relinquishing drops the
	// count to 0 until replicaCountLoop scales it back up — brief
	// unavailability gap. Accepted for now; revisit if it bites.
	p.store.MarkWorkloadDraining(seed)
	p.mu.Lock()
	delete(p.firstSeen, seed)
	p.mu.Unlock()
}

func centroidOf(snap state.Snapshot, sources []sourceCount) (coords.Coord, bool) {
	var totalWeight, cx, cy, ch float64
	for _, src := range sources {
		nv, ok := snap.Nodes[src.caller]
		if !ok || nv.VivaldiCoord == nil {
			continue
		}
		w := float64(src.count)
		totalWeight += w
		cx += nv.VivaldiCoord.X * w
		cy += nv.VivaldiCoord.Y * w
		ch += nv.VivaldiCoord.Height * w
	}
	if totalWeight == 0 {
		return coords.Coord{}, false
	}
	return coords.Coord{
		X:      cx / totalWeight,
		Y:      cy / totalWeight,
		Height: ch / totalWeight,
	}, true
}
