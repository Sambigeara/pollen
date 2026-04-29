// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"sync"
	"time"
)

type callCountsEmitter func(counts map[string]uint64)

// callTracker records per-seed call counts originating at this node and
// flushes them to gossip on a fixed window. Remote counts are read from
// the gossip snapshot — no local cache.
type callTracker struct {
	emit     callCountsEmitter
	local    map[string]uint64
	window   time.Duration
	mu       sync.Mutex
	occupied bool
}

func newCallTracker(window time.Duration, emit callCountsEmitter) *callTracker {
	return &callTracker{
		window: window,
		emit:   emit,
		local:  make(map[string]uint64),
	}
}

func (t *callTracker) RecordCall(seed string) {
	t.mu.Lock()
	t.local[seed]++
	t.mu.Unlock()
}

// localCount lets the replica-count loop see in-window invocations
// before the next flush gossips them.
func (t *callTracker) localCount(seed string) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.local[seed]
}

func (t *callTracker) flush() {
	t.mu.Lock()
	counts := t.local
	t.local = make(map[string]uint64)
	occupied := t.occupied
	t.occupied = len(counts) > 0
	t.mu.Unlock()
	if len(counts) == 0 && !occupied {
		return
	}
	t.emit(counts)
}

func (t *callTracker) Run(ctx context.Context) {
	ticker := time.NewTicker(t.window)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.flush()
		}
	}
}
