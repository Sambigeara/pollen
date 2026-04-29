// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"sync"
	"time"
)

type backoffEmitter func(ttl time.Duration)

type backoffConfig struct {
	ttl time.Duration
}

// backoff tracks local refusal pressure: SignalRefusal flips the node
// active and emits a gossip TTL. Sustained refusals keep extending the
// active window by re-emitting the same TTL — no exponential growth.
// Active state expires once real time passes lastEmit+ttl, so recovery
// is purely TTL-driven and snaps back to healthy within one TTL window
// after refusals stop.
type backoff struct {
	lastEmit time.Time
	emit     backoffEmitter
	now      func() time.Time
	cfg      backoffConfig
	mu       sync.Mutex
	active   bool
}

func newBackoff(cfg backoffConfig, emit backoffEmitter) *backoff {
	return &backoff{
		cfg:  cfg,
		emit: emit,
		now:  time.Now,
	}
}

// IsLocallyOverloaded reports whether the node is currently inside an
// active backoff window. Returns false once now > lastEmit+ttl,
// flipping the cached active flag off lazily on read so callers don't
// need a separate timer goroutine.
func (b *backoff) IsLocallyOverloaded() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.active {
		return false
	}
	if b.now().After(b.lastEmit.Add(b.cfg.ttl)) {
		b.active = false
		return false
	}
	return true
}

// SignalRefusal flips the node active and re-emits the configured TTL
// via the gossip hook. Repeat refusals during an existing window keep
// the active deadline fresh without growing it — admission is
// deterministic, so a single TTL avoids the cascading-stickiness that
// exponential growth produces under sustained pressure.
func (b *backoff) SignalRefusal() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.active = true
	b.lastEmit = b.now()
	b.emit(b.cfg.ttl)
}
