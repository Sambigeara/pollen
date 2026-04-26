// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Caller dispatches a seed invocation. Satisfied by placement.Service's
// Call method; declared at the evaluator layer so the seed-backed PDP
// factory lives here without cross-layer imports.
type Caller interface {
	Call(ctx context.Context, seed, function string, input []byte) ([]byte, error)
}

// Circuit breaker defaults. Tuned for the PDP-unreachable case: five
// quick failures trip, ten-second cooldown before the next probe.
const (
	defaultFailThreshold = 5
	defaultCooldown      = 10 * time.Second
)

// breakerState tracks the seed evaluator's failure-isolation breaker.
// closed permits calls; open short-circuits with ErrCircuitOpen; halfOpen
// is the single-probe window after cooldown — only the first caller
// through admit() enters halfOpen, the rest see ErrCircuitOpen.
type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

// checkFunction is the WASM export name every PDP seed exposes. The
// dispatch site always invokes this export.
const checkFunction = "check"

// seedEvaluator delegates authorisation decisions to a named seed's
// `check` export. Wraps a consecutive-failure circuit breaker so a dead
// PDP doesn't get hammered on every gate check.
type seedEvaluator struct {
	openedAt time.Time
	caller   Caller
	now      func() time.Time
	seed     string
	failures int
	state    breakerState
	mu       sync.Mutex
}

func newSeedEvaluator(seedName string, caller Caller) *seedEvaluator {
	return &seedEvaluator{
		seed:   seedName,
		caller: caller,
		now:    time.Now,
	}
}

func (e *seedEvaluator) Allow(ctx context.Context, req Request) (Decision, error) {
	// Marshal before admit so a malformed Request — a programmer error,
	// not an evaluator outage — doesn't strand a half-open breaker.
	// admit() in halfOpen claims the single probe slot; once claimed,
	// the breaker won't progress until recordFailure or recordSuccess
	// runs.
	body, err := json.Marshal(req)
	if err != nil {
		return Decision{}, fmt.Errorf("seed evaluator: marshal request: %w", err)
	}
	if err := e.admit(); err != nil {
		return Decision{}, err
	}
	out, err := e.caller.Call(ctx, e.seed, checkFunction, body)
	if err != nil {
		e.recordFailure()
		return Decision{}, fmt.Errorf("seed evaluator: call %q: %w", e.seed, err)
	}
	var d Decision
	if err := json.Unmarshal(out, &d); err != nil {
		e.recordFailure()
		return Decision{}, fmt.Errorf("seed evaluator: unmarshal decision: %w", err)
	}
	e.recordSuccess()
	return d, nil
}

func (e *seedEvaluator) admit() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch e.state {
	case breakerClosed:
		return nil
	case breakerOpen:
		if e.now().Sub(e.openedAt) < defaultCooldown {
			return ErrCircuitOpen
		}
		// Cooldown elapsed — promote this caller to the single probe.
		// Concurrent callers observing halfOpen keep seeing
		// ErrCircuitOpen until the probe resolves.
		e.state = breakerHalfOpen
		return nil
	case breakerHalfOpen:
		return ErrCircuitOpen
	}
	panic(fmt.Sprintf("seed evaluator: invalid breaker state %d", e.state))
}

// recordFailure runs only after admit() returned nil, so the breaker is
// in breakerClosed or breakerHalfOpen — never breakerOpen.
func (e *seedEvaluator) recordFailure() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state == breakerHalfOpen {
		// Probe failed — re-open and restart the cooldown clock.
		e.state = breakerOpen
		e.openedAt = e.now()
		return
	}
	e.failures++
	if e.failures >= defaultFailThreshold {
		e.state = breakerOpen
		e.openedAt = e.now()
	}
}

func (e *seedEvaluator) recordSuccess() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.state = breakerClosed
	e.failures = 0
}
