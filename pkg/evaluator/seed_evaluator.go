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

// Caller dispatches a seed invocation. Declared here so the seed-backed
// PDP factory avoids cross-layer imports.
type Caller interface {
	Call(ctx context.Context, seed, function string, input []byte) ([]byte, error)
}

const (
	defaultFailThreshold = 5
	defaultCooldown      = 10 * time.Second
)

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

const checkFunction = "check"

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
	// Marshal before admit so a malformed Request doesn't strand a
	// half-open breaker whose single probe slot was already claimed.
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
		e.state = breakerHalfOpen
		return nil
	case breakerHalfOpen:
		return ErrCircuitOpen
	}
	panic(fmt.Sprintf("seed evaluator: invalid breaker state %d", e.state))
}

func (e *seedEvaluator) recordFailure() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state == breakerHalfOpen {
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
