// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"errors"
	"sync"
)

// ErrAtCapacity is returned by the gate when the workload's concurrency
// cap is reached. Maps to gRPC ResourceExhausted at the API boundary;
// internally signals callers to propagate backpressure upstream rather
// than queue indefinitely.
//
// Composes with ErrOverloaded: the gate caps concurrency/CPU; admission
// caps node memory. Either can reject a call independently.
var ErrAtCapacity = errors.New("workload gate at capacity")

// callKey identifies an admission unit: a specific exported function within
// a WASM module. The module hash is the replication unit (placement, CAS,
// warm instance); the function is the cost unit (admission, latency, SLO).
// Per-function gating stops a slow function in a module from head-of-line-
// blocking fast functions that share the same binary.
type callKey struct {
	Hash     string
	Function string
}

// workloadGate bounds in-flight invocations for a single (hash, function)
// on this node. Acquire is non-blocking: at-capacity returns ErrAtCapacity
// immediately. Bounded admission (running calls, no queue) IS the
// concurrency bound — at most cap simultaneous calls per (hash, function).
type workloadGate struct {
	cap      int
	inflight int
	mu       sync.Mutex
}

func newWorkloadGate(initial int) *workloadGate {
	if initial < 1 {
		initial = 1
	}
	return &workloadGate{cap: initial}
}

// SetCap adjusts the gate's concurrency ceiling. Honoured immediately for
// new acquires. In-flight calls are unaffected — releases just bring the
// inflight count down past the new (lower) cap before the next acquire
// admits anyone.
func (g *workloadGate) SetCap(n int) {
	if n < 1 {
		n = 1
	}
	g.mu.Lock()
	g.cap = n
	g.mu.Unlock()
}

// acquire admits a call if a slot is free, else returns ErrAtCapacity.
func (g *workloadGate) acquire() (release func(), err error) {
	g.mu.Lock()
	if g.inflight >= g.cap {
		g.mu.Unlock()
		return nil, ErrAtCapacity
	}
	g.inflight++
	g.mu.Unlock()
	return g.release, nil
}

func (g *workloadGate) release() {
	g.mu.Lock()
	g.inflight--
	g.mu.Unlock()
}

// gateRegistry holds per-(hash, function) workloadGates. Each module hash
// has a single size target — set by the reconciler from cluster-wide
// signals — and every function of that module gets its own gate sized
// to that target. New gates materialise on first acquire, so we don't
// need to pre-enumerate a module's exported functions.
type gateRegistry struct {
	gates       map[callKey]*workloadGate
	hashSizes   map[string]int
	defaultSize int
	mu          sync.Mutex
}

func newGateRegistry(defaultSize int) *gateRegistry {
	if defaultSize < 1 {
		defaultSize = 1
	}
	return &gateRegistry{
		gates:       make(map[callKey]*workloadGate),
		hashSizes:   make(map[string]int),
		defaultSize: defaultSize,
	}
}

func (r *gateRegistry) acquire(key callKey) (func(), error) {
	r.mu.Lock()
	g, ok := r.gates[key]
	if !ok {
		size, hasSize := r.hashSizes[key.Hash]
		if !hasSize {
			size = r.defaultSize
		}
		g = newWorkloadGate(size)
		r.gates[key] = g
	}
	r.mu.Unlock()
	return g.acquire()
}

// SetHashSize records a per-module concurrency target and resizes every
// existing function gate for that module. Functions not yet observed
// inherit the stored size when their gate is first created.
func (r *gateRegistry) SetHashSize(hash string, n int) {
	if n < 1 {
		n = 1
	}
	r.mu.Lock()
	r.hashSizes[hash] = n
	affected := make([]*workloadGate, 0)
	for key, g := range r.gates {
		if key.Hash == hash {
			affected = append(affected, g)
		}
	}
	r.mu.Unlock()
	for _, g := range affected {
		g.SetCap(n)
	}
}

// Clear drops every gate belonging to the given module hash (called on
// unseed).
func (r *gateRegistry) Clear(hash string) {
	r.mu.Lock()
	for key := range r.gates {
		if key.Hash == hash {
			delete(r.gates, key)
		}
	}
	delete(r.hashSizes, hash)
	r.mu.Unlock()
}
