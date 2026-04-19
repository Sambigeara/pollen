// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"errors"
	"slices"
)

// ErrCycle is returned when a workload hash appears twice in a local call
// chain — a signal that nested pollen_request calls would self-deadlock on
// the instance pool. Callers should treat it as a non-retryable application
// error.
var ErrCycle = errors.New("placement: call cycle detected")

// callChain is the set of workload hashes currently active in a nested
// local call chain. Carried via context to detect self-recursive cycles
// that would deadlock on per-workload instance pools.
type callChain []string

type chainKey struct{}

// withChain returns a ctx that includes hash in the current call chain.
func withChain(ctx context.Context, hash string) context.Context {
	existing, _ := ctx.Value(chainKey{}).(callChain)
	return context.WithValue(ctx, chainKey{}, append(slices.Clone(existing), hash))
}

// chainContains reports whether hash is already active in ctx's call chain.
func chainContains(ctx context.Context, hash string) bool {
	existing, _ := ctx.Value(chainKey{}).(callChain)
	return slices.Contains(existing, hash)
}
