// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"errors"
	"slices"
)

// ErrCycle is returned when a workload appears twice in the local call
// chain — a self-recursive nested call that would deadlock on the
// instance pool. Non-retryable.
var ErrCycle = errors.New("placement: call cycle detected")

type callChain []string

type chainKey struct{}

func withChain(ctx context.Context, hash string) context.Context {
	existing, _ := ctx.Value(chainKey{}).(callChain)
	return context.WithValue(ctx, chainKey{}, append(slices.Clone(existing), hash))
}

func chainContains(ctx context.Context, hash string) bool {
	existing, _ := ctx.Value(chainKey{}).(callChain)
	return slices.Contains(existing, hash)
}
