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

func withChainSnapshot(ctx context.Context, chain []string) context.Context {
	return context.WithValue(ctx, chainKey{}, callChain(slices.Clone(chain)))
}

func chainSnapshot(ctx context.Context) []string {
	existing, _ := ctx.Value(chainKey{}).(callChain)
	return slices.Clone(existing)
}

func chainForForward(ctx context.Context, current string) []string {
	chain := chainSnapshot(ctx)
	if len(chain) > 0 && chain[len(chain)-1] == current {
		return slices.Clone(chain[:len(chain)-1])
	}
	return chain
}

func chainContains(ctx context.Context, hash string) bool {
	existing, _ := ctx.Value(chainKey{}).(callChain)
	return slices.Contains(existing, hash)
}
