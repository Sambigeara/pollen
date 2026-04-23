// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import "context"

// AllowAll is the router's default fallback evaluator.
type AllowAll struct{}

func (AllowAll) Allow(_ context.Context, _ Request) (Decision, error) {
	return Decision{Decision: true}, nil
}

// Cacheable returns false: caching a "yes, always" decision costs more
// (JSON-marshal of the cache key) than it saves.
func (AllowAll) Cacheable() bool { return false }
