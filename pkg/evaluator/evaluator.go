// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import (
	"context"
	"errors"
)

// Evaluator implementations must be safe for concurrent use.
type Evaluator interface {
	Allow(ctx context.Context, req Request) (Decision, error)
}

// Cacheable is an optional trait. Router.Allow skips the decision cache
// for evaluators returning false, avoiding the per-call JSON-marshal
// cost for trivially-fast evaluators (the cache only earns its keep
// when Allow is expensive — e.g. a seed-backed PDP).
type Cacheable interface {
	Cacheable() bool
}

// ErrDenied is the sentinel for denied requests. Callers typically map
// to codes.PermissionDenied at their RPC boundary.
var ErrDenied = errors.New("authz denied")

// DeniedError wraps ErrDenied; every denial satisfies
// errors.Is(err, ErrDenied).
type DeniedError struct {
	Reason string
}

func (e *DeniedError) Error() string {
	if e.Reason == "" {
		return "authz denied"
	}
	return "authz denied: " + e.Reason
}

func (e *DeniedError) Is(target error) bool {
	return target == ErrDenied
}
