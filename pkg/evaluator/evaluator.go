// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import (
	"context"
	"errors"
)

type Evaluator interface {
	Allow(ctx context.Context, req Request) (Decision, error)
}

// Cacheable is an optional trait. Router.Allow skips the decision cache
// for evaluators returning false, avoiding the per-call JSON-marshal
// cost when Allow is cheaper than the cache key itself.
type Cacheable interface {
	Cacheable() bool
}

var ErrDenied = errors.New("authz denied")

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
