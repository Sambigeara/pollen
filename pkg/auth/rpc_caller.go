// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"

	"github.com/sambigeara/pollen/pkg/identity"
)

// The authenticated caller behind a control RPC is carried as a resolved
// identity.Principal. Handlers read it via CallerFromContext to make
// scoping, ownership and capability decisions against the caller's own
// authority rather than re-interpreting a raw Grant per call site.

type callerKey struct{}

func WithCaller(ctx context.Context, caller identity.Principal) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}

func CallerFromContext(ctx context.Context) (identity.Principal, bool) {
	c, ok := ctx.Value(callerKey{}).(identity.Principal)
	return c, ok
}
