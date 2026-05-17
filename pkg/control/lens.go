// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/view"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// callerPrincipal resolves the single authority a read, scoping or
// ownership decision is made against. The interceptor (injectCaller) is
// the primary source. The unix-socket daemon-self fall-through that
// predates the interceptor is kept for the read path; the gate-path
// callerGrant deliberately refuses that fallback so the wire-mode
// security boundary stays at one edge. A fully unidentified caller
// resolves to the default-deny zero Principal, so a misconfigured daemon
// never serves an admin view.
func (s *Service) callerPrincipal(ctx context.Context) identity.Principal {
	if c, ok := auth.CallerFromContext(ctx); ok && c.Valid() {
		return c
	}
	if s.creds != nil {
		return identity.PrincipalFromGrant(s.creds.Grant())
	}
	return identity.Principal{}
}

// operatorRequest reports whether the caller sees the serving node as
// its own: a cluster admin, or the daemon acting on its own behalf over
// the local transport (unix socket / in-process). These callers get the
// serving node's own summary, telemetry and credential; a wire tenant
// gets the projected view. Operator-ness is keyed on the request
// transport, not on grant-subject equality with the serving node, so a
// wire caller can never inherit the operator view by key coincidence.
func (s *Service) operatorRequest(ctx context.Context, lens view.Lens) bool {
	return lens.Admin() || isLocalCallerCtx(ctx)
}

// authoriseOwnership rejects non-admin callers who are not the
// resource's publisher. Admins bypass the check.
func (s *Service) authoriseOwnership(ctx context.Context, publisher types.PeerKey) error {
	if s.callerPrincipal(ctx).Permits(publisher) {
		return nil
	}
	return status.Error(codes.PermissionDenied, "caller is not the resource publisher")
}
