// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/view"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scopeGrant resolves the Grant a read/scoping decision is made
// against. Unlike the gate-path callerGrant (which deliberately refuses
// a daemon-self fallback so the wire security boundary stays at one
// edge), the read path keeps the unix-socket daemon-self fall-through
// that predates the caller interceptor. A fully unidentified caller
// resolves to nil, which LensFor turns into the default-deny zero lens,
// so a misconfigured daemon never serves an admin view.
func (s *Service) scopeGrant(ctx context.Context) *identityv1.Grant {
	if caller, ok := auth.RPCCallerFromContext(ctx); ok && caller.Grant() != nil {
		return caller.Grant()
	}
	if s.creds != nil {
		return s.creds.Grant()
	}
	return nil
}

// lens derives the read and ownership authority of an incoming control
// RPC from the caller's verified Grant, never from the serving node's
// own identity.
func (s *Service) lens(ctx context.Context) view.Lens {
	return view.LensFor(s.scopeGrant(ctx))
}

// operatorView reports whether the lens sees the serving node as its
// own: an admin, or the daemon acting on its own behalf over the unix
// socket. These callers get the serving node's own summary, telemetry
// and credential; a wire tenant gets the projected view instead.
func operatorView(lens view.Lens, snap state.Snapshot) bool {
	return lens.Admin() || lens.Subject() == snap.LocalID
}

// authoriseOwnership rejects non-admin callers who are not the
// resource's publisher. Admins bypass the check.
func (s *Service) authoriseOwnership(ctx context.Context, publisher types.PeerKey) error {
	if s.lens(ctx).Permits(publisher) {
		return nil
	}
	return status.Error(codes.PermissionDenied, "caller is not the resource publisher")
}
