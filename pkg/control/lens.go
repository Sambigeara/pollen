// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
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

// authoriseOwnership rejects callers the cluster visibility rule does
// not let act on publisher's resources. The rule lives in view.Permits;
// this is the gate that funnels every mutation through it. Callers
// resolve the single authority that owns the resource being acted on
// first: an exposed service has one local owner, and a publication
// unpublish only ever tombstones this node's own (authority, key)
// register, so there is exactly one publisher to authorise against.
func (s *Service) authoriseOwnership(ctx context.Context, snap state.Snapshot, publisher types.PeerKey) error {
	if view.Permits(s.callerPrincipal(ctx), publisher, snap) {
		return nil
	}
	return status.Error(codes.PermissionDenied, "caller is not the resource publisher")
}

// unpublishKind selects which publication register an unpublish
// authorises against. Centralising the selection is the whole point of
// authoriseUnpublish: an open-coded per-call-site predicate choice is
// the one thing that diverged across the handlers and produced a
// cross-tenant ownership defect.
type unpublishKind int

const (
	unpublishWorkload unpublishKind = iota
	unpublishStatic
	unpublishBlob
)

// authoriseUnpublish is the single ownership chokepoint for the
// non-presigned unpublish handlers. When the local node is the
// publisher of id under kind, the caller must be that publisher or an
// admin. A non-locally-published id is left to the self-scoping store
// mutation, which only ever tombstones this node's own (authority,
// key) register, so a non-owner is a harmless no-op.
func (s *Service) authoriseUnpublish(ctx context.Context, snap state.Snapshot, kind unpublishKind, id string) error {
	var published bool
	switch kind {
	case unpublishWorkload:
		published = snap.LocalPublishesWorkload(id, s.localPeerKey())
	case unpublishStatic:
		published = snap.LocalPublishesStatic(id, s.localPeerKey())
	case unpublishBlob:
		published = snap.LocalPublishesBlob(id, s.localPeerKey())
	}
	if !published {
		return nil
	}
	return s.authoriseOwnership(ctx, snap, s.localPeerKey())
}
