// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// viewScope captures the read-side authority of an incoming control
// RPC. Admin callers (can_admit) get the unfiltered cluster view;
// everyone else sees only resources where Publisher matches their
// grant's subject pub. valid distinguishes an "unidentified caller"
// (zero scope, deny everything) from a "real leaf with zero-pub"
// (impossible in practice, but the flag makes the deny explicit).
type viewScope struct {
	showAll bool
	valid   bool
	caller  types.PeerKey
}

func (v viewScope) permits(publisher types.PeerKey) bool {
	if !v.valid {
		return false
	}
	return v.showAll || publisher == v.caller
}

// viewScope derives a scope from the request context's RPCCaller, with
// a fall-through for daemon-self paths that pre-date the interceptor.
// Fully unidentified callers get an invalid leaf-scope (default-deny)
// so a misconfigured daemon never leaks admin views.
func (s *Service) viewScope(ctx context.Context) viewScope {
	if caller, ok := auth.RPCCallerFromContext(ctx); ok && caller.Grant() != nil {
		return viewScope{
			showAll: caller.CanAdmit(),
			valid:   true,
			caller:  caller.SubjectPub(),
		}
	}
	if s.creds != nil && s.creds.Grant() != nil {
		grant := s.creds.Grant()
		return viewScope{
			showAll: grant.GetClaims().GetCapabilities().GetCanAdmit(),
			valid:   true,
			caller:  types.PeerKeyFromBytes(grant.GetClaims().GetSubjectPub()),
		}
	}
	return viewScope{}
}

// authoriseOwnership rejects non-admin callers who aren't the resource's
// publisher. Admins (can_admit) bypass the check.
func (s *Service) authoriseOwnership(ctx context.Context, publisher types.PeerKey) error {
	scope := s.viewScope(ctx)
	if scope.permits(publisher) {
		return nil
	}
	return status.Error(codes.PermissionDenied, "caller is not the resource publisher")
}
