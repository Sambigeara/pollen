// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

// Permits is the cluster's visibility rule. Returns true if lens may
// see resources owned by publisher. Mutation gating is a separate
// per-kind publish-cap check layered on top.
//
// Three universal clauses (admin shortcut, self, own subtree) plus two
// role-specific extras: a workspace-admin additionally sees its chain
// ancestors; any other workspaced grant additionally sees its workspace
// peers (see identity.WorkspaceOf). Subtree visibility is transparent
// through any nested workspaces beneath the lens.
func Permits(lens Lens, publisher types.PeerKey, snap state.Snapshot) bool {
	if !lens.Valid() {
		return false
	}
	if lens.Admin() {
		return true
	}
	if publisher == lens.Subject() {
		return true
	}
	pubGrant := snap.GrantFor(publisher[:])
	if pubGrant == nil {
		return false
	}
	if identity.AncestorIn(lens.Subject(), pubGrant) {
		return true
	}
	if lens.IsWorkspaceAdmin() {
		return identity.AncestorIn(publisher, lens.Grant)
	}
	lensWS := identity.WorkspaceOf(lens.Grant)
	if lensWS == (types.PeerKey{}) {
		return false
	}
	return identity.WorkspaceOf(pubGrant) == lensWS
}
