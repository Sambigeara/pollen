// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

// AncestorIn reports whether x is a delegation ancestor of y by
// scanning y's embedded chain. The visibility rule uses this for
// "does the lens stand above the publisher in the delegation tree".
func AncestorIn(x types.PeerKey, y *identityv1.Grant) bool {
	for _, g := range y.GetChain() {
		if types.PeerKeyFromBytes(g.GetClaims().GetSubjectPub()) == x {
			return true
		}
	}
	return false
}

// WorkspaceOf returns the workspace identity of grant: the subject_pub
// of the nearest workspace-admin in its chain (or the grant itself if
// it is one). Two grants share a workspace iff their WorkspaceOf
// matches and is non-zero. A grant with no workspace-admin in its
// chain has the zero PeerKey as its workspace, which never matches.
func WorkspaceOf(grant *identityv1.Grant) types.PeerKey {
	if grant.GetClaims().GetCapabilities().GetIsWorkspaceAdmin() {
		return types.PeerKeyFromBytes(grant.GetClaims().GetSubjectPub())
	}
	// Chain runs leaf-to-root: the nearest workspace-admin is the
	// first entry whose capabilities carry the bit.
	for _, g := range grant.GetChain() {
		if g.GetClaims().GetCapabilities().GetIsWorkspaceAdmin() {
			return types.PeerKeyFromBytes(g.GetClaims().GetSubjectPub())
		}
	}
	return types.PeerKey{}
}

// IsInfrastructure reports whether grant marks an operator-run shared
// node: one that may relay for any mesh member and is visible as
// infrastructure. Set explicitly at issuance, never inferred from admit
// or delegate authority.
func IsInfrastructure(grant *identityv1.Grant) bool {
	return grant.GetClaims().GetCapabilities().GetIsInfrastructure()
}

// MayRelay reports whether a node holding self should forward relayed
// traffic on behalf of its immediately-upstream peer holding upstream.
// Infrastructure, cluster admins and nodes outside any workspace relay
// for any member. A tenant-confined node (inside a workspace, without
// admit or infrastructure authority) relays only within its own
// workspace or delegation line, so a sibling tenant's traffic stays out
// of its relay fabric even when laundered through infrastructure.
// Enforced at every hop against the cryptographically-verified previous
// hop, the per-hop check composes into transitive tenant isolation.
func MayRelay(self, upstream *identityv1.Grant) bool {
	if self == nil {
		return false
	}
	sw := WorkspaceOf(self)
	confined := sw != (types.PeerKey{}) && !self.GetClaims().GetCapabilities().GetCanAdmit() && !IsInfrastructure(self)
	if !confined {
		return true
	}
	if upstream == nil {
		return false
	}
	if sw == WorkspaceOf(upstream) {
		return true
	}
	selfKey := types.PeerKeyFromBytes(self.GetClaims().GetSubjectPub())
	upKey := types.PeerKeyFromBytes(upstream.GetClaims().GetSubjectPub())
	return AncestorIn(selfKey, upstream) || AncestorIn(upKey, self)
}
