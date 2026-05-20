// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"crypto/ed25519"
	"fmt"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

// Principal is the resolved authority behind a request: who the subject
// is, what their grant lets them do, their budget, and the horizon past
// which the grant no longer vouches for them. It is the single resolved
// identity the rest of the system reads; control scoping, the read lens
// and the admission pipeline all project from one of these rather than
// each re-interpreting a raw Grant. It is a derived aggregate, not a
// wire type: the Grant is the gossiped record.
//
// The zero Principal is the unidentified caller. It is not valid and
// permits nothing, so a request that fails to resolve a caller defaults
// to leaking nothing rather than everything.
type Principal struct {
	GrantDeadline time.Time
	Grant         *identityv1.Grant
	Capabilities  *identityv1.Capabilities
	Budget        *identityv1.Budget
	SubjectPub    ed25519.PublicKey
	valid         bool
}

// ResolvePrincipal validates a grant and projects it to a Principal.
// The grant must be OK (chain to root, within horizon, not denied);
// otherwise the subject has no current authority.
func ResolvePrincipal(
	grant *identityv1.Grant,
	rootPub []byte,
	now time.Time,
	denied DenyChecker,
) (Principal, error) {
	chk := CheckGrant(grant, rootPub, now, nil, denied)
	if !chk.Status.Valid() {
		return Principal{}, fmt.Errorf("%w: grant %s: %s", ErrGrantInvalid, chk.Status, chk.Reason)
	}
	gc := grant.GetClaims()
	return Principal{
		SubjectPub:    ed25519.PublicKey(gc.GetSubjectPub()),
		Grant:         grant,
		Capabilities:  gc.GetCapabilities(),
		Budget:        gc.GetBudget(),
		GrantDeadline: chk.GrantDeadline,
		valid:         true,
	}, nil
}

// PrincipalFromGrant projects an already-trusted grant to a Principal
// without re-walking the chain. It is for callers whose grant was
// cryptographically verified upstream (the mTLS handshake verifies the
// Session and its embedded grant before the request reaches a handler),
// so the only question left is the field projection. A nil grant or one
// without claims yields the default-deny zero Principal.
func PrincipalFromGrant(grant *identityv1.Grant) Principal {
	gc := grant.GetClaims()
	if gc == nil {
		return Principal{}
	}
	var deadline time.Time
	if d := gc.GetGrantDeadlineUnix(); d > 0 {
		deadline = time.Unix(d, 0)
	}
	return Principal{
		SubjectPub:    ed25519.PublicKey(gc.GetSubjectPub()),
		Grant:         grant,
		Capabilities:  gc.GetCapabilities(),
		Budget:        gc.GetBudget(),
		GrantDeadline: deadline,
		valid:         true,
	}
}

// Principal projects an already-verified session to its Principal
// without re-walking the grant chain.
func (v *VerifiedSession) Principal() Principal {
	return Principal{
		SubjectPub:    v.SubjectPub,
		Grant:         v.Grant,
		Capabilities:  v.Capabilities,
		Budget:        v.Budget,
		GrantDeadline: v.GrantDeadline,
		valid:         true,
	}
}

// Valid reports whether this Principal resolved to a real authority. The
// zero Principal is not valid.
func (p Principal) Valid() bool { return p.valid }

// Subject is the principal's own key. Resources it published are keyed
// by this, and it is the identity a tenant is scoped to.
func (p Principal) Subject() types.PeerKey {
	return types.PeerKeyFromBytes(p.SubjectPub)
}

// Admin reports cluster-admin authority (the can_admit capability):
// the principal sees and may act across the whole cluster.
func (p Principal) Admin() bool {
	return p.Capabilities.GetCanAdmit()
}

// CanDelegate reports whether the principal may issue child grants.
func (p Principal) CanDelegate() bool {
	return p.Capabilities.GetCanDelegate()
}

// IsWorkspaceAdmin reports whether the principal founds a workspace:
// a delegation-tree boundary inside which member grants see each other.
// A workspace-admin sees its chain ancestors and its own subtree
// (transparent through nested workspaces) without reaching laterally
// across the cluster. The full visibility rule lives in view.Permits.
func (p Principal) IsWorkspaceAdmin() bool {
	return p.Capabilities.GetIsWorkspaceAdmin()
}
