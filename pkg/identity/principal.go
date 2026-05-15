// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"crypto/ed25519"
	"fmt"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
)

// Principal is the resolved view of a subject's authority: who they
// are, what their grant lets them do, their budget, and the horizon
// past which the grant no longer vouches for them. It is a derived
// aggregate, not a wire type — the Grant is the gossiped record.
type Principal struct {
	GrantDeadline time.Time
	Capabilities  *identityv1.Capabilities
	Budget        *identityv1.Budget
	SubjectPub    ed25519.PublicKey
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
		Capabilities:  gc.GetCapabilities(),
		Budget:        gc.GetBudget(),
		GrantDeadline: chk.GrantDeadline,
	}, nil
}

// Principal projects an already-verified session to its Principal
// without re-walking the grant chain.
func (v *VerifiedSession) Principal() Principal {
	return Principal{
		SubjectPub:    v.SubjectPub,
		Capabilities:  v.Capabilities,
		Budget:        v.Budget,
		GrantDeadline: v.GrantDeadline,
	}
}
