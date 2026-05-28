// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"testing"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestRenewGrant_RefusesNonRenewable pins the server-side contract that
// a `pln invite --expire-after` grant is a hard cap: even if the daemon
// or wire CLI somehow reached this handler, the issuer must refuse to
// re-mint it. Without this, the deadline is whatever the next renewal
// happens to set (DefaultGrantDeadlineTTL on success), and the
// non-renewable flag on the claim is only honoured on the client.
func TestRenewGrant_RefusesNonRenewable(t *testing.T) {
	rootPub, rootPriv := ed25519Pair(t)
	rootGrant := issuePrincipalGrant(t, rootPriv, nil, rootPub, identity.FullCapabilities())

	now := time.Now()
	tenantPub, _ := ed25519Pair(t)
	nonRenewable, err := identity.IssueGrant(rootPriv, rootGrant, tenantPub,
		identity.PublisherCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Minute), now.Add(time.Hour), true)
	require.NoError(t, err)

	svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, nil, nil)
	_, err = svc.RenewGrant(callerCtx(nonRenewable), &controlv1.RenewGrantRequest{})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "non-renewable")
}

// TestRenewGrant_AllowsRenewableTenant proves the refusal is scoped: a
// regular finite-horizon delegated grant (no non_renewable flag) still
// passes through, so the renewal loop's happy path keeps working.
func TestRenewGrant_AllowsRenewableTenant(t *testing.T) {
	rootPub, rootPriv := ed25519Pair(t)
	rootGrant := issuePrincipalGrant(t, rootPriv, nil, rootPub, identity.FullCapabilities())

	now := time.Now()
	tenantPub, _ := ed25519Pair(t)
	renewable, err := identity.IssueGrant(rootPriv, rootGrant, tenantPub,
		identity.PublisherCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Minute), now.Add(time.Hour), false)
	require.NoError(t, err)

	svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, nil, nil)
	resp, err := svc.RenewGrant(callerCtx(renewable), &controlv1.RenewGrantRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.GetGrant())
}
