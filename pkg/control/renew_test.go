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
	// The serving node is root, the tenant's immediate issuer, so renewal
	// re-mints under the same parent and adds no ancestry.
	svc.creds = identity.NewCredentials(rootPub, rootPriv, rootGrant)
	resp, err := svc.RenewGrant(callerCtx(renewable), &controlv1.RenewGrantRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.GetGrant())
}

// TestRenewGrant_RefusesReparentingOutOfSubtree pins revocation scope: a
// tenant delegated under workspace-admin A must not be able to renew
// against a sibling workspace-admin B, which would re-parent it under B and
// drop A's authority to revoke it.
func TestRenewGrant_RefusesReparentingOutOfSubtree(t *testing.T) {
	rootPub, rootPriv := ed25519Pair(t)
	rootGrant := issuePrincipalGrant(t, rootPriv, nil, rootPub, identity.FullCapabilities())

	wsAPub, wsAPriv := ed25519Pair(t)
	wsAGrant := issuePrincipalGrant(t, rootPriv, rootGrant, wsAPub, identity.WorkspaceCapabilities())
	now := time.Now()
	tenantPub, _ := ed25519Pair(t)
	tenant, err := identity.IssueGrant(wsAPriv, wsAGrant, tenantPub,
		identity.LeafCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Minute), now.Add(time.Hour), false)
	require.NoError(t, err)

	// Serving node is sibling workspace-admin B: a delegating peer, but
	// neither the tenant's issuer nor an ancestor of it.
	wsBPub, wsBPriv := ed25519Pair(t)
	wsBGrant := issuePrincipalGrant(t, rootPriv, rootGrant, wsBPub, identity.WorkspaceCapabilities())

	svc := newAuthorityService(t, &stubMembership{rootPriv: wsBPriv}, nil, nil)
	svc.creds = identity.NewCredentials(rootPub, wsBPriv, wsBGrant)
	_, err = svc.RenewGrant(callerCtx(tenant), &controlv1.RenewGrantRequest{})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "re-parenting")
}

// TestRenewGrant_AllowsBeneathIssuer proves the scope is not over-tight: a
// node beneath the tenant's issuer may renew it, since that only adds
// ancestors and preserves the issuer's authority, so renewal works without
// the original issuer online.
func TestRenewGrant_AllowsBeneathIssuer(t *testing.T) {
	rootPub, rootPriv := ed25519Pair(t)
	rootGrant := issuePrincipalGrant(t, rootPriv, nil, rootPub, identity.FullCapabilities())

	wsPub, wsPriv := ed25519Pair(t)
	wsGrant := issuePrincipalGrant(t, rootPriv, rootGrant, wsPub, identity.WorkspaceCapabilities())

	now := time.Now()
	tenantPub, _ := ed25519Pair(t)
	tenant, err := identity.IssueGrant(wsPriv, wsGrant, tenantPub,
		identity.LeafCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Minute), now.Add(time.Hour), false)
	require.NoError(t, err)

	// Serving node is a sub-admin delegated under the tenant's issuer ws, so
	// the issuer ws appears in the serving node's chain.
	subPub, subPriv := ed25519Pair(t)
	subGrant := issuePrincipalGrant(t, wsPriv, wsGrant, subPub, identity.WorkspaceCapabilities())

	svc := newAuthorityService(t, &stubMembership{rootPriv: subPriv}, nil, nil)
	svc.creds = identity.NewCredentials(rootPub, subPriv, subGrant)
	resp, err := svc.RenewGrant(callerCtx(tenant), &controlv1.RenewGrantRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.GetGrant())
}
