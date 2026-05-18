// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func kp(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

// A holder of any delegating grant owns that grant's subject key, so it
// can sign a child grant proto directly with signGrant, bypassing the
// issuance-time applyParent guard entirely. These tests prove the
// authority boundary is verification, not issuance: such a forged child
// must fail CheckGrant even though every signature in it is valid and
// the chain anchors at the real cluster root.

func TestForgedCapabilityEscalationRejected(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := kp(t)
	dPub, dPriv := kp(t)
	attackerPub, _ := kp(t)

	// Root delegates a narrow grant to D: may publish sites, may
	// delegate, no admit.
	dGrant, err := IssueGrant(rootPriv, nil, dPub,
		&identityv1.Capabilities{CanDelegate: true, MaxDepth: 5, Publish: &identityv1.PublishCapability{Sites: true}},
		&identityv1.Budget{}, now.Add(-time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)

	// D forges a wildly escalated child by signing the proto itself.
	forged, err := signGrant(dPriv, []*identityv1.Grant{dGrant}, attackerPub,
		&identityv1.Capabilities{
			CanAdmit:    true,
			CanDelegate: true,
			MaxDepth:    255,
			Publish:     &identityv1.PublishCapability{Functions: true, Blobs: true, Sites: true, Services: true},
		},
		&identityv1.Budget{}, now, now.Add(3650*24*time.Hour))
	require.NoError(t, err)

	chk := CheckGrant(forged, rootPub, now, nil, nil)
	require.Equal(t, GrantStatusInvalidChain, chk.Status, chk.Reason)
	require.Contains(t, chk.Reason, "escalation")
}

func TestForgedHorizonExtensionRejected(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := kp(t)
	dPub, dPriv := kp(t)
	attackerPub, _ := kp(t)

	dCaps := &identityv1.Capabilities{CanDelegate: true, MaxDepth: 5, Publish: &identityv1.PublishCapability{Sites: true}}
	dGrant, err := IssueGrant(rootPriv, nil, dPub, dCaps, &identityv1.Budget{},
		now.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)

	// Same capabilities, but a horizon a decade past the parent's.
	forged, err := signGrant(dPriv, []*identityv1.Grant{dGrant}, attackerPub,
		&identityv1.Capabilities{CanDelegate: true, MaxDepth: 5, Publish: &identityv1.PublishCapability{Sites: true}},
		&identityv1.Budget{}, now, now.Add(3650*24*time.Hour))
	require.NoError(t, err)

	chk := CheckGrant(forged, rootPub, now, nil, nil)
	require.Equal(t, GrantStatusInvalidChain, chk.Status, chk.Reason)
	require.Contains(t, chk.Reason, "horizon")
}

func TestForgedAttributeEscalationRejected(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := kp(t)
	dPub, dPriv := kp(t)
	attackerPub, _ := kp(t)

	dGrant, err := IssueGrant(rootPriv, nil, dPub,
		&identityv1.Capabilities{CanDelegate: true, MaxDepth: 5},
		&identityv1.Budget{}, now.Add(-time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)

	attrs, err := structpb.NewStruct(map[string]any{"role": "admin"})
	require.NoError(t, err)
	forged, err := signGrant(dPriv, []*identityv1.Grant{dGrant}, attackerPub,
		&identityv1.Capabilities{CanDelegate: true, MaxDepth: 5, Attributes: attrs},
		&identityv1.Budget{}, now, now.Add(24*time.Hour))
	require.NoError(t, err)

	chk := CheckGrant(forged, rootPub, now, nil, nil)
	require.Equal(t, GrantStatusInvalidChain, chk.Status, chk.Reason)
}

func TestLegitimateDeepChainStillVerifies(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := kp(t)
	aPub, aPriv := kp(t)
	bPub, bPriv := kp(t)
	cPub, _ := kp(t)

	aGrant, err := IssueGrant(rootPriv, nil, aPub, FullCapabilities(), UnlimitedBudget(),
		now.Add(-time.Hour), time.Time{})
	require.NoError(t, err)
	bGrant, err := IssueGrant(aPriv, []*identityv1.Grant{aGrant}, bPub,
		&identityv1.Capabilities{CanDelegate: true, MaxDepth: 10, Publish: &identityv1.PublishCapability{Sites: true}},
		&identityv1.Budget{}, now.Add(-time.Minute), now.Add(30*24*time.Hour))
	require.NoError(t, err)
	cGrant, err := IssueGrant(bPriv, []*identityv1.Grant{bGrant, aGrant}, cPub,
		&identityv1.Capabilities{Publish: &identityv1.PublishCapability{Sites: true}},
		&identityv1.Budget{}, now, now.Add(7*24*time.Hour))
	require.NoError(t, err)

	chk := CheckGrant(cGrant, rootPub, now, nil, nil)
	require.Equal(t, GrantStatusOK, chk.Status, chk.Reason)
}

// TestForgedDepthBudgetRejected proves max_depth is a real subtree
// budget enforced at verification, not just a monotone scalar at
// issuance: a holder of a max_depth=1 grant can issue (issuance only
// checks child<=parent), but the resulting over-deep chain fails
// CheckGrant.
func TestForgedDepthBudgetRejected(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := kp(t)
	mPub, mPriv := kp(t)

	// M may root a subtree at most one delegation hop deep.
	mGrant, err := IssueGrant(rootPriv, nil, mPub,
		&identityv1.Capabilities{CanDelegate: true, MaxDepth: 1, Publish: &identityv1.PublishCapability{Sites: true}},
		&identityv1.Budget{}, now.Add(-time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)

	// One hop below M: within budget, verifies.
	l1Pub, _ := kp(t)
	l1, err := IssueGrant(mPriv, []*identityv1.Grant{mGrant}, l1Pub,
		&identityv1.Capabilities{Publish: &identityv1.PublishCapability{Sites: true}},
		&identityv1.Budget{}, now, now.Add(24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, GrantStatusOK, CheckGrant(l1, rootPub, now, nil, nil).Status)

	// Issuance allows N (monotone 1<=1), but N->L2 puts M two hops deep.
	nPub, nPriv := kp(t)
	nGrant, err := IssueGrant(mPriv, []*identityv1.Grant{mGrant}, nPub,
		&identityv1.Capabilities{CanDelegate: true, MaxDepth: 1, Publish: &identityv1.PublishCapability{Sites: true}},
		&identityv1.Budget{}, now, now.Add(24*time.Hour))
	require.NoError(t, err)
	l2Pub, _ := kp(t)
	l2, err := IssueGrant(nPriv, []*identityv1.Grant{nGrant, mGrant}, l2Pub,
		&identityv1.Capabilities{Publish: &identityv1.PublishCapability{Sites: true}},
		&identityv1.Budget{}, now, now.Add(24*time.Hour))
	require.NoError(t, err)

	chk := CheckGrant(l2, rootPub, now, nil, nil)
	require.Equal(t, GrantStatusInvalidChain, chk.Status, chk.Reason)
	require.Contains(t, chk.Reason, "max_depth")
}

// TestForgedBudgetRejected proves a per-Principal count budget is a real
// chain constraint enforced at verification, not just an issuance
// courtesy: a holder of a budget-limited grant can sign a child proto
// with a fatter budget directly (bypassing applyParent), but the chain
// fails CheckGrant. Issuance also rejects the same attempt early.
func TestForgedBudgetRejected(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := kp(t)
	mPub, mPriv := kp(t)

	// M may publish at most 5 functions.
	mGrant, err := IssueGrant(rootPriv, nil, mPub,
		&identityv1.Capabilities{CanDelegate: true, MaxDepth: 5, Publish: &identityv1.PublishCapability{Functions: true}},
		&identityv1.Budget{MaxFunctions: 5}, now.Add(-time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)

	childCaps := &identityv1.Capabilities{Publish: &identityv1.PublishCapability{Functions: true}}

	// Within budget: issues and verifies.
	okPub, _ := kp(t)
	ok, err := IssueGrant(mPriv, []*identityv1.Grant{mGrant}, okPub,
		childCaps, &identityv1.Budget{MaxFunctions: 3}, now, now.Add(24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, GrantStatusOK, CheckGrant(ok, rootPub, now, nil, nil).Status)

	// Issuance rejects a child that asks for more than M holds.
	fatPub, _ := kp(t)
	_, err = IssueGrant(mPriv, []*identityv1.Grant{mGrant}, fatPub,
		childCaps, &identityv1.Budget{MaxFunctions: 100}, now, now.Add(24*time.Hour))
	require.ErrorContains(t, err, "child budget exceeds parent: functions")

	// Forge it directly with signGrant, bypassing applyParent. Every
	// signature is valid and the chain anchors at root, but verification
	// is the authority boundary and rejects the budget escalation.
	forged, err := signGrant(mPriv, []*identityv1.Grant{mGrant}, fatPub,
		childCaps, &identityv1.Budget{MaxFunctions: 100}, now, now.Add(24*time.Hour))
	require.NoError(t, err)
	chk := CheckGrant(forged, rootPub, now, nil, nil)
	require.Equal(t, GrantStatusInvalidChain, chk.Status, chk.Reason)
	require.Contains(t, chk.Reason, "child budget exceeds parent")
}

func TestOverDeepChainRejected(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := kp(t)
	subPub, subPriv := kp(t)

	g0, err := IssueGrant(rootPriv, nil, subPub, FullCapabilities(), UnlimitedBudget(),
		now.Add(-time.Hour), time.Time{})
	require.NoError(t, err)

	leaf, err := signGrant(subPriv, []*identityv1.Grant{g0}, subPub,
		FullCapabilities(), UnlimitedBudget(), now, time.Time{})
	require.NoError(t, err)
	filler := make([]*identityv1.Grant, maxGrantChainDepth+1)
	for i := range filler {
		filler[i] = &identityv1.Grant{Claims: g0.GetClaims(), Signature: g0.GetSignature()}
	}
	leaf.Chain = filler

	chk := CheckGrant(leaf, rootPub, now, nil, nil)
	require.Equal(t, GrantStatusInvalidChain, chk.Status, chk.Reason)
}
