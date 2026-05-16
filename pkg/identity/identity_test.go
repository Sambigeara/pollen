// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/stretchr/testify/require"
)

func newKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

// chain builds a root grant (admin-signed, no horizon) and a delegated
// child grant for subject, returning the cluster root pub to verify
// against plus the subject key.
func chain(t *testing.T, now, childDeadline time.Time) (rootPub ed25519.PublicKey, child *identityv1.Grant, subPub ed25519.PublicKey, subPriv, rnPriv ed25519.PrivateKey) {
	t.Helper()
	adminPub, adminPriv := newKeyPair(t)
	rnPub, rnPrivK := newKeyPair(t)
	subPub, subPriv = newKeyPair(t)

	root, err := identity.IssueGrant(adminPriv, nil, rnPub, identity.FullCapabilities(), identity.UnlimitedBudget(), now.Add(-time.Hour), time.Time{})
	require.NoError(t, err)

	child, err = identity.IssueGrant(rnPrivK, []*identityv1.Grant{root}, subPub,
		identity.PublisherCapabilities(), &identityv1.Budget{MaxSites: 3},
		now.Add(-time.Minute), childDeadline)
	require.NoError(t, err)

	return adminPub, child, subPub, subPriv, rnPrivK
}

// TestSessionRenewalIsLocalAndOffline is the load-bearing property:
// once a node holds a grant and its key, it mints and re-mints
// sessions with no issuer and no mesh, indefinitely until the grant
// horizon.
func TestSessionRenewalIsLocalAndOffline(t *testing.T) {
	now := time.Now()
	rootPub, grant, _, subPriv, _ := chain(t, now, now.Add(30*24*time.Hour))

	s1, err := identity.MintSession(grant, subPriv, now, time.Hour)
	require.NoError(t, err)
	vs, err := identity.VerifySession(s1, rootPub, now, nil, nil)
	require.NoError(t, err)
	require.Equal(t, grant.GetClaims().GetSubjectPub(), []byte(vs.SubjectPub))

	// Past the session window: the old proof is rejected.
	later := now.Add(2 * time.Hour)
	_, err = identity.VerifySession(s1, rootPub, later, nil, nil)
	require.ErrorIs(t, err, identity.ErrSessionInvalid)
	require.ErrorContains(t, err, "session expired")

	// Re-mint locally from the held grant + key alone. No network.
	s2, err := identity.MintSession(grant, subPriv, later, time.Hour)
	require.NoError(t, err)
	_, err = identity.VerifySession(s2, rootPub, later, nil, nil)
	require.NoError(t, err)
}

func TestVerifySessionFailClosed(t *testing.T) {
	now := time.Now()

	t.Run("past grant deadline", func(t *testing.T) {
		rootPub, grant, _, subPriv, _ := chain(t, now, now.Add(time.Hour))
		s, err := identity.MintSession(grant, subPriv, now, time.Hour)
		require.NoError(t, err)
		_, err = identity.VerifySession(s, rootPub, now.Add(2*time.Hour), nil, nil)
		require.ErrorIs(t, err, identity.ErrSessionInvalid)
		require.ErrorContains(t, err, "expired")
	})

	t.Run("denied subject", func(t *testing.T) {
		rootPub, grant, subPub, subPriv, _ := chain(t, now, now.Add(30*24*time.Hour))
		s, err := identity.MintSession(grant, subPriv, now, time.Hour)
		require.NoError(t, err)
		denied := func(p []byte) bool { return string(p) == string(subPub) }
		_, err = identity.VerifySession(s, rootPub, now, nil, denied)
		require.ErrorContains(t, err, "revoked")
	})

	t.Run("denied chain ancestor", func(t *testing.T) {
		rootPub, grant, _, subPriv, rnPriv := chain(t, now, now.Add(30*24*time.Hour))
		rnPub := rnPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
		s, err := identity.MintSession(grant, subPriv, now, time.Hour)
		require.NoError(t, err)
		denied := func(p []byte) bool { return string(p) == string(rnPub) }
		_, err = identity.VerifySession(s, rootPub, now, nil, denied)
		require.ErrorContains(t, err, "revoked")
	})

	t.Run("wrong root", func(t *testing.T) {
		_, grant, _, subPriv, _ := chain(t, now, now.Add(30*24*time.Hour))
		other, _ := newKeyPair(t)
		s, err := identity.MintSession(grant, subPriv, now, time.Hour)
		require.NoError(t, err)
		_, err = identity.VerifySession(s, other, now, nil, nil)
		require.ErrorContains(t, err, "chain root mismatch")
	})

	t.Run("leaf binding mismatch", func(t *testing.T) {
		rootPub, grant, _, subPriv, _ := chain(t, now, now.Add(30*24*time.Hour))
		wrongLeaf, _ := newKeyPair(t)
		s, err := identity.MintSession(grant, subPriv, now, time.Hour)
		require.NoError(t, err)
		_, err = identity.VerifySession(s, rootPub, now, wrongLeaf, nil)
		require.ErrorContains(t, err, "subject does not match expected")
	})

	t.Run("tampered session signature", func(t *testing.T) {
		rootPub, grant, _, subPriv, _ := chain(t, now, now.Add(30*24*time.Hour))
		s, err := identity.MintSession(grant, subPriv, now, time.Hour)
		require.NoError(t, err)
		s.Signature[0] ^= 0xff
		_, err = identity.VerifySession(s, rootPub, now, nil, nil)
		require.ErrorContains(t, err, "signature invalid")
	})

	t.Run("not yet valid grant", func(t *testing.T) {
		adminPub, adminPriv := newKeyPair(t)
		subPub, subPriv := newKeyPair(t)
		grant, err := identity.IssueGrant(adminPriv, nil, subPub,
			identity.FullCapabilities(), identity.UnlimitedBudget(),
			now.Add(time.Hour), now.Add(48*time.Hour))
		require.NoError(t, err)
		s, err := identity.MintSession(grant, subPriv, now.Add(time.Hour), time.Hour)
		require.NoError(t, err)
		_, err = identity.VerifySession(s, adminPub, now, nil, nil)
		require.ErrorContains(t, err, "not yet valid")
	})
}

func TestIssueGrantChildCannotExceedParent(t *testing.T) {
	now := time.Now()
	_, adminPriv := newKeyPair(t)
	rnPub, rnPriv := newKeyPair(t)

	// Parent may publish sites only, no delegation downstream beyond it.
	parentCaps := &identityv1.Capabilities{
		CanDelegate: true,
		MaxDepth:    2,
		Publish:     &identityv1.PublishCapability{Sites: true},
	}
	parent, err := identity.IssueGrant(adminPriv, nil, rnPub, parentCaps, identity.UnlimitedBudget(), now.Add(-time.Hour), time.Time{})
	require.NoError(t, err)

	subPub, _ := newKeyPair(t)

	_, err = identity.IssueGrant(rnPriv, []*identityv1.Grant{parent}, subPub,
		&identityv1.Capabilities{Publish: &identityv1.PublishCapability{Functions: true}},
		identity.UnlimitedBudget(), now, now.Add(time.Hour))
	require.ErrorContains(t, err, "publish functions")

	_, err = identity.IssueGrant(rnPriv, []*identityv1.Grant{parent}, subPub,
		&identityv1.Capabilities{CanAdmit: true, Publish: &identityv1.PublishCapability{}},
		identity.UnlimitedBudget(), now, now.Add(time.Hour))
	require.ErrorContains(t, err, "CanAdmit")

	// Within bounds: allowed, and the horizon clamps to the parent's.
	ok, err := identity.IssueGrant(rnPriv, []*identityv1.Grant{parent}, subPub,
		&identityv1.Capabilities{Publish: &identityv1.PublishCapability{Sites: true}},
		identity.UnlimitedBudget(), now, now.Add(time.Hour))
	require.NoError(t, err)
	require.NotNil(t, ok)
}

func TestCheckGrantStatuses(t *testing.T) {
	now := time.Now()
	adminPub, adminPriv := newKeyPair(t)
	subPub, _ := newKeyPair(t)

	cases := []struct {
		name   string
		nb, gd time.Time
		root   ed25519.PublicKey
		denied identity.DenyChecker
		want   identity.GrantStatus
	}{
		{"ok", now.Add(-time.Hour), now.Add(24 * time.Hour), adminPub, nil, identity.GrantStatusOK},
		{"not yet valid", now.Add(time.Hour), now.Add(24 * time.Hour), adminPub, nil, identity.GrantStatusNotYetValid},
		{"expired", now.Add(-48 * time.Hour), now.Add(-time.Hour), adminPub, nil, identity.GrantStatusExpired},
		{"revoked", now.Add(-time.Hour), now.Add(24 * time.Hour), adminPub, func([]byte) bool { return true }, identity.GrantStatusRevoked},
		{"wrong root", now.Add(-time.Hour), now.Add(24 * time.Hour), subPub, nil, identity.GrantStatusInvalidChain},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, err := identity.IssueGrant(adminPriv, nil, subPub, identity.FullCapabilities(), identity.UnlimitedBudget(), tc.nb, tc.gd)
			require.NoError(t, err)
			chk := identity.CheckGrant(g, tc.root, now, nil, tc.denied)
			require.Equal(t, tc.want, chk.Status, chk.Reason)
		})
	}
}

func TestCheckGrantSubjectMismatch(t *testing.T) {
	now := time.Now()
	adminPub, adminPriv := newKeyPair(t)
	subPub, _ := newKeyPair(t)
	other, _ := newKeyPair(t)

	g, err := identity.IssueGrant(adminPriv, nil, subPub,
		identity.FullCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Hour), now.Add(24*time.Hour))
	require.NoError(t, err)

	ok := identity.CheckGrant(g, adminPub, now, subPub, nil)
	require.Equal(t, identity.GrantStatusOK, ok.Status, ok.Reason)

	mism := identity.CheckGrant(g, adminPub, now, other, nil)
	require.Equal(t, identity.GrantStatusSubjectMismatch, mism.Status)
}

func TestCredentialsRoundTripAndLocalRenewal(t *testing.T) {
	dir := t.TempDir()
	keys := identity.IdentityPath(dir)
	now := time.Now()

	_, nodePub, err := identity.EnsureIdentityKey(keys)
	require.NoError(t, err)

	creds, err := identity.EnsureLocalRootGrant(keys, nodePub, nil, now)
	require.NoError(t, err)
	require.NotNil(t, creds.Grant())

	loaded, err := identity.LoadCredentials(keys)
	require.NoError(t, err)
	require.Equal(t, creds.Grant().GetClaims().GetSerial(), loaded.Grant().GetClaims().GetSerial())

	s1, err := loaded.EnsureFreshSession(now, time.Hour, 5*time.Minute)
	require.NoError(t, err)
	s2, err := loaded.EnsureFreshSession(now.Add(time.Minute), time.Hour, 5*time.Minute)
	require.NoError(t, err)
	require.Same(t, s1, s2, "session reused while fresh")

	s3, err := loaded.EnsureFreshSession(now.Add(57*time.Minute), time.Hour, 5*time.Minute)
	require.NoError(t, err)
	require.NotSame(t, s1, s3, "session re-minted near expiry")

	vs, err := identity.VerifySession(s3, loaded.RootPub(), now.Add(57*time.Minute), nil, nil)
	require.NoError(t, err)
	require.Equal(t, []byte(nodePub), []byte(vs.SubjectPub))
}

func TestResolvePrincipal(t *testing.T) {
	now := time.Now()
	rootPub, grant, subPub, _, _ := chain(t, now, now.Add(30*24*time.Hour))

	p, err := identity.ResolvePrincipal(grant, rootPub, now, nil)
	require.NoError(t, err)
	require.Equal(t, []byte(subPub), []byte(p.SubjectPub))
	require.True(t, p.Capabilities.GetPublish().GetSites())
	require.Equal(t, uint32(3), p.Budget.GetMaxSites())

	denied := func(p []byte) bool { return string(p) == string(subPub) }
	_, err = identity.ResolvePrincipal(grant, rootPub, now, denied)
	require.ErrorIs(t, err, identity.ErrGrantInvalid)
}

func TestGrantTokenRoundTrip(t *testing.T) {
	now := time.Now()
	adminPub, adminPriv := newKeyPair(t)
	joinerPub, _ := newKeyPair(t)

	grant, err := identity.IssueGrant(adminPriv, nil, joinerPub,
		identity.PublisherCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)

	tok, err := identity.IssueGrantToken(adminPriv, grant, nil, adminPub, now, time.Hour)
	require.NoError(t, err)

	enc, err := identity.EncodeGrantToken(tok)
	require.NoError(t, err)
	dec, err := identity.DecodeGrantToken(enc)
	require.NoError(t, err)

	v, err := identity.VerifyGrantToken(dec, joinerPub, now)
	require.NoError(t, err)
	require.Equal(t, []byte(joinerPub), v.Grant.GetClaims().GetSubjectPub())
	require.Equal(t, []byte(adminPub), []byte(v.RootPub))

	_, err = identity.VerifyGrantToken(dec, joinerPub, now.Add(time.Hour+2*time.Minute))
	require.ErrorContains(t, err, "expired")

	other, _ := newKeyPair(t)
	_, err = identity.VerifyGrantToken(dec, other, now)
	require.ErrorContains(t, err, "subject mismatch")

	dec.Signature[0] ^= 0xff
	_, err = identity.VerifyGrantToken(dec, joinerPub, now)
	require.ErrorContains(t, err, "signature invalid")
}

func TestEnrollGrant(t *testing.T) {
	now := time.Now()
	dir := identity.IdentityPath(t.TempDir())
	_, nodePub, err := identity.EnsureIdentityKey(dir)
	require.NoError(t, err)

	adminPub, adminPriv := newKeyPair(t)
	grant, err := identity.IssueGrant(adminPriv, nil, nodePub,
		identity.PublisherCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)
	tok, err := identity.IssueGrantToken(adminPriv, grant, nil, adminPub, now, time.Hour)
	require.NoError(t, err)

	creds, err := identity.EnrollGrant(dir, nodePub, tok, now)
	require.NoError(t, err)
	require.NotNil(t, creds.Grant())

	loaded, err := identity.LoadCredentials(dir)
	require.NoError(t, err)
	require.Equal(t, grant.GetClaims().GetSerial(), loaded.Grant().GetClaims().GetSerial())

	// A token from a different cluster root must be refused once enrolled.
	adminBPub, adminBPriv := newKeyPair(t)
	grantB, err := identity.IssueGrant(adminBPriv, nil, nodePub,
		identity.PublisherCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)
	tokB, err := identity.IssueGrantToken(adminBPriv, grantB, nil, adminBPub, now, time.Hour)
	require.NoError(t, err)
	_, err = identity.EnrollGrant(dir, nodePub, tokB, now)
	require.ErrorIs(t, err, identity.ErrDifferentCluster)
}

func TestInviteTicketRedeemAndConsume(t *testing.T) {
	now := time.Now()
	adminPub, adminPriv := newKeyPair(t)
	hostPub, hostPriv := newKeyPair(t)
	joinerPub, _ := newKeyPair(t)

	hostGrant, err := identity.IssueGrant(adminPriv, nil, hostPub,
		identity.FullCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)

	bootstrap := []*admissionv1.BootstrapPeer{{
		PeerPub: bytes.Repeat([]byte{0x07}, 32),
		Addrs:   []string{"203.0.113.7:60611"},
	}}
	ticket, err := identity.IssueInviteTicket(hostPriv, bootstrap, joinerPub,
		identity.PublisherCapabilities(), &identityv1.Budget{MaxSites: 1},
		now.Add(30*24*time.Hour), now, time.Hour)
	require.NoError(t, err)

	_, err = identity.VerifyInviteTicket(ticket, joinerPub, now)
	require.NoError(t, err)
	other, _ := newKeyPair(t)
	_, err = identity.VerifyInviteTicket(ticket, other, now)
	require.ErrorContains(t, err, "subject mismatch")
	_, err = identity.VerifyInviteTicket(ticket, joinerPub, now.Add(time.Hour+2*time.Minute))
	require.ErrorContains(t, err, "expired")

	_, otherPriv := newKeyPair(t)
	_, err = identity.RedeemInviteTicket(otherPriv, []*identityv1.Grant{hostGrant}, adminPub, ticket, joinerPub, now, time.Hour)
	require.ErrorContains(t, err, "issuer is not the redeeming host")

	tok, err := identity.RedeemInviteTicket(hostPriv, []*identityv1.Grant{hostGrant}, adminPub, ticket, joinerPub, now, time.Hour)
	require.NoError(t, err)
	v, err := identity.VerifyGrantToken(tok, joinerPub, now)
	require.NoError(t, err)
	require.Equal(t, []byte(joinerPub), v.Grant.GetClaims().GetSubjectPub())

	c := identity.NewInviteConsumer(nil)
	ok, err := c.TryConsume(ticket, now)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = c.TryConsume(ticket, now)
	require.NoError(t, err)
	require.False(t, ok, "second redemption of the same ticket is refused")

	rebuilt := identity.NewInviteConsumer(c.Export())
	ok, err = rebuilt.TryConsume(ticket, now)
	require.NoError(t, err)
	require.False(t, ok, "consumed set survives a rebuild so a ticket cannot be replayed")
}

func TestVerifiedSessionPrincipal(t *testing.T) {
	now := time.Now()
	rootPub, grant, subPub, subPriv, _ := chain(t, now, now.Add(30*24*time.Hour))
	s, err := identity.MintSession(grant, subPriv, now, time.Hour)
	require.NoError(t, err)
	vs, err := identity.VerifySession(s, rootPub, now, nil, nil)
	require.NoError(t, err)

	p := vs.Principal()
	require.Equal(t, []byte(subPub), []byte(p.SubjectPub))
	require.True(t, p.Capabilities.GetPublish().GetSites())
	require.Equal(t, uint32(3), p.Budget.GetMaxSites())
}

func TestCheckGrantRejectsSplicedChain(t *testing.T) {
	now := time.Now()
	rootPub, child, _, _, _ := chain(t, now, now.Add(30*24*time.Hour))

	// Splice an unrelated cluster's grant in as the chain anchor: the
	// leaf's issuer no longer matches the chain link, so the walk fails.
	adminBPub, adminBPriv := newKeyPair(t)
	bogus, err := identity.IssueGrant(adminBPriv, nil, adminBPub,
		identity.FullCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Hour), time.Time{})
	require.NoError(t, err)
	child.Chain[0] = bogus

	chk := identity.CheckGrant(child, rootPub, now, nil, nil)
	require.Equal(t, identity.GrantStatusInvalidChain, chk.Status, chk.Reason)
}
