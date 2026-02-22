package auth_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestIssueJoinTokenWithDelegatedAdmin(t *testing.T) {
	genesisPub, genesisPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(genesisPub)

	delegatedPub, delegatedPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()
	issuer, err := auth.IssueAdminCert(
		genesisPriv,
		trust.GetClusterId(),
		delegatedPub,
		now.Add(-time.Minute),
		now.Add(10*365*24*time.Hour),
	)
	require.NoError(t, err)

	token, err := auth.IssueJoinTokenWithIssuer(
		delegatedPriv,
		trust,
		issuer,
		subjectPub,
		nil,
		now,
		time.Hour,
	)
	require.NoError(t, err)

	verified, err := auth.VerifyJoinToken(token, subjectPub, now)
	require.NoError(t, err)
	require.EqualValues(t, delegatedPub, verified.Cert.GetClaims().GetIssuerAdminPub())
}

func TestIssueJoinTokenRejectsUnsignedDelegatedAdmin(t *testing.T) {
	genesisPub, _ := newKeyPair(t)
	trust := auth.NewTrustBundle(genesisPub)

	_, delegatedPriv := newKeyPair(t)

	subjectPub, _ := newKeyPair(t)

	_, err := auth.IssueJoinToken(
		delegatedPriv,
		trust,
		subjectPub,
		nil,
		time.Now(),
		time.Hour,
	)
	require.ErrorContains(t, err, "issuer admin certificate required")
}

func TestInviteTokenOpenSubjectAndSingleUse(t *testing.T) {
	adminPub, adminPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(adminPub)

	now := time.Now()
	issuer, err := auth.IssueAdminCert(
		adminPriv,
		trust.GetClusterId(),
		adminPub,
		now.Add(-time.Minute),
		now.Add(5*365*24*time.Hour),
	)
	require.NoError(t, err)

	consumed, err := config.LoadConsumedInvites(t.TempDir(), now)
	require.NoError(t, err)
	signer := &auth.AdminSigner{Priv: adminPriv, Trust: trust, Issuer: issuer, Consumed: consumed}

	bootstrapPub, _ := newKeyPair(t)
	invite, err := auth.IssueInviteTokenWithSigner(
		signer,
		nil,
		[]*admissionv1.BootstrapPeer{{PeerPub: bootstrapPub, Addrs: []string{"127.0.0.1:60611"}}},
		now,
		time.Hour,
	)
	require.NoError(t, err)

	subjectPub, _ := newKeyPair(t)
	_, err = auth.VerifyInviteToken(invite, subjectPub, now)
	require.NoError(t, err)

	accepted, err := signer.Consumed.TryConsume(invite, now)
	require.NoError(t, err)
	require.True(t, accepted)

	accepted, err = signer.Consumed.TryConsume(invite, now.Add(time.Second))
	require.NoError(t, err)
	require.False(t, accepted)
}

func TestInviteTokenSubjectBoundMismatch(t *testing.T) {
	adminPub, adminPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(adminPub)

	now := time.Now()
	issuer, err := auth.IssueAdminCert(
		adminPriv,
		trust.GetClusterId(),
		adminPub,
		now.Add(-time.Minute),
		now.Add(5*365*24*time.Hour),
	)
	require.NoError(t, err)

	consumed, err := config.LoadConsumedInvites(t.TempDir(), now)
	require.NoError(t, err)
	signer := &auth.AdminSigner{Priv: adminPriv, Trust: trust, Issuer: issuer, Consumed: consumed}

	bootstrapPub, _ := newKeyPair(t)
	boundSubject, _ := newKeyPair(t)
	invite, err := auth.IssueInviteTokenWithSigner(
		signer,
		boundSubject,
		[]*admissionv1.BootstrapPeer{{PeerPub: bootstrapPub, Addrs: []string{"127.0.0.1:60611"}}},
		now,
		time.Hour,
	)
	require.NoError(t, err)

	otherSubject, _ := newKeyPair(t)
	_, err = auth.VerifyInviteToken(invite, otherSubject, now)
	require.ErrorContains(t, err, "subject mismatch")
}

func TestVerifyMembershipCertRejectsIssuerKeyMismatch(t *testing.T) {
	// An attacker steals a valid admin cert and attaches it to a membership
	// cert signed by a different key. VerifyMembershipCert must reject
	// the mismatch between issuer.claims.admin_pub and cert.claims.issuer_admin_pub.

	genesisPub, genesisPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(genesisPub)

	now := time.Now()

	// Legitimate admin cert (signed by genesis).
	legitimateAdminPub, _ := newKeyPair(t)
	legitimateIssuer, err := auth.IssueAdminCert(
		genesisPriv,
		trust.GetClusterId(),
		legitimateAdminPub,
		now.Add(-time.Minute),
		now.Add(10*365*24*time.Hour),
	)
	require.NoError(t, err)

	// Attacker issues a membership cert signed by their own key
	// but attaches the legitimate admin cert as issuer.
	attackerPub, attackerPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)

	// Use the attacker's key to issue a membership cert. The attacker's
	// own admin cert is created here just so IssueMembershipCertWithIssuer
	// passes its internal checks — we swap the issuer afterward.
	attackerIssuer, err := auth.IssueAdminCert(
		genesisPriv,
		trust.GetClusterId(),
		attackerPub,
		now.Add(-time.Minute),
		now.Add(10*365*24*time.Hour),
	)
	require.NoError(t, err)

	forgedCert, err := auth.IssueMembershipCertWithIssuer(
		attackerPriv,
		attackerIssuer,
		trust.GetClusterId(),
		subjectPub,
		now.Add(-time.Minute),
		now.Add(365*24*time.Hour),
	)
	require.NoError(t, err)

	// Swap in the legitimate (but unrelated) issuer cert.
	forgedCert.Issuer = legitimateIssuer

	err = auth.VerifyMembershipCert(forgedCert, trust, now, subjectPub)
	require.ErrorContains(t, err, "issuer key mismatch")
}

func newKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	return pub, priv
}
