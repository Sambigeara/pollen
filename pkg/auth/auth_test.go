package auth_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type testConsumer struct {
	seen map[string]struct{}
}

func (c *testConsumer) TryConsume(token *admissionv1.InviteToken, _ time.Time) (bool, error) {
	id := token.GetClaims().GetTokenId()
	if _, ok := c.seen[id]; ok {
		return false, nil
	}
	c.seen[id] = struct{}{}
	return true, nil
}

func TestIssueDelegationCert_RootIssued(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()
	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	err = auth.VerifyDelegationCert(cert, trust, now, subjectPub)
	require.NoError(t, err)
}

func TestIssueDelegationCert_Delegated(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	delegatedPub, delegatedPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()

	// Root issues a delegation cert for the delegated admin.
	adminCert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), delegatedPub,
		auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(10*365*24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	// Delegated admin issues a leaf cert.
	parentChain := []*admissionv1.DelegationCert{adminCert}
	leafCert, err := auth.IssueDelegationCert(
		delegatedPriv, parentChain, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	err = auth.VerifyDelegationCert(leafCert, trust, now, subjectPub)
	require.NoError(t, err)
	require.EqualValues(t, delegatedPub, leafCert.GetClaims().GetIssuerPub())
}

func TestIssueDelegationCert_AttenuationEnforced(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	delegatedPub, delegatedPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()

	// Root issues a leaf cert (no delegation capability).
	leafCert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), delegatedPub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	// Leaf tries to issue another cert — should fail because can_delegate is false.
	parentChain := []*admissionv1.DelegationCert{leafCert}
	_, err = auth.IssueDelegationCert(
		delegatedPriv, parentChain, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour),
		time.Time{},
	)
	require.ErrorContains(t, err, "parent cert lacks delegation capability")
}

func TestIssueDelegationCert_MaxDepthEnforced(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	delegatedPub, delegatedPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()

	// Root issues a cert with max_depth=1.
	parentCert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), delegatedPub,
		&admissionv1.Capabilities{CanDelegate: true, CanAdmit: true, MaxDepth: 1},
		now.Add(-time.Minute), now.Add(24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	// Delegated admin tries to issue with max_depth=1 (must be < parent's 1) — should fail.
	parentChain := []*admissionv1.DelegationCert{parentCert}
	_, err = auth.IssueDelegationCert(
		delegatedPriv, parentChain, trust.GetClusterId(), subjectPub,
		&admissionv1.Capabilities{CanDelegate: true, CanAdmit: true, MaxDepth: 1},
		now.Add(-time.Minute), now.Add(24*time.Hour),
		time.Time{},
	)
	require.ErrorContains(t, err, "child max_depth must be less than parent max_depth")

	// With max_depth=0 it should work.
	leafCert, err := auth.IssueDelegationCert(
		delegatedPriv, parentChain, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)
	require.NoError(t, auth.VerifyDelegationCert(leafCert, trust, now, subjectPub))
}

func TestVerifyDelegationCert_ChainWalk(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	adminPub, adminPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()

	// Root -> Admin
	adminCert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), adminPub,
		auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(10*365*24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	// Admin -> Member
	memberCert, err := auth.IssueDelegationCert(
		adminPriv,
		[]*admissionv1.DelegationCert{adminCert},
		trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	// Full chain walk.
	require.NoError(t, auth.VerifyDelegationCert(memberCert, trust, now, subjectPub))

	// Wrong trust bundle should fail.
	otherPub, _ := newKeyPair(t)
	otherTrust := auth.NewTrustBundle(otherPub)
	require.Error(t, auth.VerifyDelegationCert(memberCert, otherTrust, now, subjectPub))
}

func TestVerifyDelegationCert_Expired(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()
	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-48*time.Hour), now.Add(-24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	err = auth.VerifyDelegationCert(cert, trust, now, subjectPub)
	require.ErrorContains(t, err, "outside validity window")
}

func TestIssueJoinTokenWithDelegatedAdmin(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	delegatedPub, delegatedPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()

	issuer, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), delegatedPub,
		auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(10*365*24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	token, err := auth.IssueJoinTokenWithIssuer(
		delegatedPriv, trust, issuer, subjectPub, nil, now,
		time.Hour, 4*time.Hour, time.Time{},
	)
	require.NoError(t, err)

	verified, err := auth.VerifyJoinToken(token, subjectPub, now)
	require.NoError(t, err)
	require.EqualValues(t, delegatedPub, verified.Cert.GetClaims().GetIssuerPub())
}

func TestIssueJoinTokenRejectsUnsignedDelegatedAdmin(t *testing.T) {
	rootPub, _ := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	_, delegatedPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)

	_, err := auth.IssueJoinToken(
		delegatedPriv, trust, subjectPub, nil, time.Now(),
		time.Hour, 4*time.Hour, time.Time{},
	)
	require.ErrorContains(t, err, "issuer delegation certificate required")
}

func TestEnsureNodeCredentialsFromTokenReplacesExistingCert(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	nodePub, _ := newKeyPair(t)

	now := time.Now()
	oldCert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), nodePub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	pollenDir := t.TempDir()
	require.NoError(t, auth.SaveNodeCredentials(pollenDir, &auth.NodeCredentials{Trust: trust, Cert: oldCert}))

	token, err := auth.IssueJoinToken(
		rootPriv, trust, nodePub, nil, now,
		time.Hour, 48*time.Hour, time.Time{},
	)
	require.NoError(t, err)

	verified, err := auth.VerifyJoinToken(token, nodePub, now)
	require.NoError(t, err)

	creds, err := auth.EnsureNodeCredentialsFromToken(pollenDir, nodePub, token, now)
	require.NoError(t, err)
	require.True(t, proto.Equal(verified.Cert, creds.Cert))
	require.NotEqual(t, oldCert.GetClaims().GetNotAfterUnix(), creds.Cert.GetClaims().GetNotAfterUnix())

	loaded, err := auth.LoadExistingNodeCredentials(pollenDir, nodePub, now)
	require.NoError(t, err)
	require.True(t, proto.Equal(verified.Cert, loaded.Cert))
}

func TestEnsureNodeCredentialsFromTokenKeepsLongerTTLCert(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	nodePub, _ := newKeyPair(t)

	now := time.Now()
	oldCert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), nodePub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(48*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	pollenDir := t.TempDir()
	require.NoError(t, auth.SaveNodeCredentials(pollenDir, &auth.NodeCredentials{Trust: trust, Cert: oldCert}))

	token, err := auth.IssueJoinToken(
		rootPriv, trust, nodePub, nil, now,
		time.Hour, 24*time.Hour, time.Time{},
	)
	require.NoError(t, err)

	creds, err := auth.EnsureNodeCredentialsFromToken(pollenDir, nodePub, token, now)
	require.NoError(t, err)
	require.True(t, proto.Equal(oldCert, creds.Cert), "existing longer-TTL cert should be kept")
}

func TestEnsureNodeCredentialsFromTokenReplacesExpiredCert(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	nodePub, _ := newKeyPair(t)

	now := time.Now()
	expiredCert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), nodePub,
		auth.LeafCapabilities(),
		now.Add(-48*time.Hour), now.Add(-24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	pollenDir := t.TempDir()
	require.NoError(t, auth.SaveNodeCredentials(pollenDir, &auth.NodeCredentials{Trust: trust, Cert: expiredCert}))

	token, err := auth.IssueJoinToken(
		rootPriv, trust, nodePub, nil, now,
		time.Hour, 24*time.Hour, time.Time{},
	)
	require.NoError(t, err)

	verified, err := auth.VerifyJoinToken(token, nodePub, now)
	require.NoError(t, err)

	creds, err := auth.EnsureNodeCredentialsFromToken(pollenDir, nodePub, token, now)
	require.NoError(t, err)
	require.True(t, proto.Equal(verified.Cert, creds.Cert), "expired cert should be replaced")
}

func TestCertExpiresAt(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()
	expiry := now.Add(24 * time.Hour)
	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), expiry,
		time.Time{},
	)
	require.NoError(t, err)
	require.Equal(t, expiry.Unix(), auth.CertExpiresAt(cert).Unix())
}

func TestIsCertExpired(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()
	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	require.False(t, auth.IsCertExpired(cert, now))
	require.False(t, auth.IsCertExpired(cert, now.Add(24*time.Hour+30*time.Second)))
	require.True(t, auth.IsCertExpired(cert, now.Add(24*time.Hour+2*time.Minute)))
}

func TestInviteTokenOpenSubjectAndSingleUse(t *testing.T) {
	adminPub, adminPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(adminPub)

	now := time.Now()
	issuer, err := auth.IssueDelegationCert(
		adminPriv, nil, trust.GetClusterId(), adminPub,
		auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(5*365*24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	signer := &auth.DelegationSigner{Priv: adminPriv, Trust: trust, Issuer: issuer, Consumed: &testConsumer{seen: make(map[string]struct{})}}

	bootstrapPub, _ := newKeyPair(t)
	invite, err := auth.IssueInviteTokenWithSigner(
		signer, nil,
		[]*admissionv1.BootstrapPeer{{PeerPub: bootstrapPub, Addrs: []string{"127.0.0.1:60611"}}},
		now, time.Hour, 0,
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
	issuer, err := auth.IssueDelegationCert(
		adminPriv, nil, trust.GetClusterId(), adminPub,
		auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(5*365*24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	signer := &auth.DelegationSigner{Priv: adminPriv, Trust: trust, Issuer: issuer, Consumed: &testConsumer{seen: make(map[string]struct{})}}

	bootstrapPub, _ := newKeyPair(t)
	boundSubject, _ := newKeyPair(t)
	invite, err := auth.IssueInviteTokenWithSigner(
		signer, boundSubject,
		[]*admissionv1.BootstrapPeer{{PeerPub: bootstrapPub, Addrs: []string{"127.0.0.1:60611"}}},
		now, time.Hour, 0,
	)
	require.NoError(t, err)

	otherSubject, _ := newKeyPair(t)
	_, err = auth.VerifyInviteToken(invite, otherSubject, now)
	require.ErrorContains(t, err, "subject mismatch")
}

func TestIsCapSubset(t *testing.T) {
	full := auth.FullCapabilities()
	leaf := auth.LeafCapabilities()

	require.True(t, auth.IsCapSubset(leaf, full))
	require.True(t, auth.IsCapSubset(full, full))
	require.False(t, auth.IsCapSubset(full, leaf))
}

func TestCertTTL(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()
	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now, now.Add(4*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)
	require.Equal(t, 4*time.Hour, auth.CertTTL(cert))
}

func TestRenewalPreservesTTL(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now().Truncate(time.Second)
	ttl := 6 * time.Hour

	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now, now.Add(ttl),
		time.Time{},
	)
	require.NoError(t, err)
	require.Equal(t, ttl, auth.CertTTL(cert))

	// Simulate renewal: extract TTL from existing cert, reissue with same duration.
	renewedNow := now.Add(3 * time.Hour)
	extractedTTL := auth.CertTTL(cert)

	renewed, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		renewedNow, renewedNow.Add(extractedTTL),
		time.Time{},
	)
	require.NoError(t, err)
	require.Equal(t, ttl, auth.CertTTL(renewed))
}

func TestAccessDeadlineCert(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now().Truncate(time.Second)
	deadline := now.Add(24 * time.Hour)

	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(4*time.Hour),
		deadline,
	)
	require.NoError(t, err)

	dl, ok := auth.CertAccessDeadline(cert)
	require.True(t, ok)
	require.Equal(t, deadline.Unix(), dl.Unix())
}

func TestAccessDeadlineCertNone(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()

	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(4*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	_, ok := auth.CertAccessDeadline(cert)
	require.False(t, ok)
}

func TestAccessDeadlineJoinToken(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now().Truncate(time.Second)
	deadline := now.Add(24 * time.Hour)

	token, err := auth.IssueJoinToken(
		rootPriv, trust, subjectPub, nil, now,
		time.Hour, 4*time.Hour, deadline,
	)
	require.NoError(t, err)

	verified, err := auth.VerifyJoinToken(token, subjectPub, now)
	require.NoError(t, err)

	// Member cert should have the deadline.
	dl, ok := auth.CertAccessDeadline(verified.Cert)
	require.True(t, ok)
	require.Equal(t, deadline.Unix(), dl.Unix())

	// notAfter should be clamped: min(now+4h, deadline). Since deadline=now+24h > now+4h,
	// notAfter should be approximately now+4h.
	require.InDelta(t, now.Add(4*time.Hour).Unix(), verified.Cert.GetClaims().GetNotAfterUnix(), 2)
}

func TestAccessDeadlineJoinTokenClamps(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now().Truncate(time.Second)
	deadline := now.Add(2 * time.Hour)

	token, err := auth.IssueJoinToken(
		rootPriv, trust, subjectPub, nil, now,
		time.Hour, 4*time.Hour, deadline,
	)
	require.NoError(t, err)

	verified, err := auth.VerifyJoinToken(token, subjectPub, now)
	require.NoError(t, err)

	// notAfter should be clamped to deadline since deadline < now+4h.
	require.Equal(t, deadline.Unix(), verified.Cert.GetClaims().GetNotAfterUnix())

	// Deadline should be preserved.
	dl, ok := auth.CertAccessDeadline(verified.Cert)
	require.True(t, ok)
	require.Equal(t, deadline.Unix(), dl.Unix())
}

func TestVerifyDelegationCertChain_AcceptsExpiredCerts(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()
	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-48*time.Hour), now.Add(-24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	// Full verification should fail (expired).
	err = auth.VerifyDelegationCert(cert, trust, now, subjectPub)
	require.ErrorIs(t, err, auth.ErrCertExpired)

	// Chain-only verification should pass (crypto-valid).
	err = auth.VerifyDelegationCertChain(cert, trust, subjectPub)
	require.NoError(t, err)
}

func TestVerifyDelegationCertChain_RejectsBadSignature(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()
	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	// Corrupt signature.
	cert.Signature[0] ^= 0xff

	err = auth.VerifyDelegationCertChain(cert, trust, subjectPub)
	require.Error(t, err)
}

func TestLoadExistingNodeCredentials_ReturnsCredsAndErrCertExpired(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	nodePub, _ := newKeyPair(t)

	now := time.Now()
	expiredCert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), nodePub,
		auth.LeafCapabilities(),
		now.Add(-48*time.Hour), now.Add(-24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	pollenDir := t.TempDir()
	require.NoError(t, auth.SaveNodeCredentials(pollenDir, &auth.NodeCredentials{
		Trust: trust,
		Cert:  expiredCert,
	}))

	creds, err := auth.LoadExistingNodeCredentials(pollenDir, nodePub, now)
	require.ErrorIs(t, err, auth.ErrCertExpired)
	require.NotNil(t, creds, "creds should be returned even when expired")
	require.Equal(t, expiredCert.GetClaims().GetNotAfterUnix(), creds.Cert.GetClaims().GetNotAfterUnix())
}

func TestIsCertWithinReconnectWindow(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()
	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-48*time.Hour), now.Add(-24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	reconnectWindow := 7 * 24 * time.Hour

	// 24h expired, 7d window → within window.
	require.True(t, auth.IsCertWithinReconnectWindow(cert, now, reconnectWindow))

	// Move time to just past the window.
	require.False(t, auth.IsCertWithinReconnectWindow(cert, now.Add(8*24*time.Hour), reconnectWindow))
}

func TestIsCertWithinReconnectWindow_NotYetExpired(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	trust := auth.NewTrustBundle(rootPub)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()
	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, trust.GetClusterId(), subjectPub,
		auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour),
		time.Time{},
	)
	require.NoError(t, err)

	// Not expired → always within window.
	require.True(t, auth.IsCertWithinReconnectWindow(cert, now, time.Hour))
}

func newKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}
