// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/internal/testauth"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

var mockTime = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// testIdentityDir creates the per-node identity directory tests use to
// stand in for production's `<pollenDir>/keys/`. Returns the keys subdir
// directly so tests pass it to functions expecting an identityDir, matching
// what `auth.IdentityPath(pollenDir)` returns at runtime.
func testIdentityDir(t *testing.T) string {
	t.Helper()
	dir := auth.IdentityPath(t.TempDir())
	require.NoError(t, os.MkdirAll(dir, 0o700))
	return dir
}

func newKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

func issueRootJoinToken(t *testing.T, rootPriv ed25519.PrivateKey, rootPub, subject ed25519.PublicKey, now time.Time, membershipTTL time.Duration, accessDeadline time.Time) *admissionv1.JoinToken {
	t.Helper()
	issuer, err := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(membershipTTL), time.Time{})
	require.NoError(t, err)
	signer := testauth.LoadTestSigner(t, rootPriv, rootPub, issuer)
	token, err := signer.IssueJoinToken(subject, nil, now, time.Hour, membershipTTL, accessDeadline, nil)
	require.NoError(t, err)
	return token
}

func invalidateSignature(sig []byte) []byte {
	bad := make([]byte, len(sig))
	copy(bad, sig)
	bad[0] ^= 0xFF
	return bad
}

func TestLoadNodeCredentials_ExpiredCertStillLoads(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	nodePub, _ := newKeyPair(t)

	now := time.Now()
	expiredCert, err := auth.IssueDelegationCert(
		rootPriv, nil, nodePub, auth.LeafCapabilities(),
		now.Add(-48*time.Hour), now.Add(-24*time.Hour), time.Time{},
	)
	require.NoError(t, err)

	pollenDir := testIdentityDir(t)
	require.NoError(t, auth.SaveNodeCredentials(pollenDir, auth.NewNodeCredentials(rootPub, expiredCert)))

	creds, err := auth.LoadNodeCredentials(pollenDir)
	require.NoError(t, err)
	require.True(t, auth.IsCertExpired(creds.Cert(), now))
	require.Equal(t, expiredCert.GetClaims().GetNotAfterUnix(), creds.Cert().GetClaims().GetNotAfterUnix())
}

func TestEnrollReplacesExistingCert(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	nodePub, _ := newKeyPair(t)

	now := time.Now()
	oldCert, err := auth.IssueDelegationCert(
		rootPriv, nil, nodePub, auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour), time.Time{},
	)
	require.NoError(t, err)

	pollenDir := testIdentityDir(t)
	require.NoError(t, auth.SaveNodeCredentials(pollenDir, auth.NewNodeCredentials(rootPub, oldCert)))

	token := issueRootJoinToken(t, rootPriv, rootPub, nodePub, now, 48*time.Hour, time.Time{})
	verified, err := auth.VerifyJoinToken(token, nodePub, now)
	require.NoError(t, err)

	creds, err := auth.EnrollNodeCredentials(pollenDir, nodePub, token, now)
	require.NoError(t, err)
	require.True(t, proto.Equal(verified.Cert, creds.Cert()))
	require.NotEqual(t, oldCert.GetClaims().GetNotAfterUnix(), creds.Cert().GetClaims().GetNotAfterUnix())

	loaded, err := auth.LoadNodeCredentials(pollenDir)
	require.NoError(t, err)
	require.True(t, proto.Equal(verified.Cert, loaded.Cert()))
}

func TestEnrollKeepsLongerTTLCert(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	nodePub, _ := newKeyPair(t)

	now := time.Now()
	oldCert, err := auth.IssueDelegationCert(
		rootPriv, nil, nodePub, auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(48*time.Hour), time.Time{},
	)
	require.NoError(t, err)

	pollenDir := testIdentityDir(t)
	require.NoError(t, auth.SaveNodeCredentials(pollenDir, auth.NewNodeCredentials(rootPub, oldCert)))

	token := issueRootJoinToken(t, rootPriv, rootPub, nodePub, now, 24*time.Hour, time.Time{})

	creds, err := auth.EnrollNodeCredentials(pollenDir, nodePub, token, now)
	require.NoError(t, err)
	require.True(t, proto.Equal(oldCert, creds.Cert()), "existing longer-TTL cert should be kept")
}

func TestEnrollReplacesExpiredCert(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	nodePub, _ := newKeyPair(t)

	now := time.Now()
	expiredCert, err := auth.IssueDelegationCert(
		rootPriv, nil, nodePub, auth.LeafCapabilities(),
		now.Add(-48*time.Hour), now.Add(-24*time.Hour), time.Time{},
	)
	require.NoError(t, err)

	pollenDir := testIdentityDir(t)
	require.NoError(t, auth.SaveNodeCredentials(pollenDir, auth.NewNodeCredentials(rootPub, expiredCert)))

	token := issueRootJoinToken(t, rootPriv, rootPub, nodePub, now, 24*time.Hour, time.Time{})
	verified, err := auth.VerifyJoinToken(token, nodePub, now)
	require.NoError(t, err)

	creds, err := auth.EnrollNodeCredentials(pollenDir, nodePub, token, now)
	require.NoError(t, err)
	require.True(t, proto.Equal(verified.Cert, creds.Cert()), "expired cert should be replaced")
}

func TestCertTTLAndExpiry(t *testing.T) {
	_, rootPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()
	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, subjectPub, auth.LeafCapabilities(),
		now, now.Add(4*time.Hour), time.Time{},
	)
	require.NoError(t, err)

	require.Equal(t, 4*time.Hour, auth.CertTTL(cert))
	require.Equal(t, now.Add(4*time.Hour).Unix(), auth.CertExpiresAt(cert).Unix())

	require.False(t, auth.IsCertExpired(cert, now))
	require.False(t, auth.IsCertExpired(cert, now.Add(4*time.Hour+30*time.Second)))
	require.True(t, auth.IsCertExpired(cert, now.Add(4*time.Hour+2*time.Minute)))
}

func TestRenewalPreservesTTL(t *testing.T) {
	_, rootPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)

	now := time.Now().Truncate(time.Second)
	ttl := 6 * time.Hour

	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, subjectPub, auth.LeafCapabilities(),
		now, now.Add(ttl), time.Time{},
	)
	require.NoError(t, err)

	renewedNow := now.Add(3 * time.Hour)
	renewed, err := auth.IssueDelegationCert(
		rootPriv, nil, subjectPub, auth.LeafCapabilities(),
		renewedNow, renewedNow.Add(auth.CertTTL(cert)), time.Time{},
	)
	require.NoError(t, err)
	require.Equal(t, ttl, auth.CertTTL(renewed))
}

func TestAccessDeadline(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)

	now := time.Now().Truncate(time.Second)
	deadline := now.Add(24 * time.Hour)

	t.Run("cert with deadline", func(t *testing.T) {
		cert, err := auth.IssueDelegationCert(
			rootPriv, nil, subjectPub, auth.LeafCapabilities(),
			now.Add(-time.Minute), now.Add(4*time.Hour), deadline,
		)
		require.NoError(t, err)

		dl, ok := auth.CertAccessDeadline(cert)
		require.True(t, ok)
		require.Equal(t, deadline.Unix(), dl.Unix())
	})

	t.Run("cert without deadline", func(t *testing.T) {
		cert, err := auth.IssueDelegationCert(
			rootPriv, nil, subjectPub, auth.LeafCapabilities(),
			now.Add(-time.Minute), now.Add(4*time.Hour), time.Time{},
		)
		require.NoError(t, err)

		_, ok := auth.CertAccessDeadline(cert)
		require.False(t, ok)
	})

	t.Run("join token propagates deadline", func(t *testing.T) {
		token := issueRootJoinToken(t, rootPriv, rootPub, subjectPub, now, 4*time.Hour, deadline)
		verified, err := auth.VerifyJoinToken(token, subjectPub, now)
		require.NoError(t, err)

		dl, ok := auth.CertAccessDeadline(verified.Cert)
		require.True(t, ok)
		require.Equal(t, deadline.Unix(), dl.Unix())
		require.InDelta(t, now.Add(4*time.Hour).Unix(), verified.Cert.GetClaims().GetNotAfterUnix(), 2)
	})

	t.Run("join token clamps to deadline", func(t *testing.T) {
		shortDeadline := now.Add(2 * time.Hour)
		token := issueRootJoinToken(t, rootPriv, rootPub, subjectPub, now, 4*time.Hour, shortDeadline)
		verified, err := auth.VerifyJoinToken(token, subjectPub, now)
		require.NoError(t, err)

		require.Equal(t, shortDeadline.Unix(), verified.Cert.GetClaims().GetNotAfterUnix())
	})
}

func TestReconnectWindow(t *testing.T) {
	_, rootPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)

	now := time.Now()
	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, subjectPub, auth.LeafCapabilities(),
		now.Add(-48*time.Hour), now.Add(-24*time.Hour), time.Time{},
	)
	require.NoError(t, err)

	reconnectWindow := 7 * 24 * time.Hour
	expiresAt := auth.CertExpiresAt(cert)

	require.True(t, now.Before(expiresAt.Add(reconnectWindow)))
	require.False(t, now.Add(8*24*time.Hour).Before(expiresAt.Add(reconnectWindow)))
}

func TestVerifyDelegationCert(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	nodePub, _ := newKeyPair(t)
	randomPub, _ := newKeyPair(t)

	rootCert, err := auth.IssueDelegationCert(
		rootPriv, nil, rootPub, auth.FullCapabilities(),
		mockTime.Add(-time.Hour), mockTime.Add(time.Hour), time.Time{},
	)
	require.NoError(t, err)

	tests := []struct {
		name            string
		setup           func() *admissionv1.DelegationCert
		rootPub         []byte
		expectedSubject []byte
		now             time.Time
		wantErrContains string
	}{
		{
			name:            "valid root cert",
			setup:           func() *admissionv1.DelegationCert { return rootCert },
			rootPub:         rootPub,
			expectedSubject: rootPub,
			now:             mockTime,
		},
		{
			name: "valid delegated cert",
			setup: func() *admissionv1.DelegationCert {
				cert, err := auth.IssueDelegationCert(
					rootPriv, []*admissionv1.DelegationCert{rootCert}, nodePub,
					auth.LeafCapabilities(), mockTime.Add(-time.Hour), mockTime.Add(time.Hour), time.Time{},
				)
				require.NoError(t, err)
				return cert
			},
			rootPub:         rootPub,
			expectedSubject: nodePub,
			now:             mockTime,
		},
		{
			name:            "expired cert",
			setup:           func() *admissionv1.DelegationCert { return rootCert },
			rootPub:         rootPub,
			expectedSubject: rootPub,
			now:             mockTime.Add(2 * time.Hour),
			wantErrContains: "delegation cert expired",
		},
		{
			name:            "wrong root pub",
			setup:           func() *admissionv1.DelegationCert { return rootCert },
			rootPub:         randomPub,
			expectedSubject: rootPub,
			now:             mockTime,
			wantErrContains: "delegation cert chain root mismatch",
		},
		{
			name: "tampered signature",
			setup: func() *admissionv1.DelegationCert {
				cert, err := auth.IssueDelegationCert(
					rootPriv, nil, rootPub, auth.FullCapabilities(),
					mockTime.Add(-time.Hour), mockTime.Add(time.Hour), time.Time{},
				)
				require.NoError(t, err)
				cert.Signature = invalidateSignature(cert.Signature)
				return cert
			},
			rootPub:         rootPub,
			expectedSubject: rootPub,
			now:             mockTime,
			wantErrContains: "delegation cert root signature invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := auth.VerifyDelegationCert(tt.setup(), tt.rootPub, tt.now, tt.expectedSubject)
			if tt.wantErrContains != "" {
				require.ErrorContains(t, err, tt.wantErrContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDelegationSignerAndTokens(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, nodePriv := newKeyPair(t)
	now := time.Now()

	_, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
	require.NoError(t, err)

	signer, err := auth.NewDelegationSigner(dir, nodePriv)
	require.NoError(t, err)
	require.True(t, signer.IsRoot())

	t.Run("invite token", func(t *testing.T) {
		bootstrap := []*admissionv1.BootstrapPeer{{PeerPub: nodePub, Addrs: []string{"127.0.0.1:60611"}}}
		invite, err := signer.IssueInviteToken(nodePub, bootstrap, now, 10*time.Minute, 24*time.Hour, nil)
		require.NoError(t, err)

		require.NoError(t, auth.VerifyInviteToken(invite, nodePub, now))
		require.ErrorContains(t, auth.VerifyInviteToken(invite, nodePub, now.Add(20*time.Minute)), "invite token expired")

		invite.Signature = invalidateSignature(invite.Signature)
		require.ErrorContains(t, auth.VerifyInviteToken(invite, nodePub, now), "invite token signature invalid")
	})

	t.Run("join token", func(t *testing.T) {
		join, err := signer.IssueJoinToken(nodePub, nil, now, 10*time.Minute, 24*time.Hour, time.Time{}, nil)
		require.NoError(t, err)

		verified, err := auth.VerifyJoinToken(join, nodePub, now)
		require.NoError(t, err)
		require.Len(t, verified.RootPub, ed25519.PublicKeySize)

		_, err = auth.VerifyJoinToken(join, nodePub, now.Add(20*time.Minute))
		require.ErrorContains(t, err, "join token expired")
	})

	t.Run("delegated admin", func(t *testing.T) {
		delegatedPub, delegatedPriv := newKeyPair(t)

		issuer, err := auth.IssueDelegationCert(
			nodePriv, nil, delegatedPub, auth.FullCapabilities(),
			now.Add(-time.Hour), now.Add(time.Hour), time.Time{},
		)
		require.NoError(t, err)

		delegatedSigner := testauth.LoadTestSigner(t, delegatedPriv, nodePub, issuer)
		subjectPub, _ := newKeyPair(t)

		token, err := delegatedSigner.IssueJoinToken(subjectPub, nil, now, time.Hour, 4*time.Hour, time.Time{}, nil)
		require.NoError(t, err)

		verified, err := auth.VerifyJoinToken(token, subjectPub, now)
		require.NoError(t, err)
		require.EqualValues(t, delegatedPub, verified.Cert.GetClaims().GetIssuerPub())
	})
}

func TestEnsureLocalRootCredentials_NoOpOnFreshUnchangedCert(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, _ := newKeyPair(t)
	now := time.Now()

	first, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
	require.NoError(t, err)

	second, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now.Add(time.Minute), 24*time.Hour)
	require.NoError(t, err)
	require.True(t, proto.Equal(first.Cert(), second.Cert()),
		"unchanged attrs and unexpired cert should not trigger re-issue")
}

func TestEnsureLocalRootCredentials_ReissuesOnAttrDrift(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, _ := newKeyPair(t)
	now := time.Now()

	original, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
	require.NoError(t, err)
	require.Nil(t, original.Cert().GetClaims().GetCapabilities().GetAttributes())

	attrs, err := structpb.NewStruct(map[string]any{"role": "primary"})
	require.NoError(t, err)

	updated, err := auth.EnsureLocalRootCredentials(dir, nodePub, attrs, now.Add(time.Minute), 24*time.Hour)
	require.NoError(t, err)
	require.False(t, proto.Equal(original.Cert(), updated.Cert()),
		"attrs change should trigger re-issue")
	require.Equal(t, "primary",
		updated.Cert().GetClaims().GetCapabilities().GetAttributes().GetFields()["role"].GetStringValue())
}

func TestEnsureLocalRootCredentials_ReissuesOnExpiry(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, _ := newKeyPair(t)
	now := time.Now()

	original, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, time.Hour)
	require.NoError(t, err)

	renewed, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now.Add(2*time.Hour), time.Hour)
	require.NoError(t, err)
	require.False(t, proto.Equal(original.Cert(), renewed.Cert()), "expired cert should be re-issued")
	require.Greater(t, renewed.Cert().GetClaims().GetNotAfterUnix(), original.Cert().GetClaims().GetNotAfterUnix())
}

// Chains issued by the root before a self-rotation still verify against the
// same RootPub after rotation, because rotation re-uses the same admin key.
func TestRootSelfRotation_PreservesDownstreamChainValidity(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, nodePriv := newKeyPair(t)
	now := time.Now()

	original, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
	require.NoError(t, err)

	signer, err := auth.NewDelegationSigner(dir, nodePriv)
	require.NoError(t, err)

	subjectPub, _ := newKeyPair(t)
	downstream, err := signer.IssueMemberCert(
		subjectPub, auth.LeafCapabilities(),
		now, now.Add(time.Hour), time.Time{},
	)
	require.NoError(t, err)
	require.NoError(t, auth.VerifyDelegationCert(downstream, original.RootPub(), now, subjectPub))

	attrs, err := structpb.NewStruct(map[string]any{"role": "primary"})
	require.NoError(t, err)
	rotated, err := auth.EnsureLocalRootCredentials(dir, nodePub, attrs, now.Add(time.Minute), 24*time.Hour)
	require.NoError(t, err)
	require.False(t, proto.Equal(original.Cert(), rotated.Cert()))

	require.NoError(t, auth.VerifyDelegationCert(downstream, rotated.RootPub(), now.Add(2*time.Minute), subjectPub),
		"downstream cert minted before rotation must still verify after rotation")
}

func TestEnsureLocalRootCredentials_ReissuesOnSubjectDrift(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, _ := newKeyPair(t)
	now := time.Now()

	original, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
	require.NoError(t, err)

	rotatedPub, _ := newKeyPair(t)
	updated, err := auth.EnsureLocalRootCredentials(dir, rotatedPub, nil, now.Add(time.Minute), 24*time.Hour)
	require.NoError(t, err)
	require.False(t, proto.Equal(original.Cert(), updated.Cert()),
		"node identity drift must trigger a re-issue with the new subject")
	require.Equal(t, []byte(rotatedPub), updated.Cert().GetClaims().GetSubjectPub())
}

func TestEnsureLocalRootCredentials_ReissuesOnAdminKeyDrift(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, _ := newKeyPair(t)
	now := time.Now()

	original, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
	require.NoError(t, err)
	originalRootPub := append(ed25519.PublicKey{}, original.RootPub()...)

	require.NoError(t, os.Remove(filepath.Join(dir, "admin_ed25519.key")))
	require.NoError(t, os.Remove(filepath.Join(dir, "admin_ed25519.pub")))

	updated, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now.Add(time.Minute), 24*time.Hour)
	require.NoError(t, err)
	require.False(t, proto.Equal(original.Cert(), updated.Cert()))
	require.NotEqual(t, []byte(originalRootPub), []byte(updated.RootPub()),
		"admin key rotation must produce a different root pub")
}

func TestEnsureLocalRootCredentials_TreatsNilAndEmptyAttrsAsEqual(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, _ := newKeyPair(t)
	now := time.Now()

	first, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
	require.NoError(t, err)

	emptyAttrs, err := structpb.NewStruct(map[string]any{})
	require.NoError(t, err)
	second, err := auth.EnsureLocalRootCredentials(dir, nodePub, emptyAttrs, now.Add(time.Minute), 24*time.Hour)
	require.NoError(t, err)
	require.True(t, proto.Equal(first.Cert(), second.Cert()),
		"nil attrs and empty Struct must be treated as equivalent — neither should trigger a spurious re-issue")
}

func TestNewDelegationSigner_RefreshesStaleRootCert(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, nodePriv := newKeyPair(t)
	now := time.Now()

	_, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
	require.NoError(t, err)

	adminPriv, _, err := auth.LoadAdminKey(dir)
	require.NoError(t, err)
	expired, err := auth.IssueDelegationCert(adminPriv, nil, nodePub, auth.FullCapabilities(),
		now.Add(-2*time.Hour), now.Add(-time.Hour), time.Time{})
	require.NoError(t, err)
	expiredCreds := auth.NewNodeCredentials(adminPriv.Public().(ed25519.PublicKey), expired)
	require.NoError(t, auth.SaveNodeCredentials(dir, expiredCreds))

	signer, err := auth.NewDelegationSigner(dir, nodePriv)
	require.NoError(t, err, "stale root cert must self-refresh during signer construction")
	require.True(t, signer.IsRoot())

	loaded, err := auth.LoadNodeCredentials(dir)
	require.NoError(t, err)
	require.False(t, auth.IsCertExpired(loaded.Cert(), time.Now()),
		"refresh must persist a fresh cert, not just hold one in memory")
}

func TestNewDelegationSigner_RejectsSubjectMismatchForDelegated(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	subjectPub, _ := newKeyPair(t)
	_, otherPriv := newKeyPair(t)

	now := time.Now()
	cert, err := auth.IssueDelegationCert(rootPriv, nil, subjectPub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	require.NoError(t, err)

	dir := testIdentityDir(t)
	require.NoError(t, auth.SaveNodeCredentials(dir, auth.NewNodeCredentials(rootPub, cert)))

	_, err = auth.NewDelegationSigner(dir, otherPriv)
	require.ErrorContains(t, err, "subject does not match")
}

// TestAdminKeyDriftSelfHeals models the daemon-boot path: the daemon gates
// refresh on HasLocalAdminKey (not on creds.RootPub equality), so a rotated
// admin key with a stale persisted cert recovers — rootCertHealthy detects
// the issuer/rootPub drift and EnsureLocalRootCredentials reissues.
func TestAdminKeyDriftSelfHeals(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, _ := newKeyPair(t)
	now := time.Now()

	creds, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
	require.NoError(t, err)
	original := creds.Cert()

	require.NoError(t, os.Remove(filepath.Join(dir, "admin_ed25519.key")))
	require.NoError(t, os.Remove(filepath.Join(dir, "admin_ed25519.pub")))
	_, ok, err := auth.LocalRootAuthority(dir, creds)
	require.NoError(t, err)
	require.False(t, ok, "admin key gone — node no longer holds root authority")

	updated, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now.Add(time.Minute), 24*time.Hour)
	require.NoError(t, err)
	_, ok, err = auth.LocalRootAuthority(dir, updated)
	require.NoError(t, err)
	require.True(t, ok, "EnsureLocalRootCredentials must recreate the admin key + root-shape cert")
	require.False(t, proto.Equal(original, updated.Cert()),
		"admin-key rotation must trigger reissue via the rootCertHealthy predicate")
}

func TestEnsureLocalRootCredentials_ReissuesOnTruncatedCaps(t *testing.T) {
	tests := []struct {
		mutate func(c *admissionv1.Capabilities)
		name   string
	}{
		{name: "no_can_delegate", mutate: func(c *admissionv1.Capabilities) { c.CanDelegate = false }},
		{name: "no_can_admit", mutate: func(c *admissionv1.Capabilities) { c.CanAdmit = false }},
		{name: "zero_max_depth", mutate: func(c *admissionv1.Capabilities) { c.MaxDepth = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := testIdentityDir(t)
			nodePub, _ := newKeyPair(t)
			now := time.Now()

			adminPriv, adminPub, err := auth.EnsureAdminKey(dir)
			require.NoError(t, err)

			caps := auth.FullCapabilities()
			tt.mutate(caps)
			truncated, err := auth.IssueDelegationCert(adminPriv, nil, nodePub, caps,
				now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
			require.NoError(t, err)
			require.NoError(t, auth.SaveNodeCredentials(dir, auth.NewNodeCredentials(adminPub, truncated)))

			updated, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
			require.NoError(t, err)
			require.False(t, proto.Equal(truncated, updated.Cert()),
				"truncated root caps must trigger reissue")
			gotCaps := updated.Cert().GetClaims().GetCapabilities()
			want := auth.FullCapabilities()
			require.True(t, gotCaps.GetCanDelegate())
			require.True(t, gotCaps.GetCanAdmit())
			require.Equal(t, want.MaxDepth, gotCaps.GetMaxDepth())
		})
	}
}

func TestEnsureLocalRootCredentials_ReissuesOnNonEmptyChain(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, _ := newKeyPair(t)
	now := time.Now()

	adminPriv, adminPub, err := auth.EnsureAdminKey(dir)
	require.NoError(t, err)

	parent, err := auth.IssueDelegationCert(adminPriv, nil, adminPub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	require.NoError(t, err)
	chained, err := auth.IssueDelegationCert(adminPriv, []*admissionv1.DelegationCert{parent}, nodePub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	require.NoError(t, err)
	require.NoError(t, auth.SaveNodeCredentials(dir, auth.NewNodeCredentials(adminPub, chained)))

	updated, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
	require.NoError(t, err)
	require.False(t, proto.Equal(chained, updated.Cert()),
		"non-empty chain on a 'root' cert must trigger reissue")
	require.Empty(t, updated.Cert().GetChain())
}

// TestLocalRootAuthority_DelegatedAdminWithStrayAdminKeyIsNotRoot pins the
// fix for a cluster-corruption bug: a delegated admin (joined via token,
// holds a delegated cert) that also happens to have a local admin key
// (e.g. a leftover from a prior cluster, or a `pln admin keygen` mistake)
// must NOT be classified as root. Otherwise EnsureLocalRootCredentials
// would self-issue a fresh root cert and overwrite root.pub with the stray
// local admin pub, forking trust away from the cluster's actual root.
func TestLocalRootAuthority_DelegatedAdminWithStrayAdminKeyIsNotRoot(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, _ := newKeyPair(t)
	rootPub, rootPriv := newKeyPair(t)
	now := time.Now()

	rootCert, err := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	require.NoError(t, err)
	delegated, err := auth.IssueDelegationCert(rootPriv, []*admissionv1.DelegationCert{rootCert}, nodePub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	require.NoError(t, err)
	creds := auth.NewNodeCredentials(rootPub, delegated)
	require.NoError(t, auth.SaveNodeCredentials(dir, creds))

	_, _, err = auth.EnsureAdminKey(dir)
	require.NoError(t, err)

	_, ok, err := auth.LocalRootAuthority(dir, creds)
	require.NoError(t, err)
	require.False(t, ok,
		"a delegated cert (non-empty chain) must defeat the root-authority gate even when an admin key is locally present")
}

// TestNewDelegationSigner_DelegatedAdminWithStrayAdminKeyDoesNotRewriteRoot
// is the end-to-end pin: signer construction on a delegated admin with a
// stray local admin key must not mutate the persisted creds (cert or
// root.pub). The corruption mode pre-fix was: HasLocalAdminKey gates the
// refresh path, rootCertHealthy fails (issuer != localAdminPub), refresh
// reissues — silently overwriting root.pub with the stray admin pub.
func TestNewDelegationSigner_DelegatedAdminWithStrayAdminKeyDoesNotRewriteRoot(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, nodePriv := newKeyPair(t)
	rootPub, rootPriv := newKeyPair(t)
	now := time.Now()

	rootCert, err := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	require.NoError(t, err)
	delegated, err := auth.IssueDelegationCert(rootPriv, []*admissionv1.DelegationCert{rootCert}, nodePub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	require.NoError(t, err)
	require.NoError(t, auth.SaveNodeCredentials(dir, auth.NewNodeCredentials(rootPub, delegated)))
	_, _, err = auth.EnsureAdminKey(dir)
	require.NoError(t, err)

	rootPubBefore, err := os.ReadFile(filepath.Join(dir, "root.pub"))
	require.NoError(t, err)

	signer, err := auth.NewDelegationSigner(dir, nodePriv)
	require.NoError(t, err)
	require.False(t, signer.IsRoot(),
		"signer.IsRoot must be false — local admin key alone does not confer root authority")

	rootPubAfter, err := os.ReadFile(filepath.Join(dir, "root.pub"))
	require.NoError(t, err)
	require.Equal(t, rootPubBefore, rootPubAfter,
		"root.pub must not be rewritten — that would fork trust away from the cluster")

	loaded, err := auth.LoadNodeCredentials(dir)
	require.NoError(t, err)
	require.True(t, proto.Equal(delegated, loaded.Cert()),
		"persisted cert must be untouched — stray admin keys do not authorise reissue")
}

func TestLocalRootAuthority_FreshDirIsNotRoot(t *testing.T) {
	dir := testIdentityDir(t)
	_, ok, err := auth.LocalRootAuthority(dir, nil)
	require.NoError(t, err)
	require.False(t, ok, "empty identity dir must not appear root")
}

func TestLocalRootAuthority_AfterEnsureIsRoot(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, _ := newKeyPair(t)
	now := time.Now()

	creds, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
	require.NoError(t, err)

	adminPub, ok, err := auth.LocalRootAuthority(dir, creds)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte(creds.RootPub()), []byte(adminPub),
		"the returned adminPub must match creds.RootPub for a healthy root")
}

func TestInviteTokenOpenSubject(t *testing.T) {
	adminPub, adminPriv := newKeyPair(t)

	now := time.Now()
	issuer, err := auth.IssueDelegationCert(
		adminPriv, nil, adminPub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(time.Hour), time.Time{},
	)
	require.NoError(t, err)

	signer := testauth.LoadTestSigner(t, adminPriv, adminPub, issuer)

	bootstrapPub, _ := newKeyPair(t)
	invite, err := signer.IssueInviteToken(
		nil,
		[]*admissionv1.BootstrapPeer{{PeerPub: bootstrapPub, Addrs: []string{"127.0.0.1:60611"}}},
		now, time.Hour, 0, nil,
	)
	require.NoError(t, err)

	subjectPub, _ := newKeyPair(t)
	require.NoError(t, auth.VerifyInviteToken(invite, subjectPub, now))
}

func TestInviteTokenSubjectBoundMismatch(t *testing.T) {
	adminPub, adminPriv := newKeyPair(t)

	now := time.Now()
	issuer, err := auth.IssueDelegationCert(
		adminPriv, nil, adminPub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(time.Hour), time.Time{},
	)
	require.NoError(t, err)

	signer := testauth.LoadTestSigner(t, adminPriv, adminPub, issuer)

	bootstrapPub, _ := newKeyPair(t)
	boundSubject, _ := newKeyPair(t)
	invite, err := signer.IssueInviteToken(
		boundSubject,
		[]*admissionv1.BootstrapPeer{{PeerPub: bootstrapPub, Addrs: []string{"127.0.0.1:60611"}}},
		now, time.Hour, 0, nil,
	)
	require.NoError(t, err)

	otherSubject, _ := newKeyPair(t)
	require.ErrorContains(t, auth.VerifyInviteToken(invite, otherSubject, now), "subject mismatch")
}

func TestInviteConsumer(t *testing.T) {
	initialState := []*statev1.ConsumedInvite{
		{TokenId: "already-used-1", ExpiryUnix: mockTime.Add(10 * time.Minute).Unix()},
		{TokenId: "expired-and-gone", ExpiryUnix: mockTime.Add(-10 * time.Minute).Unix()},
	}

	consumer := auth.NewInviteConsumer(initialState)

	t.Run("reject already consumed", func(t *testing.T) {
		token := &admissionv1.InviteToken{
			Claims: &admissionv1.InviteTokenClaims{
				TokenId:       "already-used-1",
				ExpiresAtUnix: mockTime.Add(10 * time.Minute).Unix(),
			},
		}

		ok, err := consumer.TryConsume(token, mockTime)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("accept fresh and clean up expired", func(t *testing.T) {
		freshID := uuid.NewString()
		token := &admissionv1.InviteToken{
			Claims: &admissionv1.InviteTokenClaims{
				TokenId:       freshID,
				ExpiresAtUnix: mockTime.Add(5 * time.Minute).Unix(),
			},
		}

		ok, err := consumer.TryConsume(token, mockTime)
		require.NoError(t, err)
		require.True(t, ok)

		exported := consumer.Export()
		stateMap := make(map[string]bool)
		for _, e := range exported {
			stateMap[e.TokenId] = true
		}
		require.True(t, stateMap[freshID])
		require.False(t, stateMap["expired-and-gone"])
	})
}

func TestInviteConsumer_ExportAndLoad(t *testing.T) {
	adminPub, adminPriv := newKeyPair(t)

	now := time.Now()
	issuer, err := auth.IssueDelegationCert(
		adminPriv, nil, adminPub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(5*365*24*time.Hour), time.Time{},
	)
	require.NoError(t, err)

	signer := testauth.LoadTestSigner(t, adminPriv, adminPub, issuer)
	consumer := auth.NewInviteConsumer(nil)

	bootstrapPub, _ := newKeyPair(t)
	bootstrapPeers := []*admissionv1.BootstrapPeer{{PeerPub: bootstrapPub, Addrs: []string{"127.0.0.1:60611"}}}

	invite1, err := signer.IssueInviteToken(nil, bootstrapPeers, now, time.Hour, 0, nil)
	require.NoError(t, err)
	invite2, err := signer.IssueInviteToken(nil, bootstrapPeers, now, time.Hour, 0, nil)
	require.NoError(t, err)

	ok, err := consumer.TryConsume(invite1, now)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = consumer.TryConsume(invite2, now)
	require.NoError(t, err)
	require.True(t, ok)

	exported := consumer.Export()
	require.Len(t, exported, 2)

	consumer2 := auth.NewInviteConsumer(exported)

	ok, err = consumer2.TryConsume(invite1, now)
	require.NoError(t, err)
	require.False(t, ok, "invite1 should be rejected by restored consumer")

	ok, err = consumer2.TryConsume(invite2, now)
	require.NoError(t, err)
	require.False(t, ok, "invite2 should be rejected by restored consumer")
}

func TestCertAttributesRoundTrip(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)

	attrs, err := structpb.NewStruct(map[string]any{
		"role":  "worker",
		"team":  "infra",
		"count": float64(42),
	})
	require.NoError(t, err)

	caps := auth.LeafCapabilities()
	caps.Attributes = attrs

	cert, err := auth.IssueDelegationCert(rootPriv, nil, rootPub, caps, now, now.Add(24*time.Hour), time.Time{})
	require.NoError(t, err)

	require.NoError(t, auth.VerifyDelegationCert(cert, rootPub, now, rootPub))

	got := cert.GetClaims().GetCapabilities().GetAttributes()
	require.NotNil(t, got)
	require.Equal(t, "worker", got.GetFields()["role"].GetStringValue())
	require.Equal(t, "infra", got.GetFields()["team"].GetStringValue())
	require.Equal(t, float64(42), got.GetFields()["count"].GetNumberValue())

	raw, err := proto.Marshal(cert)
	require.NoError(t, err)
	restored := &admissionv1.DelegationCert{}
	require.NoError(t, proto.Unmarshal(raw, restored))
	require.NoError(t, auth.VerifyDelegationCert(restored, rootPub, now, rootPub))

	restoredAttrs := restored.GetClaims().GetCapabilities().GetAttributes()
	require.Equal(t, "worker", restoredAttrs.GetFields()["role"].GetStringValue())
}

func TestInviteAttributesTransfer(t *testing.T) {
	dir := testIdentityDir(t)
	nodePub, nodePriv := newKeyPair(t)
	now := time.Now()

	_, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour)
	require.NoError(t, err)

	signer, err := auth.NewDelegationSigner(dir, nodePriv)
	require.NoError(t, err)

	attrs, err := structpb.NewStruct(map[string]any{"role": "relay"})
	require.NoError(t, err)

	subjectPub, _ := newKeyPair(t)
	token, err := signer.IssueJoinToken(subjectPub, nil, now, time.Hour, 24*time.Hour, time.Time{}, attrs)
	require.NoError(t, err)

	memberCert := token.GetClaims().GetMemberCert()
	got := memberCert.GetClaims().GetCapabilities().GetAttributes()
	require.NotNil(t, got)
	require.Equal(t, "relay", got.GetFields()["role"].GetStringValue())
}

func TestValidateAttributesSizeLimit(t *testing.T) {
	require.NoError(t, auth.ValidateAttributes(nil))

	small, err := structpb.NewStruct(map[string]any{"k": "v"})
	require.NoError(t, err)
	require.NoError(t, auth.ValidateAttributes(small))

	big := make(map[string]any)
	for i := range 500 {
		big[fmt.Sprintf("key-%04d", i)] = "a]long-value-that-adds-up-to-exceed-the-4kb-limit-easily"
	}
	bigStruct, err := structpb.NewStruct(big)
	require.NoError(t, err)
	require.ErrorContains(t, auth.ValidateAttributes(bigStruct), "attributes too large")
}
