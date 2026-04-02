package auth_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

var mockTime = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// testPollenDir creates a temp pollen directory with the keys subdirectory,
// mirroring what plnfs.Provision creates in production.
func testPollenDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "keys"), 0o700))
	return dir
}

func newKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

func loadTestSigner(t *testing.T, priv ed25519.PrivateKey, rootPub ed25519.PublicKey, issuer *admissionv1.DelegationCert) *auth.DelegationSigner {
	t.Helper()
	dir := testPollenDir(t)
	require.NoError(t, auth.SaveNodeCredentials(dir, auth.NewNodeCredentials(rootPub, issuer)))
	signer, err := auth.NewDelegationSigner(dir, priv, 24*time.Hour)
	require.NoError(t, err)
	return signer
}

func issueRootJoinToken(t *testing.T, rootPriv ed25519.PrivateKey, rootPub, subject ed25519.PublicKey, now time.Time, membershipTTL time.Duration, accessDeadline time.Time) *admissionv1.JoinToken {
	t.Helper()
	issuer, err := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(membershipTTL), time.Time{})
	require.NoError(t, err)
	signer := loadTestSigner(t, rootPriv, rootPub, issuer)
	token, err := signer.IssueJoinToken(subject, nil, now, time.Hour, membershipTTL, accessDeadline)
	require.NoError(t, err)
	return token
}

func invalidateSignature(sig []byte) []byte {
	bad := make([]byte, len(sig))
	copy(bad, sig)
	bad[0] ^= 0xFF
	return bad
}

// --- Credential persistence ---

func TestLoadNodeCredentials_ExpiredCertStillLoads(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	nodePub, _ := newKeyPair(t)

	now := time.Now()
	expiredCert, err := auth.IssueDelegationCert(
		rootPriv, nil, nodePub, auth.LeafCapabilities(),
		now.Add(-48*time.Hour), now.Add(-24*time.Hour), time.Time{},
	)
	require.NoError(t, err)

	pollenDir := testPollenDir(t)
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

	pollenDir := testPollenDir(t)
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

	pollenDir := testPollenDir(t)
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

	pollenDir := testPollenDir(t)
	require.NoError(t, auth.SaveNodeCredentials(pollenDir, auth.NewNodeCredentials(rootPub, expiredCert)))

	token := issueRootJoinToken(t, rootPriv, rootPub, nodePub, now, 24*time.Hour, time.Time{})
	verified, err := auth.VerifyJoinToken(token, nodePub, now)
	require.NoError(t, err)

	creds, err := auth.EnrollNodeCredentials(pollenDir, nodePub, token, now)
	require.NoError(t, err)
	require.True(t, proto.Equal(verified.Cert, creds.Cert()), "expired cert should be replaced")
}

// --- Cert properties ---

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

// --- Delegation cert verification ---

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

// --- Signer and token issuance ---

func TestDelegationSignerAndTokens(t *testing.T) {
	dir := testPollenDir(t)
	nodePub, nodePriv := newKeyPair(t)
	now := time.Now()

	_, err := auth.EnsureLocalRootCredentials(dir, nodePub, now, 24*time.Hour, 24*time.Hour)
	require.NoError(t, err)

	signer, err := auth.NewDelegationSigner(dir, nodePriv, 24*time.Hour)
	require.NoError(t, err)
	require.True(t, signer.IsRoot())

	t.Run("invite token", func(t *testing.T) {
		bootstrap := []*admissionv1.BootstrapPeer{{PeerPub: nodePub, Addrs: []string{"127.0.0.1:60611"}}}
		invite, err := signer.IssueInviteToken(nodePub, bootstrap, now, 10*time.Minute, 24*time.Hour)
		require.NoError(t, err)

		require.NoError(t, auth.VerifyInviteToken(invite, nodePub, now))
		require.ErrorContains(t, auth.VerifyInviteToken(invite, nodePub, now.Add(20*time.Minute)), "invite token expired")

		invite.Signature = invalidateSignature(invite.Signature)
		require.ErrorContains(t, auth.VerifyInviteToken(invite, nodePub, now), "invite token signature invalid")
	})

	t.Run("join token", func(t *testing.T) {
		join, err := signer.IssueJoinToken(nodePub, nil, now, 10*time.Minute, 24*time.Hour, time.Time{})
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

		delegatedSigner := loadTestSigner(t, delegatedPriv, nodePub, issuer)
		subjectPub, _ := newKeyPair(t)

		token, err := delegatedSigner.IssueJoinToken(subjectPub, nil, now, time.Hour, 4*time.Hour, time.Time{})
		require.NoError(t, err)

		verified, err := auth.VerifyJoinToken(token, subjectPub, now)
		require.NoError(t, err)
		require.EqualValues(t, delegatedPub, verified.Cert.GetClaims().GetIssuerPub())
	})
}

// --- Invite token specifics ---

func TestInviteTokenOpenSubject(t *testing.T) {
	adminPub, adminPriv := newKeyPair(t)

	now := time.Now()
	issuer, err := auth.IssueDelegationCert(
		adminPriv, nil, adminPub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(time.Hour), time.Time{},
	)
	require.NoError(t, err)

	signer := loadTestSigner(t, adminPriv, adminPub, issuer)

	bootstrapPub, _ := newKeyPair(t)
	invite, err := signer.IssueInviteToken(
		nil,
		[]*admissionv1.BootstrapPeer{{PeerPub: bootstrapPub, Addrs: []string{"127.0.0.1:60611"}}},
		now, time.Hour, 0,
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

	signer := loadTestSigner(t, adminPriv, adminPub, issuer)

	bootstrapPub, _ := newKeyPair(t)
	boundSubject, _ := newKeyPair(t)
	invite, err := signer.IssueInviteToken(
		boundSubject,
		[]*admissionv1.BootstrapPeer{{PeerPub: bootstrapPub, Addrs: []string{"127.0.0.1:60611"}}},
		now, time.Hour, 0,
	)
	require.NoError(t, err)

	otherSubject, _ := newKeyPair(t)
	require.ErrorContains(t, auth.VerifyInviteToken(invite, otherSubject, now), "subject mismatch")
}

// --- Invite consumer ---

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

	signer := loadTestSigner(t, adminPriv, adminPub, issuer)
	consumer := auth.NewInviteConsumer(nil)

	bootstrapPub, _ := newKeyPair(t)
	bootstrapPeers := []*admissionv1.BootstrapPeer{{PeerPub: bootstrapPub, Addrs: []string{"127.0.0.1:60611"}}}

	invite1, err := signer.IssueInviteToken(nil, bootstrapPeers, now, time.Hour, 0)
	require.NoError(t, err)
	invite2, err := signer.IssueInviteToken(nil, bootstrapPeers, now, time.Hour, 0)
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
