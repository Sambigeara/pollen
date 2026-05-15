// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestCheckCertStatusMatrix(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	subjPub, _ := newKeyPair(t)
	otherPub, _ := newKeyPair(t)
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)

	const day = 24 * time.Hour

	tests := []struct {
		name           string
		notBefore      time.Time
		notAfter       time.Time
		accessDeadline time.Time
		rootPub        []byte
		expectSubject  []byte
		denied         auth.DenyChecker
		want           auth.CertStatus
		canAuth        bool
		canRenew       bool
	}{
		{
			name:           "valid",
			notBefore:      now.Add(-time.Hour),
			notAfter:       now.Add(time.Hour),
			accessDeadline: now.Add(30 * day),
			rootPub:        rootPub,
			want:           auth.CertStatusOK,
			canAuth:        true,
			canRenew:       true,
		},
		{
			name:           "not yet valid",
			notBefore:      now.Add(time.Hour),
			notAfter:       now.Add(2 * time.Hour),
			accessDeadline: now.Add(30 * day),
			rootPub:        rootPub,
			want:           auth.CertStatusNotYetValid,
		},
		{
			name:           "past not_after within access_deadline is renewable",
			notBefore:      now.Add(-2 * time.Hour),
			notAfter:       now.Add(-time.Hour),
			accessDeadline: now.Add(29 * day),
			rootPub:        rootPub,
			want:           auth.CertStatusNeedsRenewal,
			canRenew:       true,
		},
		{
			name:           "past access_deadline is expired",
			notBefore:      now.Add(-30 * day),
			notAfter:       now.Add(-29 * day),
			accessDeadline: now.Add(-time.Hour),
			rootPub:        rootPub,
			want:           auth.CertStatusExpired,
		},
		{
			// not_after just passed but inside the skew grace window: the
			// cert must still authenticate, not flip to renewable.
			name:           "not_after within skew grace still ok",
			notBefore:      now.Add(-2 * time.Hour),
			notAfter:       now.Add(-30 * time.Second),
			accessDeadline: now.Add(30 * day),
			rootPub:        rootPub,
			want:           auth.CertStatusOK,
			canAuth:        true,
			canRenew:       true,
		},
		{
			// access_deadline just passed but inside the skew grace
			// window: the cert is still renewable, not yet hard-expired.
			name:           "access_deadline within skew grace still renewable",
			notBefore:      now.Add(-5 * time.Hour),
			notAfter:       now.Add(-4 * time.Hour),
			accessDeadline: now.Add(-30 * time.Second),
			rootPub:        rootPub,
			want:           auth.CertStatusNeedsRenewal,
			canRenew:       true,
		},
		{
			name:           "legacy cert no access_deadline past not_after is expired not renewable",
			notBefore:      now.Add(-5 * time.Hour),
			notAfter:       now.Add(-time.Hour),
			accessDeadline: time.Time{},
			rootPub:        rootPub,
			want:           auth.CertStatusExpired,
		},
		{
			name:           "legacy cert no access_deadline still valid before not_after",
			notBefore:      now.Add(-time.Hour),
			notAfter:       now.Add(time.Hour),
			accessDeadline: time.Time{},
			rootPub:        rootPub,
			want:           auth.CertStatusOK,
			canAuth:        true,
			canRenew:       true,
		},
		{
			name:           "wrong root",
			notBefore:      now.Add(-time.Hour),
			notAfter:       now.Add(time.Hour),
			accessDeadline: now.Add(30 * day),
			rootPub:        otherPub,
			want:           auth.CertStatusInvalidChain,
		},
		{
			name:           "subject mismatch",
			notBefore:      now.Add(-time.Hour),
			notAfter:       now.Add(time.Hour),
			accessDeadline: now.Add(30 * day),
			rootPub:        rootPub,
			expectSubject:  otherPub,
			want:           auth.CertStatusSubjectMismatch,
		},
		{
			name:           "denied subject",
			notBefore:      now.Add(-time.Hour),
			notAfter:       now.Add(time.Hour),
			accessDeadline: now.Add(30 * day),
			rootPub:        rootPub,
			denied:         func([]byte) bool { return true },
			want:           auth.CertStatusRevoked,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cert, err := auth.IssueDelegationCert(
				rootPriv, nil, subjPub, auth.LeafCapabilities(),
				tc.notBefore, tc.notAfter, tc.accessDeadline,
			)
			require.NoError(t, err)

			got := auth.CheckCert(cert, tc.rootPub, now, tc.expectSubject, tc.denied)
			require.Equal(t, tc.want, got.Status, "reason: %s", got.Reason)
			require.Equal(t, tc.canAuth, got.Status.CanAuthenticate())
			require.Equal(t, tc.canRenew, got.Status.CanRenew())
		})
	}
}

func TestCheckCertChainAncestorDenied(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	adminPub, adminPriv := newKeyPair(t)
	leafPub, _ := newKeyPair(t)
	now := time.Now()

	rootCert, err := auth.IssueDelegationCert(
		rootPriv, nil, rootPub, auth.FullCapabilities(),
		now.Add(-time.Hour), now.Add(24*time.Hour), time.Time{},
	)
	require.NoError(t, err)
	adminCert, err := auth.IssueDelegationCert(
		rootPriv, []*admissionv1.DelegationCert{rootCert}, adminPub, auth.FullCapabilities(),
		now.Add(-time.Hour), now.Add(24*time.Hour), time.Time{},
	)
	require.NoError(t, err)
	leafCert, err := auth.IssueDelegationCert(
		adminPriv, []*admissionv1.DelegationCert{adminCert}, leafPub, auth.LeafCapabilities(),
		now.Add(-time.Hour), now.Add(time.Hour), now.Add(30*24*time.Hour),
	)
	require.NoError(t, err)

	// Leaf itself is not denied, but its issuing admin is. The
	// chain-aware check must still reject the leaf.
	deniedAdmin := auth.DenyChecker(func(sub []byte) bool {
		return string(sub) == string(adminPub)
	})
	got := auth.CheckCert(leafCert, rootPub, now, leafPub, deniedAdmin)
	require.Equal(t, auth.CertStatusRevoked, got.Status, "reason: %s", got.Reason)

	// With no denial the same leaf validates.
	ok := auth.CheckCert(leafCert, rootPub, now, leafPub, nil)
	require.Equal(t, auth.CertStatusOK, ok.Status, "reason: %s", ok.Reason)
}

func TestCheckCertNilCert(t *testing.T) {
	got := auth.CheckCert(nil, nil, time.Now(), nil, nil)
	require.Equal(t, auth.CertStatusInvalidChain, got.Status)
}

func TestCheckCertTamperedSignature(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	subjPub, _ := newKeyPair(t)
	now := time.Now()

	cert, err := auth.IssueDelegationCert(
		rootPriv, nil, subjPub, auth.LeafCapabilities(),
		now.Add(-time.Hour), now.Add(time.Hour), now.Add(30*24*time.Hour),
	)
	require.NoError(t, err)
	cert.Signature = invalidateSignature(cert.GetSignature())

	got := auth.CheckCert(cert, rootPub, now, nil, nil)
	require.Equal(t, auth.CertStatusInvalidChain, got.Status)
	require.NotEmpty(t, got.Reason)
}
