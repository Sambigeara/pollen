// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/sambigeara/pollen/internal/testauth"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestAdmitMeshCert(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	const window = time.Hour

	tests := []struct {
		name   string
		chk    auth.CertCheck
		window time.Duration
		ok     bool
	}{
		{
			name: "ok admitted",
			chk:  auth.CertCheck{Status: auth.CertStatusOK},
			ok:   true,
		},
		{
			name:   "needs-renewal within reconnect window admitted",
			chk:    auth.CertCheck{Status: auth.CertStatusNeedsRenewal, NotAfter: now.Add(-30 * time.Minute), AccessDeadline: now.Add(29 * 24 * time.Hour)},
			window: window,
			ok:     true,
		},
		{
			name:   "needs-renewal beyond reconnect window rejected",
			chk:    auth.CertCheck{Status: auth.CertStatusNeedsRenewal, NotAfter: now.Add(-2 * time.Hour), AccessDeadline: now.Add(29 * 24 * time.Hour)},
			window: window,
			ok:     false,
		},
		{
			name:   "needs-renewal with no reconnect window rejected",
			chk:    auth.CertCheck{Status: auth.CertStatusNeedsRenewal, NotAfter: now.Add(-time.Minute), AccessDeadline: now.Add(29 * 24 * time.Hour)},
			window: 0,
			ok:     false,
		},
		{
			name:   "access_deadline ceiling passed rejected even within window",
			chk:    auth.CertCheck{Status: auth.CertStatusExpired, NotAfter: now.Add(-30 * time.Minute), AccessDeadline: now.Add(-time.Minute)},
			window: window,
			ok:     false,
		},
		{
			name:   "legacy no-ceiling expired within window admitted",
			chk:    auth.CertCheck{Status: auth.CertStatusExpired, NotAfter: now.Add(-30 * time.Minute)},
			window: window,
			ok:     true,
		},
		{
			name:   "legacy no-ceiling expired beyond window rejected",
			chk:    auth.CertCheck{Status: auth.CertStatusExpired, NotAfter: now.Add(-2 * time.Hour)},
			window: window,
			ok:     false,
		},
		{
			name:   "revoked rejected",
			chk:    auth.CertCheck{Status: auth.CertStatusRevoked, NotAfter: now.Add(-time.Minute)},
			window: window,
			ok:     false,
		},
		{
			name:   "invalid chain rejected",
			chk:    auth.CertCheck{Status: auth.CertStatusInvalidChain},
			window: window,
			ok:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := admitMeshCert(tc.chk, tc.window, now)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestVerifyDelegatedCounterpartyStrict(t *testing.T) {
	cluster := testauth.NewClusterAuth(t)
	rootPub := cluster.CredsFor(t, mustPub(t)).RootPub()
	signer := cluster.Signer(t)
	now := time.Now()

	rawCertsFor := func(t *testing.T, notAfter, accessDeadline time.Time) [][]byte {
		t.Helper()
		pub, priv := mustKeyPair(t)
		notBefore := notAfter.Add(-2 * time.Hour)
		cert, err := signer.IssueMemberCert(pub, auth.LeafCapabilities(), notBefore, notAfter, accessDeadline)
		require.NoError(t, err)
		id, err := GenerateIdentityCert(priv, cert, time.Hour)
		require.NoError(t, err)
		return id.Certificate
	}

	t.Run("valid cert admitted", func(t *testing.T) {
		verify := VerifyDelegatedCounterparty(rootPub, nil)
		require.NoError(t, verify(rawCertsFor(t, now.Add(time.Hour), now.Add(30*24*time.Hour)), nil))
	})

	t.Run("needs-renewal admitted at handshake (interceptor gates RPC scope)", func(t *testing.T) {
		// Past not_after but within access_deadline: the handshake must
		// admit so the caller can reach RenewCert. The control service
		// interceptor restricts such callers to the RenewCert RPC.
		verify := VerifyDelegatedCounterparty(rootPub, nil)
		require.NoError(t, verify(rawCertsFor(t, now.Add(-time.Hour), now.Add(30*24*time.Hour)), nil))
	})

	t.Run("past access_deadline rejected", func(t *testing.T) {
		verify := VerifyDelegatedCounterparty(rootPub, nil)
		require.Error(t, verify(rawCertsFor(t, now.Add(-2*time.Hour), now.Add(-time.Hour)), nil))
	})

	t.Run("denied subject rejected", func(t *testing.T) {
		verify := VerifyDelegatedCounterparty(rootPub, func([]byte) bool { return true })
		require.Error(t, verify(rawCertsFor(t, now.Add(time.Hour), now.Add(30*24*time.Hour)), nil))
	})
}

func mustKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

func mustPub(t *testing.T) ed25519.PublicKey {
	t.Helper()
	pub, _ := mustKeyPair(t)
	return pub
}
