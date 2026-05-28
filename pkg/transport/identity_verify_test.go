// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package transport_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/stretchr/testify/require"
)

func keyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

// sessionCertFor builds a root-signed grant for leafPub and a session
// minted by sessionPriv, returns the cluster root pub and the DER leaf
// the counterparty would present (the cert key is leafCertPriv).
func sessionCertFor(t *testing.T, now time.Time, sessionTTL time.Duration, leafPub ed25519.PublicKey, leafGrantPriv, leafCertPriv ed25519.PrivateKey) (rootPub, der []byte) {
	t.Helper()
	adminPub, adminPriv := keyPair(t)
	grant, err := identity.IssueGrant(adminPriv, nil, leafPub,
		identity.PublisherCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	session, err := identity.MintSession(grant, leafGrantPriv, now, sessionTTL)
	require.NoError(t, err)
	cert, err := transport.GenerateIdentityCert(leafCertPriv, session, time.Hour)
	require.NoError(t, err)
	return adminPub, cert.Certificate[0]
}

func TestVerifyDelegatedCounterparty(t *testing.T) {
	now := time.Now()
	leafPub, leafPriv := keyPair(t)

	t.Run("valid session admitted", func(t *testing.T) {
		root, der := sessionCertFor(t, now, time.Hour, leafPub, leafPriv, leafPriv)
		require.NoError(t, transport.VerifyDelegatedCounterparty(root, nil)([][]byte{der}, nil))
	})

	t.Run("wrong root rejected", func(t *testing.T) {
		_, der := sessionCertFor(t, now, time.Hour, leafPub, leafPriv, leafPriv)
		other, _ := keyPair(t)
		require.Error(t, transport.VerifyDelegatedCounterparty(other, nil)([][]byte{der}, nil))
	})

	t.Run("denied subject rejected", func(t *testing.T) {
		root, der := sessionCertFor(t, now, time.Hour, leafPub, leafPriv, leafPriv)
		denied := func(p []byte) bool { return string(p) == string(leafPub) }
		err := transport.VerifyDelegatedCounterparty(root, denied)([][]byte{der}, nil)
		require.ErrorContains(t, err, "rejected")
	})

	t.Run("leaf key not bound to grant subject", func(t *testing.T) {
		// The cert is keyed by a different pair than the session's grant
		// subject: the TLS-leaf-to-grant binding must reject it.
		_, otherCertPriv := keyPair(t)
		root, der := sessionCertFor(t, now, time.Hour, leafPub, leafPriv, otherCertPriv)
		require.Error(t, transport.VerifyDelegatedCounterparty(root, nil)([][]byte{der}, nil))
	})

	t.Run("expired session rejected", func(t *testing.T) {
		// Session minted two hours ago with a one-minute window: stale,
		// so verification at the current time must reject it.
		root, der := sessionCertFor(t, now.Add(-2*time.Hour), time.Minute, leafPub, leafPriv, leafPriv)
		require.Error(t, transport.VerifyDelegatedCounterparty(root, nil)([][]byte{der}, nil))
	})

	t.Run("missing session extension rejected", func(t *testing.T) {
		root, _ := keyPair(t)
		bare, err := transport.GenerateIdentityCert(leafPriv, nil, time.Hour)
		require.NoError(t, err)
		err = transport.VerifyDelegatedCounterparty(root, nil)([][]byte{bare.Certificate[0]}, nil)
		require.ErrorContains(t, err, "session extension")
	})

	t.Run("no peer certificate rejected", func(t *testing.T) {
		root, _ := keyPair(t)
		require.Error(t, transport.VerifyDelegatedCounterparty(root, nil)(nil, nil))
	})
}
