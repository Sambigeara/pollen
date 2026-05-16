// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/stretchr/testify/require"
)

// TestVerifyMeshPeerCertLeafBinding covers the mesh-QUIC verify
// callback directly: the leaf ed25519 key derived from the cert must be
// the session grant's subject, the grant must chain to the configured
// root, and a stale or wrong-root session is refused.
func TestVerifyMeshPeerCertLeafBinding(t *testing.T) {
	now := time.Now()
	leafPub, leafPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	adminPub, adminPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	grant, err := identity.IssueGrant(adminPriv, nil, leafPub,
		identity.PublisherCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)

	mint := func(certPriv ed25519.PrivateKey, ttl time.Duration, at time.Time) []byte {
		t.Helper()
		session, err := identity.MintSession(grant, leafPriv, at, ttl)
		require.NoError(t, err)
		cert, err := GenerateIdentityCert(certPriv, session, time.Hour)
		require.NoError(t, err)
		return cert.Certificate[0]
	}

	t.Run("bound leaf admitted", func(t *testing.T) {
		der := mint(leafPriv, time.Hour, now)
		fn := verifyMeshPeerCert(verifyMeshPeerOpts{rootPub: adminPub})
		require.NoError(t, fn([][]byte{der}, nil))
	})

	t.Run("leaf key not the grant subject is rejected", func(t *testing.T) {
		_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		der := mint(otherPriv, time.Hour, now)
		fn := verifyMeshPeerCert(verifyMeshPeerOpts{rootPub: adminPub})
		require.Error(t, fn([][]byte{der}, nil))
	})

	t.Run("wrong root is rejected", func(t *testing.T) {
		der := mint(leafPriv, time.Hour, now)
		otherRoot, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		fn := verifyMeshPeerCert(verifyMeshPeerOpts{rootPub: otherRoot})
		require.Error(t, fn([][]byte{der}, nil))
	})

	t.Run("stale session is rejected", func(t *testing.T) {
		der := mint(leafPriv, time.Minute, now.Add(-2*time.Hour))
		fn := verifyMeshPeerCert(verifyMeshPeerOpts{rootPub: adminPub})
		require.Error(t, fn([][]byte{der}, nil))
	})
}
