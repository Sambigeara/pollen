// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/stretchr/testify/require"
)

// TestServerCertProviderRotates pins control-TLS rotation: the listener is
// installed once, so the per-handshake cert must re-mint before the
// embedded Session expires (see ServerCertProvider).
func TestServerCertProviderRotates(t *testing.T) {
	signPub, signPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	// Root self-issue: issuer == subject == signing key, so the credentials
	// can mint sessions locally with no issuer round-trip.
	grant, err := identity.IssueGrant(signPriv, nil, signPub,
		identity.FullCapabilities(), identity.UnlimitedBudget(),
		time.Now().Add(-time.Hour), time.Time{}, false)
	require.NoError(t, err)
	creds := identity.NewCredentials(signPub, signPriv, grant)

	ttl := time.Hour
	p := NewServerCertProvider(creds, signPriv, ttl)

	base := time.Now()
	p.now = func() time.Time { return base }

	first, err := p.GetCertificate(nil)
	require.NoError(t, err)
	require.NotNil(t, first)

	cached, err := p.GetCertificate(nil)
	require.NoError(t, err)
	require.Same(t, first, cached, "cert must be cached within its refresh window")

	p.now = func() time.Time { return base.Add(ttl/2 + time.Minute) }
	rotated, err := p.GetCertificate(nil)
	require.NoError(t, err)
	require.NotSame(t, first, rotated, "cert must rotate before the embedded session expires")
}
