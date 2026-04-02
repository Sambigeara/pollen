//go:build integration

package cluster

import (
	"crypto/ed25519"
	"crypto/x509"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestClusterAuth_NodeCredentials(t *testing.T) {
	ca := NewClusterAuth(t)

	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pub := priv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	tlsCert, dc := ca.NodeCredentials(priv)

	// TLS cert should be valid.
	require.NotEmpty(t, tlsCert.Certificate)
	require.NotNil(t, tlsCert.Leaf)
	require.Equal(t, priv, tlsCert.PrivateKey)

	// Delegation cert should be valid.
	require.NotNil(t, dc)
	err = auth.VerifyDelegationCert(dc, ca.RootPub(), time.Now(), pub)
	require.NoError(t, err)

	// TLS cert should contain extensions beyond standard ones (delegation cert).
	leaf, err := x509.ParseCertificate(tlsCert.Certificate[0])
	require.NoError(t, err)
	require.NotEmpty(t, leaf.Extensions, "TLS cert must contain extensions (including delegation cert)")
}

func TestClusterAuth_TwoNodesSameTrust(t *testing.T) {
	ca := NewClusterAuth(t)

	_, privA, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, privB, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	_, dcA := ca.NodeCredentials(privA)
	_, dcB := ca.NodeCredentials(privB)

	pubA := privA.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	pubB := privB.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	require.NoError(t, auth.VerifyDelegationCert(dcA, ca.RootPub(), time.Now(), pubA))
	require.NoError(t, auth.VerifyDelegationCert(dcB, ca.RootPub(), time.Now(), pubB))
}
