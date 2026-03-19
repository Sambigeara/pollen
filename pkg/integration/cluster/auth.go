//go:build integration

package cluster

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/stretchr/testify/require"
)

// ClusterAuth bootstraps cryptographic identity for a test cluster.
type ClusterAuth struct {
	t       testing.TB
	trust   *admissionv1.TrustBundle
	rootKey ed25519.PrivateKey
}

// NewClusterAuth creates a test cluster identity with a root key pair.
func NewClusterAuth(t testing.TB) *ClusterAuth { //nolint:thelper
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pub := priv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	trust := auth.NewTrustBundle(pub)
	return &ClusterAuth{trust: trust, rootKey: priv, t: t}
}

// TrustBundle returns the shared trust bundle for the cluster.
func (ca *ClusterAuth) TrustBundle() *admissionv1.TrustBundle {
	return ca.trust
}

// NodeCredentials mints a TLS certificate (with embedded delegation cert) for a node.
func (ca *ClusterAuth) NodeCredentials(nodePriv ed25519.PrivateKey) (tls.Certificate, *admissionv1.DelegationCert) {
	t := ca.t
	nodePub := nodePriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	now := time.Now()
	dc, err := auth.IssueDelegationCert(
		ca.rootKey,
		nil, // root issues directly
		ca.trust.GetClusterId(),
		nodePub,
		auth.FullCapabilities(),
		now.Add(-time.Minute),
		now.Add(24*time.Hour), //nolint:mnd
		time.Time{},           // no access deadline
	)
	require.NoError(t, err)

	tlsCert, err := transport.GenerateIdentityCert(nodePriv, dc, 24*time.Hour) //nolint:mnd
	require.NoError(t, err)

	return tlsCert, dc
}
