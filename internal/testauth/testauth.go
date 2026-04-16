package testauth

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/stretchr/testify/require"
)

type ClusterAuth struct {
	adminPriv ed25519.PrivateKey
	rootPub   ed25519.PublicKey
}

func NewClusterAuth(t testing.TB) *ClusterAuth { //nolint:thelper
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return &ClusterAuth{adminPriv: priv, rootPub: pub}
}

func (c *ClusterAuth) CredsFor(t testing.TB, subject ed25519.PublicKey) *auth.NodeCredentials { //nolint:thelper
	t.Helper()
	cert, err := auth.IssueDelegationCert(c.adminPriv, nil, subject, auth.LeafCapabilities(), time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), time.Time{}) //nolint:mnd
	require.NoError(t, err)
	return auth.NewNodeCredentials(c.rootPub, cert)
}

func (c *ClusterAuth) Signer(t testing.TB) *auth.DelegationSigner { //nolint:thelper
	t.Helper()
	return c.newSigner(t, 365*24*time.Hour) //nolint:mnd
}

func (c *ClusterAuth) TokenFor(t testing.TB, subject, bootstrapPub ed25519.PublicKey, bootstrapAddr string) *admissionv1.JoinToken { //nolint:thelper
	t.Helper()
	now := time.Now()
	membershipTTL := config.DefaultMembershipTTL
	signer := c.newSigner(t, membershipTTL)
	token, err := signer.IssueJoinToken(subject, []*admissionv1.BootstrapPeer{{
		PeerPub: bootstrapPub,
		Addrs:   []string{bootstrapAddr},
	}}, now, time.Hour, membershipTTL, time.Time{}, nil)
	require.NoError(t, err)
	return token
}

func (c *ClusterAuth) newSigner(t testing.TB, notAfter time.Duration) *auth.DelegationSigner { //nolint:thelper
	t.Helper()
	issuer, err := auth.IssueDelegationCert(c.adminPriv, nil, c.rootPub, auth.FullCapabilities(), time.Now().Add(-time.Minute), time.Now().Add(notAfter), time.Time{})
	require.NoError(t, err)
	return LoadTestSigner(t, c.adminPriv, c.rootPub, issuer)
}

func LoadTestSigner(t testing.TB, priv ed25519.PrivateKey, rootPub ed25519.PublicKey, issuer *admissionv1.DelegationCert) *auth.DelegationSigner { //nolint:thelper
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, auth.SaveNodeCredentials(dir, auth.NewNodeCredentials(rootPub, issuer)))
	signer, err := auth.NewDelegationSigner(dir, priv, 24*time.Hour) //nolint:mnd
	require.NoError(t, err)
	return signer
}
