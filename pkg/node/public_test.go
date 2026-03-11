package node

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/stretchr/testify/require"
)

func newMinimalNode(t *testing.T, bootstrapPublic bool) *Node {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	adminPub, adminPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	trust := auth.NewTrustBundle(adminPub)
	cert, err := auth.IssueDelegationCert(adminPriv, nil, trust.GetClusterId(), pub, auth.LeafCapabilities(), time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), time.Time{})
	require.NoError(t, err)

	creds := &auth.NodeCredentials{Trust: trust, Cert: cert}
	stateStore, err := store.Load(t.TempDir(), pub)
	require.NoError(t, err)

	conf := &Config{
		Port:             0,
		AdvertisedIPs:    []string{"127.0.0.1"},
		GossipInterval:   time.Second,
		PeerTickInterval: time.Second,
		TLSIdentityTTL:   config.CertTTLs{}.TLSIdentityTTL(),
		MembershipTTL:    config.CertTTLs{}.MembershipTTL(),
		BootstrapPublic:  bootstrapPublic,
	}

	n, err := New(conf, priv, creds, stateStore, peer.NewStore(), t.TempDir())
	require.NoError(t, err)
	return n
}

func TestBootstrapPublicSetsAccessibleImmediately(t *testing.T) {
	n := newMinimalNode(t, true)
	require.True(t, n.store.IsPubliclyAccessible(n.store.LocalID))
}

func TestBootstrapPublicFalseIsNotAccessible(t *testing.T) {
	n := newMinimalNode(t, false)
	require.False(t, n.store.IsPubliclyAccessible(n.store.LocalID))
}
