package supervisor

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/stretchr/testify/require"
)

func newMinimalNode(t *testing.T, bootstrapPublic bool) *Supervisor {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	adminPub, adminPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	cert, err := auth.IssueDelegationCert(adminPriv, nil, pub, auth.LeafCapabilities(), time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), time.Time{})
	require.NoError(t, err)

	creds := auth.NewNodeCredentials(adminPub, cert)

	opts := Options{
		SigningKey:       priv,
		PollenDir:        t.TempDir(),
		ListenPort:       0,
		AdvertisedIPs:    []string{"127.0.0.1"},
		GossipInterval:   time.Second,
		PeerTickInterval: time.Second,
		BootstrapPublic:  bootstrapPublic,
	}

	n, err := New(opts, creds, nil)
	require.NoError(t, err)
	return n
}

func TestBootstrapPublicSetsAccessibleImmediately(t *testing.T) {
	n := newMinimalNode(t, true)
	snap := n.store.Snapshot()
	require.True(t, snap.Nodes[snap.LocalID].PubliclyAccessible)
}

func TestBootstrapPublicFalseIsNotAccessible(t *testing.T) {
	n := newMinimalNode(t, false)
	snap := n.store.Snapshot()
	require.False(t, snap.Nodes[snap.LocalID].PubliclyAccessible)
}
