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
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func newMinimalNode(t *testing.T, bootstrapPublic bool) *Node {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	adminPub, adminPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	trust := auth.NewTrustBundle(adminPub)
	cert, err := auth.IssueMembershipCert(adminPriv, trust.GetClusterId(), pub, time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), config.CertTTLs{}.AdminTTL())
	require.NoError(t, err)

	creds := &auth.NodeCredentials{Trust: trust, Cert: cert}
	stateStore, err := store.Load(t.TempDir(), pub, trust)
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

func fakePeerKey(t *testing.T) types.PeerKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return types.PeerKeyFromBytes(pub)
}

func TestEvaluatePublicAccessibilityRequiresTwoPeers(t *testing.T) {
	n := newMinimalNode(t, false)

	peerA := fakePeerKey(t)
	peerB := fakePeerKey(t)
	addr := "1.2.3.4:60611"

	// One peer is not enough.
	n.publicConfirmations[peerA] = publicConfirmation{addr: addr, at: time.Now()}
	n.evaluatePublicAccessibility()
	require.False(t, n.store.IsPubliclyAccessible(n.store.LocalID))

	// Two peers confirming the same address is enough.
	n.publicConfirmations[peerB] = publicConfirmation{addr: addr, at: time.Now()}
	n.evaluatePublicAccessibility()
	require.True(t, n.store.IsPubliclyAccessible(n.store.LocalID))
}

func TestEvaluatePublicAccessibilityDifferentAddrs(t *testing.T) {
	n := newMinimalNode(t, false)

	peerA := fakePeerKey(t)
	peerB := fakePeerKey(t)

	// Two peers with different addresses should NOT trigger public.
	n.publicConfirmations[peerA] = publicConfirmation{addr: "1.2.3.4:60611", at: time.Now()}
	n.publicConfirmations[peerB] = publicConfirmation{addr: "5.6.7.8:60611", at: time.Now()}
	n.evaluatePublicAccessibility()
	require.False(t, n.store.IsPubliclyAccessible(n.store.LocalID))
}

func TestEvaluatePublicAccessibilityExpiresStale(t *testing.T) {
	n := newMinimalNode(t, false)

	peerA := fakePeerKey(t)
	peerB := fakePeerKey(t)
	addr := "1.2.3.4:60611"

	// Set up two confirmations, but one is stale.
	n.publicConfirmations[peerA] = publicConfirmation{addr: addr, at: time.Now().Add(-publicConfirmTTL - time.Minute)}
	n.publicConfirmations[peerB] = publicConfirmation{addr: addr, at: time.Now()}
	n.evaluatePublicAccessibility()
	require.False(t, n.store.IsPubliclyAccessible(n.store.LocalID))

	// Stale entry should have been cleaned up.
	_, exists := n.publicConfirmations[peerA]
	require.False(t, exists)
}

func TestEvaluatePublicAccessibilityClearsWhenConfirmationsExpire(t *testing.T) {
	n := newMinimalNode(t, false)

	peerA := fakePeerKey(t)
	peerB := fakePeerKey(t)
	addr := "1.2.3.4:60611"

	// First: set as publicly accessible.
	n.publicConfirmations[peerA] = publicConfirmation{addr: addr, at: time.Now()}
	n.publicConfirmations[peerB] = publicConfirmation{addr: addr, at: time.Now()}
	n.evaluatePublicAccessibility()
	require.True(t, n.store.IsPubliclyAccessible(n.store.LocalID))

	// Expire both confirmations.
	n.publicConfirmations[peerA] = publicConfirmation{addr: addr, at: time.Now().Add(-publicConfirmTTL - time.Minute)}
	n.publicConfirmations[peerB] = publicConfirmation{addr: addr, at: time.Now().Add(-publicConfirmTTL - time.Minute)}
	n.evaluatePublicAccessibility()
	require.False(t, n.store.IsPubliclyAccessible(n.store.LocalID))
}

func TestBootstrapPublicSetsAccessibleImmediately(t *testing.T) {
	n := newMinimalNode(t, true)
	require.True(t, n.store.IsPubliclyAccessible(n.store.LocalID))
}

func TestBootstrapPublicSkipsEvaluation(t *testing.T) {
	n := newMinimalNode(t, true)

	// Even with no confirmations, bootstrap-public nodes stay public.
	n.evaluatePublicAccessibility()
	require.True(t, n.store.IsPubliclyAccessible(n.store.LocalID))
}
