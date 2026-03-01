package mesh_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type meshHarness struct {
	mesh    mesh.Mesh
	peerKey types.PeerKey
	pubKey  ed25519.PublicKey
	port    int
	cancel  context.CancelFunc
}

type clusterAuth struct {
	adminPriv ed25519.PrivateKey
	trust     *admissionv1.TrustBundle
}

func newClusterAuth(t *testing.T) *clusterAuth {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return &clusterAuth{adminPriv: priv, trust: auth.NewTrustBundle(pub)}
}

func (c *clusterAuth) credsFor(t *testing.T, subject ed25519.PublicKey) *auth.NodeCredentials {
	t.Helper()
	cert, err := auth.IssueMembershipCert(c.adminPriv, c.trust.GetClusterId(), subject, time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), config.CertTTLs{}.AdminTTL())
	require.NoError(t, err)
	return &auth.NodeCredentials{Trust: c.trust, Cert: cert}
}

func (c *clusterAuth) tokenFor(t *testing.T, subject ed25519.PublicKey, bootstrap *meshHarness) *admissionv1.JoinToken {
	t.Helper()
	token, err := auth.IssueJoinToken(c.adminPriv, c.trust, subject, []*admissionv1.BootstrapPeer{{
		PeerPub: bootstrap.pubKey,
		Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(bootstrap.port))},
	}}, time.Now(), time.Hour, config.CertTTLs{}.MembershipTTL(), config.CertTTLs{}.AdminTTL())
	require.NoError(t, err)
	return token
}

func (c *clusterAuth) signer(t *testing.T) *auth.AdminSigner {
	t.Helper()
	adminPub, ok := c.adminPriv.Public().(ed25519.PublicKey)
	require.True(t, ok)

	issuer, err := auth.IssueAdminCert(
		c.adminPriv,
		c.trust.GetClusterId(),
		adminPub,
		time.Now().Add(-time.Minute),
		time.Now().Add(365*24*time.Hour),
	)
	require.NoError(t, err)

	consumed, err := config.LoadConsumedInvites(t.TempDir(), time.Now())
	require.NoError(t, err)

	return &auth.AdminSigner{
		Priv:     c.adminPriv,
		Trust:    c.trust,
		Issuer:   issuer,
		Consumed: consumed,
	}
}

func TestJoinWithTokenHappyPath(t *testing.T) {
	cluster := newClusterAuth(t)
	bootstrap := startMeshHarness(t, cluster)
	joiner := startMeshHarness(t, cluster)

	token := cluster.tokenFor(t, joiner.pubKey, bootstrap)

	joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, joiner.mesh.JoinWithToken(joinCtx, token))

	require.Eventually(t, func() bool {
		_, ok := joiner.mesh.GetConn(bootstrap.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

	require.Eventually(t, func() bool {
		_, ok := bootstrap.mesh.GetConn(joiner.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)
}

func TestConnectRejectsCrossClusterPeer(t *testing.T) {
	clusterA := newClusterAuth(t)
	clusterB := newClusterAuth(t)

	a := startMeshHarness(t, clusterA)
	b := startMeshHarness(t, clusterB)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := a.mesh.Connect(ctx, b.peerKey, []*net.UDPAddr{{IP: net.IPv4(127, 0, 0, 1), Port: b.port}})
	require.Error(t, err)

	_, ok := a.mesh.GetConn(b.peerKey)
	require.False(t, ok)
}

func TestJoinWithInviteHappyPath(t *testing.T) {
	cluster := newClusterAuth(t)

	bootstrapPub, bootstrapPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	bootstrapCreds := cluster.credsFor(t, bootstrapPub)
	signer := cluster.signer(t)
	bootstrapCreds.InviteSigner = signer
	bootstrap := startMeshHarnessWithCreds(t, bootstrapPriv, bootstrapPub, bootstrapCreds)

	joiner := startMeshHarness(t, cluster)

	invite, err := auth.IssueInviteTokenWithSigner(
		cluster.signer(t),
		nil,
		[]*admissionv1.BootstrapPeer{{
			PeerPub: bootstrap.pubKey,
			Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(bootstrap.port))},
		}},
		time.Now(),
		time.Hour,
		0,
	)
	require.NoError(t, err)

	joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	joinToken, err := joiner.mesh.JoinWithInvite(joinCtx, invite)
	require.NoError(t, err)
	_, err = auth.VerifyJoinToken(joinToken, joiner.pubKey, time.Now())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, ok := joiner.mesh.GetConn(bootstrap.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

	require.Eventually(t, func() bool {
		_, ok := bootstrap.mesh.GetConn(joiner.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)
}

func TestJoinWithInviteRejectsExpiredInviteTTL(t *testing.T) {
	cluster := newClusterAuth(t)

	bootstrapPub, bootstrapPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	bootstrapCreds := cluster.credsFor(t, bootstrapPub)
	signer := cluster.signer(t)
	bootstrapCreds.InviteSigner = signer
	bootstrap := startMeshHarnessWithCreds(t, bootstrapPriv, bootstrapPub, bootstrapCreds)

	joiner := startMeshHarness(t, cluster)

	invite, err := auth.IssueInviteTokenWithSigner(
		cluster.signer(t),
		nil,
		[]*admissionv1.BootstrapPeer{{
			PeerPub: bootstrap.pubKey,
			Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(bootstrap.port))},
		}},
		time.Now().Add(-2*time.Second),
		time.Second,
		0,
	)
	require.NoError(t, err)

	joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = joiner.mesh.JoinWithInvite(joinCtx, invite)
	require.Error(t, err)
}

func TestConnectReturnsErrIdentityMismatch(t *testing.T) {
	cluster := newClusterAuth(t)

	a := startMeshHarness(t, cluster)
	b := startMeshHarness(t, cluster)

	// Generate a random key that differs from b's actual key.
	wrongPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	wrongKey := types.PeerKeyFromBytes(wrongPub)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Dial b's address but expect the wrong key.
	err = a.mesh.Connect(ctx, wrongKey, []*net.UDPAddr{{IP: net.IPv4(127, 0, 0, 1), Port: b.port}})
	require.Error(t, err)
	require.True(t, errors.Is(err, mesh.ErrIdentityMismatch), "expected ErrIdentityMismatch, got: %v", err)
}

func startMeshHarness(t *testing.T, cluster *clusterAuth) *meshHarness {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	return startMeshHarnessWithCreds(t, priv, pub, cluster.credsFor(t, pub))
}

func startMeshHarnessWithCreds(
	t *testing.T,
	priv ed25519.PrivateKey,
	pub ed25519.PublicKey,
	creds *auth.NodeCredentials,
) *meshHarness {
	t.Helper()

	m, err := mesh.NewMesh(0, priv, creds, config.CertTTLs{}.TLSIdentityTTL(), config.CertTTLs{}.MembershipTTL(), 0, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, m.Start(ctx))

	h := &meshHarness{
		mesh:    m,
		peerKey: types.PeerKeyFromBytes(pub),
		pubKey:  pub,
		port:    m.ListenPort(),
		cancel:  cancel,
	}

	t.Cleanup(func() {
		h.cancel()
		_ = h.mesh.Close()
	})

	return h
}
