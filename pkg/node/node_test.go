package node_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"strconv"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type testNode struct {
	node    *node.Node
	svc     *node.NodeService
	store   *store.Store
	peers   *peer.Store
	peerKey types.PeerKey
	pubKey  ed25519.PublicKey
	ips     []string
	port    int
	dir     string
	privKey ed25519.PrivateKey
	creds   *auth.NodeCredentials
	cancel  context.CancelFunc
	errCh   chan error
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
	cert, err := auth.IssueMembershipCert(c.adminPriv, c.trust.GetClusterId(), subject, time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour))
	require.NoError(t, err)
	return &auth.NodeCredentials{Trust: c.trust, Cert: cert}
}

func (c *clusterAuth) tokenFor(t *testing.T, subject ed25519.PublicKey, bootstrap *testNode) *admissionv1.JoinToken {
	t.Helper()
	token, err := auth.IssueJoinToken(c.adminPriv, c.trust, subject, []*admissionv1.BootstrapPeer{{
		PeerPub: bootstrap.pubKey,
		Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(bootstrap.port))},
	}}, time.Now(), time.Hour)
	require.NoError(t, err)
	return token
}

func newTestNode(t *testing.T, cluster *clusterAuth, port int, ips []string) *testNode {
	t.Helper()

	dir := t.TempDir()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	tn := &testNode{
		peerKey: types.PeerKeyFromBytes(pub),
		pubKey:  pub,
		ips:     ips,
		port:    port,
		dir:     dir,
		privKey: priv,
		creds:   cluster.credsFor(t, pub),
	}

	t.Cleanup(func() {
		tn.stop()
	})

	return tn
}

func (tn *testNode) start(t *testing.T, token *admissionv1.JoinToken) {
	t.Helper()

	pub := tn.privKey.Public().(ed25519.PublicKey)

	stateStore, err := store.Load(tn.dir, pub)
	require.NoError(t, err)

	peerStore := peer.NewStore()

	conf := &node.Config{
		Port:                tn.port,
		AdvertisedIPs:       tn.ips,
		GossipInterval:      100 * time.Millisecond,
		PeerTickInterval:    100 * time.Millisecond,
		DisableGossipJitter: true,
	}

	n, err := node.New(conf, tn.privKey, tn.creds, stateStore, peerStore)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- n.Start(ctx, token)
	}()

	tn.node = n
	tn.svc = node.NewNodeService(n, cancel)
	tn.store = stateStore
	tn.peers = peerStore
	tn.cancel = cancel
	tn.errCh = errCh
}

func (tn *testNode) stop() {
	if tn.cancel == nil {
		return
	}
	tn.cancel()
	<-tn.errCh
	tn.cancel = nil
	tn.errCh = nil
}

func (tn *testNode) restart(t *testing.T) {
	t.Helper()
	tn.stop()
	waitForPort(t, tn.port)
	tn.start(t, nil)
}

func waitForPort(t *testing.T, port int) {
	t.Helper()
	require.Eventually(t, func() bool {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond, "port %d not released", port)
}

func TestJoinTokenFlow(t *testing.T) {
	nodeIPs := []string{"127.0.0.1", "192.0.2.1"}
	cluster := newClusterAuth(t)

	a := newTestNode(t, cluster, 19100, nodeIPs)
	a.start(t, nil)

	b := newTestNode(t, cluster, 19101, nodeIPs)
	b.start(t, cluster.tokenFor(t, b.pubKey, a))

	assertPeersConnected(t, a, b)

	a.restart(t)
	time.Sleep(150 * time.Millisecond)
	b.restart(t)

	assertPeersConnected(t, a, b)
}

func assertPeersConnected(t *testing.T, a, b *testNode) {
	t.Helper()

	require.Eventually(t, func() bool {
		return len(a.node.GetConnectedPeers()) >= 1
	}, 5*time.Second, 50*time.Millisecond, "node A should have a connected peer")

	require.Eventually(t, func() bool {
		return len(b.node.GetConnectedPeers()) >= 1
	}, 5*time.Second, 50*time.Millisecond, "node B should have a connected peer")

	require.Contains(t, a.node.GetConnectedPeers(), b.peerKey)
	require.Contains(t, b.node.GetConnectedPeers(), a.peerKey)

	require.True(t, a.store.IsConnected(a.peerKey, b.peerKey))
	require.True(t, b.store.IsConnected(b.peerKey, a.peerKey))

	require.Eventually(t, func() bool {
		_, ok := b.store.Get(a.peerKey)
		return ok
	}, 5*time.Second, 50*time.Millisecond, "B's store should know about A via gossip")

	require.Eventually(t, func() bool {
		_, ok := a.store.Get(b.peerKey)
		return ok
	}, 5*time.Second, 50*time.Millisecond, "A's store should know about B via gossip")
}
