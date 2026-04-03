package supervisor_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"strconv"
	"testing"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/control"
	"github.com/sambigeara/pollen/pkg/supervisor"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// --- Shared Harness Logic ---

type testNode struct {
	node             *supervisor.Supervisor
	svc              *control.Service
	peerKey          types.PeerKey
	pubKey           ed25519.PublicKey
	ips              []string
	port             int
	dir              string
	privKey          ed25519.PrivateKey
	creds            *auth.NodeCredentials
	cancel           context.CancelFunc
	errCh            chan error
	bootstrapPeers   []supervisor.BootstrapTarget
	peerTickInterval time.Duration
}

type clusterAuth struct {
	adminPriv ed25519.PrivateKey
	rootPub   ed25519.PublicKey
}

func newClusterAuth(t *testing.T) *clusterAuth {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return &clusterAuth{adminPriv: priv, rootPub: pub}
}

func (c *clusterAuth) credsFor(t *testing.T, subject ed25519.PublicKey) *auth.NodeCredentials {
	t.Helper()
	cert, err := auth.IssueDelegationCert(c.adminPriv, nil, subject, auth.LeafCapabilities(), time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), time.Time{})
	require.NoError(t, err)
	return auth.NewNodeCredentials(c.rootPub, cert)
}

func newTestNode(t *testing.T, cluster *clusterAuth, ips []string) *testNode {
	t.Helper()
	dir := t.TempDir()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	tn := &testNode{
		peerKey: types.PeerKeyFromBytes(pub),
		pubKey:  pub,
		ips:     ips,
		port:    0,
		dir:     dir,
		privKey: priv,
		creds:   cluster.credsFor(t, pub),
	}
	t.Cleanup(tn.stop)
	return tn
}

func (tn *testNode) start(t *testing.T) {
	t.Helper()
	peerTick := tn.peerTickInterval
	if peerTick == 0 {
		peerTick = 100 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())
	opts := supervisor.Options{
		SigningKey:          tn.privKey,
		PollenDir:           tn.dir,
		ListenPort:          tn.port,
		AdvertisedIPs:       tn.ips,
		GossipInterval:      100 * time.Millisecond,
		PeerTickInterval:    peerTick,
		DisableGossipJitter: true,
		BootstrapPeers:      tn.bootstrapPeers,
		ShutdownFunc:        cancel,
	}

	n, err := supervisor.New(opts, tn.creds, nil)
	require.NoError(t, err)
	errCh := make(chan error, 1)

	go func() { errCh <- n.Run(ctx) }()

	select {
	case <-n.Ready():
	case err := <-errCh:
		t.Fatalf("supervisor.Run failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for node.Ready()")
	}

	tn.port = n.ListenPort()
	tn.node = n
	tn.svc = n.ControlService()
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

func newMinimalNode(t *testing.T, bootstrapPublic bool) *supervisor.Supervisor {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	adminPub, adminPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	cert, err := auth.IssueDelegationCert(adminPriv, nil, pub, auth.LeafCapabilities(), time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), time.Time{})
	require.NoError(t, err)

	opts := supervisor.Options{
		SigningKey:       priv,
		PollenDir:        t.TempDir(),
		ListenPort:       0,
		AdvertisedIPs:    []string{"127.0.0.1"},
		GossipInterval:   time.Second,
		PeerTickInterval: time.Second,
		BootstrapPublic:  bootstrapPublic,
	}

	n, err := supervisor.New(opts, auth.NewNodeCredentials(adminPub, cert), nil)
	require.NoError(t, err)
	return n
}

// --- Tests ---

func TestConnectPeerFlow(t *testing.T) {
	nodeIPs := []string{"127.0.0.1"}
	cluster := newClusterAuth(t)

	a := newTestNode(t, cluster, nodeIPs)
	a.start(t)
	b := newTestNode(t, cluster, nodeIPs)
	b.start(t)

	_, err := a.svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerPub: b.pubKey,
		Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(b.port))},
	})
	require.NoError(t, err)

	assertPeersConnected(t, a, b)

	statusResp, err := a.svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)
	require.NotNil(t, statusResp.Self)
	require.Len(t, statusResp.Nodes, 1)
	require.Equal(t, b.peerKey.Bytes(), statusResp.Nodes[0].Node.PeerPub)
	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_ONLINE, statusResp.Nodes[0].Status)
}

func TestInitialVivaldiCoordPropagatesAfterConnect(t *testing.T) {
	nodeIPs := []string{"127.0.0.1"}
	cluster := newClusterAuth(t)

	a := newTestNode(t, cluster, nodeIPs)
	a.start(t)
	b := newTestNode(t, cluster, nodeIPs)
	b.start(t)

	_, err := a.svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerPub: b.pubKey,
		Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(b.port))},
	})
	require.NoError(t, err)
	assertPeersConnected(t, a, b)

	require.Eventually(t, func() bool {
		nv, ok := a.node.StateStore().Snapshot().Nodes[b.peerKey]
		return ok && nv.VivaldiCoord != nil
	}, 5*time.Second, 50*time.Millisecond)
}

func TestConnectPeerAfterPriorConnection(t *testing.T) {
	nodeIPs := []string{"127.0.0.1"}
	cluster := newClusterAuth(t)

	a := newTestNode(t, cluster, nodeIPs)
	a.start(t)

	c := newTestNode(t, cluster, nodeIPs)
	c.start(t)
	_, err := a.svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerPub: c.pubKey,
		Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(c.port))},
	})
	require.NoError(t, err)
	assertPeersConnected(t, a, c)

	c.stop()
	require.Eventually(t, func() bool { return len(a.node.GetConnectedPeers()) == 0 }, 5*time.Second, 50*time.Millisecond)

	b := newTestNode(t, cluster, nodeIPs)
	b.start(t)
	_, err = a.svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerPub: b.pubKey,
		Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(b.port))},
	})
	require.NoError(t, err)

	assertPeersConnected(t, a, b)
}

func assertPeersConnected(t *testing.T, a, b *testNode) {
	t.Helper()

	require.Eventually(t, func() bool { return len(a.node.GetConnectedPeers()) >= 1 }, 5*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool { return len(b.node.GetConnectedPeers()) >= 1 }, 5*time.Second, 50*time.Millisecond)

	require.Contains(t, a.node.GetConnectedPeers(), b.peerKey)
	require.Contains(t, b.node.GetConnectedPeers(), a.peerKey)

	_, aReachesB := a.node.StateStore().Snapshot().Nodes[a.peerKey].Reachable[b.peerKey]
	require.True(t, aReachesB)
}

func TestEnsureIdentityKeyAndReadIdentityPub(t *testing.T) {
	dir := t.TempDir()

	priv, pub, err := auth.EnsureIdentityKey(dir)
	require.NoError(t, err)
	require.Len(t, priv, ed25519.PrivateKeySize)
	require.Len(t, pub, ed25519.PublicKeySize)

	gotPub, err := auth.ReadIdentityPub(dir)
	require.NoError(t, err)
	require.Equal(t, pub, gotPub)
}

func TestBootstrapPublicSetsAccessibleImmediately(t *testing.T) {
	n := newMinimalNode(t, true)
	snap := n.StateStore().Snapshot()
	require.True(t, snap.Nodes[snap.LocalID].PubliclyAccessible)
}

func TestBootstrapPublicFalseIsNotAccessible(t *testing.T) {
	n := newMinimalNode(t, false)
	snap := n.StateStore().Snapshot()
	require.False(t, snap.Nodes[snap.LocalID].PubliclyAccessible)
}
