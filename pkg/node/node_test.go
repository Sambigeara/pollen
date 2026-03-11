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
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type testNode struct {
	node             *node.Node
	svc              *node.NodeService
	store            *store.Store
	peers            *peer.Store
	peerKey          types.PeerKey
	pubKey           ed25519.PublicKey
	ips              []string
	port             int
	dir              string
	privKey          ed25519.PrivateKey
	creds            *auth.NodeCredentials
	cancel           context.CancelFunc
	errCh            chan error
	bootstrapPeers   []node.BootstrapPeer
	peerTickInterval time.Duration
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
	cert, err := auth.IssueDelegationCert(c.adminPriv, nil, c.trust.GetClusterId(), subject, auth.LeafCapabilities(), time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour), time.Time{})
	require.NoError(t, err)
	return &auth.NodeCredentials{Trust: c.trust, Cert: cert}
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

	t.Cleanup(func() {
		tn.stop()
	})

	return tn
}

func (tn *testNode) start(t *testing.T) {
	t.Helper()

	pub := tn.privKey.Public().(ed25519.PublicKey)

	stateStore, err := store.Load(tn.dir, pub)
	require.NoError(t, err)

	peerStore := peer.NewStore()

	peerTick := tn.peerTickInterval
	if peerTick == 0 {
		peerTick = 100 * time.Millisecond
	}

	conf := &node.Config{
		Port:                tn.port,
		AdvertisedIPs:       tn.ips,
		GossipInterval:      100 * time.Millisecond,
		PeerTickInterval:    peerTick,
		DisableGossipJitter: true,
		BootstrapPeers:      tn.bootstrapPeers,
		TLSIdentityTTL:      config.CertTTLs{}.TLSIdentityTTL(),
		MembershipTTL:       config.CertTTLs{}.MembershipTTL(),
	}

	n, err := node.New(conf, tn.privKey, tn.creds, stateStore, peerStore, tn.dir)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- n.Start(ctx)
	}()

	select {
	case <-n.Ready():
	case err := <-errCh:
		t.Fatalf("node.Start failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for node.Ready()")
	}

	tn.port = n.ListenPort()
	tn.node = n
	tn.svc = node.NewNodeService(n, cancel, tn.creds)
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

func TestConnectPeerFlow(t *testing.T) {
	nodeIPs := []string{"127.0.0.1"}
	cluster := newClusterAuth(t)

	a := newTestNode(t, cluster, nodeIPs)
	a.start(t)

	b := newTestNode(t, cluster, nodeIPs)
	b.start(t)

	// Simulate `pollen bootstrap ssh` with running daemon: call ConnectPeer RPC on A → B.
	req := &controlv1.ConnectPeerRequest{
		PeerId: b.pubKey,
		Addrs:  []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(b.port))},
	}
	_, err := a.svc.ConnectPeer(context.Background(), req)
	require.NoError(t, err)

	assertPeersConnected(t, a, b)

	// Verify GetStatus returns the remote peer (this is what `pollen status` uses).
	statusResp, err := a.svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)
	require.NotNil(t, statusResp.Self)
	require.Len(t, statusResp.Nodes, 1, "GetStatus should list the remote peer")
	require.Equal(t, b.peerKey.Bytes(), statusResp.Nodes[0].Node.PeerId)
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
		PeerId: b.pubKey,
		Addrs:  []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(b.port))},
	})
	require.NoError(t, err)

	assertPeersConnected(t, a, b)

	require.Eventually(t, func() bool {
		coord, ok := a.store.PeerVivaldiCoord(b.peerKey)
		return ok && coord != nil
	}, 5*time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		coord, ok := b.store.PeerVivaldiCoord(a.peerKey)
		return ok && coord != nil
	}, 5*time.Second, 50*time.Millisecond)
}

// TestConnectPeerAfterPriorConnection simulates the production scenario where
// the daemon previously connected to a peer, then later ConnectPeer is called
// for a new relay. Verifies that state propagates to the new peer.
func TestConnectPeerAfterPriorConnection(t *testing.T) {
	nodeIPs := []string{"127.0.0.1"}
	cluster := newClusterAuth(t)

	a := newTestNode(t, cluster, nodeIPs)
	a.start(t)

	// First connection: A connects to C via ConnectPeer RPC.
	c := newTestNode(t, cluster, nodeIPs)
	c.start(t)
	_, err := a.svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerId: c.pubKey,
		Addrs:  []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(c.port))},
	})
	require.NoError(t, err)
	assertPeersConnected(t, a, c)

	// C disconnects.
	c.stop()
	require.Eventually(t, func() bool {
		return len(a.node.GetConnectedPeers()) == 0
	}, 5*time.Second, 50*time.Millisecond, "A should see C disconnect")

	// Now simulate `pollen bootstrap ssh`: ConnectPeer to new node B.
	b := newTestNode(t, cluster, nodeIPs)
	b.start(t)

	req := &controlv1.ConnectPeerRequest{
		PeerId: b.pubKey,
		Addrs:  []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(b.port))},
	}
	_, err = a.svc.ConnectPeer(context.Background(), req)
	require.NoError(t, err)

	assertPeersConnected(t, a, b)

	// Verify GetStatus returns B as online.
	statusResp, err := a.svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)

	foundB := false
	for _, n := range statusResp.Nodes {
		if types.PeerKeyFromBytes(n.Node.PeerId) == b.peerKey {
			require.Equal(t, controlv1.NodeStatus_NODE_STATUS_ONLINE, n.Status)
			foundB = true
		}
	}
	require.True(t, foundB, "GetStatus should include node B")
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

func TestGenIdentityKeyAndReadIdentityPub(t *testing.T) {
	dir := t.TempDir()

	priv, pub, err := node.GenIdentityKey(dir)
	require.NoError(t, err)
	require.Len(t, priv, ed25519.PrivateKeySize)
	require.Len(t, pub, ed25519.PublicKeySize)

	// ReadIdentityPub must return the same key (exercises decodePubKeyPEM).
	gotPub, err := node.ReadIdentityPub(dir)
	require.NoError(t, err)
	require.Equal(t, pub, gotPub)

	// Calling GenIdentityKey again returns the same keys (idempotent).
	priv2, pub2, err := node.GenIdentityKey(dir)
	require.NoError(t, err)
	require.Equal(t, priv, priv2)
	require.Equal(t, pub, pub2)
}

func TestReadIdentityPubMissingFile(t *testing.T) {
	_, err := node.ReadIdentityPub(t.TempDir())
	require.Error(t, err)
}

func getNodeStatus(t *testing.T, observer, target *testNode) controlv1.NodeStatus {
	t.Helper()
	resp, err := observer.svc.GetStatus(context.Background(), &controlv1.GetStatusRequest{})
	require.NoError(t, err)
	for _, n := range resp.Nodes {
		if types.PeerKeyFromBytes(n.Node.PeerId) == target.peerKey {
			return n.Status
		}
	}
	t.Fatalf("target peer %x not found in GetStatus response", target.peerKey)
	return 0
}

func TestGetStatusDisconnectedPeerIsOffline(t *testing.T) {
	nodeIPs := []string{"127.0.0.1"}
	cluster := newClusterAuth(t)

	a := newTestNode(t, cluster, nodeIPs)
	a.start(t)

	b := newTestNode(t, cluster, nodeIPs)
	b.start(t)

	_, err := a.svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerId: b.pubKey,
		Addrs:  []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(b.port))},
	})
	require.NoError(t, err)
	assertPeersConnected(t, a, b)
	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_ONLINE, getNodeStatus(t, a, b))

	// B disconnects.
	b.stop()
	require.Eventually(t, func() bool {
		return len(a.node.GetConnectedPeers()) == 0
	}, 5*time.Second, 50*time.Millisecond)

	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_OFFLINE, getNodeStatus(t, a, b))
}

// setupThreeNodeChain creates three nodes A↔B↔C with peerTickInterval=time.Hour
// (to prevent auto-topology from connecting A↔C), connects A↔B and B↔C, and
// waits for A to learn about B→C reachability via gossip.
func setupThreeNodeChain(t *testing.T) (a, b, c *testNode) {
	t.Helper()
	nodeIPs := []string{"127.0.0.1"}
	cluster := newClusterAuth(t)

	a = newTestNode(t, cluster, nodeIPs)
	a.peerTickInterval = time.Hour
	a.start(t)

	b = newTestNode(t, cluster, nodeIPs)
	b.peerTickInterval = time.Hour
	b.start(t)

	c = newTestNode(t, cluster, nodeIPs)
	c.peerTickInterval = time.Hour
	c.start(t)

	// A↔B
	_, err := a.svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerId: b.pubKey,
		Addrs:  []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(b.port))},
	})
	require.NoError(t, err)
	assertPeersConnected(t, a, b)

	// B↔C
	_, err = b.svc.ConnectPeer(context.Background(), &controlv1.ConnectPeerRequest{
		PeerId: c.pubKey,
		Addrs:  []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(c.port))},
	})
	require.NoError(t, err)
	assertPeersConnected(t, b, c)

	// Wait for A to learn about C via B's gossip reachability.
	require.Eventually(t, func() bool {
		return a.store.IsConnected(b.peerKey, c.peerKey)
	}, 5*time.Second, 50*time.Millisecond, "A should learn B→C reachability via gossip")

	return a, b, c
}

func TestGetStatusIndirectPeerViaDirectPeer(t *testing.T) {
	a, b, c := setupThreeNodeChain(t)

	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_ONLINE, getNodeStatus(t, a, b))
	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_INDIRECT, getNodeStatus(t, a, c))
}

func TestGetStatusStaleReachabilityNotIndirect(t *testing.T) {
	a, b, c := setupThreeNodeChain(t)

	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_INDIRECT, getNodeStatus(t, a, c))

	// B disconnects — its stale reachability claim about C should not keep C as indirect.
	b.stop()
	require.Eventually(t, func() bool {
		return len(a.node.GetConnectedPeers()) == 0
	}, 5*time.Second, 50*time.Millisecond, "A should see B disconnect")

	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_OFFLINE, getNodeStatus(t, a, b))
	require.Equal(t, controlv1.NodeStatus_NODE_STATUS_OFFLINE, getNodeStatus(t, a, c))
}

func TestBootstrapPeerConnectsAtStartup(t *testing.T) {
	nodeIPs := []string{"127.0.0.1"}
	cluster := newClusterAuth(t)

	// Start B first so it's listening.
	b := newTestNode(t, cluster, nodeIPs)
	b.start(t)

	// Start A with B as a bootstrap peer.
	a := newTestNode(t, cluster, nodeIPs)
	a.bootstrapPeers = []node.BootstrapPeer{{
		PeerKey: b.peerKey,
		Addrs:   []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(b.port))},
	}}
	a.start(t)

	assertPeersConnected(t, a, b)
}
