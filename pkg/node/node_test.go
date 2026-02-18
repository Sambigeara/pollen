package node_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testNode struct {
	node    *node.Node
	svc     *node.NodeService
	store   *store.Store
	peers   *peer.Store
	invites admission.Store
	peerKey types.PeerKey
	ips     []string
	port    int
	dir     string
	privKey ed25519.PrivateKey
	cancel  context.CancelFunc
	errCh   chan error
}

func startNode(t *testing.T, port int, ips []string, token ...string) *testNode {
	t.Helper()

	dir := t.TempDir()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	tn := &testNode{
		peerKey: types.PeerKeyFromBytes(pub),
		ips:     ips,
		port:    port,
		dir:     dir,
		privKey: priv,
	}

	tn.start(t, token...)

	t.Cleanup(func() {
		tn.stop()
	})

	return tn
}

func (tn *testNode) start(t *testing.T, token ...string) {
	t.Helper()

	pub := tn.privKey.Public().(ed25519.PublicKey)

	stateStore, err := store.Load(tn.dir, pub)
	require.NoError(t, err)

	peerStore := peer.NewStore()

	invitesStore, err := admission.Load(tn.dir)
	require.NoError(t, err)

	conf := &node.Config{
		Port:                tn.port,
		AdvertisedIPs:       tn.ips,
		GossipInterval:      100 * time.Millisecond,
		PeerTickInterval:    100 * time.Millisecond,
		DisableGossipJitter: true,
	}

	n, err := node.New(conf, tn.privKey, stateStore, peerStore, invitesStore)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	var invite *admissionv1.Invite
	if len(token) > 0 && token[0] != "" {
		invite, err = node.DecodeToken(token[0])
		require.NoError(t, err)
	}

	go func() {
		errCh <- n.Start(ctx, invite)
	}()

	tn.node = n
	tn.svc = node.NewNodeService(n)
	tn.store = stateStore
	tn.peers = peerStore
	tn.invites = invitesStore
	tn.cancel = cancel
	tn.errCh = errCh
}

func (tn *testNode) stop() {
	tn.cancel()
	<-tn.errCh
}

func (tn *testNode) restart(t *testing.T) {
	t.Helper()
	tn.stop()
	waitForPort(t, tn.port)
	tn.start(t)
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

func TestInviteFlow(t *testing.T) {
	nodeIPs := []string{"127.0.0.1", "192.0.2.1"}

	a := startNode(t, 19100, nodeIPs)

	resp, err := a.svc.CreateInvite(context.Background(), &controlv1.CreateInviteRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Token)

	invite, err := node.DecodeToken(resp.Token)
	require.NoError(t, err)

	b := startNode(t, 19101, nodeIPs, resp.Token)

	assertPeersConnected(t, a, b, nodeIPs)

	ok, err := a.invites.ConsumeToken(invite.Id)
	require.NoError(t, err)
	require.False(t, ok, "invite token should already be consumed")

	// Restart both nodes and assert the same state is reached without an invite.
	a.restart(t)
	// TODO(saml): remove this delay once restart ordering is deterministic.
	time.Sleep(150 * time.Millisecond)
	b.restart(t)

	assertPeersConnected(t, a, b, nodeIPs)
}

func assertPeersConnected(t *testing.T, a, b *testNode, nodeIPs []string) {
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

	expectedIPs := make([]net.IP, len(nodeIPs))
	for i, ip := range nodeIPs {
		expectedIPs[i] = net.ParseIP(ip)
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		p, ok := a.peers.Get(b.peerKey)
		assert.True(c, ok)
		assert.Equal(c, peer.PeerStateConnected, p.State)
		assert.Equal(c, peer.ConnectStageDirect, p.Stage)
		assert.False(c, p.ConnectedAt.IsZero())
		assert.Equal(c, expectedIPs, p.Ips)
		assert.Equal(c, b.port, p.ObservedPort)
	}, 5*time.Second, 50*time.Millisecond)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		p, ok := b.peers.Get(a.peerKey)
		assert.True(c, ok)
		assert.Equal(c, peer.PeerStateConnected, p.State)
		assert.Equal(c, peer.ConnectStageDirect, p.Stage)
		assert.False(c, p.ConnectedAt.IsZero())
		assert.Equal(c, expectedIPs, p.Ips)
		assert.Equal(c, a.port, p.ObservedPort)
	}, 5*time.Second, 50*time.Millisecond)
}
