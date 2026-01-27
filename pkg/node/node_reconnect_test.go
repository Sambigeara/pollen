package node

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/internal/testutil/memtransport"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNode_GracefulRestart_LeafRejoins(t *testing.T) {
	network := memtransport.NewNetwork()

	relay := startNode(t, network, t.TempDir(), 0, []string{"127.0.0.1"}, nil)
	defer relay.stop(t)

	inviteB := mustInvite(t, relay.port)
	inviteC := mustInvite(t, relay.port)
	relay.node.AdmissionStore.AddInvite(inviteB)
	relay.node.AdmissionStore.AddInvite(inviteC)

	leafDir := t.TempDir()
	leaf := startNode(t, network, leafDir, 0, []string{"127.0.0.1"}, inviteB)
	defer leaf.stop(t)

	peerNode := startNode(t, network, t.TempDir(), 0, []string{"127.0.0.1"}, inviteC)
	defer peerNode.stop(t)

	awaitCluster(t, relay.node, leaf.node, peerNode.node)

	recvLeaf := registerReceiver(leaf.node)
	recvPeer := registerReceiver(peerNode.node)

	verifyReachability(t, leaf.node, peerKey(peerNode.node), recvPeer, []byte("hello"), "leaf -> peer")
	verifyReachability(t, peerNode.node, peerKey(leaf.node), recvLeaf, []byte("hello"), "peer -> leaf")

	leafID := leaf.node.Store.Cluster.LocalID
	leaf.stop(t)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.False(c, containsPeer(relay.node.Peers.GetAll(peer.PeerStateConnected), leafID))
		assert.False(c, containsPeer(peerNode.node.Peers.GetAll(peer.PeerStateConnected), leafID))
	}, 1*time.Second, 20*time.Millisecond, "leaf should disconnect")

	leaf = startNode(t, network, leafDir, leaf.port, []string{"127.0.0.1"}, nil)
	defer leaf.stop(t)

	require.Equal(t, leafID, leaf.node.Store.Cluster.LocalID)
	awaitCluster(t, relay.node, leaf.node, peerNode.node)

	recvLeaf = registerReceiver(leaf.node)
	verifyReachability(t, leaf.node, peerKey(relay.node), registerReceiver(relay.node), []byte("hello"), "leaf -> relay")
	verifyReachability(t, peerNode.node, peerKey(leaf.node), recvLeaf, []byte("hello"), "peer -> leaf")
	verifyReachability(t, leaf.node, peerKey(peerNode.node), recvPeer, []byte("hello"), "leaf -> peer")
}

func TestNode_GracefulRestart_RelayRestarts(t *testing.T) {
	network := memtransport.NewNetwork()

	relayDir := t.TempDir()
	relay := startNode(t, network, relayDir, 0, []string{"127.0.0.1"}, nil)

	inviteB := mustInvite(t, relay.port)
	inviteC := mustInvite(t, relay.port)
	relay.node.AdmissionStore.AddInvite(inviteB)
	relay.node.AdmissionStore.AddInvite(inviteC)

	leaf := startNode(t, network, t.TempDir(), 0, []string{"127.0.0.1"}, inviteB)
	defer leaf.stop(t)

	peerNode := startNode(t, network, t.TempDir(), 0, []string{"127.0.0.1"}, inviteC)
	defer peerNode.stop(t)

	awaitCluster(t, relay.node, leaf.node, peerNode.node)

	recvLeaf := registerReceiver(leaf.node)
	recvPeer := registerReceiver(peerNode.node)
	verifyReachability(t, leaf.node, peerKey(peerNode.node), recvPeer, []byte("hello"), "leaf -> peer")

	relayID := relay.node.Store.Cluster.LocalID
	relay.stop(t)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.False(c, containsPeer(leaf.node.Peers.GetAll(peer.PeerStateConnected), relayID))
		assert.False(c, containsPeer(peerNode.node.Peers.GetAll(peer.PeerStateConnected), relayID))
	}, 1*time.Second, 20*time.Millisecond, "relay should disconnect")

	verifyReachability(t, peerNode.node, peerKey(leaf.node), recvLeaf, []byte("hello"), "peer -> leaf")

	relay = startNode(t, network, relayDir, relay.port, []string{"127.0.0.1"}, nil)
	defer relay.stop(t)

	require.Equal(t, relayID, relay.node.Store.Cluster.LocalID)
	awaitCluster(t, relay.node, leaf.node, peerNode.node)

	verifyReachability(t, relay.node, peerKey(leaf.node), registerReceiver(leaf.node), []byte("hello"), "relay -> leaf")
	verifyReachability(t, relay.node, peerKey(peerNode.node), registerReceiver(peerNode.node), []byte("hello"), "relay -> peer")
}

type runningNode struct {
	node         *Node
	cancel       context.CancelFunc
	done         chan error
	dir          string
	port         int
	advertisedIP []string
	stopOnce     sync.Once
}

func startNode(t *testing.T, network *memtransport.Network, dir string, port int, advertisedIPs []string, token *peerv1.Invite) *runningNode {
	t.Helper()

	n, p := newNode(t, dir, port, network, advertisedIPs)
	ctx, cancel := context.WithCancel(t.Context())
	ch := make(chan error, 1)
	go func() {
		ch <- n.Start(ctx, token)
	}()

	return &runningNode{
		node:         n,
		cancel:       cancel,
		done:         ch,
		dir:          dir,
		port:         p,
		advertisedIP: advertisedIPs,
	}
}

func (r *runningNode) stop(t *testing.T) {
	t.Helper()
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		r.cancel()
		require.NoError(t, <-r.done)
	})
}

func mustInvite(t *testing.T, port int) *peerv1.Invite {
	t.Helper()
	inv, err := NewInvite([]string{"127.0.0.1"}, fmt.Sprintf("%d", port))
	require.NoError(t, err)
	return inv
}

func awaitCluster(t *testing.T, nodes ...*Node) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for _, n := range nodes {
			assert.Equal(c, len(nodes), len(n.Store.Cluster.Nodes.GetAll()))
		}
	}, 2*time.Second, 50*time.Millisecond, "nodes failed to converge state")
}

func registerReceiver(n *Node) chan []byte {
	ch := make(chan []byte, 1)
	n.Link.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, payload []byte) error {
		select {
		case ch <- payload:
		default:
		}
		return nil
	})
	return ch
}

func verifyReachability(t *testing.T, sender *Node, targetKey types.PeerKey, recvChan <-chan []byte, payload []byte, msg string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		err := sender.Link.Send(t.Context(), targetKey, types.Envelope{
			Type:    types.MsgTypeTest,
			Payload: payload,
		})
		assert.NoError(c, err)

		select {
		case got := <-recvChan:
			assert.Equal(c, payload, got)
		case <-time.After(100 * time.Millisecond):
			assert.Fail(c, "timed out waiting for message")
		}
	}, 1*time.Second, 100*time.Millisecond, msg)
}

func peerKey(n *Node) types.PeerKey {
	return types.PeerKeyFromBytes(n.crypto.noisePubKey)
}

func containsPeer(list []types.PeerKey, target types.PeerKey) bool {
	return slices.Contains(list, target)
}
