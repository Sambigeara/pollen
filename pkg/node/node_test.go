package node

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/internal/testutil/memtransport"
	"github.com/sambigeara/pollen/pkg/link"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestNode(t *testing.T) {
	ctx := t.Context()
	reachabilityTimeout := 1 * time.Second

	verifyReachability := func(t *testing.T, sender *Node, targetKey types.PeerKey, recvChan <-chan []byte, payload []byte, msg string) {
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
			case <-time.After(150 * time.Millisecond):
				assert.Fail(c, "timed out waiting for message")
			}
		}, reachabilityTimeout, 200*time.Millisecond, msg)
	}

	t.Run("converge after invite", func(t *testing.T) {
		network := memtransport.NewNetwork()
		dirA := t.TempDir()
		nodeA, portA := newNode(t, dirA, 0, network, nil)

		token, err := NewInvite([]string{"127.0.0.1"}, fmt.Sprintf("%d", portA))
		require.NoError(t, err)

		nodeA.AdmissionStore.AddInvite(token)

		go func() {
			require.NoError(t, nodeA.Start(ctx, nil))
		}()

		dirB := t.TempDir()
		nodeB, _ := newNode(t, dirB, 0, network, nil)
		go func() {
			require.NoError(t, nodeB.Start(ctx, token))
		}()

		expectPeers := 2
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, expectPeers, len(nodeA.Store.Cluster.Nodes.GetAll()), "Node A should see peer B")
			assert.Equal(c, expectPeers, len(nodeB.Store.Cluster.Nodes.GetAll()), "Node B should see peer A")
		}, 2*time.Second, 50*time.Millisecond, "nodes failed to converge state")
	})

	t.Run("converge after gossip", func(t *testing.T) {
		network := memtransport.NewNetwork()
		dirA := t.TempDir()
		dirB := t.TempDir()
		dirC := t.TempDir()

		var expectedA, expectedB, expectedC *statev1.Node
		var portA, portB, portC int

		t.Run("XXPsk2", func(t *testing.T) {
			var nodeA *Node
			nodeA, portA = newNode(t, dirA, 0, network, nil)

			tokenForB, err := NewInvite([]string{"127.0.0.1"}, fmt.Sprintf("%d", portA))
			require.NoError(t, err)

			tokenForC, err := NewInvite([]string{"127.0.0.1"}, fmt.Sprintf("%d", portA))
			require.NoError(t, err)

			nodeA.AdmissionStore.AddInvite(tokenForB)
			nodeA.AdmissionStore.AddInvite(tokenForC)

			initCtx, cancelFn := context.WithCancel(ctx)
			t.Cleanup(cancelFn)

			stopA := make(chan error)
			stopB := make(chan error)
			stopC := make(chan error)
			go func() { stopA <- nodeA.Start(initCtx, nil) }()

			var nodeB *Node
			nodeB, portB = newNode(t, dirB, 0, network, nil)
			go func() { stopB <- nodeB.Start(initCtx, tokenForB) }()

			var nodeC *Node
			nodeC, portC = newNode(t, dirC, 0, network, nil)
			go func() { stopC <- nodeC.Start(initCtx, tokenForC) }()

			var storeA, storeB, storeC map[types.PeerKey]*statev1.Node
			expectPeers := 3

			// Wait for state convergence
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				storeA = nodeA.Store.Cluster.Nodes.GetAll()
				storeB = nodeB.Store.Cluster.Nodes.GetAll()
				storeC = nodeC.Store.Cluster.Nodes.GetAll()

				assert.Equal(c, expectPeers, len(storeA))
				assert.Equal(c, expectPeers, len(storeB))
				assert.Equal(c, expectPeers, len(storeC))
			}, 2*time.Second, 50*time.Millisecond, "nodes failed to converge state")

			idA := nodeA.Store.Cluster.LocalID
			idB := nodeB.Store.Cluster.LocalID
			idC := nodeC.Store.Cluster.LocalID

			expectedA = &statev1.Node{
				Id:        idA.String(),
				Ips:       []string{"127.0.0.1"},
				LocalPort: uint32(portA),
				Keys:      nodeA.crypto.GetStateKeys(),
			}
			expectedB = &statev1.Node{
				Id:        idB.String(),
				Ips:       []string{"127.0.0.1"},
				LocalPort: uint32(portB),
				Keys:      nodeB.crypto.GetStateKeys(),
			}
			expectedC = &statev1.Node{
				Id:        idC.String(),
				Ips:       []string{"127.0.0.1"},
				LocalPort: uint32(portC),
				Keys:      nodeC.crypto.GetStateKeys(),
			}

			// Validate Exact State Match
			opts := []cmp.Option{protocmp.Transform(), protocmp.SortRepeatedFields(&statev1.Node{}, "addresses")}
			require.Empty(t, cmp.Diff(expectedA, storeA[idA], opts...))
			require.Empty(t, cmp.Diff(expectedA, storeB[idA], opts...))
			require.Empty(t, cmp.Diff(expectedA, storeC[idA], opts...))

			require.Empty(t, cmp.Diff(expectedB, storeA[idB], opts...))
			require.Empty(t, cmp.Diff(expectedB, storeB[idB], opts...))
			require.Empty(t, cmp.Diff(expectedB, storeC[idB], opts...))

			require.Empty(t, cmp.Diff(expectedC, storeA[idC], opts...))
			require.Empty(t, cmp.Diff(expectedC, storeB[idC], opts...))
			require.Empty(t, cmp.Diff(expectedC, storeC[idC], opts...))

			// Setup Reachability Test Handlers
			recvA := make(chan []byte, 1)
			nodeA.Link.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, b []byte) error { recvA <- b; return nil })
			recvB := make(chan []byte, 1)
			nodeB.Link.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, b []byte) error { recvB <- b; return nil })
			recvC := make(chan []byte, 1)
			nodeC.Link.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, b []byte) error { recvC <- b; return nil })

			skA := types.PeerKeyFromBytes(nodeA.crypto.noisePubKey)
			skB := types.PeerKeyFromBytes(nodeB.crypto.noisePubKey)
			skC := types.PeerKeyFromBytes(nodeC.crypto.noisePubKey)

			payload := []byte("hello")

			// Execute Reachability Tests
			verifyReachability(t, nodeB, skA, recvA, payload, "nodeB -> nodeA")
			verifyReachability(t, nodeC, skA, recvA, payload, "nodeC -> nodeA")
			verifyReachability(t, nodeB, skC, recvC, payload, "nodeB -> nodeC")
			verifyReachability(t, nodeC, skB, recvB, payload, "nodeC -> nodeB")
			verifyReachability(t, nodeA, skB, recvB, payload, "nodeA -> nodeB")
			verifyReachability(t, nodeA, skC, recvC, payload, "nodeA -> nodeC")

			// Stop nodes to release ports for next test
			cancelFn()
			<-stopA
			<-stopB
			<-stopC
		})

		t.Run("IK", func(t *testing.T) {
			// Recreate new nodes with the same local state (persisted in dirs).
			nodeA, _ := newNode(t, dirA, portA, network, nil)
			nodeB, _ := newNode(t, dirB, portB, network, nil)
			nodeC, _ := newNode(t, dirC, portC, network, nil)

			idA := nodeA.Store.Cluster.LocalID
			idB := nodeB.Store.Cluster.LocalID
			idC := nodeC.Store.Cluster.LocalID

			opts := []cmp.Option{protocmp.Transform(), protocmp.SortRepeatedFields(&statev1.Node{}, "addresses")}
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				// ensure the state is consistent from post-XXPsk2
				storeA := nodeA.Store.Cluster.Nodes.GetAll()
				storeB := nodeB.Store.Cluster.Nodes.GetAll()
				storeC := nodeC.Store.Cluster.Nodes.GetAll()

				require.Empty(c, cmp.Diff(expectedA, storeA[idA], opts...))
				require.Empty(c, cmp.Diff(expectedB, storeA[idB], opts...))
				require.Empty(c, cmp.Diff(expectedC, storeA[idC], opts...))
				require.Empty(c, cmp.Diff(expectedA, storeB[idA], opts...))
				require.Empty(c, cmp.Diff(expectedB, storeB[idB], opts...))
				require.Empty(c, cmp.Diff(expectedC, storeB[idC], opts...))
				require.Empty(c, cmp.Diff(expectedA, storeC[idA], opts...))
				require.Empty(c, cmp.Diff(expectedB, storeC[idB], opts...))
				require.Empty(c, cmp.Diff(expectedC, storeC[idC], opts...))
			}, 2*time.Second, 25*time.Millisecond, "nodes failed to converge state")

			// Setup Reachability Test Handlers
			recvA := make(chan []byte, 1)
			nodeA.Link.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, b []byte) error {
				select {
				case recvA <- b:
				default:
				}
				return nil
			})
			recvB := make(chan []byte, 1)
			nodeB.Link.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, b []byte) error {
				select {
				case recvB <- b:
				default:
				}
				return nil
			})
			recvC := make(chan []byte, 1)
			nodeC.Link.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, b []byte) error {
				select {
				case recvC <- b:
				default:
				}
				return nil
			})

			go func() {
				require.NoError(t, nodeA.Start(ctx, nil))
			}()
			go func() {
				require.NoError(t, nodeB.Start(ctx, nil))
			}()
			go func() {
				require.NoError(t, nodeC.Start(ctx, nil))
			}()

			skB := types.PeerKeyFromBytes(nodeB.crypto.noisePubKey)
			skC := types.PeerKeyFromBytes(nodeC.crypto.noisePubKey)

			payload := []byte("hello")

			// Execute Reachability Tests
			verifyReachability(t, nodeA, skB, recvB, payload, "nodeA -> nodeB")
			verifyReachability(t, nodeA, skC, recvC, payload, "nodeA -> nodeC")
			verifyReachability(t, nodeB, skC, recvC, payload, "nodeB -> nodeC")
			verifyReachability(t, nodeC, skB, recvB, payload, "nodeC -> nodeB")
		})
	})

	t.Run("punch coordination after direct failure", func(t *testing.T) {
		network := memtransport.NewNetwork()

		dirA := t.TempDir()
		nodeA, portA := newNode(t, dirA, 0, network, []string{"127.0.0.1"})

		tokenForB, err := NewInvite([]string{"127.0.0.1"}, fmt.Sprintf("%d", portA))
		require.NoError(t, err)

		tokenForC, err := NewInvite([]string{"127.0.0.1"}, fmt.Sprintf("%d", portA))
		require.NoError(t, err)

		nodeA.AdmissionStore.AddInvite(tokenForB)
		nodeA.AdmissionStore.AddInvite(tokenForC)

		initCtx, cancelFn := context.WithCancel(ctx)
		t.Cleanup(cancelFn)

		go func() {
			require.NoError(t, nodeA.Start(initCtx, nil))
		}()

		nodeB, portB := newNode(t, t.TempDir(), 0, network, []string{"10.0.0.2"})
		go func() {
			require.NoError(t, nodeB.Start(initCtx, tokenForB))
		}()

		nodeC, portC := newNode(t, t.TempDir(), 0, network, []string{"10.0.0.3"})
		go func() {
			require.NoError(t, nodeC.Start(initCtx, tokenForC))
		}()

		idB := nodeB.Store.Cluster.LocalID
		idC := nodeC.Store.Cluster.LocalID

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			storeA := nodeA.Store.Cluster.Nodes.GetAll()
			storeB := nodeB.Store.Cluster.Nodes.GetAll()
			storeC := nodeC.Store.Cluster.Nodes.GetAll()

			assert.Equal(c, 3, len(storeA))
			assert.Equal(c, 3, len(storeB))
			assert.Equal(c, 3, len(storeC))

			if nodeRec, ok := storeB[idC]; ok {
				expectedAddresses := []string{}
				for _, ip := range nodeRec.Ips {
					expectedAddresses = append(expectedAddresses, net.JoinHostPort(ip, fmt.Sprintf("%d", portC)))
				}
				assert.Equal(c, advertisedAddrs([]string{"10.0.0.3"}, portC), expectedAddresses)
			}
			if nodeRec, ok := storeC[idB]; ok {
				expectedAddresses := []string{}
				for _, ip := range nodeRec.Ips {
					expectedAddresses = append(expectedAddresses, net.JoinHostPort(ip, fmt.Sprintf("%d", portB)))
				}
				assert.Equal(c, advertisedAddrs([]string{"10.0.0.2"}, portB), expectedAddresses)
			}
		}, 2*time.Second, 50*time.Millisecond, "nodes failed to converge state")

		recvB := make(chan []byte, 1)
		nodeB.Link.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, b []byte) error {
			select {
			case recvB <- b:
			default:
			}
			return nil
		})
		recvC := make(chan []byte, 1)
		nodeC.Link.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, b []byte) error {
			select {
			case recvC <- b:
			default:
			}
			return nil
		})

		skB := types.PeerKeyFromBytes(nodeB.crypto.noisePubKey)
		skC := types.PeerKeyFromBytes(nodeC.crypto.noisePubKey)

		payload := []byte("hello")
		verifyReachability(t, nodeB, skC, recvC, payload, "nodeB -> nodeC (punch)")
		verifyReachability(t, nodeC, skB, recvB, payload, "nodeC -> nodeB (punch)")
	})
}

var portCounter uint32 = 10000

func nextPort() int {
	return int(atomic.AddUint32(&portCounter, 1))
}

func newNode(t *testing.T, dir string, port int, network *memtransport.Network, advertisedIPs []string) (*Node, int) {
	t.Helper()
	require.NotNil(t, network)

	if port == 0 {
		port = nextPort()
	}
	if len(advertisedIPs) == 0 {
		advertisedIPs = []string{"127.0.0.1"}
	}

	conf := &Config{
		Port:             port,
		AdvertisedIPs:    advertisedIPs,
		GossipInterval:   10 * time.Millisecond,
		PeerTickInterval: 10 * time.Millisecond,
		PollenDir:        dir,
		PeerConfig: &peer.Config{
			FirstBackoff:                  5 * time.Millisecond,
			BaseBackoff:                   5 * time.Millisecond,
			MaxBackoff:                    20 * time.Millisecond,
			DirectAttemptThreshold:        1,
			PunchAttemptThreshold:         1,
			PunchBirthdayAttemptThreshold: 1,
			UnreachableRetryInterval:      20 * time.Millisecond,
			DisconnectedRetryInterval:     20 * time.Millisecond,
		},
		LinkOptions: []link.Option{
			link.WithEnsurePeerInterval(5 * time.Millisecond),
			link.WithEnsurePeerTimeout(250 * time.Millisecond),
			link.WithHolepunchAttempts(2),
		},
		PunchAttemptTimeout: 4 * time.Second,
		DisableGossipJitter: true,
	}

	node, err := New(conf)
	require.NoError(t, err)

	return node, conf.Port
}

func advertisedAddrs(ips []string, port int) []string {
	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		out = append(out, net.JoinHostPort(ip, fmt.Sprintf("%d", port)))
	}
	return out
}
