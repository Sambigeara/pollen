//go:build integration

package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestPublicMesh_GossipConvergence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := PublicMesh(t, 4, ctx) //nolint:mnd

	t.Run("AllNodesSeeSamePeers", func(t *testing.T) {
		c.RequireConverged(t)
	})

	t.Run("StateConvergesAfterMutation", func(t *testing.T) {
		c.Node("node-0").Node().UpsertService(8080, "http") //nolint:mnd
		// Use eagerTimeout: state must arrive via eager push, not clock sync.
		c.RequireEventually(t, func() bool {
			pk := c.PeerKeyByName("node-0")
			for _, n := range c.Nodes() {
				if n.PeerKey() == pk {
					continue
				}
				if !n.Store().HasServicePort(pk, 8080) { //nolint:mnd
					return false
				}
			}
			return true
		}, eagerTimeout, "service did not propagate eagerly")
	})
}

func TestPublicMesh_TopologyStabilization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	// Build a sparse chain (0→1→2→3→4→5) so topology selection must establish
	// additional connections beyond the initial introductions. PublicMesh
	// introduces all pairs which bypasses topology entirely.
	c := New(t).
		SetDefaultLatency(5*time.Millisecond). //nolint:mnd
		SetDefaultJitter(0.15).                //nolint:mnd
		AddNode("node-0", Public).
		AddNode("node-1", Public).
		AddNode("node-2", Public).
		AddNode("node-3", Public).
		AddNode("node-4", Public).
		AddNode("node-5", Public).
		Introduce("node-0", "node-1").
		Introduce("node-1", "node-2").
		Introduce("node-2", "node-3").
		Introduce("node-3", "node-4").
		Introduce("node-4", "node-5").
		Start(ctx)

	c.RequireConverged(t)

	t.Run("VivaldiCoordinatesPropagated", func(t *testing.T) {
		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				for _, other := range c.Nodes() {
					if n.PeerKey() == other.PeerKey() {
						continue
					}
					coord, ok := n.Store().PeerVivaldiCoord(other.PeerKey())
					if !ok || (coord.X == 0 && coord.Y == 0 && coord.Height == 0) {
						return false
					}
				}
			}
			return true
		}, assertTimeout, "vivaldi coordinates did not propagate")
	})

	t.Run("PeerSelectionSettles", func(t *testing.T) {
		// Each node starts with only 1-2 connections (chain). Topology
		// selection should establish additional connections so every node
		// ends up with ≥2 peers.
		for _, n := range c.Nodes() {
			c.RequireConnectedPeers(t, n.Name(), 2) //nolint:mnd
		}
	})
}

// minimalWASM is the smallest valid WASM module (magic + version header).
var minimalWASM = []byte("\x00asm\x01\x00\x00\x00")

func TestPublicMesh_WorkloadPlacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := PublicMesh(t, 4, ctx) //nolint:mnd
	c.RequireConverged(t)

	var hash string

	t.Run("PropagatesViaGossip", func(t *testing.T) {
		var err error
		hash, err = c.Node("node-0").Node().SeedWorkload(minimalWASM, 2, 0, 0) //nolint:mnd
		require.NoError(t, err)

		// Use eagerTimeout: spec must arrive via eager push, not clock sync.
		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				specs := n.Store().AllWorkloadSpecs()
				if _, ok := specs[hash]; !ok {
					return false
				}
			}
			return true
		}, eagerTimeout, "workload spec did not propagate eagerly")
	})

	t.Run("ClaimTracking", func(t *testing.T) {
		c.RequireWorkloadReplicas(t, hash, 2) //nolint:mnd
	})
}

func TestPublicMesh_NodeJoinLeave(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := PublicMesh(t, 3, ctx) //nolint:mnd
	c.RequireConverged(t)

	t.Run("JoinReceivesOngoingGossip", func(t *testing.T) {
		c.Node("node-0").Node().UpsertService(9090, "grpc") //nolint:mnd

		joiner := c.AddNodeAndStart(t, "late-joiner", Public, ctx)

		introCtx, introCancel := context.WithTimeout(ctx, 5*time.Second) //nolint:mnd
		defer introCancel()
		err := c.Node("node-0").Node().ConnectPeer(introCtx, joiner.PeerKey(), []*net.UDPAddr{joiner.VirtualAddr()})
		require.NoError(t, err)

		c.RequirePeerVisible(t, "late-joiner")

		pk0 := c.PeerKeyByName("node-0")
		c.RequireEventually(t, func() bool {
			return joiner.Store().HasServicePort(pk0, 9090) //nolint:mnd
		}, assertTimeout, "late joiner did not see pre-existing service")
	})

	t.Run("LeaveTriggersDisconnection", func(t *testing.T) {
		pk2 := c.PeerKeyByName("node-2")
		c.Node("node-2").Stop()

		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				if n.Name() == "node-2" {
					continue
				}
				for _, cp := range n.ConnectedPeers() {
					if cp == pk2 {
						return false
					}
				}
			}
			return true
		}, assertTimeout, "node-2 was not dropped from connected peers")
	})
}

func TestPublicMesh_ServiceExposure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := PublicMesh(t, 4, ctx) //nolint:mnd
	c.RequireConverged(t)

	t.Run("VisibleClusterWide", func(t *testing.T) {
		c.Node("node-1").Node().UpsertService(3000, "web") //nolint:mnd
		pk1 := c.PeerKeyByName("node-1")
		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				if n.PeerKey() == pk1 {
					continue
				}
				if !n.Store().HasServicePort(pk1, 3000) { //nolint:mnd
					return false
				}
			}
			return true
		}, eagerTimeout, "service did not propagate eagerly")
	})

	t.Run("ConnectFromRemoteNode", func(t *testing.T) {
		pk1 := c.PeerKeyByName("node-1")
		c.Node("node-2").Node().AddDesiredConnection(pk1, 3000, 4000) //nolint:mnd

		c.RequireEventually(t, func() bool {
			for _, conn := range c.Node("node-2").Store().DesiredConnections() {
				if conn.PeerID == pk1 && conn.RemotePort == 3000 && conn.LocalPort == 4000 { //nolint:mnd
					return true
				}
			}
			return false
		}, assertTimeout, "desired connection not recorded")
	})
}

func TestPublicMesh_InviteFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := PublicMesh(t, 2, ctx) //nolint:mnd
	c.RequireConverged(t)

	t.Run("JoinWithValidToken", func(t *testing.T) {
		joiner := c.AddNodeAndStart(t, "invited-joiner", Public, ctx)

		node0 := c.Node("node-0")
		bootstrap := []*admissionv1.BootstrapPeer{{
			PeerPub: node0.PeerKey().Bytes(),
			Addrs:   []string{node0.VirtualAddr().String()},
		}}

		invite, err := auth.IssueInviteTokenWithSigner(
			node0.Node().Credentials().DelegationKey,
			joiner.PeerKey().Bytes(),
			bootstrap,
			time.Now(),
			5*time.Minute, //nolint:mnd
			24*time.Hour,  //nolint:mnd
		)
		require.NoError(t, err)

		_, err = joiner.Node().JoinWithInvite(ctx, invite)
		require.NoError(t, err)

		c.RequirePeerVisible(t, "invited-joiner")
	})

	t.Run("JoinWithInvalidToken", func(t *testing.T) {
		joiner := c.AddNodeAndStart(t, "bad-invite-joiner", Public, ctx)

		node0 := c.Node("node-0")
		bootstrap := []*admissionv1.BootstrapPeer{{
			PeerPub: node0.PeerKey().Bytes(),
			Addrs:   []string{node0.VirtualAddr().String()},
		}}

		wrongSubject, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)

		invite, err := auth.IssueInviteTokenWithSigner(
			node0.Node().Credentials().DelegationKey,
			wrongSubject,
			bootstrap,
			time.Now(),
			5*time.Minute, //nolint:mnd
			24*time.Hour,  //nolint:mnd
		)
		require.NoError(t, err)

		_, err = joiner.Node().JoinWithInvite(ctx, invite)
		require.Error(t, err)
	})

	t.Run("TokenSingleUse", func(t *testing.T) {
		first := c.AddNodeAndStart(t, "single-use-first", Public, ctx)
		second := c.AddNodeAndStart(t, "single-use-second", Public, ctx)

		node0 := c.Node("node-0")
		bootstrap := []*admissionv1.BootstrapPeer{{
			PeerPub: node0.PeerKey().Bytes(),
			Addrs:   []string{node0.VirtualAddr().String()},
		}}

		invite, err := auth.IssueInviteTokenWithSigner(
			node0.Node().Credentials().DelegationKey,
			nil,
			bootstrap,
			time.Now(),
			5*time.Minute, //nolint:mnd
			24*time.Hour,  //nolint:mnd
		)
		require.NoError(t, err)

		_, err = first.Node().JoinWithInvite(ctx, invite)
		require.NoError(t, err)

		_, err = second.Node().JoinWithInvite(ctx, invite)
		require.Error(t, err)
	})

	t.Run("BootstrapSSHDelegation", func(t *testing.T) {
		joiner := c.AddNodeAndStart(t, "ssh-bootstrap-joiner", Public, ctx)

		node0 := c.Node("node-0")
		introCtx, introCancel := context.WithTimeout(ctx, 5*time.Second) //nolint:mnd
		defer introCancel()
		err := node0.Node().ConnectPeer(introCtx, joiner.PeerKey(), []*net.UDPAddr{joiner.VirtualAddr()})
		require.NoError(t, err)

		pk0 := node0.PeerKey()
		c.RequireEventually(t, func() bool {
			for _, kp := range joiner.Store().KnownPeers() {
				if kp.PeerID == pk0 {
					return true
				}
			}
			return false
		}, assertTimeout, "joiner did not discover node-0 via gossip")

		require.NotNil(t, joiner.Node().Credentials())
	})

	t.Run("InvitedNodeConverges", func(t *testing.T) {
		joiner := c.AddNodeAndStart(t, "converge-joiner", Public, ctx)

		node0 := c.Node("node-0")
		bootstrap := []*admissionv1.BootstrapPeer{{
			PeerPub: node0.PeerKey().Bytes(),
			Addrs:   []string{node0.VirtualAddr().String()},
		}}

		invite, err := auth.IssueInviteTokenWithSigner(
			node0.Node().Credentials().DelegationKey,
			joiner.PeerKey().Bytes(),
			bootstrap,
			time.Now(),
			5*time.Minute, //nolint:mnd
			24*time.Hour,  //nolint:mnd
		)
		require.NoError(t, err)

		_, err = joiner.Node().JoinWithInvite(ctx, invite)
		require.NoError(t, err)

		pk0 := node0.PeerKey()
		pk1 := c.Node("node-1").PeerKey()
		c.RequireEventually(t, func() bool {
			known := joiner.Store().KnownPeers()
			var saw0, saw1 bool
			for _, kp := range known {
				if kp.PeerID == pk0 {
					saw0 = true
				}
				if kp.PeerID == pk1 {
					saw1 = true
				}
			}
			return saw0 && saw1
		}, assertTimeout, "invited node did not converge with original cluster")
	})
}

func TestPublicMesh_DenyPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := PublicMesh(t, 3, ctx) //nolint:mnd
	c.RequireConverged(t)
	c.RequireHealthy(t)

	pk2 := c.PeerKeyByName("node-2")

	t.Run("DeniedPeerDisconnected", func(t *testing.T) {
		c.Node("node-0").Node().DenyPeer(pk2)

		c.RequireEventually(t, func() bool {
			return len(c.Node("node-2").ConnectedPeers()) == 0
		}, assertTimeout, "denied peer still has connections")
	})

	t.Run("DeniedPeerCannotReconnect", func(t *testing.T) {
		c.RequireNever(t, func() bool {
			return len(c.Node("node-2").ConnectedPeers()) > 0
		}, 2*time.Second, "denied peer reconnected") //nolint:mnd
	})
}

func TestPublicMesh_EagerGossipPropagation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := PublicMesh(t, 3, ctx) //nolint:mnd
	c.RequireConverged(t)

	// Mutate state on node-0 by registering a service.
	c.Node("node-0").Node().UpsertService(5050, "eager-prop") //nolint:mnd

	// The mutation must arrive at every other node within eagerTimeout (500ms),
	// well under the 1s gossip tick configured in test clusters.
	pk0 := c.PeerKeyByName("node-0")
	for _, n := range c.Nodes() {
		if n.PeerKey() == pk0 {
			continue
		}
		name := n.Name()
		c.RequireEventually(t, func() bool {
			return n.Store().HasServicePort(pk0, 5050) //nolint:mnd
		}, eagerTimeout, name+" did not see service within eager timeout")
	}
}

func TestRelayRegions_GossipConvergence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := RelayRegions(t, 2, 2, ctx) //nolint:mnd

	t.Run("ConvergesAcrossRegions", func(t *testing.T) {
		c.RequireConverged(t)
	})

	t.Run("PrivateNodesDiscoverViaRelay", func(t *testing.T) {
		c.RequirePeerVisible(t, "region-0-node-0")
		c.RequirePeerVisible(t, "region-1-node-0")
	})
}

func TestRelayRegions_NATpunchthrough(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := New(t).
		SetDefaultLatency(2*time.Millisecond). //nolint:mnd
		SetDefaultJitter(0.15).                //nolint:mnd
		AddNode("relay-0", Public).
		AddNode("relay-1", Public).
		AddNode("region-0-node-0", Private).
		EnableNATPunch("region-0-node-0").
		AddNode("region-1-node-0", Private).
		EnableNATPunch("region-1-node-0").
		SetLatency("relay-0", "relay-1", 50*time.Millisecond). //nolint:mnd
		Introduce("relay-0", "relay-1").
		Introduce("region-0-node-0", "relay-0").
		Introduce("region-1-node-0", "relay-1").
		Start(ctx)

	c.RequireConverged(t)

	priv0Key := c.PeerKeyByName("region-0-node-0")
	priv1Key := c.PeerKeyByName("region-1-node-0")

	t.Run("PrivateToPrivateCrossRegion", func(t *testing.T) {
		c.RequireEventually(t, func() bool {
			for _, pk := range c.Node("region-0-node-0").ConnectedPeers() {
				if pk == priv1Key {
					return true
				}
			}
			return false
		}, assertTimeout, "private nodes should establish direct connection via NAT punch")
	})

	t.Run("CoordinationViaRelay", func(t *testing.T) {
		c.RequireEventually(t, func() bool {
			for _, pk := range c.Node("region-1-node-0").ConnectedPeers() {
				if pk == priv0Key {
					return true
				}
			}
			return false
		}, assertTimeout, "relay should mediate NAT punch coordination")
	})
}

func TestRelayRegions_PartitionAndHeal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := RelayRegions(t, 2, 1, ctx) //nolint:mnd
	c.RequireConverged(t)

	region0 := []string{"relay-0", "region-0-node-0"}
	region1 := []string{"relay-1", "region-1-node-0"}

	t.Run("PartitionBlocksPropagation", func(t *testing.T) {
		c.Partition(region0, region1)

		c.Node("relay-0").Node().UpsertService(7070, "isolated") //nolint:mnd

		pk0 := c.PeerKeyByName("relay-0")
		c.RequireNever(t, func() bool {
			return c.Node("relay-1").Store().HasServicePort(pk0, 7070) //nolint:mnd
		}, 2*time.Second, "partitioned relay should not see service") //nolint:mnd
	})

	t.Run("HealedNetworkPropagatesNewEvents", func(t *testing.T) {
		c.Heal(region0, region1)

		pk0 := c.PeerKeyByName("relay-0")
		c.RequireEventually(t, func() bool {
			return c.Node("relay-1").Store().HasServicePort(pk0, 7070) //nolint:mnd
		}, assertTimeout, "service did not propagate after heal")

		c.RequireConverged(t)
	})
}

func TestRelayRegions_WorkloadPlacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := RelayRegions(t, 2, 2, ctx) //nolint:mnd
	c.RequireConverged(t)

	relays := c.NodesByRole(Public)

	var hash string

	t.Run("CrossRegionSpecVisibility", func(t *testing.T) {
		var err error
		hash, err = c.Node(relays[0].Name()).Node().SeedWorkload(minimalWASM, 2, 0, 0) //nolint:mnd
		require.NoError(t, err)

		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				specs := n.Store().AllWorkloadSpecs()
				if _, ok := specs[hash]; !ok {
					return false
				}
			}
			return true
		}, eagerTimeout, "workload spec did not propagate eagerly across regions")
	})

	t.Run("CrossRegionClaimTracking", func(t *testing.T) {
		c.RequireWorkloadReplicas(t, hash, 2) //nolint:mnd
	})
}
