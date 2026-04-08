//go:build integration

package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"net/netip"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/types"
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
		c.Node("node-0").Node().Tunneling().ExposeService(8080, "http", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP) //nolint:mnd
		// Use eagerTimeout: state must arrive via eager push, not digest sync.
		c.RequireEventually(t, func() bool {
			pk := c.PeerKeyByName("node-0")
			for _, n := range c.Nodes() {
				if n.PeerKey() == pk {
					continue
				}
				if !snapshotHasService(n.Store().Snapshot(), pk, 8080, "http") { //nolint:mnd
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
					nv, ok := n.Store().Snapshot().Nodes[other.PeerKey()]
					if !ok || nv.VivaldiCoord == nil || (nv.VivaldiCoord.X == 0 && nv.VivaldiCoord.Y == 0 && nv.VivaldiCoord.Height == 0) {
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
		hash, err = c.Node("node-0").Node().SeedWorkload(minimalWASM, "test-workload", 2, 0, 0, 0) //nolint:mnd
		require.NoError(t, err)

		// Use eagerTimeout: spec must arrive via eager push, not digest sync.
		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				specs := n.Store().Snapshot().Specs
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

	t.Run("Unseed", func(t *testing.T) {
		err := c.Node("node-0").Node().UnseedWorkload(hash)
		require.NoError(t, err)
		c.RequireWorkloadGone(t, hash)
	})

	t.Run("Reseed", func(t *testing.T) {
		var err error
		hash, err = c.Node("node-0").Node().SeedWorkload(minimalWASM, "test-workload", 3, 0, 0, 0) //nolint:mnd
		require.NoError(t, err)

		c.RequireWorkloadSpecOnAllNodes(t, hash, 3) //nolint:mnd
		c.RequireWorkloadReplicas(t, hash, 3)       //nolint:mnd
	})

	t.Run("ReplicaCountUpdate", func(t *testing.T) {
		err := c.Node("node-0").Node().UnseedWorkload(hash)
		require.NoError(t, err)
		c.RequireWorkloadGone(t, hash)

		var err2 error
		hash, err2 = c.Node("node-0").Node().SeedWorkload(minimalWASM, "test-workload", 1, 0, 0, 0)
		require.NoError(t, err2)

		c.RequireWorkloadSpecOnAllNodes(t, hash, 1)
		c.RequireWorkloadReplicas(t, hash, 1)
	})
}

func TestPublicMesh_NodeJoinLeave(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := PublicMesh(t, 3, ctx) //nolint:mnd
	c.RequireConverged(t)

	t.Run("JoinReceivesOngoingGossip", func(t *testing.T) {
		c.Node("node-0").Node().Tunneling().ExposeService(9090, "grpc", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP) //nolint:mnd

		joiner := c.AddNodeAndStart(t, "late-joiner", Public, ctx)

		introCtx, introCancel := context.WithTimeout(ctx, 5*time.Second) //nolint:mnd
		defer introCancel()
		err := c.Node("node-0").Node().Connect(introCtx, joiner.PeerKey(), []netip.AddrPort{joiner.VirtualAddr().AddrPort()})
		require.NoError(t, err)

		c.RequirePeerVisible(t, "late-joiner")

		pk0 := c.PeerKeyByName("node-0")
		c.RequireEventually(t, func() bool {
			return snapshotHasService(joiner.Store().Snapshot(), pk0, 9090, "grpc") //nolint:mnd
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
		c.Node("node-1").Node().Tunneling().ExposeService(3000, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP) //nolint:mnd
		pk1 := c.PeerKeyByName("node-1")
		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				if n.PeerKey() == pk1 {
					continue
				}
				if !snapshotHasService(n.Store().Snapshot(), pk1, 3000, "web") { //nolint:mnd
					return false
				}
			}
			return true
		}, eagerTimeout, "service did not propagate eagerly")
	})

	t.Run("ConnectFromRemoteNode", func(t *testing.T) {
		pk1 := c.PeerKeyByName("node-1")
		c.Node("node-2").Node().AddDesiredConnection(pk1, 3000, 4000, statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP) //nolint:mnd

		c.RequireEventually(t, func() bool {
			for _, conn := range c.Node("node-2").Node().DesiredConnections() {
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

		invite, err := node0.Node().Credentials().DelegationKey().IssueInviteToken(
			joiner.PeerKey().Bytes(),
			bootstrap,
			time.Now(),
			5*time.Minute, //nolint:mnd
			24*time.Hour,  //nolint:mnd
			nil,
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

		invite, err := node0.Node().Credentials().DelegationKey().IssueInviteToken(
			wrongSubject,
			bootstrap,
			time.Now(),
			5*time.Minute, //nolint:mnd
			24*time.Hour,  //nolint:mnd
			nil,
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

		invite, err := node0.Node().Credentials().DelegationKey().IssueInviteToken(
			nil,
			bootstrap,
			time.Now(),
			5*time.Minute, //nolint:mnd
			24*time.Hour,  //nolint:mnd
			nil,
		)
		require.NoError(t, err)

		_, err = first.Node().JoinWithInvite(ctx, invite)
		require.NoError(t, err)

		_, err = second.Node().JoinWithInvite(ctx, invite)
		require.Error(t, err)
	})

	t.Run("BootstrapGossipAndCredentials", func(t *testing.T) {
		joiner := c.AddNodeAndStart(t, "ssh-bootstrap-joiner", Public, ctx)

		node0 := c.Node("node-0")
		introCtx, introCancel := context.WithTimeout(ctx, 5*time.Second) //nolint:mnd
		defer introCancel()
		err := node0.Node().Connect(introCtx, joiner.PeerKey(), []netip.AddrPort{joiner.VirtualAddr().AddrPort()})
		require.NoError(t, err)

		pk0 := node0.PeerKey()
		c.RequireEventually(t, func() bool {
			_, ok := joiner.Store().Snapshot().Nodes[pk0]
			return ok
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

		invite, err := node0.Node().Credentials().DelegationKey().IssueInviteToken(
			joiner.PeerKey().Bytes(),
			bootstrap,
			time.Now(),
			5*time.Minute, //nolint:mnd
			24*time.Hour,  //nolint:mnd
			nil,
		)
		require.NoError(t, err)

		_, err = joiner.Node().JoinWithInvite(ctx, invite)
		require.NoError(t, err)

		pk0 := node0.PeerKey()
		pk1 := c.Node("node-1").PeerKey()
		c.RequireEventually(t, func() bool {
			snap := joiner.Store().Snapshot()
			var saw0, saw1 bool
			for pk := range snap.Nodes {
				if pk == pk0 {
					saw0 = true
				}
				if pk == pk1 {
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
		c.Node("node-0").Node().Membership().DenyPeer(pk2) //nolint:errcheck

		c.RequireEventually(t, func() bool {
			return len(c.Node("node-2").ConnectedPeers()) == 0
		}, assertTimeout, "denied peer still has connections")
	})

	t.Run("DeniedKeyInSnapshot", func(t *testing.T) {
		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				if n.Name() == "node-2" {
					continue
				}
				snap := n.Store().Snapshot()
				found := false
				for _, dk := range snap.DeniedKeys {
					if dk == pk2 {
						found = true
						break
					}
				}
				if !found {
					return false
				}
			}
			return true
		}, assertTimeout, "denied peer key not propagated to all surviving nodes")
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
	c.Node("node-0").Node().Tunneling().ExposeService(5050, "eager-prop", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP) //nolint:mnd

	// The mutation must arrive at every other node within eagerTimeout (500ms),
	// well under the 1s gossip tick configured in test clusters.
	pk0 := c.PeerKeyByName("node-0")
	for _, n := range c.Nodes() {
		if n.PeerKey() == pk0 {
			continue
		}
		name := n.Name()
		c.RequireEventually(t, func() bool {
			return snapshotHasService(n.Store().Snapshot(), pk0, 5050, "eager-prop") //nolint:mnd
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
		SetAddr("region-0-node-0", &net.UDPAddr{IP: net.IPv4(10, 1, 0, 1), Port: 60611}). //nolint:mnd
		EnableNATPunch("region-0-node-0").
		AddNode("region-1-node-0", Private).
		SetAddr("region-1-node-0", &net.UDPAddr{IP: net.IPv4(10, 2, 0, 1), Port: 60611}). //nolint:mnd
		EnableNATPunch("region-1-node-0").
		SetLatency("relay-0", "relay-1", 50*time.Millisecond). //nolint:mnd
		Introduce("relay-0", "relay-1").
		Introduce("region-0-node-0", "relay-0").
		Introduce("region-1-node-0", "relay-1").
		Introduce("region-1-node-0", "relay-0").
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

func TestRelayRegions_ExternalPortDistinct(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	// Two private nodes on the same LAN behind a shared NAT gateway.
	// The NAT assigns each a distinct external port on the same public IP.
	c := New(t).
		SetDefaultLatency(2*time.Millisecond). //nolint:mnd
		SetDefaultJitter(0.15).                //nolint:mnd
		AddNode("relay", Public).
		SetAddr("relay", &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 60611}). //nolint:mnd
		AddNode("lan-0", Private).
		SetAddr("lan-0", &net.UDPAddr{IP: net.IPv4(10, 1, 0, 1), Port: 60611}).          //nolint:mnd
		SetNATMapping("lan-0", &net.UDPAddr{IP: net.IPv4(203, 0, 113, 1), Port: 45678}). //nolint:mnd
		AddNode("lan-1", Private).
		SetAddr("lan-1", &net.UDPAddr{IP: net.IPv4(10, 1, 0, 2), Port: 60611}).          //nolint:mnd
		SetNATMapping("lan-1", &net.UDPAddr{IP: net.IPv4(203, 0, 113, 1), Port: 45999}). //nolint:mnd
		Introduce("lan-0", "relay").
		Introduce("lan-1", "relay").
		Start(ctx)

	c.RequireConverged(t)

	pk0 := c.PeerKeyByName("lan-0")
	pk1 := c.PeerKeyByName("lan-1")

	t.Run("SameObservedExternalIP", func(t *testing.T) {
		c.RequireEventually(t, func() bool {
			snap := c.Node("relay").Store().Snapshot()
			nv0, ok0 := snap.Nodes[pk0]
			nv1, ok1 := snap.Nodes[pk1]
			if !ok0 || !ok1 {
				return false
			}
			return nv0.ObservedExternalIP == "203.0.113.1" &&
				nv1.ObservedExternalIP == "203.0.113.1"
		}, assertTimeout, "both LAN nodes should share same observed external IP")
	})

	t.Run("DistinctExternalPorts", func(t *testing.T) {
		c.RequireEventually(t, func() bool {
			snap := c.Node("relay").Store().Snapshot()
			nv0 := snap.Nodes[pk0]
			nv1 := snap.Nodes[pk1]
			return nv0.ExternalPort != 0 && nv1.ExternalPort != 0 &&
				nv0.ExternalPort != nv1.ExternalPort
		}, assertTimeout, "LAN nodes behind same NAT must have distinct external ports")
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

		c.Node("relay-0").Node().Tunneling().ExposeService(7070, "isolated", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP) //nolint:mnd

		pk0 := c.PeerKeyByName("relay-0")
		c.RequireNever(t, func() bool {
			return snapshotHasService(c.Node("relay-1").Store().Snapshot(), pk0, 7070, "isolated") //nolint:mnd
		}, 2*time.Second, "partitioned relay should not see service") //nolint:mnd
	})

	t.Run("HealedNetworkPropagatesNewEvents", func(t *testing.T) {
		c.Heal(region0, region1)

		pk0 := c.PeerKeyByName("relay-0")
		c.RequireEventually(t, func() bool {
			return snapshotHasService(c.Node("relay-1").Store().Snapshot(), pk0, 7070, "isolated") //nolint:mnd
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
		hash, err = c.Node(relays[0].Name()).Node().SeedWorkload(minimalWASM, "test-workload", 2, 0, 0, 0) //nolint:mnd
		require.NoError(t, err)

		c.RequireEventually(t, func() bool {
			for _, n := range c.Nodes() {
				specs := n.Store().Snapshot().Specs
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

	t.Run("Unseed", func(t *testing.T) {
		err := c.Node(relays[0].Name()).Node().UnseedWorkload(hash)
		require.NoError(t, err)
		c.RequireWorkloadGone(t, hash)
	})
}

func TestPublicMesh_WorkloadMigration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := PublicMesh(t, 4, ctx) //nolint:mnd
	c.RequireConverged(t)

	hash, err := c.Node("node-0").Node().SeedWorkload(minimalWASM, "test-workload", 2, 0, 0, 0) //nolint:mnd
	require.NoError(t, err)
	c.RequireWorkloadReplicas(t, hash, 2) //nolint:mnd

	// Find a node that claimed the workload.
	var victim string
	require.Eventually(t, func() bool {
		for _, n := range c.Nodes() {
			snap := n.Store().Snapshot()
			if claimants, ok := snap.Claims[hash]; ok {
				if _, claimed := claimants[n.PeerKey()]; claimed {
					victim = n.Name()
					return true
				}
			}
		}
		return false
	}, assertTimeout, assertPoll, "no claimant found")

	// Stop the claimant node.
	c.Node(victim).Stop()

	// The scheduler on surviving nodes should re-claim to maintain 2 replicas.
	// Collect distinct non-victim claimants across all surviving snapshots.
	victimKey := c.PeerKeyByName(victim)
	c.RequireEventually(t, func() bool {
		distinct := make(map[types.PeerKey]struct{})
		for _, n := range c.Nodes() {
			if n.Name() == victim {
				continue
			}
			claimants, ok := n.Store().Snapshot().Claims[hash]
			if !ok {
				continue
			}
			for pk := range claimants {
				if pk != victimKey {
					distinct[pk] = struct{}{}
				}
			}
		}
		return len(distinct) >= 2 //nolint:mnd
	}, 30*time.Second, assertPoll, "replicas not maintained after node %s stopped", victim) //nolint:mnd
}

func TestPublicMesh_ServiceUnexposure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := PublicMesh(t, 3, ctx) //nolint:mnd
	c.RequireConverged(t)

	c.Node("node-0").Node().Tunneling().ExposeService(6060, "temp-svc", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP) //nolint:mnd
	c.RequireServiceVisible(t, "node-0", 6060, "temp-svc")                                                            //nolint:mnd

	require.NoError(t, c.Node("node-0").Node().Tunneling().UnexposeService("temp-svc"))

	c.RequireServiceGone(t, "node-0", 6060, "temp-svc") //nolint:mnd
}

func TestPublicMesh_ExpiredInviteToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	c := PublicMesh(t, 2, ctx) //nolint:mnd
	c.RequireConverged(t)

	joiner := c.AddNodeAndStart(t, "expired-joiner", Public, ctx)

	node0 := c.Node("node-0")
	bootstrap := []*admissionv1.BootstrapPeer{{
		PeerPub: node0.PeerKey().Bytes(),
		Addrs:   []string{node0.VirtualAddr().String()},
	}}

	// Issue a token with now=1h ago and TTL=1min → expired 59min ago.
	invite, err := node0.Node().Credentials().DelegationKey().IssueInviteToken(
		joiner.PeerKey().Bytes(),
		bootstrap,
		time.Now().Add(-time.Hour),
		time.Minute,
		24*time.Hour, //nolint:mnd
		nil,
	)
	require.NoError(t, err)

	_, err = joiner.Node().JoinWithInvite(ctx, invite)
	require.Error(t, err, "expired invite token should be rejected")
}

func TestPublicMesh_VivaldiDistancesReflectTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	t.Cleanup(cancel)

	// Three fully connected nodes with asymmetric link latencies:
	//   near ↔ origin: 10ms    far ↔ origin: 50ms    near ↔ far: 50ms
	// Vivaldi should embed origin closer to near than to far.
	c := New(t).
		SetDefaultLatency(50*time.Millisecond). //nolint:mnd
		AddNode("origin", Public).
		AddNode("near", Public).
		AddNode("far", Public).
		SetLatency("origin", "near", 10*time.Millisecond). //nolint:mnd
		Introduce("origin", "near").
		Introduce("origin", "far").
		Introduce("near", "far").
		Start(ctx)

	c.RequireConverged(t)

	pkOrigin := c.PeerKeyByName("origin")
	pkNear := c.PeerKeyByName("near")
	pkFar := c.PeerKeyByName("far")

	c.RequireEventually(t, func() bool {
		snap := c.Node("origin").Store().Snapshot()
		nvOrigin, ok0 := snap.Nodes[pkOrigin]
		nvNear, ok1 := snap.Nodes[pkNear]
		nvFar, ok2 := snap.Nodes[pkFar]
		if !ok0 || !ok1 || !ok2 {
			return false
		}
		if nvOrigin.VivaldiCoord == nil || nvNear.VivaldiCoord == nil || nvFar.VivaldiCoord == nil {
			return false
		}
		distNear := coords.Distance(*nvOrigin.VivaldiCoord, *nvNear.VivaldiCoord)
		distFar := coords.Distance(*nvOrigin.VivaldiCoord, *nvFar.VivaldiCoord)
		return distNear > 0 && distFar > 0 && distNear < distFar
	}, assertTimeout, "vivaldi: 10ms neighbor should be closer than 50ms neighbor")
}
