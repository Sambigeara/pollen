package node

import (
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func testPeerKey(b byte) types.PeerKey {
	var pk types.PeerKey
	pk[0] = b
	return pk
}

func TestBuildTargetPeerSetIncludesDesiredPeers(t *testing.T) {
	targets := []types.PeerKey{testPeerKey(1), testPeerKey(2)}
	desired := []store.Connection{
		{PeerID: testPeerKey(2), RemotePort: 80, LocalPort: 8080},
		{PeerID: testPeerKey(3), RemotePort: 443, LocalPort: 8443},
	}

	set := buildTargetPeerSet(targets, desired)

	require.Len(t, set, 3)
	_, ok1 := set[testPeerKey(1)]
	_, ok2 := set[testPeerKey(2)]
	_, ok3 := set[testPeerKey(3)]
	require.True(t, ok1)
	require.True(t, ok2)
	require.True(t, ok3)
}

func TestSyncPeersFromStateKeepsDesiredNonTargets(t *testing.T) {
	n := newMinimalNode(t, false)

	for i := range 20 {
		pk := testPeerKey(byte(i + 1))
		addKnownPeerForTopology(t, n.store, pk, i+1)
	}

	known := n.store.KnownPeers()
	peerInfos := knownPeersToPeerInfos(known)

	epoch := time.Now().Unix() / topology.EpochSeconds
	targets := topology.ComputeTargetPeers(
		n.store.LocalID,
		n.localCoord,
		peerInfos,
		topology.DefaultParams(epoch),
	)
	targetSet := make(map[types.PeerKey]struct{}, len(targets))
	for _, pk := range targets {
		targetSet[pk] = struct{}{}
	}

	var desiredPeer types.PeerKey
	var prunedPeer types.PeerKey
	for _, kp := range known {
		if _, targeted := targetSet[kp.PeerID]; targeted {
			continue
		}
		if desiredPeer == (types.PeerKey{}) {
			desiredPeer = kp.PeerID
			continue
		}
		prunedPeer = kp.PeerID
		break
	}

	require.NotEqual(t, types.PeerKey{}, desiredPeer)
	require.NotEqual(t, types.PeerKey{}, prunedPeer)
	_, desiredTargeted := targetSet[desiredPeer]
	require.False(t, desiredTargeted)
	_, prunedTargeted := targetSet[prunedPeer]
	require.False(t, prunedTargeted)

	n.peers.Step(time.Now(), peer.RetryPeer{PeerKey: desiredPeer})
	n.peers.Step(time.Now(), peer.RetryPeer{PeerKey: prunedPeer})

	_, desiredKnown := n.peers.Get(desiredPeer)
	require.True(t, desiredKnown)
	_, prunedKnown := n.peers.Get(prunedPeer)
	require.True(t, prunedKnown)

	n.store.AddDesiredConnection(desiredPeer, 8080, 18080)
	n.syncPeersFromState()

	_, desiredKnown = n.peers.Get(desiredPeer)
	require.True(t, desiredKnown, "desired connection peer should be retained")
	_, prunedKnown = n.peers.Get(prunedPeer)
	require.False(t, prunedKnown, "non-targeted peer without desired connection should be pruned")
}

func TestSyncPeersFromStateSuppressesRemotePrivateUnlessDesired(t *testing.T) {
	n := newMinimalNode(t, false)
	n.store.ApplyEvents([]*statev1.GossipEvent{{
		PeerId:  n.store.LocalID.String(),
		Counter: 2,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.1.1.20"}, LocalPort: 60611},
		},
	}}, false)
	n.store.SetObservedExternalIP("52.204.52.130")

	localGateway := testPeerKey(1)
	remotePrivate := testPeerKey(2)

	addCustomPeerForTopology(t, n.store, localGateway, []string{"10.1.1.234"}, 9001, 1, "", true)
	addCustomPeerForTopology(t, n.store, remotePrivate, []string{"10.2.2.10"}, 9002, 2, "34.252.188.39", false)

	n.syncPeersFromState()

	_, gatewayKnown := n.peers.Get(localGateway)
	require.True(t, gatewayKnown, "same-site gateway should be targeted")
	_, remoteKnown := n.peers.Get(remotePrivate)
	require.False(t, remoteKnown, "remote private peer should not be proactively targeted")

	n.store.AddDesiredConnection(remotePrivate, 8080, 18080)
	n.syncPeersFromState()

	_, remoteKnown = n.peers.Get(remotePrivate)
	require.True(t, remoteKnown, "desired connection should force remote private targeting")
}

func addKnownPeerForTopology(t *testing.T, s *store.Store, peerID types.PeerKey, ordinal int) {
	t.Helper()

	ip := fmt.Sprintf("10.1.%d.%d", ordinal/250, ordinal%250+1)
	s.ApplyEvents([]*statev1.GossipEvent{
		{
			PeerId:  peerID.String(),
			Counter: 1,
			Change: &statev1.GossipEvent_Network{
				Network: &statev1.NetworkChange{
					Ips:       []string{ip},
					LocalPort: uint32(9000 + ordinal),
				},
			},
		},
		{
			PeerId:  peerID.String(),
			Counter: 2,
			Change: &statev1.GossipEvent_Vivaldi{
				Vivaldi: &statev1.VivaldiCoordinateChange{
					X:      float64(ordinal),
					Y:      0,
					Height: 0.1,
				},
			},
		},
	}, false)
}

func addCustomPeerForTopology(t *testing.T, s *store.Store, peerID types.PeerKey, ips []string, port uint32, ordinal int, observedExternalIP string, publiclyAccessible bool) {
	t.Helper()

	counter := uint64(1)
	events := []*statev1.GossipEvent{
		{
			PeerId:  peerID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_Network{
				Network: &statev1.NetworkChange{Ips: ips, LocalPort: port},
			},
		},
		{
			PeerId:  peerID.String(),
			Counter: counter + 1,
			Change: &statev1.GossipEvent_Vivaldi{
				Vivaldi: &statev1.VivaldiCoordinateChange{X: float64(ordinal), Height: 0.1},
			},
		},
	}
	counter += 2
	if observedExternalIP != "" {
		events = append(events, &statev1.GossipEvent{
			PeerId:  peerID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_ObservedExternalIp{
				ObservedExternalIp: &statev1.ObservedExternalIPChange{Ip: observedExternalIP},
			},
		})
		counter++
	}
	if publiclyAccessible {
		events = append(events, &statev1.GossipEvent{
			PeerId:  peerID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_PubliclyAccessible{
				PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
			},
		})
	}
	s.ApplyEvents(events, false)
}

func setPubliclyAccessible(s *store.Store, peerID types.PeerKey, counter uint64) {
	s.ApplyEvents([]*statev1.GossipEvent{
		{
			PeerId:  peerID.String(),
			Counter: counter,
			Change: &statev1.GossipEvent_PubliclyAccessible{
				PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
			},
		},
	}, false)
}

// testMeshWrapper embeds a real mesh but lets tests override IsOutbound.
type testMeshWrapper struct {
	mesh.Mesh
	outbound map[types.PeerKey]bool
}

func (m *testMeshWrapper) IsOutbound(pk types.PeerKey) bool {
	return m.outbound[pk]
}

// simulateConnectedOutbound makes a peer appear connected+outbound for
// syncPeersFromState: marks it Connected in peer.Store, and registers it
// as outbound in the test mesh wrapper.
// disableFullMesh adds fake inbound peers so activePeerCount exceeds
// tinyClusterPeerThreshold and PreferFullMesh stays off.
func disableFullMesh(n *Node) {
	for i := range tinyClusterPeerThreshold + 1 {
		n.peers.Step(time.Now(), peer.ConnectPeer{
			PeerKey:      testPeerKey(byte(100 + i)),
			IP:           net.ParseIP("10.0.0.1"),
			ObservedPort: 9000,
		})
	}
}

func simulateConnectedOutbound(n *Node, pk types.PeerKey, wrapper *testMeshWrapper) {
	n.peers.Step(time.Now(), peer.ConnectPeer{
		PeerKey:      pk,
		IP:           net.ParseIP("10.0.0.1"),
		ObservedPort: 9000,
	})
	wrapper.outbound[pk] = true
}

// drainLocalPeerEvents empties the buffered localPeerEvents channel so
// repeated syncPeersFromState calls don't block.
func drainLocalPeerEvents(n *Node) {
	for {
		select {
		case <-n.localPeerEvents:
		default:
			return
		}
	}
}

// --- HMAC hysteresis tests ---

func TestHysteresisStartsInHMACMode(t *testing.T) {
	n := newMinimalNode(t, false)
	require.True(t, n.useHMACNearest, "fresh node should start in HMAC mode (coords untrusted)")
}

func TestHysteresisExitsHMACWhenErrorDrops(t *testing.T) {
	n := newMinimalNode(t, false)
	for i := range 20 {
		addKnownPeerForTopology(t, n.store, testPeerKey(byte(i+1)), i+1)
	}

	// localCoordErr starts at 1.0 → HMAC mode.
	require.True(t, n.useHMACNearest)

	// Drop error below exit threshold (0.35).
	n.localCoordErr.Store(math.Float64bits(0.30))
	n.syncPeersFromState()
	require.False(t, n.useHMACNearest, "should exit HMAC when error < 0.35")
}

func TestHysteresisStaysDistanceInDeadZone(t *testing.T) {
	n := newMinimalNode(t, false)
	for i := range 20 {
		addKnownPeerForTopology(t, n.store, testPeerKey(byte(i+1)), i+1)
	}

	// Exit HMAC first.
	n.localCoordErr.Store(math.Float64bits(0.30))
	n.syncPeersFromState()
	require.False(t, n.useHMACNearest)

	// Error rises into dead zone (0.35 < err < 0.6) — should stay distance-based.
	n.localCoordErr.Store(math.Float64bits(0.50))
	n.syncPeersFromState()
	require.False(t, n.useHMACNearest, "should stay distance-based in hysteresis dead zone")
}

func TestHysteresisReentersHMACAboveThreshold(t *testing.T) {
	n := newMinimalNode(t, false)
	for i := range 20 {
		addKnownPeerForTopology(t, n.store, testPeerKey(byte(i+1)), i+1)
	}

	// Exit HMAC.
	n.localCoordErr.Store(math.Float64bits(0.30))
	n.syncPeersFromState()
	require.False(t, n.useHMACNearest)

	// Error rises above enter threshold (0.6).
	n.localCoordErr.Store(math.Float64bits(0.65))
	n.syncPeersFromState()
	require.True(t, n.useHMACNearest, "should re-enter HMAC when error > 0.6")
}

// --- Revoke streak tests ---

func TestRevokeStreakPrivatePeerRevokedAfterThreshold(t *testing.T) {
	n := newMinimalNode(t, false)
	wrapper := &testMeshWrapper{Mesh: n.mesh, outbound: make(map[types.PeerKey]bool)}
	n.mesh = wrapper

	// Add many peers so some fall outside the target set.
	for i := range 20 {
		addKnownPeerForTopology(t, n.store, testPeerKey(byte(i+1)), i+1)
	}

	disableFullMesh(n)

	// Find a non-targeted peer and simulate it being connected+outbound.
	nonTarget := findNonTargetPeer(t, n)
	simulateConnectedOutbound(n, nonTarget, wrapper)

	// Tick 1 and 2: streak builds but peer stays connected.
	for tick := range revokeStreakThreshold - 1 {
		drainLocalPeerEvents(n)
		n.syncPeersFromState()
		_, ok := n.peers.Get(nonTarget)
		require.True(t, ok, "peer should survive at streak tick %d", tick+1)
		require.True(t, n.peers.InState(nonTarget, peer.PeerStateConnected),
			"peer should still be Connected at streak tick %d", tick+1)
	}

	// Tick 3: threshold reached → revoked.
	drainLocalPeerEvents(n)
	n.syncPeersFromState()
	_, ok := n.peers.Get(nonTarget)
	require.False(t, ok, "peer should be forgotten after %d non-target ticks", revokeStreakThreshold)
}

func TestRevokeStreakResetsOnTargetReentry(t *testing.T) {
	n := newMinimalNode(t, false)
	wrapper := &testMeshWrapper{Mesh: n.mesh, outbound: make(map[types.PeerKey]bool)}
	n.mesh = wrapper

	for i := range 20 {
		addKnownPeerForTopology(t, n.store, testPeerKey(byte(i+1)), i+1)
	}

	disableFullMesh(n)

	nonTarget := findNonTargetPeer(t, n)
	simulateConnectedOutbound(n, nonTarget, wrapper)

	// Build up streak to threshold - 1.
	for range revokeStreakThreshold - 1 {
		drainLocalPeerEvents(n)
		n.syncPeersFromState()
	}
	require.Equal(t, revokeStreakThreshold-1, n.nonTargetStreak[nonTarget])

	// Force peer back into target set via desired connection.
	n.store.AddDesiredConnection(nonTarget, 8080, 18080)
	drainLocalPeerEvents(n)
	n.syncPeersFromState()

	// Streak should be reset.
	require.Zero(t, n.nonTargetStreak[nonTarget], "streak should reset when peer re-enters target set")

	// Remove the desired connection — streak starts fresh.
	n.store.RemoveDesiredConnection(nonTarget, 8080, 18080)
	drainLocalPeerEvents(n)
	n.syncPeersFromState()
	require.Equal(t, 1, n.nonTargetStreak[nonTarget], "streak should start from 1 again after reset")
}

func TestRevokeStreakPublicPeerStickyLonger(t *testing.T) {
	n := newMinimalNode(t, false)
	wrapper := &testMeshWrapper{Mesh: n.mesh, outbound: make(map[types.PeerKey]bool)}
	n.mesh = wrapper

	// Create 20 peers, make several publicly accessible so infra slots (2)
	// are saturated and at least one public peer remains non-targeted.
	for i := range 20 {
		pk := testPeerKey(byte(i + 1))
		addKnownPeerForTopology(t, n.store, pk, i+1)
		if i < 5 {
			setPubliclyAccessible(n.store, pk, 10+uint64(i))
		}
	}

	disableFullMesh(n)

	nonTarget := findNonTargetPublicPeer(t, n)
	simulateConnectedOutbound(n, nonTarget, wrapper)

	// Run revokeStreakThreshold ticks — should NOT be revoked (needs 30 for public).
	for range revokeStreakThreshold {
		drainLocalPeerEvents(n)
		n.syncPeersFromState()
	}
	require.True(t, n.peers.InState(nonTarget, peer.PeerStateConnected),
		"public peer should survive the private threshold")

	// Run up to revokeStreakThresholdPublic - 1 total ticks.
	for range revokeStreakThresholdPublic - revokeStreakThreshold - 1 {
		drainLocalPeerEvents(n)
		n.syncPeersFromState()
	}
	require.True(t, n.peers.InState(nonTarget, peer.PeerStateConnected),
		"public peer should survive at tick %d", revokeStreakThresholdPublic-1)

	// One more tick → threshold reached, revoked.
	drainLocalPeerEvents(n)
	n.syncPeersFromState()
	_, ok := n.peers.Get(nonTarget)
	require.False(t, ok, "public peer should be forgotten after %d non-target ticks", revokeStreakThresholdPublic)
}

// --- Adaptive topology profile integration tests ---

func TestAllPublicClusterProducesFewerTargets(t *testing.T) {
	n := newMinimalNode(t, false)

	// Add 20 public peers.
	for i := range 20 {
		pk := testPeerKey(byte(i + 1))
		addCustomPeerForTopology(t, n.store, pk,
			[]string{fmt.Sprintf("203.0.113.%d", i+1)},
			uint32(9000+i), i+1, "", true)
	}

	known := n.store.KnownPeers()
	localIPs := n.store.NodeIPs(n.store.LocalID)
	shape := summarizeTopologyShape(localIPs, known)
	epoch := time.Now().Unix() / topology.EpochSeconds
	params := adaptiveTopologyParams(epoch, shape)
	params.LocalIPs = localIPs
	params.UseHMACNearest = n.useHMACNearest
	params.LocalNATType = nat.Easy

	peerInfos := knownPeersToPeerInfos(known)
	// Public peers are Easy NAT in production; set it here so NAT filtering
	// doesn't mask the budget difference we're testing.
	for i := range peerInfos {
		peerInfos[i].NatType = nat.Easy
	}
	targets := topology.ComputeTargetPeers(n.store.LocalID, n.localCoord, peerInfos, params)

	// With adaptive params (NearestK=2, RandomR=1, InfraMax=2) budget is 5.
	// Default budget would be 8 (NearestK=4, RandomR=2, InfraMax=2).
	defaultParams := topology.DefaultParams(epoch)
	defaultParams.UseHMACNearest = n.useHMACNearest
	defaultParams.LocalNATType = nat.Easy
	defaultParams.LocalIPs = localIPs
	defaultTargets := topology.ComputeTargetPeers(n.store.LocalID, n.localCoord, peerInfos, defaultParams)

	require.Less(t, len(targets), len(defaultTargets),
		"all-public cluster should produce fewer targets with adaptive params")
}

func TestMixedClusterRetainsSameSiteAndPublicPeers(t *testing.T) {
	n := newMinimalNode(t, false)

	// Local node has 10.1.x.x IPs (set by newMinimalNode as 127.0.0.1, override via store).
	// Use custom IPs for the local node.
	n.store.ApplyEvents([]*statev1.GossipEvent{
		{
			PeerId:  n.store.LocalID.String(),
			Counter: 1,
			Change: &statev1.GossipEvent_Network{
				Network: &statev1.NetworkChange{
					Ips:       []string{"10.1.0.1"},
					LocalPort: 9000,
				},
			},
		},
	}, false)

	// 2 public peers (infra backbone).
	for i := range 2 {
		pk := testPeerKey(byte(i + 1))
		addCustomPeerForTopology(t, n.store, pk,
			[]string{fmt.Sprintf("203.0.113.%d", i+1)},
			uint32(9000+i), i+1, "", true)
	}

	// 6 same-site private peers (10.1.x.x).
	for i := range 6 {
		pk := testPeerKey(byte(i + 3))
		addCustomPeerForTopology(t, n.store, pk,
			[]string{fmt.Sprintf("10.1.0.%d", i+10)},
			uint32(9010+i), i+3, "", false)
	}

	// 4 remote private peers (10.2.x.x).
	for i := range 4 {
		pk := testPeerKey(byte(i + 9))
		addCustomPeerForTopology(t, n.store, pk,
			[]string{fmt.Sprintf("10.2.0.%d", i+10)},
			uint32(9020+i), i+9, "34.252.188.39", false)
	}

	known := n.store.KnownPeers()
	localIPs := n.store.NodeIPs(n.store.LocalID)
	shape := summarizeTopologyShape(localIPs, known)

	// With 2/12 public (~17%), should keep default budgets.
	require.Less(t, float64(shape.publicCount)/float64(shape.totalCount), 0.5)

	epoch := time.Now().Unix() / topology.EpochSeconds
	params := adaptiveTopologyParams(epoch, shape)
	require.Equal(t, topology.DefaultNearestK, params.NearestK, "mixed cluster should keep default NearestK")
	require.Equal(t, topology.DefaultRandomR, params.RandomR, "mixed cluster should keep default RandomR")
}

func knownPeersToPeerInfos(peers []store.KnownPeer) []topology.PeerInfo {
	infos := make([]topology.PeerInfo, 0, len(peers))
	for _, kp := range peers {
		infos = append(infos, topology.PeerInfo{
			Key:                kp.PeerID,
			Coord:              kp.VivaldiCoord,
			IPs:                kp.IPs,
			NatType:            kp.NatType,
			ObservedExternalIP: kp.ObservedExternalIP,
			PubliclyAccessible: kp.PubliclyAccessible,
		})
	}
	return infos
}

// findNonTargetPublicPeer returns a publicly-accessible peer key that is known
// in the store but NOT in the current topology target set.
func findNonTargetPublicPeer(t *testing.T, n *Node) types.PeerKey {
	t.Helper()

	known := n.store.KnownPeers()
	peerInfos := knownPeersToPeerInfos(known)

	epoch := time.Now().Unix() / topology.EpochSeconds
	params := topology.DefaultParams(epoch)
	params.UseHMACNearest = n.useHMACNearest
	targets := topology.ComputeTargetPeers(n.store.LocalID, n.localCoord, peerInfos, params)
	targetSet := make(map[types.PeerKey]struct{}, len(targets))
	for _, pk := range targets {
		targetSet[pk] = struct{}{}
	}

	for _, kp := range known {
		if _, targeted := targetSet[kp.PeerID]; !targeted && kp.PubliclyAccessible {
			return kp.PeerID
		}
	}
	t.Fatal("no non-targeted public peer found — add more public peers")
	return types.PeerKey{}
}

// findNonTargetPeer returns a peer key that is known in the store but NOT in
// the current topology target set.
func findNonTargetPeer(t *testing.T, n *Node) types.PeerKey {
	t.Helper()

	known := n.store.KnownPeers()
	peerInfos := knownPeersToPeerInfos(known)

	epoch := time.Now().Unix() / topology.EpochSeconds
	params := topology.DefaultParams(epoch)
	params.UseHMACNearest = n.useHMACNearest
	targets := topology.ComputeTargetPeers(n.store.LocalID, n.localCoord, peerInfos, params)
	targetSet := make(map[types.PeerKey]struct{}, len(targets))
	for _, pk := range targets {
		targetSet[pk] = struct{}{}
	}

	for _, kp := range known {
		if _, targeted := targetSet[kp.PeerID]; !targeted {
			return kp.PeerID
		}
	}
	t.Fatal("all known peers are targeted — need more peers to find a non-target")
	return types.PeerKey{}
}
