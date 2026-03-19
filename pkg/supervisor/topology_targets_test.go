package supervisor

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/tunneling"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func testPeerKey(b byte) types.PeerKey {
	var pk types.PeerKey
	pk[0] = b
	return pk
}

// applyTestEvents is a test helper that applies gossip events to a store via ApplyDelta.
func applyTestEvents(tb testing.TB, s state.StateStore, events []*statev1.GossipEvent) {
	tb.Helper()
	batch := &statev1.GossipEventBatch{Events: events}
	data, err := batch.MarshalVT()
	require.NoError(tb, err)
	from := types.PeerKey{}
	if len(events) > 0 && events[0].GetPeerId() != "" {
		pk, pkErr := types.PeerKeyFromString(events[0].GetPeerId())
		if pkErr == nil {
			from = pk
		}
	}
	_, _, err = s.ApplyDelta(from, data)
	require.NoError(tb, err)
}

func TestSyncPeersFromStateConstructsLastAddrFromObservedExternalIP(t *testing.T) {
	pk := testPeerKey(1)

	tests := []struct {
		name         string
		externalPort uint32
		wantPort     int
	}{
		{
			name:         "uses ExternalPort when set",
			externalPort: 45000,
			wantPort:     45000,
		},
		{
			name:         "falls back to LocalPort when ExternalPort is zero",
			externalPort: 0,
			wantPort:     9001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newMinimalNode(t, false)
			wrapper := newTestMeshWrapper(n.mesh, n.meshInternal)
			n.mesh = wrapper
			n.meshInternal = wrapper

			counter := uint64(1)
			events := []*statev1.GossipEvent{
				{
					PeerId:  pk.String(),
					Counter: counter,
					Change: &statev1.GossipEvent_Network{
						Network: &statev1.NetworkChange{
							Ips:       []string{"10.2.2.10"},
							LocalPort: 9001,
						},
					},
				},
				{
					PeerId:  pk.String(),
					Counter: counter + 1,
					Change: &statev1.GossipEvent_Vivaldi{
						Vivaldi: &statev1.VivaldiCoordinateChange{X: 1, Height: 0.1},
					},
				},
				{
					PeerId:  pk.String(),
					Counter: counter + 2,
					Change: &statev1.GossipEvent_ObservedExternalIp{
						ObservedExternalIp: &statev1.ObservedExternalIPChange{Ip: "34.252.188.39"},
					},
				},
			}
			if tt.externalPort != 0 {
				events = append(events, &statev1.GossipEvent{
					PeerId:  pk.String(),
					Counter: counter + 3,
					Change: &statev1.GossipEvent_ExternalPort{
						ExternalPort: &statev1.ExternalPortChange{ExternalPort: tt.externalPort},
					},
				})
			}
			applyTestEvents(t, n.store, events)

			// Force the peer into the target set via desired connection.
			n.AddDesiredConnection(pk, 8080, 18080)
			n.syncPeersFromState(context.Background(), n.store.Snapshot())

			require.True(t, wrapper.HasPeer(pk), "peer should be discovered")

			recorded := wrapper.lastDiscoverAddr[pk]
			require.NotNil(t, recorded, "DiscoverPeer should receive a lastAddr from ObservedExternalIP fallback")
			require.Equal(t, "34.252.188.39", recorded.IP.String())
			require.Equal(t, tt.wantPort, recorded.Port)
		})
	}
}

func TestBuildTargetPeerSetIncludesDesiredPeers(t *testing.T) {
	targets := []types.PeerKey{testPeerKey(1), testPeerKey(2)}
	desiredPeers := []types.PeerKey{testPeerKey(2), testPeerKey(3)}

	set := buildTargetPeerSet(targets, desiredPeers)

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
	wrapper := newTestMeshWrapper(n.mesh, n.meshInternal)
	n.mesh = wrapper
	n.meshInternal = wrapper
	disableFullMesh(wrapper)

	for i := range 20 {
		pk := testPeerKey(byte(i + 1))
		addKnownPeerForTopology(t, n.store, pk, i+1)
	}

	known := knownPeersFromSnapshot(n.store.Snapshot())
	peerInfos := knownPeersToPeerInfos(known)

	targetSet := computeTargetsLikeProduction(n, peerInfos)

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

	require.NotEqual(t, types.PeerKey{}, desiredPeer, "need at least one non-targeted peer for desired")
	require.NotEqual(t, types.PeerKey{}, prunedPeer, "need at least two non-targeted peers")

	n.meshInternal.RetryPeer(desiredPeer)
	n.meshInternal.RetryPeer(prunedPeer)

	require.True(t, n.meshInternal.HasPeer(desiredPeer))
	require.True(t, n.meshInternal.HasPeer(prunedPeer))

	n.AddDesiredConnection(desiredPeer, 8080, 18080)
	n.syncPeersFromState(context.Background(), n.store.Snapshot())

	require.True(t, n.meshInternal.HasPeer(desiredPeer), "desired connection peer should be retained")
	require.False(t, n.meshInternal.HasPeer(prunedPeer), "non-targeted peer without desired connection should be pruned")
}

func TestSyncPeersFromStateSuppressesRemotePrivateUnlessDesired(t *testing.T) {
	n := newMinimalNode(t, false)
	wrapper := newTestMeshWrapper(n.mesh, n.meshInternal)
	n.mesh = wrapper
	n.meshInternal = wrapper
	disableFullMesh(wrapper)
	n.store.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.1.1.20:60611")})
	setLocalObservedExternalIP(t, n.store, "52.204.52.130")

	localGateway := testPeerKey(1)
	remotePrivate := testPeerKey(2)

	addCustomPeerForTopology(t, n.store, localGateway, []string{"10.1.1.234"}, 9001, 1, "", true)
	addCustomPeerForTopology(t, n.store, remotePrivate, []string{"10.2.2.10"}, 9002, 2, "34.252.188.39", false)

	n.syncPeersFromState(context.Background(), n.store.Snapshot())

	require.True(t, n.meshInternal.HasPeer(localGateway), "same-site gateway should be targeted")
	require.False(t, n.meshInternal.HasPeer(remotePrivate), "remote private peer should not be proactively targeted")

	n.AddDesiredConnection(remotePrivate, 8080, 18080)
	n.syncPeersFromState(context.Background(), n.store.Snapshot())

	require.True(t, n.meshInternal.HasPeer(remotePrivate), "desired connection should force remote private targeting")
}

func addKnownPeerForTopology(t *testing.T, s state.StateStore, peerID types.PeerKey, ordinal int) {
	t.Helper()

	ip := fmt.Sprintf("10.1.%d.%d", ordinal/250, ordinal%250+1)
	applyTestEvents(t, s, []*statev1.GossipEvent{
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
	})
}

func addCustomPeerForTopology(t *testing.T, s state.StateStore, peerID types.PeerKey, ips []string, port uint32, ordinal int, observedExternalIP string, publiclyAccessible bool) {
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
	applyTestEvents(t, s, events)
}

func setPubliclyAccessible(tb testing.TB, s state.StateStore, peerID types.PeerKey, counter uint64) {
	tb.Helper()
	applyTestEvents(tb, s, []*statev1.GossipEvent{{
		PeerId:  peerID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_PubliclyAccessible{
			PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
		},
	}})
}

// testMeshWrapper embeds a real mesh but lets tests override connectivity
// state. It tracks connected and outbound peers independently of the
// underlying transport's session registry.
type testMeshWrapper struct {
	Transport
	TransportInternal
	outbound         map[types.PeerKey]bool
	connected        map[types.PeerKey]bool
	lastDiscoverAddr map[types.PeerKey]*net.UDPAddr
}

func newTestMeshWrapper(t Transport, ti TransportInternal) *testMeshWrapper {
	return &testMeshWrapper{
		Transport:         t,
		TransportInternal: ti,
		outbound:          make(map[types.PeerKey]bool),
		connected:         make(map[types.PeerKey]bool),
		lastDiscoverAddr:  make(map[types.PeerKey]*net.UDPAddr),
	}
}

func (m *testMeshWrapper) DiscoverPeer(pk types.PeerKey, ips []net.IP, port int, lastAddr *net.UDPAddr, privatelyRoutable, publiclyAccessible bool) {
	m.lastDiscoverAddr[pk] = lastAddr
	m.TransportInternal.DiscoverPeer(pk, ips, port, lastAddr, privatelyRoutable, publiclyAccessible)
}

func (m *testMeshWrapper) IsOutbound(pk types.PeerKey) bool {
	return m.outbound[pk]
}

func (m *testMeshWrapper) ConnectedPeers() []types.PeerKey {
	keys := make([]types.PeerKey, 0, len(m.connected))
	for pk := range m.connected {
		keys = append(keys, pk)
	}
	return keys
}

func (m *testMeshWrapper) IsPeerConnected(pk types.PeerKey) bool {
	return m.connected[pk]
}

func (m *testMeshWrapper) MarkPeerConnected(pk types.PeerKey, ip net.IP, port int) {
	m.connected[pk] = true
	m.TransportInternal.MarkPeerConnected(pk, ip, port)
}

// disableFullMesh adds fake connected peers so activePeerCount exceeds
// tinyClusterPeerThreshold and PreferFullMesh stays off.
func disableFullMesh(w *testMeshWrapper) {
	for i := range tinyClusterPeerThreshold + 1 {
		pk := testPeerKey(byte(100 + i))
		w.MarkPeerConnected(pk, net.ParseIP("10.0.0.1"), 9000)
	}
}

func simulateConnectedOutbound(pk types.PeerKey, wrapper *testMeshWrapper) {
	wrapper.MarkPeerConnected(pk, net.ParseIP("10.0.0.1"), 9000)
	wrapper.outbound[pk] = true
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
	n.vivaldiErr.Reset(0.30)
	n.syncPeersFromState(context.Background(), n.store.Snapshot())
	require.False(t, n.useHMACNearest, "should exit HMAC when error < 0.35")
}

func TestHysteresisStaysDistanceInDeadZone(t *testing.T) {
	n := newMinimalNode(t, false)
	for i := range 20 {
		addKnownPeerForTopology(t, n.store, testPeerKey(byte(i+1)), i+1)
	}

	// Exit HMAC first.
	n.vivaldiErr.Reset(0.30)
	n.syncPeersFromState(context.Background(), n.store.Snapshot())
	require.False(t, n.useHMACNearest)

	// Error rises into dead zone (0.35 < err < 0.6) — should stay distance-based.
	n.vivaldiErr.Reset(0.50)
	n.syncPeersFromState(context.Background(), n.store.Snapshot())
	require.False(t, n.useHMACNearest, "should stay distance-based in hysteresis dead zone")
}

func TestHysteresisReentersHMACAboveThreshold(t *testing.T) {
	n := newMinimalNode(t, false)
	for i := range 20 {
		addKnownPeerForTopology(t, n.store, testPeerKey(byte(i+1)), i+1)
	}

	// Exit HMAC.
	n.vivaldiErr.Reset(0.30)
	n.syncPeersFromState(context.Background(), n.store.Snapshot())
	require.False(t, n.useHMACNearest)

	// Error rises above enter threshold (0.6).
	n.vivaldiErr.Reset(0.65)
	n.syncPeersFromState(context.Background(), n.store.Snapshot())
	require.True(t, n.useHMACNearest, "should re-enter HMAC when error > 0.6")
}

// --- Revoke streak tests ---

func TestRevokeStreakPrivatePeerRevokedAfterThreshold(t *testing.T) {
	n := newMinimalNode(t, false)
	wrapper := newTestMeshWrapper(n.mesh, n.meshInternal)
	n.mesh = wrapper
	n.meshInternal = wrapper

	// Add many peers so some fall outside the target set.
	for i := range 20 {
		addKnownPeerForTopology(t, n.store, testPeerKey(byte(i+1)), i+1)
	}

	disableFullMesh(wrapper)

	// Find a non-targeted peer and simulate it being connected+outbound.
	nonTarget := findNonTargetPeer(t, n)
	simulateConnectedOutbound(nonTarget, wrapper)

	// Tick 1 and 2: streak builds but peer stays connected.
	for tk := range revokeStreakThreshold - 1 {
		n.syncPeersFromState(context.Background(), n.store.Snapshot())
		require.True(t, n.meshInternal.HasPeer(nonTarget), "peer should survive at streak tick %d", tk+1)
		require.True(t, n.meshInternal.IsPeerConnected(nonTarget),
			"peer should still be Connected at streak tick %d", tk+1)
	}

	// Tick 3: threshold reached → revoked.
	n.syncPeersFromState(context.Background(), n.store.Snapshot())
	require.False(t, n.meshInternal.HasPeer(nonTarget), "peer should be forgotten after %d non-target ticks", revokeStreakThreshold)
}

func TestRevokeStreakResetsOnTargetReentry(t *testing.T) {
	n := newMinimalNode(t, false)
	wrapper := newTestMeshWrapper(n.mesh, n.meshInternal)
	n.mesh = wrapper
	n.meshInternal = wrapper

	for i := range 20 {
		addKnownPeerForTopology(t, n.store, testPeerKey(byte(i+1)), i+1)
	}

	disableFullMesh(wrapper)

	nonTarget := findNonTargetPeer(t, n)
	simulateConnectedOutbound(nonTarget, wrapper)

	// Build up streak to threshold - 1.
	for range revokeStreakThreshold - 1 {
		n.syncPeersFromState(context.Background(), n.store.Snapshot())
	}
	require.Equal(t, revokeStreakThreshold-1, n.nonTargetStreak[nonTarget])

	// Force peer back into target set via desired connection.
	n.AddDesiredConnection(nonTarget, 8080, 18080)
	n.syncPeersFromState(context.Background(), n.store.Snapshot())

	// Streak should be reset.
	require.Zero(t, n.nonTargetStreak[nonTarget], "streak should reset when peer re-enters target set")

	// Remove the desired connection — streak starts fresh.
	n.tunneling.(*tunneling.Service).RemoveDesiredConnectionByPorts(nonTarget, 8080, 18080)
	n.syncPeersFromState(context.Background(), n.store.Snapshot())
	require.Equal(t, 1, n.nonTargetStreak[nonTarget], "streak should start from 1 again after reset")
}

func TestRevokeStreakPublicPeerStickyLonger(t *testing.T) {
	n := newMinimalNode(t, false)
	wrapper := newTestMeshWrapper(n.mesh, n.meshInternal)
	n.mesh = wrapper
	n.meshInternal = wrapper

	// Create 20 peers, make 10 publicly accessible. With budget 8
	// (infra=2 + nearest=4 + longlinks=2) at most 8 can be targeted,
	// leaving at least 2 non-targeted public peers.
	for i := range 20 {
		pk := testPeerKey(byte(i + 1))
		addKnownPeerForTopology(t, n.store, pk, i+1)
		if i < 10 {
			setPubliclyAccessible(t, n.store, pk, 10+uint64(i))
		}
	}

	disableFullMesh(wrapper)

	nonTarget := findNonTargetPublicPeer(t, n)
	simulateConnectedOutbound(nonTarget, wrapper)

	// Run revokeStreakThreshold ticks — should NOT be revoked (needs 30 for public).
	for range revokeStreakThreshold {
		n.syncPeersFromState(context.Background(), n.store.Snapshot())
	}
	require.True(t, n.meshInternal.IsPeerConnected(nonTarget),
		"public peer should survive the private threshold")

	// Run up to revokeStreakThresholdPublic - 1 total ticks.
	for range revokeStreakThresholdPublic - revokeStreakThreshold - 1 {
		n.syncPeersFromState(context.Background(), n.store.Snapshot())
	}
	require.True(t, n.meshInternal.IsPeerConnected(nonTarget),
		"public peer should survive at tick %d", revokeStreakThresholdPublic-1)

	// One more tick → threshold reached, revoked.
	n.syncPeersFromState(context.Background(), n.store.Snapshot())
	require.False(t, n.meshInternal.HasPeer(nonTarget), "public peer should be forgotten after %d non-target ticks", revokeStreakThresholdPublic)
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

	snap := n.store.Snapshot()
	known := knownPeersFromSnapshot(snap)
	localIPs := snap.Nodes[snap.LocalID].IPs
	shape := summarizeTopologyShape(localIPs, known)
	epoch := time.Now().Unix() / membership.EpochSeconds
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
	targets := membership.ComputeTargetPeers(snap.LocalID, n.membership.ControlMetrics().LocalCoord, peerInfos, params)

	// With adaptive params (NearestK=2, RandomR=1, InfraMax=2) budget is 5.
	// Default budget would be 8 (NearestK=4, RandomR=2, InfraMax=2).
	defaultParams := membership.DefaultParams(epoch)
	defaultParams.UseHMACNearest = n.useHMACNearest
	defaultParams.LocalNATType = nat.Easy
	defaultParams.LocalIPs = localIPs
	defaultTargets := membership.ComputeTargetPeers(snap.LocalID, n.membership.ControlMetrics().LocalCoord, peerInfos, defaultParams)

	require.Less(t, len(targets), len(defaultTargets),
		"all-public cluster should produce fewer targets with adaptive params")
}

func TestMixedClusterRetainsSameSiteAndPublicPeers(t *testing.T) {
	n := newMinimalNode(t, false)

	// Local node has 10.1.x.x IPs (set by newMinimalNode as 127.0.0.1, override via store).
	// Use custom IPs for the local node.
	n.store.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.1.0.1:9000")})

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

	snap2 := n.store.Snapshot()
	known2 := knownPeersFromSnapshot(snap2)
	localIPs2 := snap2.Nodes[snap2.LocalID].IPs
	shape := summarizeTopologyShape(localIPs2, known2)

	// With 2/12 public (~17%), should keep default budgets.
	require.Less(t, float64(shape.publicCount)/float64(shape.totalCount), 0.5)

	epoch := time.Now().Unix() / membership.EpochSeconds
	params := adaptiveTopologyParams(epoch, shape)
	require.Equal(t, membership.DefaultNearestK, params.NearestK, "mixed cluster should keep default NearestK")
	require.Equal(t, membership.DefaultRandomR, params.RandomR, "mixed cluster should keep default RandomR")
}

// productionParams builds the same membership.Params that syncPeersFromState
// uses so test target computations match production.
func productionParams(n *Supervisor, epoch int64) membership.Params {
	snap := n.store.Snapshot()
	localNV := snap.Nodes[snap.LocalID]
	localIPs := localNV.IPs
	known := knownPeersFromSnapshot(snap)
	connectedPeers := n.GetConnectedPeers()
	currentOutbound := make(map[types.PeerKey]struct{}, len(connectedPeers))
	for _, pk := range connectedPeers {
		if n.meshInternal.IsOutbound(pk) {
			currentOutbound[pk] = struct{}{}
		}
	}
	shape := summarizeTopologyShape(localIPs, known)
	params := adaptiveTopologyParams(epoch, shape)
	params.PreferFullMesh = len(connectedPeers) <= tinyClusterPeerThreshold
	params.LocalIPs = localIPs
	params.CurrentOutbound = currentOutbound
	params.LocalNATType = n.natDetector.Type()
	params.LocalObservedExternalIP = localNV.ObservedExternalIP
	params.UseHMACNearest = n.useHMACNearest
	return params
}

// computeTargetsLikeProduction replicates the target computation from
// syncPeersFromState, covering both the current and next epoch to handle
// boundary crossings between test setup and syncPeersFromState execution.
func computeTargetsLikeProduction(n *Supervisor, peerInfos []membership.PeerInfo) map[types.PeerKey]struct{} {
	targetSet := make(map[types.PeerKey]struct{})
	epoch := time.Now().Unix() / membership.EpochSeconds
	for _, e := range []int64{epoch, epoch + 1} {
		for _, pk := range membership.ComputeTargetPeers(n.store.Snapshot().LocalID, n.membership.ControlMetrics().LocalCoord, peerInfos, productionParams(n, e)) {
			targetSet[pk] = struct{}{}
		}
	}
	return targetSet
}

// computeCurrentTargets computes targets for the current epoch only, matching
// what syncPeersFromState will produce when called immediately after.
func computeCurrentTargets(n *Supervisor, peerInfos []membership.PeerInfo) map[types.PeerKey]struct{} {
	epoch := time.Now().Unix() / membership.EpochSeconds
	targets := membership.ComputeTargetPeers(n.store.Snapshot().LocalID, n.membership.ControlMetrics().LocalCoord, peerInfos, productionParams(n, epoch))
	targetSet := make(map[types.PeerKey]struct{}, len(targets))
	for _, pk := range targets {
		targetSet[pk] = struct{}{}
	}
	return targetSet
}

func knownPeersToPeerInfos(peers []knownPeer) []membership.PeerInfo {
	infos := make([]membership.PeerInfo, 0, len(peers))
	for _, kp := range peers {
		infos = append(infos, membership.PeerInfo{
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
func findNonTargetPublicPeer(t *testing.T, n *Supervisor) types.PeerKey {
	t.Helper()

	known := knownPeersFromSnapshot(n.store.Snapshot())
	peerInfos := knownPeersToPeerInfos(known)
	targetSet := computeCurrentTargets(n, peerInfos)

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
func findNonTargetPeer(t *testing.T, n *Supervisor) types.PeerKey {
	t.Helper()

	known := knownPeersFromSnapshot(n.store.Snapshot())
	peerInfos := knownPeersToPeerInfos(known)
	targetSet := computeCurrentTargets(n, peerInfos)

	for _, kp := range known {
		if _, targeted := targetSet[kp.PeerID]; !targeted {
			return kp.PeerID
		}
	}
	t.Fatal("all known peers are targeted — need more peers to find a non-target")
	return types.PeerKey{}
}
