package supervisor

import (
	"fmt"
	"testing"

	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// --- Reachability Tests ---

func TestInferPrivatelyRoutable(t *testing.T) {
	require.True(t, membership.InferPrivatelyRoutable([]string{"10.2.1.10"}, []string{"10.2.2.20"}))
	require.False(t, membership.InferPrivatelyRoutable([]string{"10.2.1.10"}, []string{"10.3.2.20"}))
	require.True(t, membership.InferPrivatelyRoutable([]string{"192.168.1.10"}, []string{"192.168.200.20"}))
	require.True(t, membership.InferPrivatelyRoutable([]string{"172.16.1.10"}, []string{"172.16.200.20"}))
	require.False(t, membership.InferPrivatelyRoutable([]string{"172.16.1.10"}, []string{"172.17.200.20"}))
	require.False(t, membership.InferPrivatelyRoutable([]string{"10.2.1.10"}, []string{"203.0.113.7"}))
}

func TestInferReachability(t *testing.T) {
	require.Equal(t, reachabilityPublicDirect, inferReachability([]string{"10.2.1.10"}, []string{"203.0.113.7"}, true))
	require.Equal(t, reachabilitySameSitePrivate, inferReachability([]string{"10.2.1.10"}, []string{"10.2.9.20"}, false))
	require.Equal(t, reachabilityUnknown, inferReachability([]string{"10.2.1.10"}, []string{"10.3.9.20"}, false))
}

// --- Topology Shape & Parameters ---

func makeKnownPeers(public, sameSitePrivate, remotePrivate int) ([]string, []knownPeer) {
	localIPs := []string{"10.1.0.1"}
	peers := make([]knownPeer, 0, public+sameSitePrivate+remotePrivate)
	id := byte(1)

	for range public {
		peers = append(peers, knownPeer{
			PeerID:             testPeerKey(id),
			IPs:                []string{fmt.Sprintf("203.0.113.%d", id)},
			PubliclyAccessible: true,
		})
		id++
	}
	for range sameSitePrivate {
		peers = append(peers, knownPeer{
			PeerID: testPeerKey(id),
			IPs:    []string{fmt.Sprintf("10.1.0.%d", id)},
		})
		id++
	}
	for range remotePrivate {
		peers = append(peers, knownPeer{
			PeerID: testPeerKey(id),
			IPs:    []string{fmt.Sprintf("10.2.0.%d", id)},
		})
		id++
	}
	return localIPs, peers
}

func TestSummarizeTopologyShape(t *testing.T) {
	localIPs, peers := makeKnownPeers(5, 3, 2)
	s := summarizeTopologyShape(localIPs, peers)

	require.Equal(t, 10, s.totalCount)
	require.Equal(t, 5, s.publicCount)
	require.Equal(t, 3, s.sameSitePrivateCount)
	require.Equal(t, 2, s.remotePrivateCount)
}

func TestAdaptiveParamsMixed(t *testing.T) {
	localIPs, peers := makeKnownPeers(6, 3, 3)
	shape := summarizeTopologyShape(localIPs, peers)
	p := adaptiveTopologyParams(1, shape)

	require.Equal(t, 3, p.NearestK)
	require.Equal(t, 2, p.RandomR)
	require.Equal(t, membership.DefaultInfraMax, p.InfraMax)
}

func TestAdaptiveParamsPrivateHeavy(t *testing.T) {
	localIPs, peers := makeKnownPeers(2, 10, 5)
	shape := summarizeTopologyShape(localIPs, peers)
	p := adaptiveTopologyParams(1, shape)

	require.Equal(t, membership.DefaultNearestK, p.NearestK)
	require.Equal(t, membership.DefaultRandomR, p.RandomR)
	require.Equal(t, membership.DefaultInfraMax, p.InfraMax)
}

func testPeerKey(b byte) types.PeerKey {
	var pk types.PeerKey
	pk[0] = b
	return pk
}
