package supervisor

import (
	"fmt"
	"testing"

	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/stretchr/testify/require"
)

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

func TestSummarizeTopologyShapeEmpty(t *testing.T) {
	s := summarizeTopologyShape(nil, nil)
	require.Equal(t, topologyShape{}, s)
}

func TestAdaptiveParamsTinyCluster(t *testing.T) {
	_, peers := makeKnownPeers(5, 0, 0)
	shape := summarizeTopologyShape(nil, peers)
	p := adaptiveTopologyParams(1, shape)

	require.Equal(t, membership.DefaultNearestK, p.NearestK)
	require.Equal(t, membership.DefaultRandomR, p.RandomR)
	require.Equal(t, membership.DefaultInfraMax, p.InfraMax)
}

func TestAdaptiveParamsMostlyPublic(t *testing.T) {
	localIPs, peers := makeKnownPeers(18, 1, 1)
	shape := summarizeTopologyShape(localIPs, peers)
	p := adaptiveTopologyParams(1, shape)

	require.Equal(t, 2, p.NearestK)
	require.Equal(t, 1, p.RandomR)
	require.Equal(t, membership.DefaultInfraMax, p.InfraMax)
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

func TestAdaptiveParamsBoundaryFiftyPercent(t *testing.T) {
	localIPs, peers := makeKnownPeers(5, 3, 2)
	shape := summarizeTopologyShape(localIPs, peers)
	p := adaptiveTopologyParams(1, shape)

	require.Equal(t, 3, p.NearestK, "exactly 50%% public should get transitional budget")
	require.Equal(t, 2, p.RandomR)
}

func TestAdaptiveParamsBoundarySeventyFivePercent(t *testing.T) {
	localIPs, peers := makeKnownPeers(9, 2, 1)
	shape := summarizeTopologyShape(localIPs, peers)
	p := adaptiveTopologyParams(1, shape)

	require.Equal(t, 2, p.NearestK, "75%% public should get sparse budget")
	require.Equal(t, 1, p.RandomR)
}
