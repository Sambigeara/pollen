package node

import (
	"net"
	"testing"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestInferPrivatelyRoutable(t *testing.T) {
	require.True(t, topology.InferPrivatelyRoutable([]string{"10.2.1.10"}, []string{"10.2.2.20"}))
	require.False(t, topology.InferPrivatelyRoutable([]string{"10.2.1.10"}, []string{"10.3.2.20"}))
	require.False(t, topology.InferPrivatelyRoutable([]string{"192.168.1.10"}, []string{"192.168.200.20"}))
	require.True(t, topology.InferPrivatelyRoutable([]string{"172.16.1.10"}, []string{"172.16.200.20"}))
	require.False(t, topology.InferPrivatelyRoutable([]string{"172.16.1.10"}, []string{"172.17.200.20"}))
	require.False(t, topology.InferPrivatelyRoutable([]string{"10.2.1.10"}, []string{"203.0.113.7"}))
}

func TestInferReachability(t *testing.T) {
	require.Equal(t, reachabilityPublicDirect, inferReachability([]string{"10.2.1.10"}, []string{"203.0.113.7"}, true))
	require.Equal(t, reachabilitySameSitePrivate, inferReachability([]string{"10.2.1.10"}, []string{"10.2.9.20"}, false))
	require.Equal(t, reachabilityUnknown, inferReachability([]string{"10.2.1.10"}, []string{"10.3.9.20"}, false))
}

func TestOrderPeerAddrsPrefersPrivatelyRoutableThenPublic(t *testing.T) {
	addrs := orderPeerAddrs(
		[]string{"10.2.1.10"},
		[]net.IP{net.ParseIP("198.51.100.20"), net.ParseIP("10.2.9.20"), net.ParseIP("10.3.9.20")},
		60611,
		42424,
	)

	require.Len(t, addrs, 3)
	require.Equal(t, "10.2.9.20:60611", addrs[0].String())
	require.Equal(t, "198.51.100.20:42424", addrs[1].String())
	require.Equal(t, "10.3.9.20:60611", addrs[2].String())
}

func TestRankCoordinatorsPrefersPublicCandidates(t *testing.T) {
	n := newMinimalNode(t, false)
	localIPs := []string{"10.1.2.10"}
	targetIPs := []string{"10.2.2.10"}
	target := testPeerKey(50)

	addPeerWithIP(n.store, target, "10.2.2.10", 9050, 50)
	addPeerWithIP(n.store, testPeerKey(1), "10.1.9.20", 9001, 1)
	addPeerWithIP(n.store, testPeerKey(2), "10.3.9.20", 9002, 2)
	addPeerWithIP(n.store, testPeerKey(3), "10.4.9.20", 9003, 3)
	setPubliclyAccessible(n.store, testPeerKey(2), 100)
	setPubliclyAccessible(n.store, testPeerKey(3), 101)

	markPeerConnectedToTarget(n.store, testPeerKey(1), target, 200)
	markPeerConnectedToTarget(n.store, testPeerKey(2), target, 201)
	markPeerConnectedToTarget(n.store, testPeerKey(3), target, 202)

	// Candidate 1 shares the local site and should be excluded. Candidates 2 and 3
	// are both valid and public; deterministic ordering falls back to peer key.
	ranked := rankCoordinators(localIPs, targetIPs, "", target, []types.PeerKey{testPeerKey(1), testPeerKey(3), testPeerKey(2)}, n.store.AllRouteInfo())
	require.Equal(t, []types.PeerKey{testPeerKey(2), testPeerKey(3)}, ranked)
}

func TestRankCoordinatorsExcludesSharedObservedEgress(t *testing.T) {
	n := newMinimalNode(t, false)
	localIPs := []string{"10.1.2.10"}
	targetIPs := []string{"10.2.2.10"}
	target := testPeerKey(50)

	n.store.SetObservedExternalIP("52.204.52.130")
	addPeerWithIP(n.store, target, "10.2.2.10", 9050, 50)
	setPeerObservedExternalIP(n.store, target, "18.240.0.10", 51)

	addPeerWithIP(n.store, testPeerKey(1), "10.3.9.20", 9001, 1)
	setPubliclyAccessible(n.store, testPeerKey(1), 100)
	setPeerObservedExternalIP(n.store, testPeerKey(1), "52.204.52.130", 101)
	markPeerConnectedToTarget(n.store, testPeerKey(1), target, 200)

	addPeerWithIP(n.store, testPeerKey(2), "10.4.9.20", 9002, 2)
	setPubliclyAccessible(n.store, testPeerKey(2), 102)
	setPeerObservedExternalIP(n.store, testPeerKey(2), "18.240.0.11", 103)
	markPeerConnectedToTarget(n.store, testPeerKey(2), target, 201)

	ranked := rankCoordinators(localIPs, targetIPs, n.store.LocalRecord().ObservedExternalIP, target, []types.PeerKey{testPeerKey(1), testPeerKey(2)}, n.store.AllRouteInfo())
	require.Equal(t, []types.PeerKey{testPeerKey(2)}, ranked)
}

func markPeerConnectedToTarget(s *store.Store, source, target types.PeerKey, counter uint64) {
	s.ApplyEvents([]*statev1.GossipEvent{{
		PeerId:  source.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_Reachability{
			Reachability: &statev1.ReachabilityChange{PeerId: target.String()},
		},
	}}, true)
}

func addPeerWithIP(s *store.Store, peerID types.PeerKey, ip string, port uint32, counter uint64) {
	s.ApplyEvents([]*statev1.GossipEvent{{
		PeerId:  peerID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{ip}, LocalPort: port},
		},
	}}, true)
}

func setPeerObservedExternalIP(s *store.Store, peerID types.PeerKey, ip string, counter uint64) {
	s.ApplyEvents([]*statev1.GossipEvent{{
		PeerId:  peerID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_ObservedExternalIp{
			ObservedExternalIp: &statev1.ObservedExternalIPChange{Ip: ip},
		},
	}}, true)
}
