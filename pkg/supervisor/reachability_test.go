package supervisor

import (
	"testing"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestInferPrivatelyRoutable(t *testing.T) {
	require.True(t, membership.InferPrivatelyRoutable([]string{"10.2.1.10"}, []string{"10.2.2.20"}))
	require.False(t, membership.InferPrivatelyRoutable([]string{"10.2.1.10"}, []string{"10.3.2.20"}))
	require.False(t, membership.InferPrivatelyRoutable([]string{"192.168.1.10"}, []string{"192.168.200.20"}))
	require.True(t, membership.InferPrivatelyRoutable([]string{"172.16.1.10"}, []string{"172.16.200.20"}))
	require.False(t, membership.InferPrivatelyRoutable([]string{"172.16.1.10"}, []string{"172.17.200.20"}))
	require.False(t, membership.InferPrivatelyRoutable([]string{"10.2.1.10"}, []string{"203.0.113.7"}))
}

func TestInferReachability(t *testing.T) {
	require.Equal(t, reachabilityPublicDirect, inferReachability([]string{"10.2.1.10"}, []string{"203.0.113.7"}, true))
	require.Equal(t, reachabilitySameSitePrivate, inferReachability([]string{"10.2.1.10"}, []string{"10.2.9.20"}, false))
	require.Equal(t, reachabilityUnknown, inferReachability([]string{"10.2.1.10"}, []string{"10.3.9.20"}, false))
}

func TestRankCoordinatorsPrefersPublicCandidates(t *testing.T) {
	n := newMinimalNode(t, false)
	localIPs := []string{"10.1.2.10"}
	targetIPs := []string{"10.2.2.10"}
	target := testPeerKey(50)

	addPeerWithIP(t, n.store, target, "10.2.2.10", 9050, 50)
	addPeerWithIP(t, n.store, testPeerKey(1), "10.1.9.20", 9001, 1)
	addPeerWithIP(t, n.store, testPeerKey(2), "10.3.9.20", 9002, 2)
	addPeerWithIP(t, n.store, testPeerKey(3), "10.4.9.20", 9003, 3)
	setPubliclyAccessible(t, n.store, testPeerKey(2), 100)
	setPubliclyAccessible(t, n.store, testPeerKey(3), 101)

	markPeerConnectedToTarget(t, n.store, testPeerKey(1), target, 200)
	markPeerConnectedToTarget(t, n.store, testPeerKey(2), target, 201)
	markPeerConnectedToTarget(t, n.store, testPeerKey(3), target, 202)

	// Candidate 1 shares the local site and should be excluded. Candidates 2 and 3
	// are both valid and public; deterministic ordering falls back to peer key.
	ranked := rankCoordinators(localIPs, targetIPs, target, []types.PeerKey{testPeerKey(1), testPeerKey(3), testPeerKey(2)}, n.store.Snapshot())
	require.Equal(t, []types.PeerKey{testPeerKey(2), testPeerKey(3)}, ranked)
}

func TestRankCoordinatorsExcludesSharedObservedEgress(t *testing.T) {
	n := newMinimalNode(t, false)
	localIPs := []string{"10.1.2.10"}
	targetIPs := []string{"10.2.2.10"}
	target := testPeerKey(50)

	setLocalObservedExternalIP(t, n.store, "52.204.52.130")
	addPeerWithIP(t, n.store, target, "10.2.2.10", 9050, 50)
	setPeerObservedExternalIP(t, n.store, target, "18.240.0.10", 51)

	// Peer 1 shares observed egress with the local node ("52.204.52.130") — should be excluded.
	addPeerWithIP(t, n.store, testPeerKey(1), "10.3.9.20", 9001, 1)
	setPubliclyAccessible(t, n.store, testPeerKey(1), 100)
	setPeerObservedExternalIP(t, n.store, testPeerKey(1), "52.204.52.130", 101)
	markPeerConnectedToTarget(t, n.store, testPeerKey(1), target, 200)

	addPeerWithIP(t, n.store, testPeerKey(2), "10.4.9.20", 9002, 2)
	setPubliclyAccessible(t, n.store, testPeerKey(2), 102)
	setPeerObservedExternalIP(t, n.store, testPeerKey(2), "18.240.0.11", 103)
	markPeerConnectedToTarget(t, n.store, testPeerKey(2), target, 201)

	ranked := rankCoordinators(localIPs, targetIPs, target, []types.PeerKey{testPeerKey(1), testPeerKey(2)}, n.store.Snapshot())
	require.Equal(t, []types.PeerKey{testPeerKey(2)}, ranked)
}

func markPeerConnectedToTarget(tb testing.TB, s state.StateStore, source, target types.PeerKey, counter uint64) {
	tb.Helper()
	applyTestEvents(tb, s, []*statev1.GossipEvent{{
		PeerId:  source.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_Reachability{
			Reachability: &statev1.ReachabilityChange{PeerId: target.String()},
		},
	}})
}

func addPeerWithIP(tb testing.TB, s state.StateStore, peerID types.PeerKey, ip string, port uint32, counter uint64) {
	tb.Helper()
	applyTestEvents(tb, s, []*statev1.GossipEvent{{
		PeerId:  peerID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{ip}, LocalPort: port},
		},
	}})
}

func setPeerObservedExternalIP(tb testing.TB, s state.StateStore, peerID types.PeerKey, ip string, counter uint64) {
	tb.Helper()
	applyTestEvents(tb, s, []*statev1.GossipEvent{{
		PeerId:  peerID.String(),
		Counter: counter,
		Change: &statev1.GossipEvent_ObservedAddress{
			ObservedAddress: &statev1.ObservedAddressChange{Ip: ip},
		},
	}})
}

func setLocalObservedExternalIP(_ testing.TB, s state.StateStore, ip string) {
	s.SetLocalObservedAddress(ip, 0)
}
