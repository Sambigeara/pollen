package node

import (
	"fmt"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
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
	peerInfos := make([]topology.PeerInfo, 0, len(known))
	for _, kp := range known {
		peerInfos = append(peerInfos, topology.PeerInfo{
			Key:                kp.PeerID,
			Coord:              kp.VivaldiCoord,
			IPs:                kp.IPs,
			PubliclyAccessible: kp.PubliclyAccessible,
		})
	}

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
	})
}
