package state

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func genKey(t *testing.T) types.PeerKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return types.PeerKeyFromBytes(pub)
}

func TestSnapshot_ReflectsState(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1", "10.0.0.2"}, 9000)
	s.upsertLocalService(8080, "web")
	s.setObservedAddress("", 45000)

	snap := s.Snapshot()

	require.Equal(t, s.localID, snap.LocalID)

	localView, ok := snap.Nodes[s.localID]
	require.True(t, ok)
	require.Equal(t, []string{"10.0.0.1", "10.0.0.2"}, localView.IPs)
	require.Equal(t, uint32(9000), localView.LocalPort)
	require.Equal(t, uint32(45000), localView.ExternalPort)
	require.Contains(t, localView.Services, "web")
	require.Equal(t, uint32(8080), localView.Services["web"].Port)

	// Local node is always in PeerKeys (live component includes self).
	require.Contains(t, snap.PeerKeys, s.localID)
}

func TestSnapshot_IsImmutable(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.upsertLocalService(8080, "web")

	snap1 := s.Snapshot()

	// Mutate the store after taking the snapshot.
	s.setLocalNetwork([]string{"10.0.0.99"}, 7777)
	s.upsertLocalService(9090, "api")
	s.removeLocalService("web")

	snap2 := s.Snapshot()

	// snap1 must still reflect the original state.
	localView1 := snap1.Nodes[s.localID]
	require.Equal(t, []string{"10.0.0.1"}, localView1.IPs)
	require.Equal(t, uint32(9000), localView1.LocalPort)
	require.Contains(t, localView1.Services, "web")
	require.NotContains(t, localView1.Services, "api")

	// snap2 reflects the mutated state.
	localView2 := snap2.Nodes[s.localID]
	require.Equal(t, []string{"10.0.0.99"}, localView2.IPs)
	require.Equal(t, uint32(7777), localView2.LocalPort)
	require.Contains(t, localView2.Services, "api")
	require.NotContains(t, localView2.Services, "web")
}

func TestSnapshot_PeerKeysFiltering(t *testing.T) {
	localKey := genKey(t)
	s := newTestStore(localKey)

	peerAKey := genKey(t)

	peerBKey := genKey(t)

	peerCKey := genKey(t)

	// Add peerA with reachability from local → A.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerAKey.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerAKey.String(),
		Counter: 2,
		Change: &statev1.GossipEvent_CertExpiry{
			CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: time.Now().Add(time.Hour).Unix()},
		},
	})
	s.setLocalConnected(peerAKey, true)

	// Add peerB with reachability from local → B.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerBKey.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.3"}, LocalPort: 9002},
		},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerBKey.String(),
		Counter: 2,
		Change: &statev1.GossipEvent_CertExpiry{
			CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: time.Now().Add(time.Hour).Unix()},
		},
	})
	s.setLocalConnected(peerBKey, true)

	// Add peerC but do NOT connect local → C (disconnected peer).
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerCKey.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.4"}, LocalPort: 9003},
		},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerCKey.String(),
		Counter: 2,
		Change: &statev1.GossipEvent_CertExpiry{
			CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: time.Now().Add(time.Hour).Unix()},
		},
	})

	snap := s.Snapshot()

	// PeerKeys should include local, peerA, peerB but NOT peerC (unreachable).
	require.Contains(t, snap.PeerKeys, s.localID)
	require.Contains(t, snap.PeerKeys, peerAKey)
	require.Contains(t, snap.PeerKeys, peerBKey)
	require.NotContains(t, snap.PeerKeys, peerCKey)

	// peerC should still appear in Nodes (it's valid, just not live).
	require.Contains(t, snap.Nodes, peerCKey)

	// Deny peerB and verify it drops from both PeerKeys and Nodes.
	s.denyPeerRaw(peerBKey[:])
	snap2 := s.Snapshot()

	require.NotContains(t, snap2.PeerKeys, peerBKey)
	require.NotContains(t, snap2.Nodes, peerBKey)

	// Original snapshot is unaffected by the deny.
	require.Contains(t, snap.PeerKeys, peerBKey)
	require.Contains(t, snap.Nodes, peerBKey)
}

func TestLocalMutation_ReturnsEvents(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(pk)

	events := s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	require.NotEmpty(t, events)
}

func TestDenyApplied_ViaGossip(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(pk)

	peer2Key := genKey(t)

	// Inject peer2 into the store so its events are accepted.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peer2Key.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_CertExpiry{
			CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: time.Now().Add(time.Hour).Unix()},
		},
	})

	// peer2 denies a target via gossip.
	targetKey := genKey(t)
	result := s.applyEvents([]*statev1.GossipEvent{{
		PeerId:  peer2Key.String(),
		Counter: 2,
		Change: &statev1.GossipEvent_Deny{
			Deny: &statev1.DenyChange{PeerPub: targetKey[:]},
		},
	}})

	require.Len(t, result.deniedPeers, 1)
	require.Equal(t, targetKey, result.deniedPeers[0])
}

func TestGossipApplied_ViaApplyDelta(t *testing.T) {
	pkA := genKey(t)
	s1 := newTestStore(pkA)

	pkB := genKey(t)
	s2 := newTestStore(pkB)

	// Store1 sets local network state.
	s1.setLocalNetwork([]string{"10.0.0.1"}, 9000)

	// Get store1's full state and apply it to store2 via ApplyDelta.
	fullData := s1.EncodeFull()
	require.NotEmpty(t, fullData)

	events, rebroadcast, err := s2.ApplyDelta(pkA, fullData)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	require.NotEmpty(t, rebroadcast)
}

func TestSnapshot_Self(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(pk)
	snap := s.Snapshot()
	require.Equal(t, s.localID, snap.Self())
}

func TestSnapshot_Peers(t *testing.T) {
	localKey := genKey(t)
	s := newTestStore(localKey)

	peerAKey := genKey(t)

	// Register peerA and mark it reachable.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerAKey.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerAKey.String(),
		Counter: 2,
		Change: &statev1.GossipEvent_CertExpiry{
			CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: time.Now().Add(time.Hour).Unix()},
		},
	})
	s.setLocalConnected(peerAKey, true)

	snap := s.Snapshot()
	peers := snap.Peers()

	// Must include at least local and peerA.
	require.GreaterOrEqual(t, len(peers), 2)

	keys := make(map[types.PeerKey]struct{}, len(peers))
	for _, p := range peers {
		keys[p.Key] = struct{}{}
		// Key must also exist in Nodes.
		nv, ok := snap.Nodes[p.Key]
		require.True(t, ok)
		require.Equal(t, nv.LocalPort, p.LocalPort)
	}
	require.Contains(t, keys, s.localID)
	require.Contains(t, keys, peerAKey)
}

func TestSnapshot_Peer(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(pk)
	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	snap := s.Snapshot()

	info, ok := snap.Peer(s.localID)
	require.True(t, ok)
	require.Equal(t, s.localID, info.Key)
	require.Equal(t, uint32(9000), info.LocalPort)

	_, ok = snap.Peer(types.PeerKey{0xFF})
	require.False(t, ok)
}

func TestSnapshot_Services(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(pk)
	s.upsertLocalService(8080, "web")
	s.upsertLocalService(9090, "api")
	snap := s.Snapshot()

	svcs := snap.Services()
	require.Len(t, svcs, 2)

	names := make(map[string]uint32, len(svcs))
	for _, svc := range svcs {
		names[svc.Name] = svc.Port
		require.Equal(t, s.localID, svc.Peer)
	}
	require.Equal(t, uint32(8080), names["web"])
	require.Equal(t, uint32(9090), names["api"])
}

func TestSnapshot_Workloads(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(pk)

	// Add a workload spec.
	_, err := s.setLocalWorkloadSpec("abc123", 1, 0, 0)
	require.NoError(t, err)

	snap := s.Snapshot()
	workloads := snap.Workloads()
	require.Len(t, workloads, 1)
	require.Equal(t, "abc123", workloads[0].Hash)
	require.Equal(t, s.localID, workloads[0].Spec.Publisher)
}

func TestSnapshot_DeniedPeers(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(pk)

	// Initially no denied peers.
	snap := s.Snapshot()
	require.Empty(t, snap.DeniedPeers())

	// Deny a peer.
	targetKey := genKey(t)
	s.denyPeerRaw(targetKey[:])

	snap2 := s.Snapshot()
	denied := snap2.DeniedPeers()
	require.Len(t, denied, 1)
	require.Equal(t, targetKey, denied[0])
}
