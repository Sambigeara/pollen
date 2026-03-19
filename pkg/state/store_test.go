package state

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func newTestStore(pub []byte) *Store {
	return New(types.PeerKeyFromBytes(pub))
}

// applyEvent is a test convenience for applying a single gossip event.
func (s *Store) applyEvent(event *statev1.GossipEvent) applyResult {
	return s.applyEvents([]*statev1.GossipEvent{event})
}

// clock returns the store's digest for test assertions.
func (s *Store) clock() *statev1.GossipStateDigest {
	var result *statev1.GossipStateDigest
	s.do(func() {
		result = s.digestLocked()
	})
	return result
}

func peerKey(b byte) (types.PeerKey, string) {
	pub := make([]byte, 32)
	pub[0] = b
	pk := types.PeerKeyFromBytes(pub)
	return pk, pk.String()
}

// --- Local mutation tests ---

func TestSetLocalNetworkReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	require.Len(t, events, 1)

	ev := events[0]
	require.Equal(t, uint64(8), ev.GetCounter())
	network := ev.GetNetwork()
	require.NotNil(t, network)
	require.Equal(t, []string{"10.0.0.1"}, network.GetIps())
	require.Equal(t, uint32(9000), network.GetLocalPort())
}

func TestSetLocalNetworkNoOpWhenUnchanged(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	events := s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	require.Nil(t, events)
}

func TestSetExternalPortReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.setExternalPort(51000)
	require.Len(t, events, 1)
	require.Equal(t, uint32(51000), events[0].GetExternalPort().GetExternalPort())
}

func TestSetObservedExternalIPReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.setObservedExternalIP("52.204.52.130")
	require.Len(t, events, 1)
	require.Equal(t, "52.204.52.130", events[0].GetObservedExternalIp().GetIp())
}

func TestSetLocalConnectedConnect(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPub := make([]byte, 32)
	peerPub[0] = 2
	peerID := types.PeerKeyFromBytes(peerPub)

	events := s.setLocalConnected(peerID, true)
	require.Len(t, events, 1)
	require.False(t, events[0].GetDeleted())
	require.Equal(t, peerID.String(), events[0].GetReachability().GetPeerId())
}

func TestSetLocalConnectedDisconnect(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPub := make([]byte, 32)
	peerPub[0] = 2
	peerID := types.PeerKeyFromBytes(peerPub)

	s.setLocalConnected(peerID, true)
	events := s.setLocalConnected(peerID, false)
	require.Len(t, events, 1)
	require.True(t, events[0].GetDeleted())
}

// --- Apply event tests ---

func TestApplyEventSingleAttribute(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 3,
		Change: &statev1.GossipEvent_ExternalPort{
			ExternalPort: &statev1.ExternalPortChange{ExternalPort: 45000},
		},
	})

	rec := s.nodes[peerPK]
	require.Equal(t, uint32(45000), rec.ExternalPort)
	require.Equal(t, uint64(3), rec.maxCounter)
}

func TestApplyEventObservedExternalIP(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 3,
		Change: &statev1.GossipEvent_ObservedExternalIp{
			ObservedExternalIp: &statev1.ObservedExternalIPChange{Ip: "52.204.52.130"},
		},
	})

	rec := s.nodes[peerPK]
	require.Equal(t, "52.204.52.130", rec.ObservedExternalIP)
	require.Equal(t, uint64(3), rec.maxCounter)
}

func TestApplyEventPerKeyCounter(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 3,
		Change: &statev1.GossipEvent_ExternalPort{
			ExternalPort: &statev1.ExternalPortChange{ExternalPort: 45000},
		},
	})

	// Older event for same key is rejected.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Change: &statev1.GossipEvent_ExternalPort{
			ExternalPort: &statev1.ExternalPortChange{ExternalPort: 44000},
		},
	})

	peerPK, _ := peerKey(2)
	require.Equal(t, uint32(45000), s.nodes[peerPK].ExternalPort)
}

func TestApplyEventDifferentKeys(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 5,
		Change: &statev1.GossipEvent_ExternalPort{
			ExternalPort: &statev1.ExternalPortChange{ExternalPort: 45000},
		},
	})

	// Lower counter but different key — should apply.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})

	peerPK, _ := peerKey(2)
	rec := s.nodes[peerPK]
	require.Equal(t, uint32(45000), rec.ExternalPort)
	require.Equal(t, []string{"10.0.0.1"}, rec.IPs)
}

func TestApplyEventDeletion(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: "http", Port: 8080},
		},
	})

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Deleted: true,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: "http"},
		},
	})

	peerPK, _ := peerKey(2)
	require.NotContains(t, s.nodes[peerPK].Services, "http")
}

func TestApplyEventTombstonePreventResurrection(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 5,
		Deleted: true,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: "http"},
		},
	})

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 4,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: "http", Port: 8080},
		},
	})

	peerPK, _ := peerKey(2)
	require.NotContains(t, s.nodes[peerPK].Services, "http")
}

func TestApplyEventNetworkUpdate(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{
				Ips:       []string{"10.0.0.5", "192.168.1.1"},
				LocalPort: 7000,
			},
		},
	})

	rec := s.nodes[peerPK]
	require.Equal(t, []string{"10.0.0.5", "192.168.1.1"}, rec.IPs)
	require.Equal(t, uint32(7000), rec.LocalPort)
}

func TestApplyEventIdentityPub(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)
	idPub := make([]byte, 32)
	idPub[0] = 2

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_IdentityPub{
			IdentityPub: &statev1.IdentityChange{IdentityPub: idPub},
		},
	})

	require.Equal(t, byte(2), s.nodes[peerPK].IdentityPub[0])
}

func TestApplyEventReachablePeer(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)
	targetPK, _ := peerKey(3)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Reachability{
			Reachability: &statev1.ReachabilityChange{PeerId: targetPK.String()},
		},
	})
	require.Contains(t, s.nodes[peerPK].Reachable, targetPK)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Deleted: true,
		Change: &statev1.GossipEvent_Reachability{
			Reachability: &statev1.ReachabilityChange{PeerId: targetPK.String()},
		},
	})
	require.NotContains(t, s.nodes[peerPK].Reachable, targetPK)
}

// --- Self-state conflict ---

func TestApplyEventSelfStateConflict(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)
	localID := s.LocalID

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  localID.String(),
		Counter: 10,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	})

	require.NotEmpty(t, result.rebroadcast)
	// Adopted 10, then bumped once per attribute.
	require.Equal(t, uint64(18), s.nodes[localID].maxCounter)
}

func TestApplyEventSelfStateNoConflict(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  s.LocalID.String(),
		Counter: 2,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})

	require.Empty(t, result.rebroadcast)
}

// --- MissingFor / digest tests ---

func TestMissingForReturnsEvents(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.setExternalPort(45000)

	events := s.missingFor(&statev1.GossipStateDigest{
		Peers: map[string]*statev1.PeerDigest{
			s.LocalID.String(): {MaxCounter: 0, StateHash: 0},
		},
	})
	require.Len(t, events, 8)
}

func TestMissingForRespectsClock(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.setExternalPort(45000)

	digest := s.clock()
	localDigest := digest.GetPeers()[s.LocalID.String()]
	events := s.missingFor(&statev1.GossipStateDigest{
		Peers: map[string]*statev1.PeerDigest{
			s.LocalID.String(): {MaxCounter: 7, StateHash: localDigest.GetStateHash()},
		},
	})
	require.Len(t, events, 2)
	require.NotNil(t, events[0].GetNetwork())
	require.Equal(t, uint32(45000), events[1].GetExternalPort().GetExternalPort())
}

func TestMissingForReturnsNilForUpToDate(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)

	events := s.missingFor(s.clock())
	require.Empty(t, events)
}

func TestClockUsesMaxCounter(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.setExternalPort(45000)

	digest := s.clock()
	require.Equal(t, uint64(9), digest.GetPeers()[s.LocalID.String()].GetMaxCounter())
}

// --- Service tests ---

func TestUpsertLocalServiceReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.upsertLocalService(8080, "http")
	require.Len(t, events, 1)
	sc := events[0].GetService()
	require.Equal(t, "http", sc.GetName())
	require.Equal(t, uint32(8080), sc.GetPort())
}

func TestRemoveLocalServiceReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.upsertLocalService(8080, "http")
	events := s.removeLocalService("http")
	require.Len(t, events, 1)
	require.True(t, events[0].GetDeleted())
	require.Equal(t, "http", events[0].GetService().GetName())
}

func TestRemoveLocalServiceNoOpWhenMissing(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.removeLocalService("http")
	require.Nil(t, events)
}

// --- Publicly accessible tests ---

func TestSetLocalPubliclyAccessibleReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.setLocalPubliclyAccessible(true)
	require.Len(t, events, 1)
	require.False(t, events[0].GetDeleted())
	require.NotNil(t, events[0].GetPubliclyAccessible())
}

func TestSetLocalPubliclyAccessibleNoOpWhenUnchanged(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalPubliclyAccessible(true)
	events := s.setLocalPubliclyAccessible(true)
	require.Nil(t, events)
}

func TestSetLocalPubliclyAccessibleClear(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalPubliclyAccessible(true)
	events := s.setLocalPubliclyAccessible(false)
	require.Len(t, events, 1)
	require.True(t, events[0].GetDeleted())
	require.False(t, s.Snapshot().Nodes[s.LocalID].PubliclyAccessible)
}

func TestApplyPubliclyAccessibleFromPeer(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_PubliclyAccessible{
			PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
		},
	})
	require.True(t, s.Snapshot().Nodes[peerPK].PubliclyAccessible)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Deleted: true,
		Change: &statev1.GossipEvent_PubliclyAccessible{
			PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
		},
	})
	require.False(t, s.Snapshot().Nodes[peerPK].PubliclyAccessible)
}

func TestPubliclyAccessibleRoundTrip(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalPubliclyAccessible(true)

	missing := s.missingFor(&statev1.GossipStateDigest{
		Peers: map[string]*statev1.PeerDigest{
			s.LocalID.String(): {MaxCounter: 0, StateHash: 0},
		},
	})

	var found bool
	for _, ev := range missing {
		if ev.GetPubliclyAccessible() != nil && !ev.GetDeleted() {
			found = true
			break
		}
	}
	require.True(t, found, "expected publicly_accessible event in MissingFor output")
}

func TestPubliclyAccessibleConflictRecovery(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalPubliclyAccessible(true)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  s.LocalID.String(),
		Counter: 100,
		Change: &statev1.GossipEvent_PubliclyAccessible{
			PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
		},
	})

	require.NotEmpty(t, result.rebroadcast)
	var found bool
	for _, ev := range result.rebroadcast {
		if ev.GetPubliclyAccessible() != nil {
			found = true
			break
		}
	}
	require.True(t, found, "rebroadcast should include publicly_accessible event")
}

func TestFreshStoreGossipsPubliclyAccessibleDeletion(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	s := New(types.PeerKeyFromBytes(pub))

	missing := s.missingFor(&statev1.GossipStateDigest{
		Peers: map[string]*statev1.PeerDigest{
			s.LocalID.String(): {MaxCounter: 0, StateHash: 0},
		},
	})

	var found bool
	for _, ev := range missing {
		if ev.GetPubliclyAccessible() != nil && ev.GetDeleted() {
			require.Equal(t, uint64(2), ev.GetCounter())
			found = true
			break
		}
	}
	require.True(t, found, "fresh store should gossip a PubliclyAccessible deletion")
}

// --- Deny tests ---

func TestDenyPeerAndIsDenied(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	subjectPub := make([]byte, 32)
	subjectPub[0] = 0x42
	subjectKey := types.PeerKeyFromBytes(subjectPub)

	require.NotContains(t, s.Snapshot().DeniedPeers(), subjectKey)

	events := s.denyPeerRaw(subjectPub)
	require.Len(t, events, 1)
	require.Contains(t, s.Snapshot().DeniedPeers(), subjectKey)

	events = s.denyPeerRaw(subjectPub)
	require.Nil(t, events)
}

func TestDeniedPeersExcludedFromSnapshot(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPub := make([]byte, 32)
	peerPub[0] = 0x02
	peerKey := types.PeerKeyFromBytes(peerPub)

	s.applyEvents([]*statev1.GossipEvent{
		{PeerId: peerKey.String(), Counter: 1, Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 1234}}},
		{PeerId: peerKey.String(), Counter: 2, Change: &statev1.GossipEvent_IdentityPub{IdentityPub: &statev1.IdentityChange{IdentityPub: peerPub, CertExpiryUnix: time.Now().Add(time.Hour).Unix()}}},
	})
	require.Contains(t, s.Snapshot().Nodes, peerKey)

	s.denyPeerRaw(peerPub)
	require.NotContains(t, s.Snapshot().Nodes, peerKey)
}

func TestExpiredPeersExcludedFromSnapshot(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	pk2, peerIDStr := peerKey(2)
	pk3, peer3IDStr := peerKey(3)
	pk4, peer4IDStr := peerKey(4)

	pastExpiry := time.Now().Add(-2 * time.Hour).Unix()
	s.applyEvent(&statev1.GossipEvent{
		PeerId: peerIDStr, Counter: 1,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000}},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId: peerIDStr, Counter: 2,
		Change: &statev1.GossipEvent_IdentityPub{IdentityPub: &statev1.IdentityChange{IdentityPub: make([]byte, 32), CertExpiryUnix: pastExpiry}},
	})

	futureExpiry := time.Now().Add(24 * time.Hour).Unix()
	s.applyEvent(&statev1.GossipEvent{
		PeerId: peer3IDStr, Counter: 1,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001}},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId: peer3IDStr, Counter: 2,
		Change: &statev1.GossipEvent_IdentityPub{IdentityPub: &statev1.IdentityChange{IdentityPub: make([]byte, 32), CertExpiryUnix: futureExpiry}},
	})

	s.applyEvent(&statev1.GossipEvent{
		PeerId: peer4IDStr, Counter: 1,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.3"}, LocalPort: 9002}},
	})

	snap := s.Snapshot()
	require.NotContains(t, snap.Nodes, pk2)
	require.Contains(t, snap.Nodes, pk3)
	require.Contains(t, snap.Nodes, pk4)
}

func TestDenyEventEmittedOnGossipReceipt(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, senderPKStr := peerKey(2)
	subjectPK, _ := peerKey(3)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  senderPKStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Deny{
			Deny: &statev1.DenyChange{SubjectPub: subjectPK[:]},
		},
	})

	require.Len(t, result.deniedPeers, 1)
	require.Equal(t, subjectPK, result.deniedPeers[0])
	require.Contains(t, s.Snapshot().DeniedPeers(), subjectPK)
}

func TestNoDenyEventForAlreadyDenied(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	subjectPK, _ := peerKey(3)
	s.denyPeerRaw(subjectPK[:])

	_, senderPKStr := peerKey(2)
	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  senderPKStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Deny{
			Deny: &statev1.DenyChange{SubjectPub: subjectPK[:]},
		},
	})
	require.Empty(t, result.deniedPeers)
}

// --- Vivaldi tests ---

func TestSetLocalVivaldiCoordReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	coord := coords.Coord{X: 10.5, Y: -3.2, Height: 0.001}
	events := s.setLocalVivaldiCoord(coord)
	require.Len(t, events, 1)
	require.False(t, events[0].GetDeleted())
	v := events[0].GetVivaldi()
	require.Equal(t, 10.5, v.GetX())
	require.Equal(t, -3.2, v.GetY())
	require.Equal(t, 0.001, v.GetHeight())
}

func TestSetLocalVivaldiCoordEpsilonSuppression(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	coord := coords.Coord{X: 10.5, Y: -3.2, Height: 0.001}
	s.setLocalVivaldiCoord(coord)

	events := s.setLocalVivaldiCoord(coords.Coord{X: 10.5001, Y: -3.2, Height: 0.001})
	require.Nil(t, events)

	events = s.setLocalVivaldiCoord(coords.Coord{X: 20.0, Y: -3.2, Height: 0.001})
	require.Len(t, events, 1)
}

func TestApplyVivaldiFromPeer(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Vivaldi{
			Vivaldi: &statev1.VivaldiCoordinateChange{X: 5.0, Y: 7.0, Height: 0.1},
		},
	})

	nv := s.Snapshot().Nodes[peerPK]
	require.NotNil(t, nv.VivaldiCoord)
	require.Equal(t, 5.0, nv.VivaldiCoord.X)
}

func TestApplyVivaldiDeletionClearsToNil(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId: peerIDStr, Counter: 1,
		Change: &statev1.GossipEvent_Vivaldi{Vivaldi: &statev1.VivaldiCoordinateChange{X: 5.0, Y: 7.0, Height: 0.1}},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId: peerIDStr, Counter: 2, Deleted: true,
		Change: &statev1.GossipEvent_Vivaldi{Vivaldi: &statev1.VivaldiCoordinateChange{}},
	})

	peerPK, _ := peerKey(2)
	require.Nil(t, s.Snapshot().Nodes[peerPK].VivaldiCoord)
}

// --- Rebroadcast tests ---

func TestApplyRemoteEventRebroadcast(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})
	require.Len(t, result.rebroadcast, 1)
	require.Equal(t, peerIDStr, result.rebroadcast[0].GetPeerId())
}

func TestApplyStaleRemoteEventNoRebroadcast(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId: peerIDStr, Counter: 2,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000}},
	})

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: peerIDStr, Counter: 1,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001}},
	})
	require.Empty(t, result.rebroadcast)
}

// --- Resource telemetry tests ---

func TestResourceTelemetryDeadbandTest(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	s := newTestStore(pub)

	events := s.setLocalResourceTelemetry(10, 20, 1<<30, 4)
	require.Len(t, events, 1)

	events = s.setLocalResourceTelemetry(11, 21, 1<<30, 4)
	require.Nil(t, events, "below threshold should be suppressed")

	events = s.setLocalResourceTelemetry(12, 20, 1<<30, 4)
	require.Len(t, events, 1, "cpu crosses threshold")

	events = s.setLocalResourceTelemetry(12, 22, 1<<30, 4)
	require.Len(t, events, 1, "mem crosses threshold")

	events = s.setLocalResourceTelemetry(12, 22, 2<<30, 4)
	require.Len(t, events, 1, "mem total change")

	events = s.setLocalResourceTelemetry(12, 22, 2<<30, 8)
	require.Len(t, events, 1, "num cpu change")
}

// --- Watermark tests ---

func TestWatermarkAlwaysAdvances(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)

	s.applyEvents([]*statev1.GossipEvent{{
		PeerId: peerIDStr, Counter: 5,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000}},
	}})
	require.Equal(t, uint64(5), s.clock().GetPeers()[peerPK.String()].GetMaxCounter())

	s.applyEvents([]*statev1.GossipEvent{{
		PeerId: peerIDStr, Counter: 10,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000}},
	}})
	require.Equal(t, uint64(10), s.clock().GetPeers()[peerPK.String()].GetMaxCounter())
}

// --- Peer hash tests ---

func TestPeerHash(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)

	d1 := s.clock()
	d2 := s.clock()
	h1 := d1.GetPeers()[s.LocalID.String()].GetStateHash()
	h2 := d2.GetPeers()[s.LocalID.String()].GetStateHash()
	require.Equal(t, h1, h2)
	require.NotZero(t, h1)

	s.setExternalPort(52000)
	d3 := s.clock()
	h3 := d3.GetPeers()[s.LocalID.String()].GetStateHash()
	require.NotEqual(t, h1, h3)
}

func TestDigestHashMismatchSendsAll(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.setExternalPort(45000)

	events := s.missingFor(&statev1.GossipStateDigest{
		Peers: map[string]*statev1.PeerDigest{
			s.LocalID.String(): {MaxCounter: 100, StateHash: 12345},
		},
	})
	require.Len(t, events, 8)
}

func TestDigestMatchSkips(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	events := s.missingFor(s.clock())
	require.Empty(t, events)
}

func TestMissingForPartialClockIncludesUnknownPeers(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	pkA, strA := peerKey(2)
	pkB, strB := peerKey(3)
	pkC, strC := peerKey(4)

	for _, p := range []struct {
		str string
		ip  string
	}{
		{strA, "10.0.0.1"},
		{strB, "10.0.0.2"},
		{strC, "10.0.0.3"},
	} {
		s.applyEvent(&statev1.GossipEvent{
			PeerId: p.str, Counter: 1,
			Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{p.ip}, LocalPort: 9000}},
		})
	}

	events := s.missingFor(&statev1.GossipStateDigest{
		Peers: map[string]*statev1.PeerDigest{
			pkA.String(): {MaxCounter: 0, StateHash: 0},
		},
	})

	peerIDs := make(map[string]struct{})
	for _, ev := range events {
		peerIDs[ev.GetPeerId()] = struct{}{}
	}
	require.Contains(t, peerIDs, pkA.String())
	require.Contains(t, peerIDs, pkB.String())
	require.Contains(t, peerIDs, pkC.String())
}

func TestMissingForNilClockSendsEverything(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, strA := peerKey(2)
	_, strB := peerKey(3)

	for _, p := range []string{strA, strB} {
		s.applyEvent(&statev1.GossipEvent{
			PeerId: p, Counter: 1,
			Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000}},
		})
	}

	events := s.missingFor(nil)

	peerIDs := make(map[string]struct{})
	for _, ev := range events {
		peerIDs[ev.GetPeerId()] = struct{}{}
	}
	require.Contains(t, peerIDs, s.LocalID.String())
	require.Contains(t, peerIDs, strA)
	require.Contains(t, peerIDs, strB)
}

// --- Workload tests ---

func TestWorkloadSpecRoundTrip(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events, err := s.setLocalWorkloadSpec("abc123", 2, 16, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)

	specs := s.Snapshot().Specs
	require.Len(t, specs, 1)
	sv := specs["abc123"]
	require.Equal(t, uint32(2), sv.Spec.GetReplicas())
	require.Equal(t, s.LocalID, sv.Publisher)

	events2, err := s.setLocalWorkloadSpec("abc123", 2, 16, 0)
	require.NoError(t, err)
	require.Nil(t, events2)

	del := s.removeLocalWorkloadSpec("abc123")
	require.Len(t, del, 1)
	require.True(t, del[0].GetDeleted())
	require.Empty(t, s.Snapshot().Specs)

	del2 := s.removeLocalWorkloadSpec("abc123")
	require.Nil(t, del2)
}

func TestWorkloadClaimRoundTrip(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.setLocalWorkloadClaim("abc123", true)
	require.Len(t, events, 1)
	require.False(t, events[0].GetDeleted())
	require.Contains(t, s.Snapshot().Claims["abc123"], s.LocalID)

	events2 := s.setLocalWorkloadClaim("abc123", true)
	require.Nil(t, events2)

	rel := s.setLocalWorkloadClaim("abc123", false)
	require.Len(t, rel, 1)
	require.True(t, rel[0].GetDeleted())
	require.Empty(t, s.Snapshot().Claims)
}

func TestWorkloadGossipApply(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, remotePeerStr := peerKey(2)
	remotePK, _ := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId: remotePeerStr, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "deadbeef", Replicas: 3}},
	})
	require.Equal(t, uint32(3), s.Snapshot().Specs["deadbeef"].Spec.GetReplicas())

	s.setLocalConnected(remotePK, true)

	s.applyEvent(&statev1.GossipEvent{
		PeerId: remotePeerStr, Counter: 2,
		Change: &statev1.GossipEvent_WorkloadClaim{WorkloadClaim: &statev1.WorkloadClaimChange{Hash: "deadbeef"}},
	})
	require.Contains(t, s.Snapshot().Claims["deadbeef"], remotePK)

	s.applyEvent(&statev1.GossipEvent{
		PeerId: remotePeerStr, Counter: 3, Deleted: true,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "deadbeef"}},
	})
	require.Empty(t, s.Snapshot().Specs)
}

func TestWorkloadChangedEventEmitted(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, remotePeerStr := peerKey(2)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: remotePeerStr, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "abc", Replicas: 1}},
	})
	require.True(t, result.workloadsDirty)

	result = s.applyEvent(&statev1.GossipEvent{
		PeerId: remotePeerStr, Counter: 2,
		Change: &statev1.GossipEvent_WorkloadClaim{WorkloadClaim: &statev1.WorkloadClaimChange{Hash: "abc"}},
	})
	require.True(t, result.workloadsDirty)
}

func TestReachabilityChangeEmitsWorkloadEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, remotePeerStr := peerKey(2)
	_, targetPeerStr := peerKey(3)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: remotePeerStr, Counter: 1,
		Change: &statev1.GossipEvent_Reachability{Reachability: &statev1.ReachabilityChange{PeerId: targetPeerStr}},
	})
	require.True(t, result.workloadsDirty)
}

func TestVivaldiChangeDoesNotEmitWorkloadEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, remotePeerStr := peerKey(2)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: remotePeerStr, Counter: 1,
		Change: &statev1.GossipEvent_Vivaldi{Vivaldi: &statev1.VivaldiCoordinateChange{X: 1, Y: 2, Height: 0.1}},
	})
	require.False(t, result.workloadsDirty)
}

func TestAllWorkloadSpecs_DuplicatePublishers_LowestPeerKeyWins(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 5
	s := newTestStore(pub)

	_, err := s.setLocalWorkloadSpec("shared", 1, 0, 0)
	require.NoError(t, err)

	peer2PK, peer2Str := peerKey(2)
	_, peer9Str := peerKey(9)

	s.applyEvent(&statev1.GossipEvent{
		PeerId: peer2Str, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "shared", Replicas: 3}},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId: peer9Str, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "shared", Replicas: 7}},
	})

	specs := s.Snapshot().Specs
	require.Len(t, specs, 1)
	require.Equal(t, peer2PK, specs["shared"].Publisher)
	require.Equal(t, uint32(3), specs["shared"].Spec.GetReplicas())
}

func TestSetLocalWorkloadSpec_RejectsWhenRemoteOwns(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, remotePeerStr := peerKey(2)
	s.applyEvent(&statev1.GossipEvent{
		PeerId: remotePeerStr, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "contested", Replicas: 1}},
	})

	events, err := s.setLocalWorkloadSpec("contested", 2, 0, 0)
	require.ErrorIs(t, err, errSpecOwnedRemotely)
	require.Nil(t, events)
}

func TestRemoteSpec_TombstonesLosingLocalSpec(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 9
	s := newTestStore(pub)

	events, err := s.setLocalWorkloadSpec("contested", 3, 0, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)

	winnerPK, winnerStr := peerKey(1)
	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: winnerStr, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "contested", Replicas: 2}},
	})

	require.Len(t, result.rebroadcast, 2)
	require.True(t, result.rebroadcast[1].GetDeleted())

	specs := s.Snapshot().Specs
	require.Len(t, specs, 1)
	require.Equal(t, winnerPK, specs["contested"].Publisher)
}

func TestRemoteSpec_NoTombstoneWhenLocalWins(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, err := s.setLocalWorkloadSpec("mine", 3, 0, 0)
	require.NoError(t, err)

	_, loserStr := peerKey(9)
	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: loserStr, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "mine", Replicas: 5}},
	})

	require.Len(t, result.rebroadcast, 1)
	require.Equal(t, s.LocalID, s.Snapshot().Specs["mine"].Publisher)
}

func TestRemoteSpec_NoTombstoneWhenRemoteDenied(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 9
	s := newTestStore(pub)

	_, err := s.setLocalWorkloadSpec("contested", 3, 0, 0)
	require.NoError(t, err)

	deniedPK, deniedStr := peerKey(1)
	s.denied[deniedPK] = struct{}{}

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: deniedStr, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "contested", Replicas: 2}},
	})

	require.Len(t, result.rebroadcast, 1)
	require.Equal(t, s.LocalID, s.Snapshot().Specs["contested"].Publisher)
}

func TestRemoteSpec_NoTombstoneWhenRemoteExpired(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 9
	s := newTestStore(pub)

	_, err := s.setLocalWorkloadSpec("contested", 3, 0, 0)
	require.NoError(t, err)

	_, expiredStr := peerKey(1)
	pastExpiry := time.Now().Add(-time.Hour).Unix()
	s.applyEvent(&statev1.GossipEvent{
		PeerId: expiredStr, Counter: 1,
		Change: &statev1.GossipEvent_IdentityPub{IdentityPub: &statev1.IdentityChange{IdentityPub: make([]byte, 32), CertExpiryUnix: pastExpiry}},
	})

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: expiredStr, Counter: 2,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "contested", Replicas: 2}},
	})

	require.Len(t, result.rebroadcast, 1)
	require.Equal(t, s.LocalID, s.Snapshot().Specs["contested"].Publisher)
}

func TestWorkloadQueries_ExcludeDeniedNodes(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	deniedPK, deniedStr := peerKey(2)
	s.setLocalConnected(deniedPK, true)
	s.applyEvent(&statev1.GossipEvent{
		PeerId: deniedStr, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "deniedwl", Replicas: 1}},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId: deniedStr, Counter: 2,
		Change: &statev1.GossipEvent_WorkloadClaim{WorkloadClaim: &statev1.WorkloadClaimChange{Hash: "deniedwl"}},
	})

	require.Len(t, s.Snapshot().Specs, 1)
	require.Contains(t, s.Snapshot().Claims["deniedwl"], deniedPK)

	s.denied[deniedPK] = struct{}{}
	s.updateSnapshot()

	require.Empty(t, s.Snapshot().Specs)
	require.Empty(t, s.Snapshot().Claims)
}

func TestAllWorkloadClaims_ExcludesUnreachableNodes(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	remotePK, remoteStr := peerKey(2)

	s.setLocalConnected(remotePK, true)
	s.applyEvent(&statev1.GossipEvent{
		PeerId: remoteStr, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadClaim{WorkloadClaim: &statev1.WorkloadClaimChange{Hash: "wl1"}},
	})
	require.Contains(t, s.Snapshot().Claims["wl1"], remotePK)

	s.setLocalConnected(remotePK, false)
	require.Empty(t, s.Snapshot().Claims)

	s.setLocalWorkloadClaim("wl2", true)
	require.Contains(t, s.Snapshot().Claims["wl2"], s.LocalID)
}

func TestAllWorkloadClaims_RackFailureIgnoresStaleObservers(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	pkB, strB := peerKey(2)
	pkC, strC := peerKey(3)

	s.setLocalConnected(pkB, true)
	s.setLocalConnected(pkC, true)

	s.applyEvent(&statev1.GossipEvent{
		PeerId: strB, Counter: 1,
		Change: &statev1.GossipEvent_Reachability{Reachability: &statev1.ReachabilityChange{PeerId: strC}},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId: strC, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadClaim{WorkloadClaim: &statev1.WorkloadClaimChange{Hash: "rackwl"}},
	})
	require.Contains(t, s.Snapshot().Claims["rackwl"], pkC)

	s.setLocalConnected(pkB, false)
	s.setLocalConnected(pkC, false)
	require.Empty(t, s.Snapshot().Claims)
}

func TestAllWorkloadClaims_MultiHopReachability(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	pkB, strB := peerKey(2)
	_, strC := peerKey(3)
	pkD, strD := peerKey(4)

	s.setLocalConnected(pkB, true)

	s.applyEvent(&statev1.GossipEvent{
		PeerId: strB, Counter: 1,
		Change: &statev1.GossipEvent_Reachability{Reachability: &statev1.ReachabilityChange{PeerId: strC}},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId: strC, Counter: 1,
		Change: &statev1.GossipEvent_Reachability{Reachability: &statev1.ReachabilityChange{PeerId: strD}},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId: strD, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadClaim{WorkloadClaim: &statev1.WorkloadClaimChange{Hash: "multihop"}},
	})

	require.Contains(t, s.Snapshot().Claims["multihop"], pkD)
}

// --- Traffic heatmap tests ---

func TestTrafficHeatmap_ApplyAndQuery(t *testing.T) {
	localPK, localIDStr := peerKey(1)
	remotePK, remotePKStr := peerKey(2)

	s := newTestStore([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

	s.applyEvent(&statev1.GossipEvent{
		PeerId: remotePKStr, Counter: 1,
		Change: &statev1.GossipEvent_TrafficHeatmap{
			TrafficHeatmap: &statev1.TrafficHeatmapChange{
				Rates: []*statev1.TrafficRate{{PeerId: localIDStr, BytesIn: 100, BytesOut: 200}},
			},
		},
	})

	all := s.Snapshot().Heatmaps
	require.Contains(t, all, remotePK)
	require.Equal(t, TrafficSnapshot{BytesIn: 100, BytesOut: 200}, all[remotePK][localPK])
}

func TestExternalPort_TombstoneOnRestart(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1

	// First "lifetime": node sets an external port and captures its full state.
	s1 := newTestStore(pub)
	s1.setExternalPort(45678)
	staleState := s1.FullState()

	// Simulate restart: fresh store for the same peer key.
	s2 := newTestStore(pub)
	require.Equal(t, uint32(0), s2.Snapshot().Nodes[s2.LocalID].ExternalPort)

	// A peer gossips the pre-restart state back. handleSelfConflictLocked
	// detects the higher counter, bumps all local log entries (including
	// tombstones), and rebroadcasts. Self-events for ExternalPort are NOT
	// applied to the local record, so the value must stay zero.
	_, _, err := s2.ApplyDelta(s2.LocalID, staleState)
	require.NoError(t, err)
	require.Equal(t, uint32(0), s2.Snapshot().Nodes[s2.LocalID].ExternalPort,
		"stale ExternalPort must not survive a restart")

	// The tombstone must now have a counter higher than the stale value,
	// ensuring peers also clear the stale attribute.
	entry := s2.nodes[s2.LocalID].log[attrKey{kind: attrExternalPort}]
	require.True(t, entry.Deleted)
}

func TestTrafficHeatmap_TombstoneOnRestart(t *testing.T) {
	pub := []byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	s := newTestStore(pub)

	targetPK, _ := peerKey(4)
	events := s.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{
		{4}: {BytesIn: 500, BytesOut: 600},
	})
	require.Len(t, events, 1)

	localID := types.PeerKeyFromBytes(pub)
	require.Equal(t, TrafficSnapshot{BytesIn: 500, BytesOut: 600}, s.Snapshot().Heatmaps[localID][targetPK])

	local := s.nodes[localID]
	tombstoneStaleAttrs(&local)
	s.nodes[localID] = local

	entry := local.log[attrKey{kind: attrTrafficHeatmap}]
	require.True(t, entry.Deleted)
}

func TestTrafficHeatmap_ZeroSnapshotSkipped(t *testing.T) {
	pub := []byte{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	s := newTestStore(pub)

	events := s.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{})
	require.Empty(t, events)

	events = s.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{
		{6}: {BytesIn: 0, BytesOut: 0},
	})
	require.Empty(t, events)
}

func TestTrafficHeatmap_StaleClearing(t *testing.T) {
	pub := []byte{7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	s := newTestStore(pub)
	localID := types.PeerKeyFromBytes(pub)

	events := s.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{
		{8}: {BytesIn: 100, BytesOut: 200},
	})
	require.Len(t, events, 1)
	require.False(t, events[0].GetDeleted())
	require.Contains(t, s.Snapshot().Heatmaps, localID)

	events = s.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{})
	require.Len(t, events, 1)
	require.True(t, events[0].GetDeleted())
	require.NotContains(t, s.Snapshot().Heatmaps, localID)

	events = s.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{})
	require.Empty(t, events)
}

// --- Gossip round-trip tests ---
//
// These tests verify that every gossipable attribute type survives the
// full gossip round-trip: local mutation → encode → apply on remote → snapshot.

func TestGossipRoundTrip_AllAttributes(t *testing.T) {
	pubA := make([]byte, 32)
	pubA[0] = 1
	storeA := newTestStore(pubA)

	pubB := make([]byte, 32)
	pubB[0] = 2
	storeB := newTestStore(pubB)

	peerC, _ := peerKey(3)

	// Set every attribute type on store A.
	storeA.setLocalNetwork([]string{"10.0.0.1", "192.168.1.1"}, 9000)
	storeA.setExternalPort(45000)
	storeA.setObservedExternalIP("52.204.52.130")
	storeA.upsertLocalService(8080, "http")
	storeA.setLocalConnected(peerC, true)
	storeA.setLocalPubliclyAccessible(true)
	storeA.setLocalVivaldiCoord(coords.Coord{X: 10.5, Y: -3.2, Height: 0.001})
	storeA.setLocalNatType(nat.Easy)
	storeA.setLocalResourceTelemetry(42, 75, 1<<30, 4)
	_, err := storeA.setLocalWorkloadSpec("abc123", 2, 16, 0)
	require.NoError(t, err)
	storeA.setLocalWorkloadClaim("abc123", true)
	storeA.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{
		peerC: {BytesIn: 500, BytesOut: 600},
	})

	// Transfer A's full state to B.
	fullState := storeA.FullState()
	_, _, err2 := storeB.ApplyDelta(storeA.LocalID, fullState)
	require.NoError(t, err2)

	// Make A reachable from B so claims are in B's live component.
	storeB.setLocalConnected(storeA.LocalID, true)

	snap := storeB.Snapshot()
	nodeA := snap.Nodes[storeA.LocalID]

	// Network
	require.Equal(t, []string{"10.0.0.1", "192.168.1.1"}, nodeA.IPs)
	require.Equal(t, uint32(9000), nodeA.LocalPort)

	// ExternalPort
	require.Equal(t, uint32(45000), nodeA.ExternalPort)

	// ObservedExternalIP
	require.Equal(t, "52.204.52.130", nodeA.ObservedExternalIP)

	// Identity (set at construction)
	require.Equal(t, pubA, nodeA.IdentityPub)

	// Service
	require.Contains(t, nodeA.Services, "http")
	require.Equal(t, uint32(8080), nodeA.Services["http"].GetPort())

	// Reachability
	require.Contains(t, nodeA.Reachable, peerC)

	// PubliclyAccessible
	require.True(t, nodeA.PubliclyAccessible)

	// Vivaldi
	require.NotNil(t, nodeA.VivaldiCoord)
	require.Equal(t, 10.5, nodeA.VivaldiCoord.X)
	require.Equal(t, -3.2, nodeA.VivaldiCoord.Y)
	require.Equal(t, 0.001, nodeA.VivaldiCoord.Height)

	// NatType
	require.Equal(t, nat.Easy, nodeA.NatType)

	// ResourceTelemetry
	require.Equal(t, uint32(42), nodeA.CPUPercent)
	require.Equal(t, uint32(75), nodeA.MemPercent)
	require.Equal(t, uint64(1<<30), nodeA.MemTotalBytes)
	require.Equal(t, uint32(4), nodeA.NumCPU)

	// WorkloadSpec
	require.Contains(t, snap.Specs, "abc123")
	require.Equal(t, storeA.LocalID, snap.Specs["abc123"].Publisher)
	require.Equal(t, uint32(2), snap.Specs["abc123"].Spec.GetReplicas())

	// WorkloadClaim (A is in B's live component via setLocalConnected)
	require.Contains(t, snap.Claims, "abc123")
	require.Contains(t, snap.Claims["abc123"], storeA.LocalID)

	// TrafficHeatmap
	require.Contains(t, snap.Heatmaps, storeA.LocalID)
	require.Equal(t, TrafficSnapshot{BytesIn: 500, BytesOut: 600}, snap.Heatmaps[storeA.LocalID][peerC])
}

func TestGossipRoundTrip_DenyAttribute(t *testing.T) {
	pubA := make([]byte, 32)
	pubA[0] = 1
	storeA := newTestStore(pubA)

	pubB := make([]byte, 32)
	pubB[0] = 2
	storeB := newTestStore(pubB)

	subjectPK, _ := peerKey(9)
	storeA.denyPeerRaw(subjectPK[:])

	fullState := storeA.FullState()
	_, _, err := storeB.ApplyDelta(storeA.LocalID, fullState)
	require.NoError(t, err)

	require.Contains(t, storeB.Snapshot().DeniedPeers(), subjectPK)
}

func TestGossipRoundTrip_TrafficHeatmap_MultiplePeers(t *testing.T) {
	pubA := make([]byte, 32)
	pubA[0] = 1
	storeA := newTestStore(pubA)

	pubB := make([]byte, 32)
	pubB[0] = 2
	storeB := newTestStore(pubB)

	peerC, _ := peerKey(3)
	peerD, _ := peerKey(4)

	storeA.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{
		peerC: {BytesIn: 100, BytesOut: 200},
		peerD: {BytesIn: 300, BytesOut: 400},
	})

	fullState := storeA.FullState()
	_, _, err := storeB.ApplyDelta(storeA.LocalID, fullState)
	require.NoError(t, err)

	heatmap := storeB.Snapshot().Heatmaps[storeA.LocalID]
	require.Len(t, heatmap, 2)
	require.Equal(t, TrafficSnapshot{BytesIn: 100, BytesOut: 200}, heatmap[peerC])
	require.Equal(t, TrafficSnapshot{BytesIn: 300, BytesOut: 400}, heatmap[peerD])
}

// TestSetLocalTraffic_MultiplePeers_Retained verifies that calling SetLocalTraffic
// for multiple peers retains all peers' traffic in the heatmap. This exercises the
// same calling pattern as tunneling.Service.sampleTraffic().
func TestSetLocalTraffic_MultiplePeers_Retained(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerA, _ := peerKey(2)
	peerB, _ := peerKey(3)
	peerC, _ := peerKey(4)

	s.SetLocalTraffic(peerA, 100, 200)
	s.SetLocalTraffic(peerB, 300, 400)
	s.SetLocalTraffic(peerC, 500, 600)

	snap := s.Snapshot()
	heatmap := snap.Heatmaps[s.LocalID]
	require.Len(t, heatmap, 3, "all three peers' traffic must be retained")
	require.Equal(t, TrafficSnapshot{BytesIn: 100, BytesOut: 200}, heatmap[peerA])
	require.Equal(t, TrafficSnapshot{BytesIn: 300, BytesOut: 400}, heatmap[peerB])
	require.Equal(t, TrafficSnapshot{BytesIn: 500, BytesOut: 600}, heatmap[peerC])
}

// TestSetLocalTraffic_MultiplePeers_GossipRoundTrip verifies that per-peer
// SetLocalTraffic calls produce gossip events that, when applied on a remote
// store, reconstruct the complete multi-peer heatmap.
func TestSetLocalTraffic_MultiplePeers_GossipRoundTrip(t *testing.T) {
	pubA := make([]byte, 32)
	pubA[0] = 1
	storeA := newTestStore(pubA)

	pubB := make([]byte, 32)
	pubB[0] = 2
	storeB := newTestStore(pubB)

	peerC, _ := peerKey(3)
	peerD, _ := peerKey(4)

	storeA.SetLocalTraffic(peerC, 100, 200)
	storeA.SetLocalTraffic(peerD, 300, 400)

	fullState := storeA.FullState()
	_, _, err := storeB.ApplyDelta(storeA.LocalID, fullState)
	require.NoError(t, err)

	heatmap := storeB.Snapshot().Heatmaps[storeA.LocalID]
	require.Len(t, heatmap, 2, "remote must see both peers' traffic")
	require.Equal(t, TrafficSnapshot{BytesIn: 100, BytesOut: 200}, heatmap[peerC])
	require.Equal(t, TrafficSnapshot{BytesIn: 300, BytesOut: 400}, heatmap[peerD])
}
