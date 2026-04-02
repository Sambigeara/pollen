package state

import (
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func newTestStore(pk types.PeerKey) *store {
	return New(pk).(*store) //nolint:forcetypeassert
}

// applyEvent is a test convenience for applying a single gossip event.
func (s *store) applyEvent(event *statev1.GossipEvent) applyResult {
	return s.applyEvents([]*statev1.GossipEvent{event})
}

func (s *store) testDigest() *statev1.Digest {
	var result *statev1.Digest
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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	events := s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	require.Len(t, events, 1)

	ev := events[0]
	require.Equal(t, uint64(6), ev.GetCounter())
	network := ev.GetNetwork()
	require.NotNil(t, network)
	require.Equal(t, []string{"10.0.0.1"}, network.GetIps())
	require.Equal(t, uint32(9000), network.GetLocalPort())
}

func TestSetLocalNetworkNoOpWhenUnchanged(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	events := s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	require.Nil(t, events)
}

func TestSetObservedAddressReturnsEvent(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	events := s.setObservedAddress("52.204.52.130", 51000)
	require.Len(t, events, 1)
	require.Equal(t, uint32(51000), events[0].GetObservedAddress().GetPort())
	require.Equal(t, "52.204.52.130", events[0].GetObservedAddress().GetIp())
}

func TestSetLocalConnectedConnect(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	peerID, _ := peerKey(2)

	events := s.setLocalConnected(peerID, true)
	require.Len(t, events, 1)
	require.False(t, events[0].GetDeleted())
	require.Equal(t, peerID.String(), events[0].GetReachability().GetPeerId())
}

func TestSetLocalConnectedDisconnect(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	peerID, _ := peerKey(2)

	s.setLocalConnected(peerID, true)
	events := s.setLocalConnected(peerID, false)
	require.Len(t, events, 1)
	require.True(t, events[0].GetDeleted())
}

// --- Apply event tests ---

func TestApplyEventObservedAddress(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	peerPK, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 3,
		Change: &statev1.GossipEvent_ObservedAddress{
			ObservedAddress: &statev1.ObservedAddressChange{Port: 45000, Ip: "52.204.52.130"},
		},
	})

	rec := s.nodes[peerPK]
	require.Equal(t, uint32(45000), rec.ExternalPort)
	require.Equal(t, "52.204.52.130", rec.ObservedExternalIP)
	require.Equal(t, uint64(3), rec.maxCounter)
}

func TestApplyEventPerKeyCounter(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	_, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 3,
		Change: &statev1.GossipEvent_ObservedAddress{
			ObservedAddress: &statev1.ObservedAddressChange{Port: 45000},
		},
	})

	// Older event for same key is rejected.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Change: &statev1.GossipEvent_ObservedAddress{
			ObservedAddress: &statev1.ObservedAddressChange{Port: 44000},
		},
	})

	peerPK, _ := peerKey(2)
	require.Equal(t, uint32(45000), s.nodes[peerPK].ExternalPort)
}

func TestApplyEventDifferentKeys(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	_, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 5,
		Change: &statev1.GossipEvent_ObservedAddress{
			ObservedAddress: &statev1.ObservedAddressChange{Port: 45000},
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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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

func TestApplyEventCertExpiry(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	peerPK, peerIDStr := peerKey(2)
	expiry := time.Now().Add(time.Hour).Unix()

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_CertExpiry{
			CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: expiry},
		},
	})

	require.Equal(t, expiry, s.nodes[peerPK].CertExpiry)
}

func TestApplyEventReachablePeer(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)
	localID := s.localID

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
	require.Equal(t, uint64(16), s.nodes[localID].maxCounter)
}

func TestApplyEventSelfStateNoConflict(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  s.localID.String(),
		Counter: 2,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})

	require.Empty(t, result.rebroadcast)
}

// --- MissingFor / digest tests ---

func TestMissingForReturnsEvents(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.setObservedAddress("", 45000)

	events := s.missingFor(&statev1.Digest{
		Peers: map[string]*statev1.PeerDigest{
			s.localID.String(): {MaxCounter: 0, StateHash: 0},
		},
	})
	require.Len(t, events, 6)
}

func TestMissingForRespectsDigest(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.setObservedAddress("", 45000)

	events := s.missingFor(&statev1.Digest{
		Peers: map[string]*statev1.PeerDigest{
			s.localID.String(): {MaxCounter: 5},
		},
	})
	// Counter ahead → delta: only events above counter 5.
	require.Len(t, events, 2)
}

func TestMissingForReturnsNilForUpToDate(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)

	events := s.missingFor(s.testDigest())
	require.Empty(t, events)
}

func TestDigestUsesMaxCounter(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.setObservedAddress("", 45000)

	digest := s.testDigest()
	require.Equal(t, uint64(7), digest.GetPeers()[s.localID.String()].GetMaxCounter())
}

// --- Service tests ---

func TestUpsertLocalServiceReturnsEvent(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	events := s.upsertLocalService(8080, "http")
	require.Len(t, events, 1)
	sc := events[0].GetService()
	require.Equal(t, "http", sc.GetName())
	require.Equal(t, uint32(8080), sc.Port)
}

func TestRemoveLocalServiceReturnsEvent(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.upsertLocalService(8080, "http")
	events := s.removeLocalService("http")
	require.Len(t, events, 1)
	require.True(t, events[0].GetDeleted())
	require.Equal(t, "http", events[0].GetService().GetName())
}

func TestRemoveLocalServiceNoOpWhenMissing(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	events := s.removeLocalService("http")
	require.Nil(t, events)
}

// --- Publicly accessible tests ---

func TestSetLocalPubliclyAccessibleReturnsEvent(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	events := s.setLocalPubliclyAccessible(true)
	require.Len(t, events, 1)
	require.False(t, events[0].GetDeleted())
	require.NotNil(t, events[0].GetPubliclyAccessible())
}

func TestSetLocalPubliclyAccessibleNoOpWhenUnchanged(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalPubliclyAccessible(true)
	events := s.setLocalPubliclyAccessible(true)
	require.Nil(t, events)
}

func TestSetLocalPubliclyAccessibleClear(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalPubliclyAccessible(true)
	events := s.setLocalPubliclyAccessible(false)
	require.Len(t, events, 1)
	require.True(t, events[0].GetDeleted())
	require.False(t, s.Snapshot().Nodes[s.localID].PubliclyAccessible)
}

func TestApplyPubliclyAccessibleFromPeer(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalPubliclyAccessible(true)

	missing := s.missingFor(&statev1.Digest{
		Peers: map[string]*statev1.PeerDigest{
			s.localID.String(): {MaxCounter: 0, StateHash: 0},
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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalPubliclyAccessible(true)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  s.localID.String(),
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
	pk := genKey(t)
	s := New(pk)

	var batch statev1.GossipEventBatch
	require.NoError(t, batch.UnmarshalVT(s.EncodeFull()))

	var found bool
	for _, ev := range batch.GetEvents() {
		if ev.GetPubliclyAccessible() != nil && ev.GetDeleted() {
			found = true
			break
		}
	}
	require.True(t, found, "fresh store should gossip a PubliclyAccessible deletion")
}

// --- Deny tests ---

func TestDenyPeerAndIsDenied(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	subjectKey, _ := peerKey(0x42)

	require.NotContains(t, s.Snapshot().DeniedPeers(), subjectKey)

	events := s.denyPeerRaw(subjectKey[:])
	require.Len(t, events, 1)
	require.Contains(t, s.Snapshot().DeniedPeers(), subjectKey)

	events = s.denyPeerRaw(subjectKey[:])
	require.Nil(t, events)
}

func TestDeniedPeersExcludedFromSnapshot(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	remotePK, remotePKStr := peerKey(0x02)

	s.applyEvents([]*statev1.GossipEvent{
		{PeerId: remotePKStr, Counter: 1, Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 1234}}},
		{PeerId: remotePKStr, Counter: 2, Change: &statev1.GossipEvent_CertExpiry{CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: time.Now().Add(time.Hour).Unix()}}},
	})
	require.Contains(t, s.Snapshot().Nodes, remotePK)

	s.denyPeerRaw(remotePK[:])
	require.NotContains(t, s.Snapshot().Nodes, remotePK)
}

func TestExpiredPeersExcludedFromSnapshot(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
		Change: &statev1.GossipEvent_CertExpiry{CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: pastExpiry}},
	})

	futureExpiry := time.Now().Add(24 * time.Hour).Unix()
	s.applyEvent(&statev1.GossipEvent{
		PeerId: peer3IDStr, Counter: 1,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001}},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId: peer3IDStr, Counter: 2,
		Change: &statev1.GossipEvent_CertExpiry{CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: futureExpiry}},
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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	_, senderPKStr := peerKey(2)
	subjectPK, _ := peerKey(3)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  senderPKStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Deny{
			Deny: &statev1.DenyChange{PeerPub: subjectPK[:]},
		},
	})

	require.Len(t, result.deniedPeers, 1)
	require.Equal(t, subjectPK, result.deniedPeers[0])
	require.Contains(t, s.Snapshot().DeniedPeers(), subjectPK)
}

func TestNoDenyEventForAlreadyDenied(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	subjectPK, _ := peerKey(3)
	s.denyPeerRaw(subjectPK[:])

	_, senderPKStr := peerKey(2)
	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  senderPKStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Deny{
			Deny: &statev1.DenyChange{PeerPub: subjectPK[:]},
		},
	})
	require.Empty(t, result.deniedPeers)
}

// --- Vivaldi tests ---

func TestSetLocalVivaldiCoordReturnsEvent(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	coord := coords.Coord{X: 10.5, Y: -3.2, Height: 0.001}
	events := s.setLocalVivaldiCoord(coord, 0.5)
	require.Len(t, events, 1)
	require.False(t, events[0].GetDeleted())
	v := events[0].GetVivaldi()
	require.Equal(t, 10.5, v.GetX())
	require.Equal(t, -3.2, v.GetY())
	require.Equal(t, 0.001, v.GetHeight())
	require.Equal(t, 0.5, v.GetError())
}

func TestSetLocalVivaldiCoordEpsilonSuppression(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	coord := coords.Coord{X: 10.5, Y: -3.2, Height: 0.001}
	s.setLocalVivaldiCoord(coord, 0.5)

	events := s.setLocalVivaldiCoord(coords.Coord{X: 10.5001, Y: -3.2, Height: 0.001}, 0.5)
	require.Nil(t, events)

	events = s.setLocalVivaldiCoord(coords.Coord{X: 20.0, Y: -3.2, Height: 0.001}, 0.5)
	require.Len(t, events, 1)
}

func TestApplyVivaldiFromPeer(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk := genKey(t)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	peerPK, peerIDStr := peerKey(2)

	s.applyEvents([]*statev1.GossipEvent{{
		PeerId: peerIDStr, Counter: 5,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000}},
	}})
	require.Equal(t, uint64(5), s.testDigest().GetPeers()[peerPK.String()].GetMaxCounter())

	s.applyEvents([]*statev1.GossipEvent{{
		PeerId: peerIDStr, Counter: 10,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000}},
	}})
	require.Equal(t, uint64(10), s.testDigest().GetPeers()[peerPK.String()].GetMaxCounter())
}

// --- Peer hash tests ---

func TestPeerHash(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)

	d1 := s.testDigest()
	d2 := s.testDigest()
	h1 := d1.GetPeers()[s.localID.String()].GetStateHash()
	h2 := d2.GetPeers()[s.localID.String()].GetStateHash()
	require.Equal(t, h1, h2)
	require.NotZero(t, h1)

	s.setObservedAddress("", 52000)
	d3 := s.testDigest()
	h3 := d3.GetPeers()[s.localID.String()].GetStateHash()
	require.NotEqual(t, h1, h3)
}

func TestDigestHashMismatchAtEqualCounterSendsAll(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.setObservedAddress("", 45000)

	digest := s.testDigest()
	localDigest := digest.GetPeers()[s.localID.String()]
	events := s.missingFor(&statev1.Digest{
		Peers: map[string]*statev1.PeerDigest{
			s.localID.String(): {MaxCounter: localDigest.GetMaxCounter(), StateHash: 12345},
		},
	})
	require.Len(t, events, 6)
}

func TestDigestRemoteAheadSendsNothing(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.setObservedAddress("", 45000)

	events := s.missingFor(&statev1.Digest{
		Peers: map[string]*statev1.PeerDigest{
			s.localID.String(): {MaxCounter: 100, StateHash: 12345},
		},
	})
	require.Empty(t, events)
}

func TestDigestMatchSkips(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	s.setLocalNetwork([]string{"10.0.0.1"}, 9000)
	events := s.missingFor(s.testDigest())
	require.Empty(t, events)
}

func TestMissingForPartialDigestIncludesUnknownPeers(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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

	events := s.missingFor(&statev1.Digest{
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

func TestMissingForNilDigestSendsEverything(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	require.Contains(t, peerIDs, s.localID.String())
	require.Contains(t, peerIDs, strA)
	require.Contains(t, peerIDs, strB)
}

// --- Workload tests ---

func TestWorkloadSpecRoundTrip(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	events, err := s.setLocalWorkloadSpec("abc123", 2, 16, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)

	specs := s.Snapshot().Specs
	require.Len(t, specs, 1)
	sv := specs["abc123"]
	require.Equal(t, uint32(2), sv.Spec.GetReplicas())
	require.Equal(t, s.localID, sv.Publisher)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	events := s.setLocalWorkloadClaim("abc123", true)
	require.Len(t, events, 1)
	require.False(t, events[0].GetDeleted())
	require.Contains(t, s.Snapshot().Claims["abc123"], s.localID)

	events2 := s.setLocalWorkloadClaim("abc123", true)
	require.Nil(t, events2)

	rel := s.setLocalWorkloadClaim("abc123", false)
	require.Len(t, rel, 1)
	require.True(t, rel[0].GetDeleted())
	require.Empty(t, s.Snapshot().Claims)
}

func TestWorkloadGossipApply(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	_, remotePeerStr := peerKey(2)
	_, targetPeerStr := peerKey(3)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: remotePeerStr, Counter: 1,
		Change: &statev1.GossipEvent_Reachability{Reachability: &statev1.ReachabilityChange{PeerId: targetPeerStr}},
	})
	require.True(t, result.workloadsDirty)
}

func TestVivaldiChangeDoesNotEmitWorkloadEvent(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	_, remotePeerStr := peerKey(2)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: remotePeerStr, Counter: 1,
		Change: &statev1.GossipEvent_Vivaldi{Vivaldi: &statev1.VivaldiCoordinateChange{X: 1, Y: 2, Height: 0.1}},
	})
	require.False(t, result.workloadsDirty)
}

func TestAllWorkloadSpecs_DuplicatePublishers_LowestPeerKeyWins(t *testing.T) {
	pk, _ := peerKey(5)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk, _ := peerKey(9)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	_, err := s.setLocalWorkloadSpec("mine", 3, 0, 0)
	require.NoError(t, err)

	_, loserStr := peerKey(9)
	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: loserStr, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "mine", Replicas: 5}},
	})

	require.Len(t, result.rebroadcast, 1)
	require.Equal(t, s.localID, s.Snapshot().Specs["mine"].Publisher)
}

func TestRemoteSpec_NoTombstoneWhenRemoteDenied(t *testing.T) {
	pk, _ := peerKey(9)
	s := newTestStore(pk)

	_, err := s.setLocalWorkloadSpec("contested", 3, 0, 0)
	require.NoError(t, err)

	deniedPK, deniedStr := peerKey(1)
	s.denied[deniedPK] = struct{}{}

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: deniedStr, Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "contested", Replicas: 2}},
	})

	require.Len(t, result.rebroadcast, 1)
	require.Equal(t, s.localID, s.Snapshot().Specs["contested"].Publisher)
}

func TestRemoteSpec_NoTombstoneWhenRemoteExpired(t *testing.T) {
	pk, _ := peerKey(9)
	s := newTestStore(pk)

	_, err := s.setLocalWorkloadSpec("contested", 3, 0, 0)
	require.NoError(t, err)

	_, expiredStr := peerKey(1)
	pastExpiry := time.Now().Add(-time.Hour).Unix()
	s.applyEvent(&statev1.GossipEvent{
		PeerId: expiredStr, Counter: 1,
		Change: &statev1.GossipEvent_CertExpiry{CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: pastExpiry}},
	})

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId: expiredStr, Counter: 2,
		Change: &statev1.GossipEvent_WorkloadSpec{WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "contested", Replicas: 2}},
	})

	require.Len(t, result.rebroadcast, 1)
	require.Equal(t, s.localID, s.Snapshot().Specs["contested"].Publisher)
}

func TestWorkloadQueries_ExcludeDeniedNodes(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	require.Contains(t, s.Snapshot().Claims["wl2"], s.localID)
}

func TestAllWorkloadClaims_RackFailureIgnoresStaleObservers(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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
	pk, _ := peerKey(1)
	s := newTestStore(pk)

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

	s := newTestStore(localPK)

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

func TestObservedAddress_TombstoneOnRestart(t *testing.T) {
	pk, _ := peerKey(1)

	// First "lifetime": node sets an observed address and captures its full state.
	s1 := newTestStore(pk)
	s1.setObservedAddress("52.204.52.130", 45678)
	staleState := s1.EncodeFull()

	// Simulate restart: fresh store for the same peer key.
	s2 := newTestStore(pk)
	require.Equal(t, uint32(0), s2.Snapshot().Nodes[s2.localID].ExternalPort)

	// A peer gossips the pre-restart state back. handleSelfConflictLocked
	// detects the higher counter, bumps all local log entries (including
	// tombstones), and rebroadcasts. Self-events for ObservedAddress are NOT
	// applied to the local record, so the value must stay zero.
	_, _, err := s2.ApplyDelta(s2.localID, staleState)
	require.NoError(t, err)
	require.Equal(t, uint32(0), s2.Snapshot().Nodes[s2.localID].ExternalPort,
		"stale ObservedAddress must not survive a restart")

	// The tombstone must now have a counter higher than the stale value,
	// ensuring peers also clear the stale attribute.
	entry := s2.nodes[s2.localID].log[attrKey{kind: attrObservedAddress}]
	require.True(t, entry.Deleted)
}

func TestReachability_TombstoneOnRestart(t *testing.T) {
	pk, _ := peerKey(1)

	s1 := newTestStore(pk)
	targetPK, _ := peerKey(2)
	s1.setLocalConnected(targetPK, true)
	require.Contains(t, s1.Snapshot().Nodes[s1.localID].Reachable, targetPK)

	staleState := s1.EncodeFull()

	// Simulate restart: fresh store has no reachability.
	s2 := newTestStore(pk)
	require.Empty(t, s2.Snapshot().Nodes[s2.localID].Reachable)

	// Peer gossips pre-restart state back. handleSelfConflictLocked bumps
	// counters so the tombstone from tombstoneStaleAttrs wins.
	_, _, err := s2.ApplyDelta(s2.localID, staleState)
	require.NoError(t, err)
	require.Empty(t, s2.Snapshot().Nodes[s2.localID].Reachable,
		"stale reachability must not survive a restart")

	key := attrKey{kind: attrReachability, peer: targetPK}
	entry, ok := s2.nodes[s2.localID].log[key]
	require.True(t, ok, "tombstone entry must exist for stale reachability")
	require.True(t, entry.Deleted)
}

func TestTrafficHeatmap_TombstoneOnRestart(t *testing.T) {
	pk, _ := peerKey(3)
	s := newTestStore(pk)

	targetPK, _ := peerKey(4)
	events := s.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{
		{4}: {BytesIn: 500, BytesOut: 600},
	})
	require.Len(t, events, 1)

	localID := pk
	require.Equal(t, TrafficSnapshot{BytesIn: 500, BytesOut: 600}, s.Snapshot().Heatmaps[localID][targetPK])

	local := s.nodes[localID]
	tombstoneStaleAttrs(&local)
	s.nodes[localID] = local

	entry := local.log[attrKey{kind: attrTrafficHeatmap}]
	require.True(t, entry.Deleted)
}

func TestTrafficHeatmap_ZeroSnapshotSkipped(t *testing.T) {
	pk, _ := peerKey(5)
	s := newTestStore(pk)

	events := s.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{})
	require.Empty(t, events)

	events = s.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{
		{6}: {BytesIn: 0, BytesOut: 0},
	})
	require.Empty(t, events)
}

func TestTrafficHeatmap_StaleClearing(t *testing.T) {
	pk, _ := peerKey(7)
	s := newTestStore(pk)
	localID := pk

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
	pkA, _ := peerKey(1)
	storeA := newTestStore(pkA)

	pkB, _ := peerKey(2)
	storeB := newTestStore(pkB)

	peerC, _ := peerKey(3)

	// Set every attribute type on store A.
	storeA.setLocalNetwork([]string{"10.0.0.1", "192.168.1.1"}, 9000)
	storeA.setObservedAddress("52.204.52.130", 45000)
	storeA.upsertLocalService(8080, "http")
	storeA.setLocalConnected(peerC, true)
	storeA.setLocalPubliclyAccessible(true)
	storeA.setLocalVivaldiCoord(coords.Coord{X: 10.5, Y: -3.2, Height: 0.001}, 0.5)
	storeA.setLocalNatType(nat.Easy)
	storeA.setLocalResourceTelemetry(42, 75, 1<<30, 4)
	_, err := storeA.setLocalWorkloadSpec("abc123", 2, 16, 0)
	require.NoError(t, err)
	storeA.setLocalWorkloadClaim("abc123", true)
	storeA.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{
		peerC: {BytesIn: 500, BytesOut: 600},
	})

	// Transfer A's full state to B.
	fullState := storeA.EncodeFull()
	_, _, err2 := storeB.ApplyDelta(storeA.localID, fullState)
	require.NoError(t, err2)

	// Make A reachable from B so claims are in B's live component.
	storeB.setLocalConnected(storeA.localID, true)

	snap := storeB.Snapshot()
	nodeA := snap.Nodes[storeA.localID]

	// Network
	require.Equal(t, []string{"10.0.0.1", "192.168.1.1"}, nodeA.IPs)
	require.Equal(t, uint32(9000), nodeA.LocalPort)

	// ExternalPort
	require.Equal(t, uint32(45000), nodeA.ExternalPort)

	// ObservedExternalIP
	require.Equal(t, "52.204.52.130", nodeA.ObservedExternalIP)

	// Identity (set at construction)
	require.Equal(t, pkA[:], nodeA.PeerPub)

	// Service
	require.Contains(t, nodeA.Services, "http")
	require.Equal(t, uint32(8080), nodeA.Services["http"].Port)

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
	require.Equal(t, storeA.localID, snap.Specs["abc123"].Publisher)
	require.Equal(t, uint32(2), snap.Specs["abc123"].Spec.GetReplicas())

	// WorkloadClaim (A is in B's live component via setLocalConnected)
	require.Contains(t, snap.Claims, "abc123")
	require.Contains(t, snap.Claims["abc123"], storeA.localID)

	// TrafficHeatmap
	require.Contains(t, snap.Heatmaps, storeA.localID)
	require.Equal(t, TrafficSnapshot{BytesIn: 500, BytesOut: 600}, snap.Heatmaps[storeA.localID][peerC])
}

func TestGossipRoundTrip_DenyAttribute(t *testing.T) {
	pkA, _ := peerKey(1)
	storeA := newTestStore(pkA)

	pkB, _ := peerKey(2)
	storeB := newTestStore(pkB)

	subjectPK, _ := peerKey(9)
	storeA.denyPeerRaw(subjectPK[:])

	fullState := storeA.EncodeFull()
	_, _, err := storeB.ApplyDelta(storeA.localID, fullState)
	require.NoError(t, err)

	require.Contains(t, storeB.Snapshot().DeniedPeers(), subjectPK)
}

func TestGossipRoundTrip_TrafficHeatmap_MultiplePeers(t *testing.T) {
	pkA, _ := peerKey(1)
	storeA := newTestStore(pkA)

	pkB, _ := peerKey(2)
	storeB := newTestStore(pkB)

	peerC, _ := peerKey(3)
	peerD, _ := peerKey(4)

	storeA.setLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{
		peerC: {BytesIn: 100, BytesOut: 200},
		peerD: {BytesIn: 300, BytesOut: 400},
	})

	fullState := storeA.EncodeFull()
	_, _, err := storeB.ApplyDelta(storeA.localID, fullState)
	require.NoError(t, err)

	heatmap := storeB.Snapshot().Heatmaps[storeA.localID]
	require.Len(t, heatmap, 2)
	require.Equal(t, TrafficSnapshot{BytesIn: 100, BytesOut: 200}, heatmap[peerC])
	require.Equal(t, TrafficSnapshot{BytesIn: 300, BytesOut: 400}, heatmap[peerD])
}

// TestSetLocalTraffic_MultiplePeers_Retained verifies that calling SetLocalTraffic
// for multiple peers retains all peers' traffic in the heatmap. This exercises the
// same calling pattern as tunneling.Service.sampleTraffic().
func TestSetLocalTraffic_MultiplePeers_Retained(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	peerA, _ := peerKey(2)
	peerB, _ := peerKey(3)
	peerC, _ := peerKey(4)

	s.SetLocalTraffic(peerA, 100, 200)
	s.SetLocalTraffic(peerB, 300, 400)
	s.SetLocalTraffic(peerC, 500, 600)

	snap := s.Snapshot()
	heatmap := snap.Heatmaps[s.localID]
	require.Len(t, heatmap, 3, "all three peers' traffic must be retained")
	require.Equal(t, TrafficSnapshot{BytesIn: 100, BytesOut: 200}, heatmap[peerA])
	require.Equal(t, TrafficSnapshot{BytesIn: 300, BytesOut: 400}, heatmap[peerB])
	require.Equal(t, TrafficSnapshot{BytesIn: 500, BytesOut: 600}, heatmap[peerC])
}

// TestSetLocalTraffic_MultiplePeers_GossipRoundTrip verifies that per-peer
// SetLocalTraffic calls produce gossip events that, when applied on a remote
// store, reconstruct the complete multi-peer heatmap.
func TestSetLocalTraffic_MultiplePeers_GossipRoundTrip(t *testing.T) {
	pkA, _ := peerKey(1)
	storeA := newTestStore(pkA)

	pkB, _ := peerKey(2)
	storeB := newTestStore(pkB)

	peerC, _ := peerKey(3)
	peerD, _ := peerKey(4)

	storeA.SetLocalTraffic(peerC, 100, 200)
	storeA.SetLocalTraffic(peerD, 300, 400)

	fullState := storeA.EncodeFull()
	_, _, err := storeB.ApplyDelta(storeA.localID, fullState)
	require.NoError(t, err)

	heatmap := storeB.Snapshot().Heatmaps[storeA.localID]
	require.Len(t, heatmap, 2, "remote must see both peers' traffic")
	require.Equal(t, TrafficSnapshot{BytesIn: 100, BytesOut: 200}, heatmap[peerC])
	require.Equal(t, TrafficSnapshot{BytesIn: 300, BytesOut: 400}, heatmap[peerD])
}

func TestExportLoadLastAddrs(t *testing.T) {
	pk, _ := peerKey(1)
	s := newTestStore(pk)

	peer2, peer2Str := peerKey(2)
	peer3, peer3Str := peerKey(3)

	// Register both remote peers via gossip events.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peer2Str,
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9002},
		},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peer3Str,
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.3"}, LocalPort: 9003},
		},
	})

	s.SetPeerLastAddr(peer2, "10.0.0.2:9002")
	s.SetPeerLastAddr(peer3, "10.0.0.3:9003")

	addrs := s.ExportLastAddrs()
	require.Len(t, addrs, 2)
	require.Equal(t, "10.0.0.2:9002", addrs[peer2])
	require.Equal(t, "10.0.0.3:9003", addrs[peer3])

	// Create a fresh store and restore gossip state + last addresses.
	freshPK, _ := peerKey(1)
	s2 := newTestStore(freshPK)
	_, _, err := s2.ApplyDelta(s.localID, s.EncodeFull())
	require.NoError(t, err)

	s2.LoadLastAddrs(addrs)

	snap := s2.Snapshot()
	require.Equal(t, "10.0.0.2:9002", snap.Nodes[peer2].LastAddr)
	require.Equal(t, "10.0.0.3:9003", snap.Nodes[peer3].LastAddr)
}
