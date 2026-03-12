package store

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func newTestStore(pub []byte) *Store {
	localID := types.PeerKeyFromBytes(pub)
	s := &Store{
		LocalID: localID,
		nodes: map[types.PeerKey]nodeRecord{
			localID: {
				maxCounter:     1,
				IdentityPub:    append([]byte(nil), pub...),
				Reachable:      make(map[types.PeerKey]struct{}),
				Services:       make(map[string]*statev1.Service),
				WorkloadSpecs:  make(map[string]*statev1.WorkloadSpecChange),
				WorkloadClaims: make(map[string]struct{}),
				log: map[attrKey]logEntry{
					identityAttrKey(): {Counter: 1},
				},
			},
		},
		denied:             make(map[types.PeerKey]struct{}),
		desiredConnections: make(map[string]Connection),
		metrics:            metrics.NewGossipMetrics(nil),
	}
	local := s.nodes[localID]
	tombstoneStaleAttrs(&local)
	s.nodes[localID] = local
	return s
}

// applyEvent is a test convenience for applying a single gossip event.
func (s *Store) applyEvent(event *statev1.GossipEvent) ApplyResult {
	return s.ApplyEvents([]*statev1.GossipEvent{event}, true)
}

func peerKey(b byte) (types.PeerKey, string) {
	pub := make([]byte, 32)
	pub[0] = b
	pk := types.PeerKeyFromBytes(pub)
	return pk, pk.String()
}

func TestEagerSyncClock(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	// Fresh store with only local state → empty digest.
	digest := s.EagerSyncClock()
	require.Empty(t, digest.GetPeers())

	// Apply a remote event → real digest.
	_, peerIDStr := peerKey(2)
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 5,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})

	digest = s.EagerSyncClock()
	peers := digest.GetPeers()
	require.Len(t, peers, 2)
	require.Equal(t, uint64(6), peers[s.LocalID.String()].GetMaxCounter())
	require.NotZero(t, peers[s.LocalID.String()].GetStateHash())
	peerPK, _ := peerKey(2)
	require.Equal(t, uint64(5), peers[peerPK.String()].GetMaxCounter())
	require.NotZero(t, peers[peerPK.String()].GetStateHash())
}

func TestSetLocalNetworkReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	require.Len(t, events, 1)

	ev := events[0]
	require.Equal(t, uint64(7), ev.GetCounter())
	network := ev.GetNetwork()
	require.NotNil(t, network)
	require.Equal(t, []string{"10.0.0.1"}, network.GetIps())
	require.Equal(t, uint32(9000), network.GetLocalPort())
}

func TestSetLocalNetworkNoOpWhenUnchanged(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	events := s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	if events != nil {
		t.Fatal("expected nil for no-op, got events")
	}
}

func TestSetExternalPortReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.SetExternalPort(45000)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].GetExternalPort().GetExternalPort() != 45000 {
		t.Fatalf("expected port 45000, got %d", events[0].GetExternalPort().GetExternalPort())
	}
}

func TestSetObservedExternalIPReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.SetObservedExternalIP("52.204.52.130")
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

	events := s.SetLocalConnected(peerID, true)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if ev.GetDeleted() {
		t.Fatal("expected deleted=false for connect")
	}
	if ev.GetReachability().GetPeerId() != peerID.String() {
		t.Fatalf("expected reachability peer %q, got %q", peerID.String(), ev.GetReachability().GetPeerId())
	}
}

func TestSetLocalConnectedDisconnect(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPub := make([]byte, 32)
	peerPub[0] = 2
	peerID := types.PeerKeyFromBytes(peerPub)

	// First connect, then disconnect.
	s.SetLocalConnected(peerID, true)
	events := s.SetLocalConnected(peerID, false)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if !ev.GetDeleted() {
		t.Fatal("expected deleted=true for disconnect")
	}
}

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

	rec, ok := s.Get(peerPK)
	if !ok {
		t.Fatal("expected peer record to exist")
	}
	if rec.ExternalPort != 45000 {
		t.Fatalf("expected ExternalPort=45000, got %d", rec.ExternalPort)
	}
	if rec.maxCounter != 3 {
		t.Fatalf("expected MaxCounter=3, got %d", rec.maxCounter)
	}
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

	rec, ok := s.Get(peerPK)
	require.True(t, ok)
	require.Equal(t, "52.204.52.130", rec.ObservedExternalIP)
	require.Equal(t, uint64(3), rec.maxCounter)
}

func TestApplyEventPerKeyCounter(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	// Apply event at counter=3.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 3,
		Change: &statev1.GossipEvent_ExternalPort{
			ExternalPort: &statev1.ExternalPortChange{ExternalPort: 45000},
		},
	})

	// Try to apply older event at counter=2 for same key.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Change: &statev1.GossipEvent_ExternalPort{
			ExternalPort: &statev1.ExternalPortChange{ExternalPort: 44000},
		},
	})

	peerPK, _ := peerKey(2)
	rec, _ := s.Get(peerPK)
	if rec.ExternalPort != 45000 {
		t.Fatalf("expected ExternalPort=45000 (not overwritten), got %d", rec.ExternalPort)
	}
}

func TestApplyEventDifferentKeys(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	// Apply ext at counter=5.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 5,
		Change: &statev1.GossipEvent_ExternalPort{
			ExternalPort: &statev1.ExternalPortChange{ExternalPort: 45000},
		},
	})

	// Apply net at counter=2 (lower counter, different key — should still apply).
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})

	peerPK, _ := peerKey(2)
	rec, _ := s.Get(peerPK)
	if rec.ExternalPort != 45000 {
		t.Fatalf("expected ExternalPort=45000, got %d", rec.ExternalPort)
	}
	if len(rec.IPs) != 1 || rec.IPs[0] != "10.0.0.1" {
		t.Fatalf("expected IPs=[10.0.0.1], got %v", rec.IPs)
	}
}

func TestApplyEventDeletion(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	// Add a service.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: "http", Port: 8080},
		},
	})

	// Delete it.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Deleted: true,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: "http"},
		},
	})

	peerPK, _ := peerKey(2)
	rec, _ := s.Get(peerPK)
	if _, ok := rec.Services["http"]; ok {
		t.Fatal("service 'http' should have been deleted")
	}
}

func TestApplyEventTombstonePreventResurrection(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	// Delete at counter=5.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 5,
		Deleted: true,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: "http"},
		},
	})

	// Try to resurrect with stale event at counter=4.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 4,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: "http", Port: 8080},
		},
	})

	peerPK, _ := peerKey(2)
	rec, _ := s.Get(peerPK)
	if _, ok := rec.Services["http"]; ok {
		t.Fatal("service 'http' should remain deleted")
	}
}

func TestApplyEventSelfStateConflict(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)
	localID := s.LocalID

	// Set some local state.
	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)

	// Simulate receiving our own event with higher counter (from a peer).
	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  localID.String(),
		Counter: 10,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	})

	if len(result.Rebroadcast) == 0 {
		t.Fatal("self-state conflict should trigger rebroadcast")
	}

	rec := s.nodes[localID]
	// Adopted 10, then bumped once per attribute: net(11) + id(12) + pa(13) + nat(14) + observed_external_ip(15) + resource_telemetry(16) + traffic_heatmap(17).
	require.Equal(t, uint64(17), rec.maxCounter)
}

func TestApplyEventSelfStateNoConflict(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)

	// Receiving own event with same or lower counter — no conflict.
	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  s.LocalID.String(),
		Counter: 2,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})

	if len(result.Rebroadcast) > 0 {
		t.Fatal("should not rebroadcast when counter is not higher")
	}
}

func TestMissingForReturnsEvents(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.SetExternalPort(45000)

	// Remote knows about us but with mismatched hash → full dump.
	events := s.MissingFor(&statev1.GossipStateDigest{
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

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.SetExternalPort(45000)

	// Get real digest, then lower MaxCounter to test delta path
	// (hash match + counter behind).
	digest := s.Clock()
	localDigest := digest.GetPeers()[s.LocalID.String()]
	events := s.MissingFor(&statev1.GossipStateDigest{
		Peers: map[string]*statev1.PeerDigest{
			s.LocalID.String(): {MaxCounter: 6, StateHash: localDigest.GetStateHash()},
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

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)

	// Use real digest — hash + counter match → skip.
	events := s.MissingFor(s.Clock())

	require.Empty(t, events)
}

func TestClockUsesMaxCounter(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.SetExternalPort(45000)

	digest := s.Clock()
	require.Equal(t, uint64(8), digest.GetPeers()[s.LocalID.String()].GetMaxCounter())
}

func TestUpsertLocalServiceReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.UpsertLocalService(8080, "http")
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	sc := ev.GetService()
	if sc == nil || sc.GetName() != "http" || sc.GetPort() != 8080 {
		t.Fatalf("unexpected service value: %v", sc)
	}
}

func TestRemoveLocalServicesReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.UpsertLocalService(8080, "http")
	events := s.RemoveLocalServices("http")
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if !ev.GetDeleted() {
		t.Fatal("expected deleted=true on event")
	}
	sc := ev.GetService()
	if sc == nil || sc.GetName() != "http" {
		t.Fatalf("expected service change for http, got %v", sc)
	}
}

func TestRemoveLocalServicesNoOpWhenMissing(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.RemoveLocalServices("http")
	if events != nil {
		t.Fatal("expected nil for removing non-existent service")
	}
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

	rec, ok := s.Get(peerPK)
	if !ok {
		t.Fatal("expected peer to exist")
	}
	if len(rec.IPs) != 2 || rec.IPs[0] != "10.0.0.5" {
		t.Fatalf("unexpected IPs: %v", rec.IPs)
	}
	if rec.LocalPort != 7000 {
		t.Fatalf("expected LocalPort=7000, got %d", rec.LocalPort)
	}
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

	rec, _ := s.Get(peerPK)
	if len(rec.IdentityPub) == 0 {
		t.Fatal("expected IdentityPub to be set")
	}
	if rec.IdentityPub[0] != 2 {
		t.Fatalf("unexpected IdentityPub: %v", rec.IdentityPub)
	}
}

func TestApplyEventReachablePeer(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)
	targetPK, _ := peerKey(3)

	// Connect.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Reachability{
			Reachability: &statev1.ReachabilityChange{PeerId: targetPK.String()},
		},
	})

	rec, _ := s.Get(peerPK)
	if _, ok := rec.Reachable[targetPK]; !ok {
		t.Fatal("expected target to be reachable")
	}

	// Disconnect (tombstone).
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Deleted: true,
		Change: &statev1.GossipEvent_Reachability{
			Reachability: &statev1.ReachabilityChange{PeerId: targetPK.String()},
		},
	})

	rec, _ = s.Get(peerPK)
	if _, ok := rec.Reachable[targetPK]; ok {
		t.Fatal("expected target to be removed from reachable")
	}
}

func TestSetLastAddr(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)

	// Add the peer via a gossip event so it exists in the store.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})

	s.SetLastAddr(peerPK, "203.0.113.5:41234")

	peers := s.KnownPeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 known peer, got %d", len(peers))
	}
	if peers[0].LastAddr != "203.0.113.5:41234" {
		t.Fatalf("expected LastAddr=203.0.113.5:41234, got %q", peers[0].LastAddr)
	}
}

func TestSetLastAddrIgnoresUnknownPeer(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	unknownPK, _ := peerKey(99)
	s.SetLastAddr(unknownPK, "1.2.3.4:5678")

	peers := s.KnownPeers()
	if len(peers) != 0 {
		t.Fatalf("expected 0 known peers, got %d", len(peers))
	}
}

func TestKnownPeersIncludesPeerWithLastAddrOnly(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)
	idPub := make([]byte, 32)
	idPub[0] = 2

	// Create peer record without network fields.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_IdentityPub{
			IdentityPub: &statev1.IdentityChange{IdentityPub: idPub},
		},
	})

	s.SetLastAddr(peerPK, "203.0.113.5:41234")

	peers := s.KnownPeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 known peer, got %d", len(peers))
	}
	if peers[0].PeerID != peerPK {
		t.Fatalf("expected peer %q, got %q", peerPK.String(), peers[0].PeerID.String())
	}
	if peers[0].LastAddr != "203.0.113.5:41234" {
		t.Fatalf("expected LastAddr=203.0.113.5:41234, got %q", peers[0].LastAddr)
	}
}

func TestSaveLoadPersistsLastAddr(t *testing.T) {
	dir := t.TempDir()

	localPub := make([]byte, 32)
	localPub[0] = 1

	s, err := Load(dir, localPub)
	if err != nil {
		t.Fatalf("load store: %v", err)
	}

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

	s.SetLastAddr(peerPK, "203.0.113.5:41234")

	if err := s.Save(); err != nil {
		t.Fatalf("save store: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	s2, err := Load(dir, localPub)
	if err != nil {
		t.Fatalf("reload store: %v", err)
	}
	defer func() {
		if err := s2.Close(); err != nil {
			t.Fatalf("close reloaded store: %v", err)
		}
	}()

	rec, ok := s2.Get(peerPK)
	if !ok {
		t.Fatal("expected peer record after reload")
	}
	if rec.LastAddr != "203.0.113.5:41234" {
		t.Fatalf("expected LastAddr=203.0.113.5:41234, got %q", rec.LastAddr)
	}

	peers := s2.KnownPeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 known peer, got %d", len(peers))
	}
	if peers[0].LastAddr != "203.0.113.5:41234" {
		t.Fatalf("expected LastAddr=203.0.113.5:41234, got %q", peers[0].LastAddr)
	}
}

func TestSaveLoadDoesNotPersistServices(t *testing.T) {
	dir := t.TempDir()

	localPub := make([]byte, 32)
	localPub[0] = 1

	s, err := Load(dir, localPub)
	require.NoError(t, err)

	_, peerIDStr := peerKey(2)
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.5"}, LocalPort: 7000},
		},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Change: &statev1.GossipEvent_Service{
			Service: &statev1.ServiceChange{Name: "http", Port: 8080},
		},
	})

	require.NoError(t, s.Save())
	require.NoError(t, s.Close())

	s2, err := Load(dir, localPub)
	require.NoError(t, err)
	defer func() { require.NoError(t, s2.Close()) }()

	peerPK, _ := peerKey(2)
	rec, ok := s2.Get(peerPK)
	require.True(t, ok)
	require.Empty(t, rec.Services, "services should not survive save/load")
}

func TestSetLocalPubliclyAccessibleReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.SetLocalPubliclyAccessible(true)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if ev.GetDeleted() {
		t.Fatal("expected deleted=false for setting accessible")
	}
	if ev.GetPubliclyAccessible() == nil {
		t.Fatal("expected publicly_accessible change")
	}
}

func TestSetLocalPubliclyAccessibleNoOpWhenUnchanged(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalPubliclyAccessible(true)
	events := s.SetLocalPubliclyAccessible(true)
	if events != nil {
		t.Fatal("expected nil for no-op, got events")
	}
}

func TestSetLocalPubliclyAccessibleClear(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalPubliclyAccessible(true)
	events := s.SetLocalPubliclyAccessible(false)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if !ev.GetDeleted() {
		t.Fatal("expected deleted=true for clearing accessible")
	}

	if s.IsPubliclyAccessible(s.LocalID) {
		t.Fatal("expected PubliclyAccessible=false after clearing")
	}
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

	if !s.IsPubliclyAccessible(peerPK) {
		t.Fatal("expected peer to be publicly accessible")
	}

	// Delete it.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Deleted: true,
		Change: &statev1.GossipEvent_PubliclyAccessible{
			PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
		},
	})

	if s.IsPubliclyAccessible(peerPK) {
		t.Fatal("expected peer to no longer be publicly accessible")
	}
}

func TestPubliclyAccessibleRoundTrip(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.SetLocalPubliclyAccessible(true)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	missing := s.MissingFor(&statev1.GossipStateDigest{
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
	if !found {
		t.Fatal("expected publicly_accessible event in MissingFor output")
	}
}

func TestPubliclyAccessibleConflictRecovery(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalPubliclyAccessible(true)

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  s.LocalID.String(),
		Counter: 100,
		Change: &statev1.GossipEvent_PubliclyAccessible{
			PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
		},
	})

	if len(result.Rebroadcast) == 0 {
		t.Fatal("self-state conflict should trigger rebroadcast")
	}

	var found bool
	for _, ev := range result.Rebroadcast {
		if ev.GetPubliclyAccessible() != nil {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("rebroadcast should include publicly_accessible event")
	}
}

func TestFreshStoreGossipsPubliclyAccessibleDeletion(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	s, err := Load(t.TempDir(), pub)
	require.NoError(t, err)
	defer s.Close()

	// A peer that knows nothing about us (hash mismatch) should receive
	// the seeded PubliclyAccessible deletion event on the first sync.
	missing := s.MissingFor(&statev1.GossipStateDigest{
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
	require.True(t, found, "fresh store should gossip a PubliclyAccessible deletion to peers with stale state")
}

func TestDenyPeerAndIsDenied(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	subjectPub := make([]byte, 32)
	subjectPub[0] = 0x42

	require.False(t, s.IsDenied(subjectPub))

	events := s.DenyPeer(subjectPub)
	require.Len(t, events, 1)

	require.True(t, s.IsDenied(subjectPub))

	// Duplicate deny is a no-op
	events = s.DenyPeer(subjectPub)
	require.Nil(t, events)
}

func TestKnownPeersExcludesDenied(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPub := make([]byte, 32)
	peerPub[0] = 0x02
	peerKey := types.PeerKeyFromBytes(peerPub)

	s.ApplyEvents([]*statev1.GossipEvent{
		{PeerId: peerKey.String(), Counter: 1, Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 1234}}},
		{PeerId: peerKey.String(), Counter: 2, Change: &statev1.GossipEvent_IdentityPub{IdentityPub: &statev1.IdentityChange{IdentityPub: peerPub, CertExpiryUnix: time.Now().Add(time.Hour).Unix()}}},
	}, false)

	require.Len(t, s.KnownPeers(), 1)

	s.DenyPeer(peerPub)
	require.Len(t, s.KnownPeers(), 0)
}

func TestKnownPeersExcludesExpired(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)
	_, peer3IDStr := peerKey(3)
	_, peer4IDStr := peerKey(4)

	// Peer with past CertExpiry — should be excluded.
	pastExpiry := time.Now().Add(-2 * time.Hour).Unix()
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Change: &statev1.GossipEvent_IdentityPub{
			IdentityPub: &statev1.IdentityChange{
				IdentityPub:    make([]byte, 32),
				CertExpiryUnix: pastExpiry,
			},
		},
	})

	// Peer with future CertExpiry — should be included.
	futureExpiry := time.Now().Add(24 * time.Hour).Unix()
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peer3IDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peer3IDStr,
		Counter: 2,
		Change: &statev1.GossipEvent_IdentityPub{
			IdentityPub: &statev1.IdentityChange{
				IdentityPub:    make([]byte, 32),
				CertExpiryUnix: futureExpiry,
			},
		},
	})

	// Peer with CertExpiry == 0 (legacy) — should be included.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peer4IDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.3"}, LocalPort: 9002},
		},
	})

	peers := s.KnownPeers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 known peers (future + legacy), got %d", len(peers))
	}

	peerIDs := make(map[types.PeerKey]bool)
	for _, p := range peers {
		peerIDs[p.PeerID] = true
	}

	pk3, _ := peerKey(3)
	pk4, _ := peerKey(4)
	if !peerIDs[pk3] {
		t.Fatal("expected peer with future expiry to be included")
	}
	if !peerIDs[pk4] {
		t.Fatal("expected peer with zero expiry (legacy) to be included")
	}
}

func TestSetLocalVivaldiCoordReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	coord := topology.Coord{X: 10.5, Y: -3.2, Height: 0.001}
	events := s.SetLocalVivaldiCoord(coord)
	require.Len(t, events, 1)

	ev := events[0]
	require.False(t, ev.GetDeleted())
	v := ev.GetVivaldi()
	require.NotNil(t, v)
	require.Equal(t, 10.5, v.GetX())
	require.Equal(t, -3.2, v.GetY())
	require.Equal(t, 0.001, v.GetHeight())
}

func TestSetLocalVivaldiCoordEpsilonSuppression(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	coord := topology.Coord{X: 10.5, Y: -3.2, Height: 0.001}
	s.SetLocalVivaldiCoord(coord)

	// Tiny change within epsilon — should be suppressed.
	events := s.SetLocalVivaldiCoord(topology.Coord{X: 10.5001, Y: -3.2, Height: 0.001})
	require.Nil(t, events)

	// Large change beyond epsilon — should publish.
	events = s.SetLocalVivaldiCoord(topology.Coord{X: 20.0, Y: -3.2, Height: 0.001})
	require.Len(t, events, 1)
}

func TestSetLocalVivaldiCoordUnchangedSuppressedAtHighHeight(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	coord := topology.Coord{X: 1.0, Y: 2.0, Height: 3.0}
	s.SetLocalVivaldiCoord(coord)

	events := s.SetLocalVivaldiCoord(coord)
	require.Nil(t, events)
}

func TestFirstSetLocalVivaldiCoordAlwaysPublishes(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	coord := topology.Coord{X: 0.1, Height: 0.2}
	events := s.SetLocalVivaldiCoord(coord)
	require.Len(t, events, 1)
	require.Equal(t, coord.X, events[0].GetVivaldi().GetX())
	require.Equal(t, coord.Height, events[0].GetVivaldi().GetHeight())
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

	coord, ok := s.PeerVivaldiCoord(peerPK)
	require.True(t, ok)
	require.Equal(t, 5.0, coord.X)
	require.Equal(t, 7.0, coord.Y)
	require.Equal(t, 0.1, coord.Height)
}

func TestApplyVivaldiDeletionClearsToNil(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Vivaldi{
			Vivaldi: &statev1.VivaldiCoordinateChange{X: 5.0, Y: 7.0, Height: 0.1},
		},
	})

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Deleted: true,
		Change: &statev1.GossipEvent_Vivaldi{
			Vivaldi: &statev1.VivaldiCoordinateChange{},
		},
	})

	peerPK, _ := peerKey(2)
	coord, ok := s.PeerVivaldiCoord(peerPK)
	require.False(t, ok)
	require.Nil(t, coord)
}

func TestVivaldiRoundTrip(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalVivaldiCoord(topology.Coord{X: 10.5, Y: -3.2, Height: 0.001})

	missing := s.MissingFor(&statev1.GossipStateDigest{
		Peers: map[string]*statev1.PeerDigest{
			s.LocalID.String(): {MaxCounter: 0, StateHash: 0},
		},
	})

	var found bool
	for _, ev := range missing {
		if ev.GetVivaldi() != nil && !ev.GetDeleted() {
			found = true
			break
		}
	}
	require.True(t, found, "expected vivaldi event in MissingFor output")
}

func TestVivaldiConflictRecovery(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalVivaldiCoord(topology.Coord{X: 10.5, Y: -3.2, Height: 0.001})

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  s.LocalID.String(),
		Counter: 100,
		Change: &statev1.GossipEvent_Vivaldi{
			Vivaldi: &statev1.VivaldiCoordinateChange{X: 1, Y: 2, Height: 3},
		},
	})

	require.NotEmpty(t, result.Rebroadcast)

	var found bool
	for _, ev := range result.Rebroadcast {
		if ev.GetVivaldi() != nil {
			found = true
			break
		}
	}
	require.True(t, found, "rebroadcast should include vivaldi event")
}

func TestFreshStoreDoesNotGossipVivaldi(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	s, err := Load(t.TempDir(), pub)
	require.NoError(t, err)
	defer s.Close()

	missing := s.MissingFor(&statev1.GossipStateDigest{
		Peers: map[string]*statev1.PeerDigest{
			s.LocalID.String(): {MaxCounter: 0, StateHash: 0},
		},
	})

	var found bool
	for _, ev := range missing {
		if ev.GetVivaldi() != nil {
			found = true
			break
		}
	}
	require.False(t, found, "fresh store should not gossip vivaldi before node startup publish")
}

func TestMissingForPartialClockIncludesUnknownPeers(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	pkA, strA := peerKey(2)
	pkB, strB := peerKey(3)
	pkC, strC := peerKey(4)

	// Populate three remote peers with network events.
	for _, p := range []struct {
		str string
		ip  string
	}{
		{strA, "10.0.0.1"},
		{strB, "10.0.0.2"},
		{strC, "10.0.0.3"},
	} {
		s.applyEvent(&statev1.GossipEvent{
			PeerId:  p.str,
			Counter: 1,
			Change: &statev1.GossipEvent_Network{
				Network: &statev1.NetworkChange{Ips: []string{p.ip}, LocalPort: 9000},
			},
		})
	}

	// Digest that only mentions A with mismatched hash — B and C are
	// "unknown" to the sender. MissingFor must still return events for
	// B and C so the sender can discover quiet peers it has never seen.
	events := s.MissingFor(&statev1.GossipStateDigest{
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
			PeerId:  p,
			Counter: 1,
			Change: &statev1.GossipEvent_Network{
				Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
			},
		})
	}

	events := s.MissingFor(nil)

	// Should include events from local, peerA, and peerB.
	peerIDs := make(map[string]struct{})
	for _, ev := range events {
		peerIDs[ev.GetPeerId()] = struct{}{}
	}
	require.Contains(t, peerIDs, s.LocalID.String())
	require.Contains(t, peerIDs, strA)
	require.Contains(t, peerIDs, strB)
}

func TestKnownPeersIncludesVivaldiCoord(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Change: &statev1.GossipEvent_Vivaldi{
			Vivaldi: &statev1.VivaldiCoordinateChange{X: 5.0, Y: 7.0, Height: 0.1},
		},
	})

	peers := s.KnownPeers()
	require.Len(t, peers, 1)
	require.NotNil(t, peers[0].VivaldiCoord)
	require.Equal(t, 5.0, peers[0].VivaldiCoord.X)
}

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
	require.Len(t, result.Rebroadcast, 1)
	require.Equal(t, peerIDStr, result.Rebroadcast[0].GetPeerId())
}

func TestApplyStaleRemoteEventNoRebroadcast(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	})
	require.Empty(t, result.Rebroadcast)
}

func TestSaveLoadPersistsPubliclyAccessible(t *testing.T) {
	dir := t.TempDir()

	localPub := make([]byte, 32)
	localPub[0] = 1

	s, err := Load(dir, localPub)
	require.NoError(t, err)

	_, peerIDStr := peerKey(2)
	idPub := make([]byte, 32)
	idPub[0] = 2

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Change: &statev1.GossipEvent_IdentityPub{
			IdentityPub: &statev1.IdentityChange{IdentityPub: idPub},
		},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Change: &statev1.GossipEvent_PubliclyAccessible{
			PubliclyAccessible: &statev1.PubliclyAccessibleChange{},
		},
	})

	require.NoError(t, s.Save())
	require.NoError(t, s.Close())

	s2, err := Load(dir, localPub)
	require.NoError(t, err)
	defer func() { require.NoError(t, s2.Close()) }()

	peerPK, _ := peerKey(2)
	require.True(t, s2.IsPubliclyAccessible(peerPK))
}

func TestStaleRatioBatchLevel(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	s := newTestStore(pub)
	gm := metrics.NewGossipMetrics(nil)
	s.SetGossipMetrics(gm)

	_, peerStr := peerKey(2)

	// Push events don't update EWMA.
	s.ApplyEvents([]*statev1.GossipEvent{{
		PeerId:  peerStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	}}, false)
	require.Equal(t, 0.0, gm.StaleRatio.Value(), "EWMA should not move on push events")

	// Pull-response batch with a fresh event → batch is useful (0.0).
	s.ApplyEvents([]*statev1.GossipEvent{{
		PeerId:  peerStr,
		Counter: 2,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	}}, true)
	require.Equal(t, 0.0, gm.StaleRatio.Value(), "fresh batch should record 0.0")

	// Pull-response batch where all events are stale → batch is stale (1.0).
	prev := gm.StaleRatio.Value()
	s.ApplyEvents([]*statev1.GossipEvent{{
		PeerId:  peerStr,
		Counter: 2,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	}}, true)
	require.Greater(t, gm.StaleRatio.Value(), prev, "fully stale batch should increase EWMA")

	// Pull-response batch with mix of stale + fresh → batch is useful (0.0).
	mixed := gm.StaleRatio.Value()
	s.ApplyEvents([]*statev1.GossipEvent{
		{
			PeerId:  peerStr,
			Counter: 2, // stale
			Change: &statev1.GossipEvent_Network{
				Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
			},
		},
		{
			PeerId:  peerStr,
			Counter: 3, // fresh
			Change: &statev1.GossipEvent_Network{
				Network: &statev1.NetworkChange{Ips: []string{"10.0.0.3"}, LocalPort: 9002},
			},
		},
	}, true)
	require.Less(t, gm.StaleRatio.Value(), mixed, "mixed batch with fresh event should decrease EWMA")
}

func TestResourceTelemetryDeadband(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	s := newTestStore(pub)

	t.Run("first call emits", func(t *testing.T) {
		events := s.SetLocalResourceTelemetry(10, 20, 1<<30)
		require.Len(t, events, 1)
	})

	t.Run("below threshold suppressed", func(t *testing.T) {
		events := s.SetLocalResourceTelemetry(11, 21, 1<<30)
		require.Nil(t, events)
	})

	t.Run("cpu crosses threshold", func(t *testing.T) {
		events := s.SetLocalResourceTelemetry(12, 20, 1<<30)
		require.Len(t, events, 1)
	})

	t.Run("mem crosses threshold", func(t *testing.T) {
		events := s.SetLocalResourceTelemetry(12, 22, 1<<30)
		require.Len(t, events, 1)
	})

	t.Run("mem total change emits", func(t *testing.T) {
		events := s.SetLocalResourceTelemetry(12, 22, 2<<30)
		require.Len(t, events, 1)
	})
}

func TestWatermarkAlwaysAdvances(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)

	mkEvent := func(counter uint64) *statev1.GossipEvent {
		return &statev1.GossipEvent{
			PeerId:  peerIDStr,
			Counter: counter,
			Change: &statev1.GossipEvent_Network{
				Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
			},
		}
	}

	t.Run("push advances maxCounter", func(t *testing.T) {
		s.ApplyEvents([]*statev1.GossipEvent{mkEvent(5)}, false)
		require.Equal(t, uint64(5), s.Clock().GetPeers()[peerPK.String()].GetMaxCounter())
	})

	t.Run("pull advances maxCounter", func(t *testing.T) {
		s.ApplyEvents([]*statev1.GossipEvent{mkEvent(10)}, true)
		require.Equal(t, uint64(10), s.Clock().GetPeers()[peerPK.String()].GetMaxCounter())
	})
}

func TestPeerHash(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)

	// Deterministic: calling twice yields the same hash.
	d1 := s.Clock()
	d2 := s.Clock()
	h1 := d1.GetPeers()[s.LocalID.String()].GetStateHash()
	h2 := d2.GetPeers()[s.LocalID.String()].GetStateHash()
	require.Equal(t, h1, h2)
	require.NotZero(t, h1)

	// Hash changes when state changes.
	s.SetExternalPort(45000)
	d3 := s.Clock()
	h3 := d3.GetPeers()[s.LocalID.String()].GetStateHash()
	require.NotEqual(t, h1, h3)
}

func TestDigestHashMismatchSendsAll(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.SetExternalPort(45000)

	// Mismatched hash with high counter → full dump from counter 0.
	events := s.MissingFor(&statev1.GossipStateDigest{
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

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)

	// Pass own digest back — everything matches → empty result.
	events := s.MissingFor(s.Clock())
	require.Empty(t, events)
}

func TestOnDenyCallbackFiredOnGossipReceipt(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	var deniedPeers []types.PeerKey
	s.OnDenyPeer(func(pk types.PeerKey) {
		deniedPeers = append(deniedPeers, pk)
	})

	// Simulate a deny event from a remote peer.
	senderPK, senderPKStr := peerKey(2)
	subjectPK, _ := peerKey(3)
	_ = senderPK

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  senderPKStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Deny{
			Deny: &statev1.DenyChange{SubjectPub: subjectPK[:]},
		},
	})

	require.Len(t, deniedPeers, 1)
	require.Equal(t, subjectPK, deniedPeers[0])
	require.True(t, s.IsDenied(subjectPK[:]))
}

func TestOnDenyCallbackNotFiredForAlreadyDenied(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	subjectPK, _ := peerKey(3)
	s.DenyPeer(subjectPK[:])

	var callCount int
	s.OnDenyPeer(func(_ types.PeerKey) {
		callCount++
	})

	// Deny via gossip from a different peer — already denied, callback should not fire.
	_, senderPKStr := peerKey(2)
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  senderPKStr,
		Counter: 1,
		Change: &statev1.GossipEvent_Deny{
			Deny: &statev1.DenyChange{SubjectPub: subjectPK[:]},
		},
	})

	require.Equal(t, 0, callCount)
}

func TestWorkloadSpecRoundTrip(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	// Publish a workload spec.
	events, err := s.SetLocalWorkloadSpec("abc123", 2, 16)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.False(t, events[0].GetDeleted())

	// Query it back.
	specs := s.AllWorkloadSpecs()
	require.Len(t, specs, 1)
	sv, ok := specs["abc123"]
	require.True(t, ok)
	require.Equal(t, uint32(2), sv.Spec.GetReplicas())
	require.Equal(t, uint32(16), sv.Spec.GetMemoryPages())
	require.Equal(t, s.LocalID, sv.Publisher)

	// Idempotent — no event on identical call.
	events2, err := s.SetLocalWorkloadSpec("abc123", 2, 16)
	require.NoError(t, err)
	require.Nil(t, events2)

	// Remove it.
	del := s.RemoveLocalWorkloadSpec("abc123")
	require.Len(t, del, 1)
	require.True(t, del[0].GetDeleted())

	specs2 := s.AllWorkloadSpecs()
	require.Empty(t, specs2)

	// Idempotent removal.
	del2 := s.RemoveLocalWorkloadSpec("abc123")
	require.Nil(t, del2)
}

func TestWorkloadClaimRoundTrip(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	// Claim a workload.
	events := s.SetLocalWorkloadClaim("abc123", true)
	require.Len(t, events, 1)
	require.False(t, events[0].GetDeleted())

	claims := s.AllWorkloadClaims()
	require.Len(t, claims, 1)
	require.Contains(t, claims["abc123"], s.LocalID)

	// Idempotent.
	events2 := s.SetLocalWorkloadClaim("abc123", true)
	require.Nil(t, events2)

	// Release.
	rel := s.SetLocalWorkloadClaim("abc123", false)
	require.Len(t, rel, 1)
	require.True(t, rel[0].GetDeleted())

	claims2 := s.AllWorkloadClaims()
	require.Empty(t, claims2)
}

func TestWorkloadGossipApply(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, remotePeerStr := peerKey(2)

	// Apply a remote spec event.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  remotePeerStr,
		Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{
				Hash:     "deadbeef",
				Replicas: 3,
			},
		},
	})

	specs := s.AllWorkloadSpecs()
	require.Len(t, specs, 1)
	require.Equal(t, uint32(3), specs["deadbeef"].Spec.GetReplicas())

	// Apply a remote claim event.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  remotePeerStr,
		Counter: 2,
		Change: &statev1.GossipEvent_WorkloadClaim{
			WorkloadClaim: &statev1.WorkloadClaimChange{Hash: "deadbeef"},
		},
	})

	claims := s.AllWorkloadClaims()
	require.Len(t, claims, 1)
	remotePK, _ := peerKey(2)
	require.Contains(t, claims["deadbeef"], remotePK)

	// Delete remote spec.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  remotePeerStr,
		Counter: 3,
		Deleted: true,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "deadbeef"},
		},
	})

	specs2 := s.AllWorkloadSpecs()
	require.Empty(t, specs2)
}

func TestOnWorkloadChangeCallback(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	var callCount int
	s.OnWorkloadChange(func() { callCount++ })

	_, remotePeerStr := peerKey(2)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  remotePeerStr,
		Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "abc", Replicas: 1},
		},
	})
	require.Equal(t, 1, callCount)

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  remotePeerStr,
		Counter: 2,
		Change: &statev1.GossipEvent_WorkloadClaim{
			WorkloadClaim: &statev1.WorkloadClaimChange{Hash: "abc"},
		},
	})
	require.Equal(t, 2, callCount)
}

func TestAllWorkloadSpecs_DuplicatePublishers_LowestPeerKeyWins(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 5 // local node has PeerKey starting with 5
	s := newTestStore(pub)

	// Local node publishes spec with replicas=1 (no remote peers yet, so accepted).
	_, err := s.SetLocalWorkloadSpec("shared", 1, 0)
	require.NoError(t, err)

	// Two remote peers also publish specs for the same hash with different replicas.
	peer2PK, peer2Str := peerKey(2) // lower PeerKey → should win
	_, peer9Str := peerKey(9)       // higher PeerKey

	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peer2Str,
		Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "shared", Replicas: 3},
		},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peer9Str,
		Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "shared", Replicas: 7},
		},
	})

	specs := s.AllWorkloadSpecs()
	require.Len(t, specs, 1)
	sv := specs["shared"]
	require.Equal(t, peer2PK, sv.Publisher, "lowest PeerKey should win")
	require.Equal(t, uint32(3), sv.Spec.GetReplicas(), "should use winner's spec")
}

func TestSetLocalWorkloadSpec_RejectsWhenRemoteOwns(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	// Remote peer publishes a spec first via gossip.
	_, remotePeerStr := peerKey(2)
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  remotePeerStr,
		Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "contested", Replicas: 1},
		},
	})

	// Local attempt to publish the same hash should be rejected atomically.
	events, err := s.SetLocalWorkloadSpec("contested", 2, 0)
	require.ErrorIs(t, err, ErrSpecOwnedRemotely)
	require.Nil(t, events)

	// Only the remote spec should exist.
	specs := s.AllWorkloadSpecs()
	require.Len(t, specs, 1)
	require.Equal(t, uint32(1), specs["contested"].Spec.GetReplicas())
}

func TestRemoteSpec_TombstonesLosingLocalSpec(t *testing.T) {
	// Local node has PeerKey starting with 9 (high).
	pub := make([]byte, 32)
	pub[0] = 9
	s := newTestStore(pub)

	// Local node publishes a spec first (no remote competitors yet).
	events, err := s.SetLocalWorkloadSpec("contested", 3, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)

	specs := s.AllWorkloadSpecs()
	require.Len(t, specs, 1)
	require.Equal(t, s.LocalID, specs["contested"].Publisher)

	// Remote peer with lower PeerKey publishes the same hash via gossip.
	winnerPK, winnerStr := peerKey(1)
	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  winnerStr,
		Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "contested", Replicas: 2},
		},
	})

	// Rebroadcast should contain the original event AND the local tombstone.
	require.Len(t, result.Rebroadcast, 2)
	tombstone := result.Rebroadcast[1]
	require.True(t, tombstone.GetDeleted())
	require.Equal(t, s.LocalID.String(), tombstone.GetPeerId())

	// Only the winner's spec should remain in the merged view.
	specs = s.AllWorkloadSpecs()
	require.Len(t, specs, 1)
	require.Equal(t, winnerPK, specs["contested"].Publisher)
	require.Equal(t, uint32(2), specs["contested"].Spec.GetReplicas())

	// Local node's raw record should no longer have the spec.
	local := s.nodes[s.LocalID]
	require.NotContains(t, local.WorkloadSpecs, "contested")
}

func TestRemoteSpec_NoTombstoneWhenLocalWins(t *testing.T) {
	// Local node has PeerKey starting with 1 (low — wins).
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events, err := s.SetLocalWorkloadSpec("mine", 3, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)

	// Remote peer with higher PeerKey publishes the same hash.
	_, loserStr := peerKey(9)
	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  loserStr,
		Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "mine", Replicas: 5},
		},
	})

	// Only the original event should be rebroadcast — no tombstone.
	require.Len(t, result.Rebroadcast, 1)

	// Local spec is still the winner.
	specs := s.AllWorkloadSpecs()
	require.Equal(t, s.LocalID, specs["mine"].Publisher)
	require.Equal(t, uint32(3), specs["mine"].Spec.GetReplicas())
}

func TestRemoteSpec_NoTombstoneWhenRemoteDenied(t *testing.T) {
	// Local node has high PeerKey — would normally lose to remote.
	pub := make([]byte, 32)
	pub[0] = 9
	s := newTestStore(pub)

	events, err := s.SetLocalWorkloadSpec("contested", 3, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)

	// Remote peer with lower PeerKey — but denied.
	deniedPK, deniedStr := peerKey(1)
	s.denied[deniedPK] = struct{}{}

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  deniedStr,
		Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "contested", Replicas: 2},
		},
	})

	// No tombstone — denied peers cannot win ownership.
	require.Len(t, result.Rebroadcast, 1)

	// Local spec remains.
	specs := s.AllWorkloadSpecs()
	require.Equal(t, s.LocalID, specs["contested"].Publisher)
	require.Equal(t, uint32(3), specs["contested"].Spec.GetReplicas())
}

func TestRemoteSpec_NoTombstoneWhenRemoteExpired(t *testing.T) {
	// Local node has high PeerKey — would normally lose to remote.
	pub := make([]byte, 32)
	pub[0] = 9
	s := newTestStore(pub)

	events, err := s.SetLocalWorkloadSpec("contested", 3, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)

	// Remote peer with lower PeerKey, but expired cert.
	_, expiredStr := peerKey(1)
	pastExpiry := time.Now().Add(-time.Hour).Unix()

	// Set up the remote peer's identity with an expired cert.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  expiredStr,
		Counter: 1,
		Change: &statev1.GossipEvent_IdentityPub{
			IdentityPub: &statev1.IdentityChange{
				IdentityPub:    make([]byte, 32),
				CertExpiryUnix: pastExpiry,
			},
		},
	})

	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  expiredStr,
		Counter: 2,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "contested", Replicas: 2},
		},
	})

	// No tombstone — expired peers cannot win ownership.
	require.Len(t, result.Rebroadcast, 1)

	// Local spec remains.
	specs := s.AllWorkloadSpecs()
	require.Equal(t, s.LocalID, specs["contested"].Publisher)
	require.Equal(t, uint32(3), specs["contested"].Spec.GetReplicas())
}

func TestResolveWorkloadPrefix_ClusterWide(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	// Only a remote peer has this spec — local node has nothing.
	_, remotePeerStr := peerKey(2)
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  remotePeerStr,
		Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "abcdef1234567890", Replicas: 2},
		},
	})

	// Resolve by prefix.
	hash, ambiguous, found := s.ResolveWorkloadPrefix("abcdef")
	require.True(t, found)
	require.False(t, ambiguous)
	require.Equal(t, "abcdef1234567890", hash)

	// No match.
	_, _, found = s.ResolveWorkloadPrefix("zzz")
	require.False(t, found)

	// Ambiguous: add another spec with overlapping prefix.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  remotePeerStr,
		Counter: 2,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "abcdef9999999999", Replicas: 1},
		},
	})

	_, ambiguous, found = s.ResolveWorkloadPrefix("abcdef")
	require.True(t, ambiguous)
	require.False(t, found)
}

func TestWorkloadQueries_ExcludeDeniedNodes(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	// Remote peer publishes a spec and a claim.
	deniedPK, deniedStr := peerKey(2)
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  deniedStr,
		Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "deniedwl", Replicas: 1},
		},
	})
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  deniedStr,
		Counter: 2,
		Change: &statev1.GossipEvent_WorkloadClaim{
			WorkloadClaim: &statev1.WorkloadClaimChange{Hash: "deniedwl"},
		},
	})

	// Before denial, spec and claim are visible.
	require.Len(t, s.AllWorkloadSpecs(), 1)
	claims := s.AllWorkloadClaims()
	require.Contains(t, claims["deniedwl"], deniedPK)
	h, _, found := s.ResolveWorkloadPrefix("denied")
	require.True(t, found)
	require.Equal(t, "deniedwl", h)

	// Deny the peer.
	s.denied[deniedPK] = struct{}{}

	// After denial, spec, claim, and prefix resolution all exclude the peer.
	require.Empty(t, s.AllWorkloadSpecs())
	require.Empty(t, s.AllWorkloadClaims())
	_, _, found = s.ResolveWorkloadPrefix("denied")
	require.False(t, found)
}

func TestTrafficHeatmap_ApplyAndQuery(t *testing.T) {
	localPK, localIDStr := peerKey(1)
	remotePK, remotePKStr := peerKey(2)

	s := newTestStore([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

	// Apply a traffic heatmap event from the remote peer.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  remotePKStr,
		Counter: 1,
		Change: &statev1.GossipEvent_TrafficHeatmap{
			TrafficHeatmap: &statev1.TrafficHeatmapChange{
				Rates: []*statev1.TrafficRate{
					{PeerId: localIDStr, BytesIn: 100, BytesOut: 200},
				},
			},
		},
	})

	all := s.AllTrafficHeatmaps()
	require.Contains(t, all, remotePK)
	require.Equal(t, TrafficSnapshot{BytesIn: 100, BytesOut: 200}, all[remotePK][localPK])
}

func TestTrafficHeatmap_TombstoneOnRestart(t *testing.T) {
	pub := []byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	s := newTestStore(pub)

	targetPK, _ := peerKey(4)
	events := s.SetLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{
		{4}: {BytesIn: 500, BytesOut: 600},
	})
	require.Len(t, events, 1)

	// Verify it's stored.
	all := s.AllTrafficHeatmaps()
	localID := types.PeerKeyFromBytes(pub)
	require.Contains(t, all, localID)
	require.Equal(t, TrafficSnapshot{BytesIn: 500, BytesOut: 600}, all[localID][targetPK])

	// Simulate restart by re-tombstoning stale attrs.
	local := s.nodes[localID]
	tombstoneStaleAttrs(&local)
	s.nodes[localID] = local

	// Traffic data should be tombstoned.
	entry := local.log[trafficHeatmapAttrKey()]
	require.True(t, entry.Deleted)
}

func TestTrafficHeatmap_ZeroSnapshotSkipped(t *testing.T) {
	pub := []byte{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	s := newTestStore(pub)

	// Empty snapshot produces no gossip event when no prior data exists.
	events := s.SetLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{})
	require.Empty(t, events)

	// All-zero snapshot also produces no gossip event.
	events = s.SetLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{
		{6}: {BytesIn: 0, BytesOut: 0},
	})
	require.Empty(t, events)
}

func TestTrafficHeatmap_StaleClearing(t *testing.T) {
	pub := []byte{7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	s := newTestStore(pub)
	localID := types.PeerKeyFromBytes(pub)

	// Publish non-zero traffic.
	events := s.SetLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{
		{8}: {BytesIn: 100, BytesOut: 200},
	})
	require.Len(t, events, 1)
	require.False(t, events[0].GetDeleted())

	all := s.AllTrafficHeatmaps()
	require.Contains(t, all, localID)

	// Next tick: zero traffic. Should emit deletion event.
	events = s.SetLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{})
	require.Len(t, events, 1)
	require.True(t, events[0].GetDeleted())

	// Traffic data should be cleared.
	all = s.AllTrafficHeatmaps()
	require.NotContains(t, all, localID)

	// Subsequent zero tick: no event (already cleared).
	events = s.SetLocalTrafficHeatmap(map[types.PeerKey]TrafficSnapshot{})
	require.Empty(t, events)
}

func TestMalformedTrafficHeatmapRejected(t *testing.T) {
	_, remotePKStr := peerKey(2)
	localPK, localIDStr := peerKey(1)

	s := newTestStore([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	remotePK := types.PeerKeyFromBytes([]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

	// Apply a valid traffic heatmap from the remote peer.
	result := s.applyEvent(&statev1.GossipEvent{
		PeerId:  remotePKStr,
		Counter: 1,
		Change: &statev1.GossipEvent_TrafficHeatmap{
			TrafficHeatmap: &statev1.TrafficHeatmapChange{
				Rates: []*statev1.TrafficRate{
					{PeerId: localIDStr, BytesIn: 100, BytesOut: 200},
				},
			},
		},
	})
	require.Len(t, result.Rebroadcast, 1)

	all := s.AllTrafficHeatmaps()
	require.Equal(t, TrafficSnapshot{BytesIn: 100, BytesOut: 200}, all[remotePK][localPK])

	// Capture the log entry and counter after the valid apply.
	logBefore := s.nodes[remotePK].log[trafficHeatmapAttrKey()]

	// Apply a second heatmap with one valid and one invalid peer_id.
	result = s.applyEvent(&statev1.GossipEvent{
		PeerId:  remotePKStr,
		Counter: 2,
		Change: &statev1.GossipEvent_TrafficHeatmap{
			TrafficHeatmap: &statev1.TrafficHeatmapChange{
				Rates: []*statev1.TrafficRate{
					{PeerId: localIDStr, BytesIn: 999, BytesOut: 999},
					{PeerId: "not-a-valid-peer-key", BytesIn: 1, BytesOut: 1},
				},
			},
		},
	})

	// The malformed heatmap must be rejected entirely — original data preserved.
	all = s.AllTrafficHeatmaps()
	require.Equal(t, TrafficSnapshot{BytesIn: 100, BytesOut: 200}, all[remotePK][localPK])

	// The bad event must not advance the stored counter/log entry.
	logAfter := s.nodes[remotePK].log[trafficHeatmapAttrKey()]
	require.Equal(t, logBefore, logAfter)

	// The bad event must not be rebroadcast.
	require.Empty(t, result.Rebroadcast)
}
