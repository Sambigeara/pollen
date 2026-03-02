package store

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
)

func newTestStore(pub []byte, trustBundle *admissionv1.TrustBundle) *Store {
	localID := types.PeerKeyFromBytes(pub)
	return &Store{
		LocalID: localID,
		nodes: map[types.PeerKey]nodeRecord{
			localID: {
				maxCounter:  1,
				IdentityPub: append([]byte(nil), pub...),
				Reachable:   make(map[types.PeerKey]struct{}),
				Services:    make(map[string]*statev1.Service),
				log: map[attrKey]logEntry{
					identityAttrKey(): {Counter: 1},
				},
			},
		},
		revocations:        make(map[types.PeerKey]*admissionv1.SignedRevocation),
		trustBundle:        trustBundle,
		desiredConnections: make(map[string]Connection),
	}
}

// applyEvent is a test convenience for applying a single gossip event.
func (s *Store) applyEvent(event *statev1.GossipEvent) ApplyResult {
	return s.ApplyEvents([]*statev1.GossipEvent{event})
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
	s := newTestStore(pub, nil)

	// Fresh store with only local state → zero clock.
	clock := s.EagerSyncClock()
	if len(clock.GetCounters()) != 0 {
		t.Fatalf("expected empty counters for fresh store, got %v", clock.GetCounters())
	}

	// Apply a remote event → real clock.
	_, peerIDStr := peerKey(2)
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 5,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})

	clock = s.EagerSyncClock()
	counters := clock.GetCounters()
	if len(counters) != 2 {
		t.Fatalf("expected 2 entries in clock, got %d", len(counters))
	}
	if counters[s.LocalID.String()] != 1 {
		t.Fatalf("expected local counter=1, got %d", counters[s.LocalID.String()])
	}
	peerPK, _ := peerKey(2)
	if counters[peerPK.String()] != 5 {
		t.Fatalf("expected remote counter=5, got %d", counters[peerPK.String()])
	}
}

func TestSetLocalNetworkReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

	events := s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if ev.GetCounter() != 2 {
		t.Fatalf("expected counter=2, got %d", ev.GetCounter())
	}
	network := ev.GetNetwork()
	if network == nil {
		t.Fatal("expected network value, got nil")
	}
	if len(network.GetIps()) != 1 || network.GetIps()[0] != "10.0.0.1" {
		t.Fatalf("unexpected IPs: %v", network.GetIps())
	}
	if network.GetLocalPort() != 9000 {
		t.Fatalf("expected port 9000, got %d", network.GetLocalPort())
	}
}

func TestSetLocalNetworkNoOpWhenUnchanged(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	events := s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	if events != nil {
		t.Fatal("expected nil for no-op, got events")
	}
}

func TestSetExternalPortReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

	events := s.SetExternalPort(45000)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].GetExternalPort().GetExternalPort() != 45000 {
		t.Fatalf("expected port 45000, got %d", events[0].GetExternalPort().GetExternalPort())
	}
}

func TestSetLocalConnectedConnect(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

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

func TestApplyEventPerKeyCounter(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)
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
	// Adopted 10, then bumped once per attribute: net(11) + id(12) = MaxCounter 12.
	if rec.maxCounter != 12 {
		t.Fatalf("expected MaxCounter=12 (adopted 10 + 2 attributes), got %d", rec.maxCounter)
	}
}

func TestApplyEventSelfStateNoConflict(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000) // counter=2
	s.SetExternalPort(45000)                      // counter=3

	// Remote has counter=0 for us — should get all events (id + net + ext).
	events := s.MissingFor(&statev1.GossipVectorClock{
		Counters: map[string]uint64{
			s.LocalID.String(): 0,
		},
	})

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
}

func TestMissingForRespectsClock(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000) // counter=2
	s.SetExternalPort(45000)                      // counter=3

	// Remote has counter=2 — should only get events with counter > 2.
	events := s.MissingFor(&statev1.GossipVectorClock{
		Counters: map[string]uint64{
			s.LocalID.String(): 2,
		},
	})

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].GetExternalPort().GetExternalPort() != 45000 {
		t.Fatalf("expected ext event with port 45000, got %v", events[0].GetExternalPort())
	}
}

func TestMissingForReturnsNilForUpToDate(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000) // counter=2

	events := s.MissingFor(&statev1.GossipVectorClock{
		Counters: map[string]uint64{
			s.LocalID.String(): 2,
		},
	})

	if len(events) != 0 {
		t.Fatalf("expected 0 events for up-to-date peer, got %d", len(events))
	}
}

func TestClockUsesMaxCounter(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000) // counter=2
	s.SetExternalPort(45000)                      // counter=3

	clock := s.Clock()
	c := clock.GetCounters()[s.LocalID.String()]
	if c != 3 {
		t.Fatalf("expected clock counter=3, got %d", c)
	}
}

func TestUpsertLocalServiceReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

	events := s.RemoveLocalServices("http")
	if events != nil {
		t.Fatal("expected nil for removing non-existent service")
	}
}

func TestApplyEventNetworkUpdate(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

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

	s, err := Load(dir, localPub, nil)
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

	s2, err := Load(dir, localPub, nil)
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

func TestLoadServiceWithOrphanedProvider(t *testing.T) {
	dir := t.TempDir()

	localPub := make([]byte, 32)
	localPub[0] = 1

	// Bootstrap an empty store to create the state file.
	s, err := Load(dir, localPub, nil)
	if err != nil {
		t.Fatalf("initial load: %v", err)
	}

	// Add a peer with a service, then save.
	peerPK, peerIDStr := peerKey(2)
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

	if err := s.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Tamper with disk state: remove the peer entry but keep the service.
	// This simulates the bug where IdentityPub was empty and got skipped on load.
	d, err := openDisk(dir)
	if err != nil {
		t.Fatalf("open disk: %v", err)
	}
	st, err := d.load()
	if err != nil {
		t.Fatalf("load disk: %v", err)
	}
	st.Peers = nil // remove all peers, keep services
	if err := d.save(st); err != nil {
		t.Fatalf("save tampered state: %v", err)
	}
	if err := d.close(); err != nil {
		t.Fatalf("close disk: %v", err)
	}

	// Load should not panic even though the service references an unknown provider.
	s2, err := Load(dir, localPub, nil)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer func() { _ = s2.Close() }()

	rec, ok := s2.Get(peerPK)
	if !ok {
		t.Fatal("expected peer record to be created from orphaned service")
	}
	if _, exists := rec.Services["http"]; !exists {
		t.Fatal("expected service 'http' on orphaned provider")
	}
	if len(rec.IdentityPub) == 0 {
		t.Fatal("expected IdentityPub to be set from peer key")
	}

	idPub, found := s2.IdentityPub(peerPK)
	if !found {
		t.Fatal("IdentityPub should return true for orphaned provider")
	}
	if len(idPub) != 32 {
		t.Fatalf("expected 32-byte IdentityPub, got %d bytes", len(idPub))
	}
}

func TestSetLocalPubliclyAccessibleReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

	s.SetLocalPubliclyAccessible(true)
	events := s.SetLocalPubliclyAccessible(true)
	if events != nil {
		t.Fatal("expected nil for no-op, got events")
	}
}

func TestSetLocalPubliclyAccessibleClear(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

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
	s := newTestStore(pub, nil)

	events := s.SetLocalPubliclyAccessible(true)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	missing := s.MissingFor(&statev1.GossipVectorClock{
		Counters: map[string]uint64{s.LocalID.String(): 0},
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
	s := newTestStore(pub, nil)

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

func newTestClusterAuth(t *testing.T) (ed25519.PrivateKey, *admissionv1.TrustBundle) {
	t.Helper()
	adminPub, adminPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return adminPriv, auth.NewTrustBundle(adminPub)
}

func issueTestRevocation(t *testing.T, adminPriv ed25519.PrivateKey, trust *admissionv1.TrustBundle, subjectPub []byte) *admissionv1.SignedRevocation {
	t.Helper()
	rev, err := auth.IssueRevocation(adminPriv, trust.GetClusterId(), subjectPub, time.Now())
	if err != nil {
		t.Fatalf("issue revocation: %v", err)
	}
	return rev
}

func TestPublishRevocationAndIsRevoked(t *testing.T) {
	adminPriv, trust := newTestClusterAuth(t)

	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, trust)

	subjectPub := make([]byte, 32)
	subjectPub[0] = 2

	rev := issueTestRevocation(t, adminPriv, trust, subjectPub)

	events := s.PublishRevocation(rev)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if !s.IsSubjectRevoked(subjectPub) {
		t.Fatal("expected subject pub to be revoked")
	}

	otherPub := make([]byte, 32)
	otherPub[0] = 3
	if s.IsSubjectRevoked(otherPub) {
		t.Fatal("expected other pub to NOT be revoked")
	}
}

func TestPublishRevocationDuplicateIsNoop(t *testing.T) {
	adminPriv, trust := newTestClusterAuth(t)

	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, trust)

	rev := issueTestRevocation(t, adminPriv, trust, make([]byte, 32))

	events := s.PublishRevocation(rev)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	events = s.PublishRevocation(rev)
	if events != nil {
		t.Fatal("expected nil for duplicate revocation")
	}
}

func TestApplyRevocationEventFromPeer(t *testing.T) {
	adminPriv, trust := newTestClusterAuth(t)

	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, trust)

	peerPub := make([]byte, 32)
	peerPub[0] = 2
	peerID := types.PeerKeyFromBytes(peerPub)

	subjectPub := make([]byte, 32)
	subjectPub[0] = 3

	rev := issueTestRevocation(t, adminPriv, trust, subjectPub)

	event := &statev1.GossipEvent{
		PeerId:  peerID.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Revocation{
			Revocation: &statev1.RevocationChange{Revocation: rev},
		},
	}

	s.applyEvent(event)

	if !s.IsSubjectRevoked(subjectPub) {
		t.Fatal("expected subject pub to be revoked after applying peer event")
	}
}

func TestApplyRevocationEventRejectsWithoutTrustBundle(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, nil)

	peerPub := make([]byte, 32)
	peerPub[0] = 2
	peerID := types.PeerKeyFromBytes(peerPub)

	subjectPub := make([]byte, 32)
	subjectPub[0] = 3

	rev := &admissionv1.SignedRevocation{
		Entry: &admissionv1.RevocationEntry{
			SubjectPub: subjectPub,
		},
		Signature: make([]byte, 64),
	}

	event := &statev1.GossipEvent{
		PeerId:  peerID.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Revocation{
			Revocation: &statev1.RevocationChange{Revocation: rev},
		},
	}

	s.applyEvent(event)

	if s.IsSubjectRevoked(subjectPub) {
		t.Fatal("expected revocation to be rejected when trust bundle is nil")
	}
}

func TestApplyRevocationDeletedEventRejected(t *testing.T) {
	adminPriv, trust := newTestClusterAuth(t)

	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, trust)

	_, peerIDStr := peerKey(2)

	subjectPub := make([]byte, 32)
	subjectPub[0] = 3
	rev := issueTestRevocation(t, adminPriv, trust, subjectPub)

	event := &statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Deleted: true,
		Change: &statev1.GossipEvent_Revocation{
			Revocation: &statev1.RevocationChange{Revocation: rev},
		},
	}

	s.applyEvent(event)

	if s.IsSubjectRevoked(subjectPub) {
		t.Fatal("deleted revocation event should be silently dropped")
	}

	peerPK, _ := peerKey(2)
	rec, ok := s.Get(peerPK)
	if ok && rec.maxCounter > 0 {
		t.Fatal("deleted revocation should not advance the log")
	}
}

func TestApplyRevocationDuplicateDoesNotFireCallback(t *testing.T) {
	adminPriv, trust := newTestClusterAuth(t)

	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub, trust)

	var callCount int
	s.OnRevocation(func(types.PeerKey) { callCount++ })

	subjectPub := make([]byte, 32)
	subjectPub[0] = 3
	rev := issueTestRevocation(t, adminPriv, trust, subjectPub)

	// First peer sends revocation.
	_, peer1Str := peerKey(2)
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peer1Str,
		Counter: 1,
		Change: &statev1.GossipEvent_Revocation{
			Revocation: &statev1.RevocationChange{Revocation: rev},
		},
	})

	// Second peer sends the same revocation.
	_, peer2Str := peerKey(4)
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peer2Str,
		Counter: 1,
		Change: &statev1.GossipEvent_Revocation{
			Revocation: &statev1.RevocationChange{Revocation: rev},
		},
	})

	if callCount != 1 {
		t.Fatalf("expected onRevocation to fire exactly once, got %d", callCount)
	}

	if !s.IsSubjectRevoked(subjectPub) {
		t.Fatal("subject should still be revoked")
	}
}

func TestLoadRestoresRevocationsFromDisk(t *testing.T) {
	adminPriv, trust := newTestClusterAuth(t)
	dir := t.TempDir()

	localPub := make([]byte, 32)
	localPub[0] = 1

	s, err := Load(dir, localPub, trust)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	subjectPub := make([]byte, 32)
	subjectPub[0] = 2
	rev := issueTestRevocation(t, adminPriv, trust, subjectPub)

	s.PublishRevocation(rev)

	if err := s.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := Load(dir, localPub, trust)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if !s2.IsSubjectRevoked(subjectPub) {
		t.Fatal("revocation should survive save/load round-trip")
	}

	// The revocation should appear in MissingFor output for a new joiner.
	events := s2.MissingFor(nil)
	var found bool
	for _, ev := range events {
		if ev.GetRevocation() != nil {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected revocation event in MissingFor output after reload")
	}
}
