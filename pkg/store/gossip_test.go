package store

import (
	"testing"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

func newTestStore(pub []byte) *Store {
	localID := types.PeerKeyFromBytes(pub)
	return &Store{
		LocalID: localID,
		nodes: map[types.PeerKey]nodeRecord{
			localID: {
				MaxCounter:  0,
				IdentityPub: append([]byte(nil), pub...),
				Reachable:   make(map[types.PeerKey]struct{}),
				Services:    make(map[string]*statev1.Service),
				Log:         make(map[string]logEntry),
			},
		},
		desiredConnections: make(map[string]Connection),
	}
}

func peerKey(b byte) (types.PeerKey, string) {
	pub := make([]byte, 32)
	pub[0] = b
	pk := types.PeerKeyFromBytes(pub)
	return pk, pk.String()
}

func TestSetLocalNetworkReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if ev.GetKey() != "net" {
		t.Fatalf("expected key 'net', got %q", ev.GetKey())
	}
	if ev.GetCounter() != 1 {
		t.Fatalf("expected counter=1, got %d", ev.GetCounter())
	}
	net := ev.GetNetwork()
	if net == nil {
		t.Fatal("expected network value, got nil")
	}
	if len(net.GetIps()) != 1 || net.GetIps()[0] != "10.0.0.1" {
		t.Fatalf("unexpected IPs: %v", net.GetIps())
	}
	if net.GetLocalPort() != 9000 {
		t.Fatalf("expected port 9000, got %d", net.GetLocalPort())
	}
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
	if events[0].GetKey() != "ext" {
		t.Fatalf("expected key 'ext', got %q", events[0].GetKey())
	}
	if events[0].GetExternalPort() != 45000 {
		t.Fatalf("expected port 45000, got %d", events[0].GetExternalPort())
	}
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
	if !ev.GetReachable() {
		t.Fatal("expected reachable=true")
	}
	expectedKey := reachableKey(peerID)
	if ev.GetKey() != expectedKey {
		t.Fatalf("expected key %q, got %q", expectedKey, ev.GetKey())
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

	result := s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 3,
		Key:     "ext",
		Value:   &statev1.GossipEvent_ExternalPort{ExternalPort: 45000},
	})

	if !result.Updated {
		t.Fatal("expected Updated=true")
	}

	rec, ok := s.Get(peerPK)
	if !ok {
		t.Fatal("expected peer record to exist")
	}
	if rec.ExternalPort != 45000 {
		t.Fatalf("expected ExternalPort=45000, got %d", rec.ExternalPort)
	}
	if rec.MaxCounter != 3 {
		t.Fatalf("expected MaxCounter=3, got %d", rec.MaxCounter)
	}
}

func TestApplyEventPerKeyCounter(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	_, peerIDStr := peerKey(2)

	// Apply event at counter=3.
	s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 3,
		Key:     "ext",
		Value:   &statev1.GossipEvent_ExternalPort{ExternalPort: 45000},
	})

	// Try to apply older event at counter=2 for same key.
	result := s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Key:     "ext",
		Value:   &statev1.GossipEvent_ExternalPort{ExternalPort: 44000},
	})

	if result.Updated {
		t.Fatal("stale event should not update")
	}

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
	s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 5,
		Key:     "ext",
		Value:   &statev1.GossipEvent_ExternalPort{ExternalPort: 45000},
	})

	// Apply net at counter=2 (lower counter, different key — should still apply).
	result := s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Key:     "net",
		Value: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkInfo{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
		},
	})

	if !result.Updated {
		t.Fatal("different key event should apply regardless of counter order")
	}

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
	s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Key:     "s/http",
		Value: &statev1.GossipEvent_Service{
			Service: &statev1.Service{Name: "http", Port: 8080},
		},
	})

	// Delete it.
	result := s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Key:     "s/http",
		Deleted: true,
	})

	if !result.Updated {
		t.Fatal("delete event should update")
	}

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
	s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 5,
		Key:     "s/http",
		Deleted: true,
	})

	// Try to resurrect with stale event at counter=4.
	result := s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 4,
		Key:     "s/http",
		Value: &statev1.GossipEvent_Service{
			Service: &statev1.Service{Name: "http", Port: 8080},
		},
	})

	if result.Updated {
		t.Fatal("stale event should not resurrect a tombstoned attribute")
	}

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
	result := s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  localID.String(),
		Counter: 10,
		Key:     "net",
		Value: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkInfo{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	})

	if len(result.Rebroadcast) == 0 {
		t.Fatal("self-state conflict should trigger rebroadcast")
	}

	rec := s.nodes[localID]
	// Adopted 10, then bumped once per attribute: net(11) + id(12) = MaxCounter 12.
	if rec.MaxCounter != 12 {
		t.Fatalf("expected MaxCounter=12 (adopted 10 + 2 attributes), got %d", rec.MaxCounter)
	}
}

func TestApplyEventSelfStateNoConflict(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	// Set local state (counter becomes 1).
	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)

	// Receiving own event with same or lower counter — no conflict.
	result := s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  s.LocalID.String(),
		Counter: 1,
		Key:     "net",
		Value: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkInfo{Ips: []string{"10.0.0.1"}, LocalPort: 9000},
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

	// Set some local state.
	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000) // counter=1
	s.SetExternalPort(45000)                       // counter=2

	// Remote has counter=0 for us — should get all events.
	events := s.MissingFor(&statev1.GossipVectorClock{
		Counters: map[string]uint64{
			s.LocalID.String(): 0,
		},
	})

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestMissingForRespectsClock(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000) // counter=1
	s.SetExternalPort(45000)                       // counter=2

	// Remote has counter=1 — should only get events with counter > 1.
	events := s.MissingFor(&statev1.GossipVectorClock{
		Counters: map[string]uint64{
			s.LocalID.String(): 1,
		},
	})

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].GetKey() != "ext" {
		t.Fatalf("expected key 'ext', got %q", events[0].GetKey())
	}
}

func TestMissingForReturnsNilForUpToDate(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000) // counter=1

	events := s.MissingFor(&statev1.GossipVectorClock{
		Counters: map[string]uint64{
			s.LocalID.String(): 1,
		},
	})

	if len(events) != 0 {
		t.Fatalf("expected 0 events for up-to-date peer, got %d", len(events))
	}
}

func TestClockUsesMaxCounter(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000) // counter=1
	s.SetExternalPort(45000)                       // counter=2

	clock := s.Clock()
	c := clock.GetCounters()[s.LocalID.String()]
	if c != 2 {
		t.Fatalf("expected clock counter=2, got %d", c)
	}
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
	if ev.GetKey() != "s/http" {
		t.Fatalf("expected key 's/http', got %q", ev.GetKey())
	}
	svc := ev.GetService()
	if svc == nil || svc.GetName() != "http" || svc.GetPort() != 8080 {
		t.Fatalf("unexpected service value: %v", svc)
	}
}

func TestRemoveLocalServicesReturnsEvent(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	s.UpsertLocalService(8080, "http")
	events := s.RemoveLocalServices(8080, "http")
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if !ev.GetDeleted() {
		t.Fatal("expected deleted=true for service removal")
	}
	if ev.GetKey() != "s/http" {
		t.Fatalf("expected key 's/http', got %q", ev.GetKey())
	}
}

func TestRemoveLocalServicesNoOpWhenMissing(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	events := s.RemoveLocalServices(8080, "http")
	if events != nil {
		t.Fatal("expected nil for removing non-existent service")
	}
}

func TestApplyEventNetworkUpdate(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	peerPK, peerIDStr := peerKey(2)

	s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Key:     "net",
		Value: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkInfo{
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

	s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Key:     "id",
		Value:   &statev1.GossipEvent_IdentityPub{IdentityPub: idPub},
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
	s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 1,
		Key:     reachableKey(targetPK),
		Value:   &statev1.GossipEvent_Reachable{Reachable: true},
	})

	rec, _ := s.Get(peerPK)
	if _, ok := rec.Reachable[targetPK]; !ok {
		t.Fatal("expected target to be reachable")
	}

	// Disconnect (tombstone).
	s.ApplyEvent(&statev1.GossipEvent{
		PeerId:  peerIDStr,
		Counter: 2,
		Key:     reachableKey(targetPK),
		Deleted: true,
	})

	rec, _ = s.Get(peerPK)
	if _, ok := rec.Reachable[targetPK]; ok {
		t.Fatal("expected target to be removed from reachable")
	}
}
