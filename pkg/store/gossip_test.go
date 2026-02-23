package store

import (
	"testing"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

func TestSameServicesIgnoresOrder(t *testing.T) {
	a := []*statev1.Service{
		{Name: "alpha", Port: 80},
		{Name: "beta", Port: 443},
	}
	b := []*statev1.Service{
		{Name: "beta", Port: 443},
		{Name: "alpha", Port: 80},
	}

	if !sameServices(a, b) {
		t.Fatal("sameServices should return true for identical services in different order")
	}
}

func TestSameServicesDetectsDifference(t *testing.T) {
	a := []*statev1.Service{
		{Name: "alpha", Port: 80},
	}
	b := []*statev1.Service{
		{Name: "beta", Port: 443},
	}

	if sameServices(a, b) {
		t.Fatal("sameServices should return false for different services")
	}
}

func TestSameServicesDifferentLength(t *testing.T) {
	a := []*statev1.Service{
		{Name: "alpha", Port: 80},
		{Name: "beta", Port: 443},
	}
	b := []*statev1.Service{
		{Name: "alpha", Port: 80},
	}

	if sameServices(a, b) {
		t.Fatal("sameServices should return false for different-length slices")
	}
}

func TestToGossipNodeSortsServices(t *testing.T) {
	rec := nodeRecord{
		Services: map[string]*statev1.Service{
			"zebra": {Name: "zebra", Port: 9999},
			"alpha": {Name: "alpha", Port: 80},
			"mid":   {Name: "mid", Port: 443},
		},
		Reachable: make(map[types.PeerKey]struct{}),
	}

	// Run multiple times to confirm determinism despite map iteration order.
	for i := 0; i < 50; i++ {
		gn := toGossipNode(types.PeerKey{}, rec)
		if len(gn.Services) != 3 {
			t.Fatalf("expected 3 services, got %d", len(gn.Services))
		}
		if gn.Services[0].GetName() != "alpha" ||
			gn.Services[1].GetName() != "mid" ||
			gn.Services[2].GetName() != "zebra" {
			t.Fatalf("services not sorted: %v", gn.Services)
		}
	}
}

func TestApplyOwnStateNoSpuriousBump(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	localID := types.PeerKeyFromBytes(pub)

	s := &Store{
		LocalID: localID,
		nodes: map[types.PeerKey]nodeRecord{
			localID: {
				Counter:     5,
				IdentityPub: pub,
				IPs:         []string{"10.0.0.1"},
				LocalPort:   9000,
				Reachable:   make(map[types.PeerKey]struct{}),
				Services: map[string]*statev1.Service{
					"alpha": {Name: "alpha", Port: 80},
					"beta":  {Name: "beta", Port: 443},
				},
			},
		},
	}

	// Simulate receiving own state back with services in reverse order.
	incoming := &statev1.GossipNode{
		PeerId:      localID.String(),
		Counter:     5,
		IdentityPub: pub,
		Ips:         []string{"10.0.0.1"},
		LocalPort:   9000,
		Services: []*statev1.Service{
			{Name: "beta", Port: 443},
			{Name: "alpha", Port: 80},
		},
	}

	result := s.ApplyNode(incoming)

	if result.Rebroadcast != nil {
		t.Fatal("ApplyNode should not trigger rebroadcast when payload is logically identical")
	}

	rec := s.nodes[localID]
	if rec.Counter != 5 {
		t.Fatalf("counter should remain 5, got %d", rec.Counter)
	}
	if len(rec.Services) != 2 {
		t.Fatalf("expected 2 services, got %d", len(rec.Services))
	}
}
