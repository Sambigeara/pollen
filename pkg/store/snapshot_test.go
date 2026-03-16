package store

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func genPub(t *testing.T) ed25519.PublicKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub
}

func TestSnapshot_ReflectsState(t *testing.T) {
	pub := genPub(t)
	s := newTestStore(pub)

	s.SetLocalNetwork([]string{"10.0.0.1", "10.0.0.2"}, 9000)
	s.UpsertLocalService(8080, "web")
	s.SetExternalPort(45000)

	snap := s.Snapshot()

	require.Equal(t, s.LocalID, snap.LocalID)

	localView, ok := snap.Nodes[s.LocalID]
	require.True(t, ok)
	require.Equal(t, []string{"10.0.0.1", "10.0.0.2"}, localView.IPs)
	require.Equal(t, uint32(9000), localView.LocalPort)
	require.Equal(t, uint32(45000), localView.ExternalPort)
	require.Contains(t, localView.Services, "web")
	require.Equal(t, uint32(8080), localView.Services["web"].GetPort())

	// Local node is always in PeerKeys (live component includes self).
	require.Contains(t, snap.PeerKeys, s.LocalID)
}

func TestSnapshot_IsImmutable(t *testing.T) {
	pub := genPub(t)
	s := newTestStore(pub)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)
	s.UpsertLocalService(8080, "web")

	snap1 := s.Snapshot()

	// Mutate the store after taking the snapshot.
	s.SetLocalNetwork([]string{"10.0.0.99"}, 7777)
	s.UpsertLocalService(9090, "api")
	s.RemoveLocalServices("web")

	snap2 := s.Snapshot()

	// snap1 must still reflect the original state.
	localView1 := snap1.Nodes[s.LocalID]
	require.Equal(t, []string{"10.0.0.1"}, localView1.IPs)
	require.Equal(t, uint32(9000), localView1.LocalPort)
	require.Contains(t, localView1.Services, "web")
	require.NotContains(t, localView1.Services, "api")

	// snap2 reflects the mutated state.
	localView2 := snap2.Nodes[s.LocalID]
	require.Equal(t, []string{"10.0.0.99"}, localView2.IPs)
	require.Equal(t, uint32(7777), localView2.LocalPort)
	require.Contains(t, localView2.Services, "api")
	require.NotContains(t, localView2.Services, "web")
}

func TestSnapshot_PeerKeysFiltering(t *testing.T) {
	localPub := genPub(t)
	s := newTestStore(localPub)

	peerAPub := genPub(t)
	peerAKey := types.PeerKeyFromBytes(peerAPub)

	peerBPub := genPub(t)
	peerBKey := types.PeerKeyFromBytes(peerBPub)

	peerCPub := genPub(t)
	peerCKey := types.PeerKeyFromBytes(peerCPub)

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
		Change: &statev1.GossipEvent_IdentityPub{
			IdentityPub: &statev1.IdentityChange{IdentityPub: peerAPub},
		},
	})
	s.SetLocalConnected(peerAKey, true)

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
		Change: &statev1.GossipEvent_IdentityPub{
			IdentityPub: &statev1.IdentityChange{IdentityPub: peerBPub},
		},
	})
	s.SetLocalConnected(peerBKey, true)

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
		Change: &statev1.GossipEvent_IdentityPub{
			IdentityPub: &statev1.IdentityChange{IdentityPub: peerCPub},
		},
	})

	snap := s.Snapshot()

	// PeerKeys should include local, peerA, peerB but NOT peerC (unreachable).
	require.Contains(t, snap.PeerKeys, s.LocalID)
	require.Contains(t, snap.PeerKeys, peerAKey)
	require.Contains(t, snap.PeerKeys, peerBKey)
	require.NotContains(t, snap.PeerKeys, peerCKey)

	// peerC should still appear in Nodes (it's valid, just not live).
	require.Contains(t, snap.Nodes, peerCKey)

	// Deny peerB and verify it drops from both PeerKeys and Nodes.
	s.DenyPeer(peerBPub)
	snap2 := s.Snapshot()

	require.NotContains(t, snap2.PeerKeys, peerBKey)
	require.NotContains(t, snap2.Nodes, peerBKey)

	// Original snapshot is unaffected by the deny.
	require.Contains(t, snap.PeerKeys, peerBKey)
	require.Contains(t, snap.Nodes, peerBKey)
}

func TestEvents_EmitsOnLocalMutation(t *testing.T) {
	pub := genPub(t)
	s := newTestStore(pub)

	s.SetLocalNetwork([]string{"10.0.0.1"}, 9000)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	select {
	case ev := <-s.Events():
		lma, ok := ev.(LocalMutationApplied)
		require.True(t, ok)
		require.NotEmpty(t, lma.Events)
	case <-ctx.Done():
		t.Fatal("timed out waiting for LocalMutationApplied event")
	}
}

func TestEvents_EmitsDenyApplied(t *testing.T) {
	pub := genPub(t)
	s := newTestStore(pub)

	peerPub := genPub(t)
	peerKey := types.PeerKeyFromBytes(peerPub)

	s.DenyPeer(peerPub)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// DenyPeer is a local mutation, so we may see LocalMutationApplied first.
	// Drain until we find DenyApplied (there won't be one — DenyPeer is local
	// only, not gossip-applied). Actually, DenyPeer emits LocalMutationApplied.
	// The DenyApplied event is emitted by ApplyEvents when gossip carries a deny.
	// So let's test via gossip: have another store publish a deny, apply it here.

	// First, drain the LocalMutationApplied from the local DenyPeer call.
	select {
	case ev := <-s.Events():
		_, ok := ev.(LocalMutationApplied)
		require.True(t, ok)
	case <-ctx.Done():
		t.Fatal("timed out waiting for LocalMutationApplied from DenyPeer")
	}

	// Now test DenyApplied via gossip: create a second peer that denies a third.
	peer2Pub := genPub(t)
	peer2Key := types.PeerKeyFromBytes(peer2Pub)
	_ = peerKey

	// Inject peer2 into the store so its events are accepted.
	s.applyEvent(&statev1.GossipEvent{
		PeerId:  peer2Key.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_IdentityPub{
			IdentityPub: &statev1.IdentityChange{IdentityPub: peer2Pub},
		},
	})

	// Drain gossip-applied event from above.
	drainEvents(t, s, ctx)

	// peer2 denies peerKey via gossip.
	target := genPub(t)
	targetKey := types.PeerKeyFromBytes(target)
	s.ApplyEvents([]*statev1.GossipEvent{{
		PeerId:  peer2Key.String(),
		Counter: 2,
		Change: &statev1.GossipEvent_Deny{
			Deny: &statev1.DenyChange{SubjectPub: target},
		},
	}}, false)

	// Find DenyApplied in the event stream.
	for {
		select {
		case ev := <-s.Events():
			if da, ok := ev.(DenyApplied); ok {
				require.Equal(t, targetKey, da.PeerKey)
				return
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for DenyApplied event")
		}
	}
}

func TestEvents_EmitsGossipApplied(t *testing.T) {
	pub1 := genPub(t)
	s1 := newTestStore(pub1)

	pub2 := genPub(t)
	s2 := newTestStore(pub2)

	// Store1 sets local network state.
	s1.SetLocalNetwork([]string{"10.0.0.1"}, 9000)

	// Get store1's local events and apply them to store2.
	localEvents := s1.LocalEvents()
	require.NotEmpty(t, localEvents)

	result := s2.ApplyEvents(localEvents, false)
	require.NotEmpty(t, result.Rebroadcast)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Find GossipApplied in store2's event stream.
	for {
		select {
		case ev := <-s2.Events():
			if ga, ok := ev.(GossipApplied); ok {
				require.NotEmpty(t, ga.Rebroadcast)
				return
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for GossipApplied event")
		}
	}
}

func drainEvents(t *testing.T, s *Store, ctx context.Context) {
	t.Helper()
	for {
		select {
		case <-s.Events():
		case <-ctx.Done():
			return
		default:
			return
		}
	}
}
