package state

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/netip"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func genKey(t *testing.T) types.PeerKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return types.PeerKeyFromBytes(pub)
}

func newTestStore(t *testing.T, pk types.PeerKey) *store {
	t.Helper()
	return New(pk).(*store)
}

func newTestStoreWithClock(t *testing.T, pk types.PeerKey, now *time.Time) *store {
	t.Helper()
	s := New(pk).(*store)
	s.nowFunc = func() time.Time { return *now }
	return s
}

func applyTestEvent(t *testing.T, s StateStore, ev *statev1.GossipEvent) []Event {
	t.Helper()
	batch := &statev1.GossipEventBatch{Events: []*statev1.GossipEvent{ev}}
	data, err := batch.MarshalVT()
	require.NoError(t, err)
	events, _, err := s.ApplyDelta(s.Snapshot().LocalID, data)
	require.NoError(t, err)
	return events
}

func TestStore_SnapshotIsImmutable(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)

	s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.1:9000")})
	s.SetService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP)

	snap1 := s.Snapshot()

	s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.99:7777")})
	s.SetService(9090, "api", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP)
	s.RemoveService("web")

	snap2 := s.Snapshot()

	local1 := snap1.Nodes[snap1.LocalID]
	require.Equal(t, []string{"10.0.0.1"}, local1.IPs)
	require.Equal(t, uint32(9000), local1.LocalPort)
	require.Contains(t, local1.Services, "web")
	require.NotContains(t, local1.Services, "api")

	local2 := snap2.Nodes[snap2.LocalID]
	require.Equal(t, []string{"10.0.0.99"}, local2.IPs)
	require.Equal(t, uint32(7777), local2.LocalPort)
	require.Contains(t, local2.Services, "api")
	require.NotContains(t, local2.Services, "web")
}

func TestStore_MutationsReturnEvents(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)

	events := s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.1:9000")})
	require.Len(t, events, 1)
	require.IsType(t, TopologyChanged{}, events[0])

	events = s.SetService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP)
	require.Len(t, events, 1)
	require.Equal(t, ServiceChanged{Peer: s.localID, Name: "web"}, events[0])

	events = s.SetLocalCoord(coords.Coord{X: 1.0, Y: 2.0, Height: 0.5}, 0.5)
	require.Len(t, events, 1)
	require.IsType(t, TopologyChanged{}, events[0])

	events = s.ClaimWorkload("abc123")
	require.Len(t, events, 1)
	require.Equal(t, WorkloadChanged{Hash: "abc123"}, events[0])

	targetKey := genKey(t)
	events = s.DenyPeer(targetKey)
	require.Len(t, events, 1)
	require.Equal(t, PeerDenied{Key: targetKey}, events[0])
}

func TestStore_ApplyDeltaReturnsEvents(t *testing.T) {
	pkA := genKey(t)
	storeA := newTestStore(t, pkA)
	storeA.SetService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP)
	fullData := storeA.EncodeFull()

	pkB := genKey(t)
	storeB := newTestStore(t, pkB)

	events, rebroadcast, err := storeB.ApplyDelta(pkA, fullData)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	require.NotEmpty(t, rebroadcast)

	var hasGossipApplied, hasPeerJoined, hasService bool
	for _, ev := range events {
		switch ev.(type) {
		case GossipApplied:
			hasGossipApplied = true
		case PeerJoined:
			hasPeerJoined = true
		case ServiceChanged:
			hasService = true
		}
	}

	require.True(t, hasGossipApplied)
	require.True(t, hasPeerJoined)
	require.True(t, hasService)
}

func TestSnapshot_PeerKeysFiltering(t *testing.T) {
	localKey := genKey(t)
	s := newTestStore(t, localKey)

	peerA, peerB, peerC := genKey(t), genKey(t), genKey(t)

	// Peer A is valid
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  peerA.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	})

	// Peer B is valid
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  peerB.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.3"}, LocalPort: 9002},
		},
	})

	// Peer C is expired
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  peerC.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_CertExpiry{
			CertExpiry: &statev1.CertExpiryChange{ExpiryUnix: time.Now().Add(-time.Hour).Unix()},
		},
	})

	s.SetLocalReachable([]types.PeerKey{peerA, peerB})

	snap := s.Snapshot()

	require.Contains(t, snap.PeerKeys, localKey)
	require.Contains(t, snap.PeerKeys, peerA)
	require.Contains(t, snap.PeerKeys, peerB)
	require.NotContains(t, snap.PeerKeys, peerC)

	// Deny peer B
	s.DenyPeer(peerB)
	snap2 := s.Snapshot()

	require.NotContains(t, snap2.PeerKeys, peerB)
	require.NotContains(t, snap2.Nodes, peerB)
}

func TestStore_WorkloadSpecConflict(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)

	// Local claims spec
	s.SetWorkloadSpec("contested", "contested", 3, 0, 0, 0)

	// Remote (lower peer ID) claims spec
	winnerPK := genKey(t)
	if pk.Compare(winnerPK) < 0 {
		winnerPK, pk = pk, winnerPK // Ensure winnerPK is actually lower
		s = newTestStore(t, pk)
		s.SetWorkloadSpec("contested", "contested", 3, 0, 0, 0)
	}

	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  winnerPK.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: "contested", MinReplicas: 2},
		},
	})

	specs := s.Snapshot().Specs
	require.Len(t, specs, 1)
	require.Equal(t, winnerPK, specs["contested"].Publisher)
}

func TestStore_TrafficHeatmapRoundTrip(t *testing.T) {
	pkA := genKey(t)
	storeA := newTestStore(t, pkA)

	pkB := genKey(t)
	storeB := newTestStore(t, pkB)

	peerC := genKey(t)

	storeA.SetLocalTraffic(peerC, 100, 200)

	fullState := storeA.EncodeFull()
	_, _, err := storeB.ApplyDelta(pkA, fullState)
	require.NoError(t, err)

	rates := storeB.Snapshot().Nodes[pkA].TrafficRates
	require.Len(t, rates, 1)
	require.Equal(t, TrafficSnapshot{BytesIn: 100, BytesOut: 200}, rates[peerC])
}

func TestStore_EphemeralTombstones(t *testing.T) {
	pk := genKey(t)
	s1 := newTestStore(t, pk)
	s1.SetLocalObservedAddress("1.2.3.4", 45000)
	s1.SetPublic()

	staleState := s1.EncodeFull()

	// Restart
	s2 := newTestStore(t, pk)
	require.Equal(t, uint32(0), s2.Snapshot().Nodes[s2.localID].ExternalPort)

	// Apply stale state - should trigger conflict and tombstone wins
	_, _, err := s2.ApplyDelta(pk, staleState)
	require.NoError(t, err)

	snap := s2.Snapshot()
	require.Equal(t, uint32(0), snap.Nodes[s2.localID].ExternalPort)
	require.False(t, snap.Nodes[s2.localID].PubliclyAccessible)
}

func TestExportLoadLastAddrs(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)

	peerA := genKey(t)
	peerB := genKey(t)

	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  peerA.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9002},
		},
	})

	s.SetPeerLastAddr(peerA, "10.0.0.2:9002")
	s.SetPeerLastAddr(peerB, "10.0.0.3:9003") // peerB not yet fully known, but we map it

	addrs := s.ExportLastAddrs()
	require.Equal(t, "10.0.0.2:9002", addrs[peerA])

	s2 := newTestStore(t, genKey(t))
	s2.LoadLastAddrs(addrs)

	// LoadLastAddrs only loads into known peers to avoid reviving dead peers
	applyTestEvent(t, s2, &statev1.GossipEvent{
		PeerId:  peerA.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9002},
		},
	})
	s2.LoadLastAddrs(addrs)

	snap := s2.Snapshot()
	require.Equal(t, "10.0.0.2:9002", snap.Nodes[peerA].LastAddr)
}

func TestCalculateLiveComponent_StalePeerFiltered(t *testing.T) {
	fakeNow := time.Now()
	localKey := genKey(t)
	s := newTestStoreWithClock(t, localKey, &fakeNow)

	peerA, peerB := genKey(t), genKey(t)

	// Peer A joins
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: peerA.String(), Counter: 1,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001}},
	})

	// Peer B joins
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: peerB.String(), Counter: 1,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.3"}, LocalPort: 9002}},
	})

	// Local reaches A; A claims B reachable
	s.SetLocalReachable([]types.PeerKey{peerA})
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: peerA.String(), Counter: 2,
		Change: &statev1.GossipEvent_Reachability{Reachability: &statev1.ReachabilityChange{PeerId: peerB.String()}},
	})

	snap := s.Snapshot()
	require.Contains(t, snap.PeerKeys, peerA)
	require.Contains(t, snap.PeerKeys, peerB)

	// Advance past reachableMaxAge — A's claims become stale
	fakeNow = fakeNow.Add(reachableMaxAge + time.Second)
	s.mu.Lock()
	s.updateSnapshotLocked()
	s.mu.Unlock()

	snap = s.Snapshot()
	require.Contains(t, snap.PeerKeys, localKey)
	// A is still vouched by local's reachable set
	require.Contains(t, snap.PeerKeys, peerA)
	// B is excluded: A is stale, so A's claim about B is ignored
	require.NotContains(t, snap.PeerKeys, peerB)
}

func TestCalculateLiveComponent_FreshBumpRetainsPeer(t *testing.T) {
	fakeNow := time.Now()
	localKey := genKey(t)
	s := newTestStoreWithClock(t, localKey, &fakeNow)

	peerA, peerB := genKey(t), genKey(t)

	// Setup: local -> A -> B
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: peerA.String(), Counter: 1,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001}},
	})
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: peerB.String(), Counter: 1,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.3"}, LocalPort: 9002}},
	})
	s.SetLocalReachable([]types.PeerKey{peerA})
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: peerA.String(), Counter: 2,
		Change: &statev1.GossipEvent_Reachability{Reachability: &statev1.ReachabilityChange{PeerId: peerB.String()}},
	})

	// Advance 25s, then A sends fresh event
	fakeNow = fakeNow.Add(25 * time.Second)
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: peerA.String(), Counter: 3,
		Change: &statev1.GossipEvent_Heartbeat{Heartbeat: &statev1.HeartbeatChange{}},
	})

	// Advance another 25s (50s total, but only 25s since A's last event)
	fakeNow = fakeNow.Add(25 * time.Second)
	s.mu.Lock()
	s.updateSnapshotLocked()
	s.mu.Unlock()

	snap := s.Snapshot()
	require.Contains(t, snap.PeerKeys, peerB, "A's claims should still be trusted (25s < 30s)")
}

func TestEmitHeartbeatIfNeeded_ConditionalEmission(t *testing.T) {
	fakeNow := time.Now()
	pk := genKey(t)
	s := newTestStoreWithClock(t, pk, &fakeNow)

	// Recently emitted (store construction) — no heartbeat needed
	s.EmitHeartbeatIfNeeded()
	counterBefore := s.nodes[pk].maxCounter

	// Emit a regular event to reset lastLocalEmit
	s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.1:9000")})
	counterAfterAddr := s.nodes[pk].maxCounter
	require.Greater(t, counterAfterAddr, counterBefore)

	// Still recent — no heartbeat
	s.EmitHeartbeatIfNeeded()
	require.Equal(t, counterAfterAddr, s.nodes[pk].maxCounter, "no heartbeat should be emitted")

	// Advance past heartbeat interval
	fakeNow = fakeNow.Add(HeartbeatInterval + time.Second)
	s.EmitHeartbeatIfNeeded()
	counterAfterHB := s.nodes[pk].maxCounter
	require.Greater(t, counterAfterHB, counterAfterAddr, "heartbeat should bump counter")

	// Immediately after emitting — no heartbeat needed
	s.EmitHeartbeatIfNeeded()
	require.Equal(t, counterAfterHB, s.nodes[pk].maxCounter, "no second heartbeat")
}

func TestLoadGossipState_DeniedPeersSurviveRoundTrip(t *testing.T) {
	pk := genKey(t)
	s1 := newTestStore(t, pk)

	victim := genKey(t)
	s1.DenyPeer(victim)
	require.Contains(t, s1.Snapshot().DeniedPeers(), victim)

	saved := s1.EncodeFull()

	s2 := newTestStore(t, pk)
	require.NoError(t, s2.LoadGossipState(saved))
	require.Contains(t, s2.Snapshot().DeniedPeers(), victim, "denied peer must survive LoadGossipState round-trip")
}

func TestLoadGossipState_PeersStartStale(t *testing.T) {
	fakeNow := time.Now()
	localKey := genKey(t)

	// Build a source store with a remote peer
	source := newTestStore(t, genKey(t))
	peerA := genKey(t)
	applyTestEvent(t, source, &statev1.GossipEvent{
		PeerId: peerA.String(), Counter: 1,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001}},
	})
	applyTestEvent(t, source, &statev1.GossipEvent{
		PeerId: peerA.String(), Counter: 2,
		Change: &statev1.GossipEvent_Reachability{Reachability: &statev1.ReachabilityChange{PeerId: localKey.String()}},
	})
	savedState := source.EncodeFull()

	// Create a new store and load from disk
	s := newTestStoreWithClock(t, localKey, &fakeNow)
	require.NoError(t, s.LoadGossipState(savedState))

	// Peer A exists in nodes but with zero lastEventAt — should be stale
	s.SetLocalReachable([]types.PeerKey{peerA})

	snap := s.Snapshot()
	// A is vouched by local (direct), so A is in live set
	require.Contains(t, snap.PeerKeys, peerA)

	// But A's own claims (about localKey) should be ignored because A is stale
	// A's lastEventAt is zero, so now.Sub(zero) > reachableMaxAge
	s.mu.Lock()
	rec := s.nodes[peerA]
	require.True(t, rec.lastEventAt.IsZero(), "loaded peer should have zero lastEventAt")
	s.mu.Unlock()
}

func TestSnapshot_SpecByName(t *testing.T) {
	localKey := genKey(t)
	remoteKey := genKey(t)
	s := newTestStore(t, localKey)

	// Determine which key is lower so we can assert the winner.
	lowerKey, higherKey := localKey, remoteKey
	localHash, remoteHash := "hash-local", "hash-remote"
	lowerHash := localHash
	if remoteKey.Compare(localKey) < 0 {
		lowerKey, higherKey = remoteKey, localKey
		lowerHash = remoteHash
	}
	_ = higherKey

	// Local peer publishes spec with name "myapp" and hash "hash-local".
	s.SetWorkloadSpec("myapp", localHash, 1, 0, 0, 0)

	// Remote peer publishes spec with same name but different hash.
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  remoteKey.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	})
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  remoteKey.String(),
		Counter: 2,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: remoteHash, Name: "myapp", MinReplicas: 1},
		},
	})

	// Make remote peer visible in the live set.
	s.SetLocalReachable([]types.PeerKey{remoteKey})

	snap := s.Snapshot()

	// Both specs should exist in the map (different hashes).
	require.Contains(t, snap.Specs, localHash)
	require.Contains(t, snap.Specs, remoteHash)

	hash, view, found := snap.SpecByName("myapp")
	require.True(t, found)
	require.Equal(t, lowerKey, view.Publisher, "SpecByName should return the spec from the peer with the lower PeerKey")
	require.Equal(t, lowerHash, hash)

	// Non-existent name returns not-found.
	_, _, found = snap.SpecByName("nonexistent")
	require.False(t, found)
}

func TestSnapshot_LocalSpecByName(t *testing.T) {
	localKey := genKey(t)
	remoteKey := genKey(t)
	s := newTestStore(t, localKey)

	localHash, remoteHash := "hash-local", "hash-remote"

	// Local peer publishes spec.
	s.SetWorkloadSpec("myapp", localHash, 1, 0, 0, 0)

	// Remote peer publishes spec with the same name but different hash.
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  remoteKey.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	})
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  remoteKey.String(),
		Counter: 2,
		Change: &statev1.GossipEvent_WorkloadSpec{
			WorkloadSpec: &statev1.WorkloadSpecChange{Hash: remoteHash, Name: "myapp", MinReplicas: 1},
		},
	})

	s.SetLocalReachable([]types.PeerKey{remoteKey})

	snap := s.Snapshot()

	// LocalSpecByName should return only the local peer's hash.
	hash, found := snap.LocalSpecByName("myapp", localKey)
	require.True(t, found)
	require.Equal(t, localHash, hash)

	// Querying with the remote key returns the remote hash.
	hash, found = snap.LocalSpecByName("myapp", remoteKey)
	require.True(t, found)
	require.Equal(t, remoteHash, hash)

	// Non-existent name returns not-found.
	_, found = snap.LocalSpecByName("nonexistent", localKey)
	require.False(t, found)

	// Unknown peer returns not-found.
	unknownKey := genKey(t)
	_, found = snap.LocalSpecByName("myapp", unknownKey)
	require.False(t, found)
}

func TestSetNodeName(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)

	// Setting a name appears in snapshot.
	s.SetNodeName("sams-laptop")
	snap := s.Snapshot()
	require.Equal(t, "sams-laptop", snap.Nodes[pk].Name)

	// Idempotent: same name produces no new gossip.
	before := s.nodes[pk].maxCounter
	s.SetNodeName("sams-laptop")
	require.Equal(t, before, s.nodes[pk].maxCounter)

	// Changing the name bumps the counter.
	s.SetNodeName("work-machine")
	snap = s.Snapshot()
	require.Equal(t, "work-machine", snap.Nodes[pk].Name)
	require.Greater(t, s.nodes[pk].maxCounter, before)

	// Empty string tombstones the name.
	s.SetNodeName("")
	snap = s.Snapshot()
	require.Equal(t, "", snap.Nodes[pk].Name)

	// Tombstoning again is idempotent.
	counter := s.nodes[pk].maxCounter
	s.SetNodeName("")
	require.Equal(t, counter, s.nodes[pk].maxCounter)
}

func TestNodeNameGossip(t *testing.T) {
	pkA, pkB := genKey(t), genKey(t)
	storeA := newTestStore(t, pkA)
	storeB := newTestStore(t, pkB)

	storeA.SetNodeName("node-alpha")
	data := storeA.EncodeFull()
	_, _, err := storeB.ApplyDelta(pkA, data)
	require.NoError(t, err)

	snap := storeB.Snapshot()
	require.Equal(t, "node-alpha", snap.Nodes[pkA].Name)
}

func TestNodeNameSelfConflictAdopted(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)

	// Simulate a remote peer claiming we have a name we don't know about.
	ev := &statev1.GossipEvent{
		PeerId:  pk.String(),
		Counter: 100,
		Change:  &statev1.GossipEvent_NodeName{NodeName: &statev1.NodeNameChange{Name: "recovered-name"}},
	}
	batch := &statev1.GossipEventBatch{Events: []*statev1.GossipEvent{ev}}
	data, err := batch.MarshalVT()
	require.NoError(t, err)
	_, _, err = s.ApplyDelta(pk, data)
	require.NoError(t, err)

	// The name should be adopted (persistent attr).
	snap := s.Snapshot()
	require.Equal(t, "recovered-name", snap.Nodes[pk].Name)
}
