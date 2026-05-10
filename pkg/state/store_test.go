// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func genKey(t *testing.T) types.PeerKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return types.PeerKeyFromBytes(pub)
}

// newTestStore builds a store keyed to pk and treats pk itself as the
// cluster root. Tests that need a distinct rootPub (deny-scoping
// fixtures) pass it explicitly.
func newTestStore(t *testing.T, pk types.PeerKey, rootPub ...[]byte) *store {
	t.Helper()
	root := pk.Bytes()
	if len(rootPub) > 0 {
		root = rootPub[0]
	}
	s := New(pk, root).(*store)
	s.SetLocalSigner(fakeSpecSigner{pub: pk.Bytes()})
	return s
}

func newTestStoreWithClock(t *testing.T, pk types.PeerKey, now *time.Time) *store {
	t.Helper()
	s := New(pk, pk.Bytes()).(*store)
	s.nowFunc = func() time.Time { return *now }
	s.SetLocalSigner(fakeSpecSigner{pub: pk.Bytes()})
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

func authBy(pub types.PeerKey) *admissionv1.SpecAuth {
	return &admissionv1.SpecAuth{Publisher: &admissionv1.DelegationCert{Claims: &admissionv1.DelegationCertClaims{SubjectPub: pub.Bytes()}}}
}

func workloadSpecChange(pub types.PeerKey, spec *statev1.WorkloadSpecChange) *statev1.GossipEvent_SpecChange {
	return &statev1.GossipEvent_SpecChange{SpecChange: &statev1.SpecChange{Auth: authBy(pub), Body: &statev1.SpecChange_Workload{Workload: spec}}}
}

func serviceSpecChange(pub types.PeerKey, spec *statev1.ServiceChange) *statev1.GossipEvent_SpecChange {
	return &statev1.GossipEvent_SpecChange{SpecChange: &statev1.SpecChange{Auth: authBy(pub), Body: &statev1.SpecChange_Service{Service: spec}}}
}

func staticSpecChange(pub types.PeerKey, spec *statev1.StaticSpecChange) *statev1.GossipEvent_SpecChange {
	return &statev1.GossipEvent_SpecChange{SpecChange: &statev1.SpecChange{Auth: authBy(pub), Body: &statev1.SpecChange_Static{Static: spec}}}
}

func blobSpecChange(pub types.PeerKey, spec *statev1.BlobSpecChange) *statev1.GossipEvent_SpecChange {
	return &statev1.GossipEvent_SpecChange{SpecChange: &statev1.SpecChange{Auth: authBy(pub), Body: &statev1.SpecChange_Blob{Blob: spec}}}
}

type fakeSpecSigner struct{ pub []byte }

func (s fakeSpecSigner) IssueSpecAuth(resource *admissionv1.ResourceID, body auth.SpecBody, policy *admissionv1.Predicate, deleted bool) (*admissionv1.SpecAuth, error) {
	return &admissionv1.SpecAuth{
		Resource:  resource,
		Policy:    policy,
		BodyHash:  bytes.Repeat([]byte{0x42}, sha256Len),
		Publisher: &admissionv1.DelegationCert{Claims: &admissionv1.DelegationCertClaims{SubjectPub: s.pub}},
		Deleted:   deleted,
	}, nil
}

func TestStore_SnapshotIsImmutable(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)

	s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.1:9000")})
	_, _ = s.SetService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil)

	snap1 := s.Snapshot()

	s.SetLocalAddresses([]netip.AddrPort{netip.MustParseAddrPort("10.0.0.99:7777")})
	_, _ = s.SetService(9090, "api", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil)
	_, _ = s.RemoveService("web")

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
	require.Equal(t, []Event{TopologyChanged{Peer: s.localID}, AddressesChanged{Peer: s.localID}}, events)

	events = s.SetLocalObservedAddress("203.0.113.1", 9000)
	require.Equal(t, []Event{TopologyChanged{Peer: s.localID}, AddressesChanged{Peer: s.localID}}, events)

	events, _ = s.SetService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil)
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
	_, _ = storeA.SetService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil)
	fullData := storeA.EncodeFull()

	pkB := genKey(t)
	storeB := newTestStore(t, pkB)

	events, rebroadcast, err := storeB.ApplyDelta(pkA, fullData)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	require.NotEmpty(t, rebroadcast)

	var hasPeerJoined, hasService bool
	for _, ev := range events {
		switch ev.(type) {
		case PeerJoined:
			hasPeerJoined = true
		case ServiceChanged:
			hasService = true
		}
	}

	require.True(t, hasPeerJoined)
	require.True(t, hasService)
}

func TestSnapshot_PeerKeysFiltering(t *testing.T) {
	localKey := genKey(t)
	s := newTestStore(t, localKey)

	peerA, peerB := genKey(t), genKey(t)

	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  peerA.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
		},
	})
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  peerB.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Network{
			Network: &statev1.NetworkChange{Ips: []string{"10.0.0.3"}, LocalPort: 9002},
		},
	})

	s.SetLocalReachable([]types.PeerKey{peerA, peerB})

	snap := s.Snapshot()
	require.Contains(t, snap.PeerKeys, localKey)
	require.Contains(t, snap.PeerKeys, peerA)
	require.Contains(t, snap.PeerKeys, peerB)

	s.DenyPeer(peerB)
	snap2 := s.Snapshot()
	require.NotContains(t, snap2.PeerKeys, peerB)
	require.NotContains(t, snap2.Nodes, peerB)
}

func TestSnapshot_PeersWithBlobSkipsOfflineHolders(t *testing.T) {
	localKey := genKey(t)
	s := newTestStore(t, localKey)

	live, offline := genKey(t), genKey(t)
	for _, pk := range []types.PeerKey{live, offline} {
		applyTestEvent(t, s, &statev1.GossipEvent{
			PeerId:  pk.String(),
			Counter: 1,
			Change: &statev1.GossipEvent_Network{
				Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001},
			},
		})
		applyTestEvent(t, s, &statev1.GossipEvent{
			PeerId:  pk.String(),
			Counter: 2,
			Change: &statev1.GossipEvent_BlobAvailability{
				BlobAvailability: &statev1.BlobAvailabilityChange{Digests: [][]byte{{0xab, 0xcd}}},
			},
		})
	}

	// Only `live` is reachable from local; `offline` stays valid (cert OK)
	// but falls out of the live set, mirroring a peer that has departed
	// without its BlobAvailability gossip being superseded.
	s.SetLocalReachable([]types.PeerKey{live})
	snap := s.Snapshot()

	require.Contains(t, snap.PeerKeys, live)
	require.NotContains(t, snap.PeerKeys, offline)
	require.Contains(t, snap.Nodes, offline, "offline peer remains in snap.Nodes (cert still valid)")

	holders := snap.PeersWithBlob("abcd")
	require.ElementsMatch(t, []types.PeerKey{live}, holders)
}

func TestStore_PublishWorkloadClonesInput(t *testing.T) {
	// PublishWorkload must defensively copy its input — callers (e.g.
	// placement.Service.Seed) reuse the same proto and may mutate it
	// after the call. The persisted gossip state must not change in
	// response to such caller-side mutation.
	s := newTestStore(t, genKey(t))
	hash := strings.Repeat("a", 64)
	spec := WorkloadSpec{Hash: hash, Name: "abc", MinReplicas: 2}

	_, _ = s.PublishWorkload(spec, nil)
	spec.MinReplicas = 99 // simulate caller reusing or mutating the proto

	snap := s.Snapshot()
	got, ok := snap.Specs[hash]
	require.True(t, ok)
	require.Equal(t, uint32(2), got.Spec.MinReplicas, "store should not see caller-side mutation")
}

func TestStore_WorkloadSpecConflict(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)
	hash := strings.Repeat("a", 64)

	// Local claims spec
	_, _ = s.PublishWorkload(WorkloadSpec{Name: "contested", Hash: hash, MinReplicas: 3}, nil)

	// Remote (lower peer ID) claims spec
	winnerPK := genKey(t)
	if pk.Compare(winnerPK) < 0 {
		winnerPK, pk = pk, winnerPK // Ensure winnerPK is actually lower
		s = newTestStore(t, pk)
		_, _ = s.PublishWorkload(WorkloadSpec{Name: "contested", Hash: hash, MinReplicas: 3}, nil)
	}

	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  winnerPK.String(),
		Counter: 1,
		Change:  workloadSpecChange(winnerPK, &statev1.WorkloadSpecChange{Hash: hash, MinReplicas: 2}),
	})

	specs := s.Snapshot().Specs
	require.Len(t, specs, 1)
	require.Equal(t, winnerPK, specs[hash].Publisher)
}

func TestStore_PublishWorkloadIssuesSpecAuth(t *testing.T) {
	pk := genKey(t)
	s := newTestStore(t, pk)
	s.SetLocalSigner(fakeSpecSigner{pub: pk.Bytes()})
	policy := &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{
		{Key: "role", Equals: "worker"},
	}}}

	hash := strings.Repeat("a", 64)
	_, err := s.PublishWorkload(WorkloadSpec{Name: "echo", Hash: hash, MinReplicas: 1}, policy)
	require.NoError(t, err)

	view := s.Snapshot().Specs[hash]
	require.NotNil(t, view.Auth)
	require.True(t, proto.Equal(policy, view.Auth.GetPolicy()))
	require.Equal(t, hash, hex.EncodeToString(view.Auth.GetResource().GetSeed().GetHash()))
}

func TestStore_ApplyDeltaRejectsInvalidSpecAuth(t *testing.T) {
	pk := genKey(t)
	remote := genKey(t)
	s := newTestStore(t, pk)
	s.SetMutationValidator(func(*statev1.SpecChange) error { return errors.New("reject") })

	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  remote.String(),
		Counter: 1,
		Change:  workloadSpecChange(remote, &statev1.WorkloadSpecChange{Hash: strings.Repeat("b", 64), Name: "echo", MinReplicas: 1}),
	})

	require.Empty(t, s.Snapshot().Specs)
}

func TestStore_RejectsSpecAuthFromDifferentPeerSlot(t *testing.T) {
	local := genKey(t)
	remote := genKey(t)
	other := genKey(t)
	s := newTestStore(t, local)
	sc := &statev1.SpecChange{
		Auth: &admissionv1.SpecAuth{Publisher: &admissionv1.DelegationCert{Claims: &admissionv1.DelegationCertClaims{SubjectPub: other.Bytes()}}},
		Body: &statev1.SpecChange_Workload{Workload: &statev1.WorkloadSpecChange{Hash: strings.Repeat("c", 64), Name: "echo", MinReplicas: 1}},
	}

	applyTestEvent(t, s, &statev1.GossipEvent{PeerId: remote.String(), Counter: 1, Change: &statev1.GossipEvent_SpecChange{SpecChange: sc}})

	require.Empty(t, s.Snapshot().Specs)
}

func TestStore_RejectsForgedSelfSpecFromAttacker(t *testing.T) {
	local := genKey(t)
	other := genKey(t)
	s := newTestStore(t, local)
	s.SetMutationValidator(func(sc *statev1.SpecChange) error {
		if !bytes.Equal(sc.GetAuth().GetPublisher().GetClaims().GetSubjectPub(), local.Bytes()) {
			return errors.New("publisher does not match local")
		}
		return nil
	})

	hash := strings.Repeat("a", 64)
	sc := &statev1.SpecChange{
		Auth: &admissionv1.SpecAuth{Publisher: &admissionv1.DelegationCert{Claims: &admissionv1.DelegationCertClaims{SubjectPub: other.Bytes()}}},
		Body: &statev1.SpecChange_Workload{Workload: &statev1.WorkloadSpecChange{Hash: hash, Name: "myapp", MinReplicas: 1}},
	}

	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  local.String(),
		Counter: 1,
		Change:  &statev1.GossipEvent_SpecChange{SpecChange: sc},
	})

	require.Empty(t, s.Snapshot().Specs, "self-conflict path must validate forged spec events")
}

func TestStore_RejectsSelfConflictCounterInflation(t *testing.T) {
	local := genKey(t)
	victim := genKey(t)
	s := newTestStore(t, local)

	_, _ = s.SetService(8080, "web", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil)
	_, _ = s.SetService(9090, "api", statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP, nil)
	beforeCounter := s.nodes[local].maxCounter

	// Self-Deny events from peers are filtered by acceptableSelfEventLocked,
	// so adoption is skipped. Without the overflow guard, the inflation
	// block would still bump rec.maxCounter to ev.Counter and overflow
	// during the rebroadcast loop.
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  local.String(),
		Counter: ^uint64(0) - 1,
		Change:  &statev1.GossipEvent_Deny{Deny: &statev1.DenyChange{PeerPub: victim.Bytes()}},
	})

	require.Equal(t, beforeCounter, s.nodes[local].maxCounter, "near-MaxUint64 self-conflict counter must not advance our maxCounter")
}

func TestStore_RejectsForgedSelfDenyFromAttacker(t *testing.T) {
	local := genKey(t)
	victim := genKey(t)
	s := newTestStore(t, local)

	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  local.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_Deny{Deny: &statev1.DenyChange{
			PeerPub: victim.Bytes(),
		}},
	})

	require.NotContains(t, s.Snapshot().DeniedKeys, victim, "self-conflict path must not adopt a deny event under our peer-id")
}

func TestStore_RejectsForgedSelfCertFromAttacker(t *testing.T) {
	local := genKey(t)
	other := genKey(t)
	s := newTestStore(t, local)

	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId:  local.String(),
		Counter: 1,
		Change: &statev1.GossipEvent_DelegationCert{DelegationCert: &statev1.DelegationCertChange{
			Cert: &admissionv1.DelegationCert{Claims: &admissionv1.DelegationCertClaims{SubjectPub: other.Bytes()}},
		}},
	})

	require.Nil(t, s.Snapshot().Nodes[local].Cert, "self-conflict path must reject foreign-subject cert events")
}

func TestStore_RejectsRemoteSpecTombstone(t *testing.T) {
	local := genKey(t)
	remote := genKey(t)
	s := newTestStore(t, local)
	hash := strings.Repeat("d", 64)
	sc := &statev1.SpecChange{
		Auth: &admissionv1.SpecAuth{Publisher: &admissionv1.DelegationCert{Claims: &admissionv1.DelegationCertClaims{SubjectPub: remote.Bytes()}}},
		Body: &statev1.SpecChange_Workload{Workload: &statev1.WorkloadSpecChange{Hash: hash, Name: "echo", MinReplicas: 1}},
	}
	applyTestEvent(t, s, &statev1.GossipEvent{PeerId: remote.String(), Counter: 1, Change: &statev1.GossipEvent_SpecChange{SpecChange: sc}})
	require.Contains(t, s.Snapshot().Specs, hash)

	applyTestEvent(t, s, &statev1.GossipEvent{PeerId: remote.String(), Counter: 2, Deleted: true, Change: workloadSpecChange(remote, &statev1.WorkloadSpecChange{Hash: hash})})

	require.Contains(t, s.Snapshot().Specs, hash)
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
	require.Equal(t, TrafficSnapshot{RateIn: 100, RateOut: 200}, rates[peerC])
}

func TestStore_EphemeralTombstones(t *testing.T) {
	pk := genKey(t)
	s1 := newTestStore(t, pk)
	s1.SetLocalObservedAddress("1.2.3.4", 45000)
	s1.SetPublic()

	staleState := s1.EncodeFull()

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
	localHash, remoteHash := strings.Repeat("a", 64), strings.Repeat("b", 64)
	lowerHash := localHash
	if remoteKey.Compare(localKey) < 0 {
		lowerKey, higherKey = remoteKey, localKey
		lowerHash = remoteHash
	}
	_ = higherKey

	// Local peer publishes spec.
	_, _ = s.PublishWorkload(WorkloadSpec{Name: "myapp", Hash: localHash, MinReplicas: 1}, nil)

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
		Change:  workloadSpecChange(remoteKey, &statev1.WorkloadSpecChange{Hash: remoteHash, Name: "myapp", MinReplicas: 1}),
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

	localHash, remoteHash := strings.Repeat("a", 64), strings.Repeat("b", 64)

	// Local peer publishes spec.
	_, _ = s.PublishWorkload(WorkloadSpec{Name: "myapp", Hash: localHash, MinReplicas: 1}, nil)

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
		Change:  workloadSpecChange(remoteKey, &statev1.WorkloadSpecChange{Hash: remoteHash, Name: "myapp", MinReplicas: 1}),
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

func TestEncodeDelta_IncludesOfflinePeers(t *testing.T) {
	fakeNow := time.Now()
	localKey := genKey(t)
	s := newTestStoreWithClock(t, localKey, &fakeNow)

	offlinePeer := genKey(t)

	// Offline peer publishes a workload spec via gossip.
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: offlinePeer.String(), Counter: 1,
		Change: &statev1.GossipEvent_Network{Network: &statev1.NetworkChange{Ips: []string{"10.0.0.2"}, LocalPort: 9001}},
	})
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: offlinePeer.String(), Counter: 2,
		Change: workloadSpecChange(offlinePeer, &statev1.WorkloadSpecChange{Hash: "seed-abc", MinReplicas: 1}),
	})

	s.SetLocalReachable([]types.PeerKey{offlinePeer})

	// Peer goes offline: local can no longer reach it.
	s.SetLocalReachable(nil)

	// A new node with an empty digest asks for state. EncodeDelta should
	// include the offline peer's events (no live-set filtering).
	emptyDigest := Digest{proto: &statev1.Digest{}}
	delta := s.EncodeDelta(emptyDigest)
	var batch statev1.GossipEventBatch
	require.NoError(t, batch.UnmarshalVT(delta))
	var foundOfflinePeer bool
	for _, ev := range batch.Events {
		if ev.PeerId == offlinePeer.String() {
			foundOfflinePeer = true
			break
		}
	}
	require.True(t, foundOfflinePeer, "EncodeDelta must include events from offline peer")

	// When the requester's digest already has the offline peer's counters,
	// EncodeDelta should not redundantly re-send its events.
	upToDateDigest := s.Snapshot().Digest()
	delta = s.EncodeDelta(upToDateDigest)
	var upToDateBatch statev1.GossipEventBatch
	require.NoError(t, upToDateBatch.UnmarshalVT(delta))
	for _, ev := range upToDateBatch.Events {
		require.NotEqual(t, offlinePeer.String(), ev.PeerId,
			"EncodeDelta should skip offline peer when requester digest is up-to-date")
	}
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

// TestSpecs_DeniedPublisherEvicted covers the rule that a denied
// publisher's specs drop out of the snapshot. The publisher's gossip
// log is preserved so deny scoping can still walk the chain, but the
// gate must not surface a resource whose only publisher has lost
// authority.
func TestSpecs_DeniedPublisherEvicted(t *testing.T) {
	publisher := genKey(t)
	s := newTestStore(t, genKey(t))

	digest := make([]byte, sha256Len)
	seedEvents := []*statev1.GossipEvent{
		{Change: serviceSpecChange(publisher, &statev1.ServiceChange{Name: "web", Port: 8080})},
		{Change: workloadSpecChange(publisher, &statev1.WorkloadSpecChange{Hash: "wl-1", Name: "myapp", MinReplicas: 2})},
		{Change: staticSpecChange(publisher, &statev1.StaticSpecChange{Name: "site.example", ManifestDigest: digest})},
		{Change: blobSpecChange(publisher, &statev1.BlobSpecChange{Name: "manifest", Digest: digest})},
	}
	for i, ev := range seedEvents {
		ev.PeerId = publisher.String()
		ev.Counter = uint64(i + 1)
		applyTestEvent(t, s, ev)
	}
	require.Contains(t, s.Snapshot().Specs, "wl-1", "spec must be visible before publisher is denied")

	s.DenyPeer(publisher)

	snap := s.Snapshot()
	require.NotContains(t, snap.Specs, "wl-1", "denied publisher's workload spec must drop from snapshot")
	require.NotContains(t, snap.StaticSpecs, "site.example", "denied publisher's static spec must drop from snapshot")
	require.Empty(t, snap.BlobSpecs, "denied publisher's blob spec must drop from snapshot")
	require.NotContains(t, snap.Nodes, publisher, "denied publisher's peer-local state must be gone")
}

func TestSpecs_RemoteTombstoneRejectedKeepsWinner(t *testing.T) {
	s := newTestStore(t, genKey(t))

	// Two remote peers both publish "site.example". The lower peer wins
	// the cluster map slot.
	peerA, peerB := genKey(t), genKey(t)
	if peerA.Compare(peerB) > 0 {
		peerA, peerB = peerB, peerA
	}
	digestA, digestB := make([]byte, sha256Len), make([]byte, sha256Len)
	digestA[0], digestB[0] = 0x01, 0x02

	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: peerA.String(), Counter: 1,
		Change: staticSpecChange(peerA, &statev1.StaticSpecChange{Name: "site.example", ManifestDigest: digestA}),
	})
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: peerB.String(), Counter: 1,
		Change: staticSpecChange(peerB, &statev1.StaticSpecChange{Name: "site.example", ManifestDigest: digestB}),
	})

	require.Equal(t, peerA, s.Snapshot().StaticSpecs["site.example"].Publisher)

	// A tombstone whose SpecAuth.deleted does not match ev.Deleted is rejected
	// at admission, so an attacker cannot replay a published SpecAuth wrapped
	// in a tombstone envelope to unseed the spec.
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: peerA.String(), Counter: 2, Deleted: true,
		Change: staticSpecChange(peerA, &statev1.StaticSpecChange{Name: "site.example"}),
	})

	snap := s.Snapshot()
	require.Contains(t, snap.StaticSpecs, "site.example")
	require.Equal(t, peerA, snap.StaticSpecs["site.example"].Publisher)
	require.Equal(t, hex.EncodeToString(digestA), snap.StaticSpecs["site.example"].Spec.ManifestDigest)
}

func TestSpecs_SignedRemoteTombstonePropagates(t *testing.T) {
	publisher := genKey(t)
	src := newTestStore(t, publisher)

	digest := make([]byte, sha256Len)
	digest[0] = 0x01
	_, err := src.SetStaticSpec(StaticSpec{Name: "site.example", ManifestDigest: hex.EncodeToString(digest)}, nil)
	require.NoError(t, err)

	dst := newTestStore(t, genKey(t))
	_, _, err = dst.ApplyDelta(publisher, src.EncodeFull())
	require.NoError(t, err)
	require.Contains(t, dst.Snapshot().StaticSpecs, "site.example")

	_, err = src.DeleteStaticSpec("site.example")
	require.NoError(t, err)

	_, _, err = dst.ApplyDelta(publisher, src.EncodeFull())
	require.NoError(t, err)
	require.NotContains(t, dst.Snapshot().StaticSpecs, "site.example")
}

func TestSpecs_ValidPublisherOutranksInvalid(t *testing.T) {
	s := newTestStore(t, genKey(t))

	// stale has the lower peer key — under the old tiebreak it would win.
	// Once denied, stale loses to fresh regardless of peer-key ordering.
	stale, fresh := genKey(t), genKey(t)
	if stale.Compare(fresh) > 0 {
		stale, fresh = fresh, stale
	}
	staleDigest, freshDigest := make([]byte, sha256Len), make([]byte, sha256Len)
	staleDigest[0], freshDigest[0] = 0x01, 0x02

	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: stale.String(), Counter: 1,
		Change: staticSpecChange(stale, &statev1.StaticSpecChange{Name: "site.example", ManifestDigest: staleDigest}),
	})
	s.DenyPeer(stale)
	applyTestEvent(t, s, &statev1.GossipEvent{
		PeerId: fresh.String(), Counter: 1,
		Change: staticSpecChange(fresh, &statev1.StaticSpecChange{Name: "site.example", ManifestDigest: freshDigest}),
	})

	snap := s.Snapshot()
	require.Equal(t, fresh, snap.StaticSpecs["site.example"].Publisher,
		"valid publisher should outrank denied one regardless of peer key")
	require.Equal(t, hex.EncodeToString(freshDigest), snap.StaticSpecs["site.example"].Spec.ManifestDigest)
}

func TestSpecs_SurviveLoadGossipState(t *testing.T) {
	publisher := genKey(t)
	src := newTestStore(t, genKey(t))

	digest := make([]byte, sha256Len)
	events := []*statev1.GossipEvent{
		{Change: workloadSpecChange(publisher, &statev1.WorkloadSpecChange{Hash: "wl-1", Name: "myapp", MinReplicas: 2})},
		{Change: staticSpecChange(publisher, &statev1.StaticSpecChange{Name: "site.example", ManifestDigest: digest})},
		{Change: blobSpecChange(publisher, &statev1.BlobSpecChange{Name: "manifest", Digest: digest})},
	}
	for i, ev := range events {
		ev.PeerId = publisher.String()
		ev.Counter = uint64(i + 1)
		applyTestEvent(t, src, ev)
	}
	saved := src.EncodeFull()

	restored := newTestStore(t, genKey(t))
	require.NoError(t, restored.LoadGossipState(saved))

	snap := restored.Snapshot()
	require.Contains(t, snap.Specs, "wl-1")
	require.Contains(t, snap.StaticSpecs, "site.example")
	require.Len(t, snap.BlobSpecs, 1)
}
