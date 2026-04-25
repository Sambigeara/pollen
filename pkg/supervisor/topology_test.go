// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"fmt"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/peercache"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// --- Reachability Tests ---

func TestInferPrivatelyRoutable(t *testing.T) {
	require.True(t, membership.InferPrivatelyRoutable([]string{"10.2.1.10"}, []string{"10.2.2.20"}))
	require.False(t, membership.InferPrivatelyRoutable([]string{"10.2.1.10"}, []string{"10.3.2.20"}))
	require.True(t, membership.InferPrivatelyRoutable([]string{"192.168.1.10"}, []string{"192.168.200.20"}))
	require.True(t, membership.InferPrivatelyRoutable([]string{"172.16.1.10"}, []string{"172.16.200.20"}))
	require.False(t, membership.InferPrivatelyRoutable([]string{"172.16.1.10"}, []string{"172.17.200.20"}))
	require.False(t, membership.InferPrivatelyRoutable([]string{"10.2.1.10"}, []string{"203.0.113.7"}))
}

func TestInferReachability(t *testing.T) {
	require.Equal(t, reachabilityPublicDirect, inferReachability([]string{"10.2.1.10"}, []string{"203.0.113.7"}, true))
	require.Equal(t, reachabilitySameSitePrivate, inferReachability([]string{"10.2.1.10"}, []string{"10.2.9.20"}, false))
	require.Equal(t, reachabilityUnknown, inferReachability([]string{"10.2.1.10"}, []string{"10.3.9.20"}, false))
}

// --- Topology Shape & Parameters ---

func makeKnownPeers(public, sameSitePrivate, remotePrivate int) ([]string, []knownPeer) {
	localIPs := []string{"10.1.0.1"}
	peers := make([]knownPeer, 0, public+sameSitePrivate+remotePrivate)
	id := byte(1)

	for range public {
		peers = append(peers, knownPeer{
			PeerID:             testPeerKey(id),
			IPs:                []string{fmt.Sprintf("203.0.113.%d", id)},
			PubliclyAccessible: true,
		})
		id++
	}
	for range sameSitePrivate {
		peers = append(peers, knownPeer{
			PeerID: testPeerKey(id),
			IPs:    []string{fmt.Sprintf("10.1.0.%d", id)},
		})
		id++
	}
	for range remotePrivate {
		peers = append(peers, knownPeer{
			PeerID: testPeerKey(id),
			IPs:    []string{fmt.Sprintf("10.2.0.%d", id)},
		})
		id++
	}
	return localIPs, peers
}

func TestSummarizeTopologyShape(t *testing.T) {
	localIPs, peers := makeKnownPeers(5, 3, 2)
	s := summarizeTopologyShape(localIPs, peers)

	require.Equal(t, 10, s.totalCount)
	require.Equal(t, 5, s.publicCount)
	require.Equal(t, 3, s.sameSitePrivateCount)
	require.Equal(t, 2, s.remotePrivateCount)
}

func TestAdaptiveParamsMixed(t *testing.T) {
	localIPs, peers := makeKnownPeers(6, 3, 3)
	shape := summarizeTopologyShape(localIPs, peers)
	p := adaptiveTopologyParams(1, shape)

	require.Equal(t, 3, p.NearestK)
	require.Equal(t, 2, p.RandomR)
	require.Equal(t, membership.DefaultInfraMax, p.InfraMax)
}

func TestAdaptiveParamsPrivateHeavy(t *testing.T) {
	localIPs, peers := makeKnownPeers(2, 10, 5)
	shape := summarizeTopologyShape(localIPs, peers)
	p := adaptiveTopologyParams(1, shape)

	require.Equal(t, membership.DefaultNearestK, p.NearestK)
	require.Equal(t, membership.DefaultRandomR, p.RandomR)
	require.Equal(t, membership.DefaultInfraMax, p.InfraMax)
}

func testPeerKey(b byte) types.PeerKey {
	var pk types.PeerKey
	pk[0] = b
	return pk
}

// --- knownPeers merge ---

func newTestCache(t *testing.T) *peercache.Store {
	t.Helper()
	cache, err := peercache.Open(t.TempDir())
	require.NoError(t, err)
	return cache
}

func TestKnownPeersStateOnly(t *testing.T) {
	self := testPeerKey(1)
	peer := testPeerKey(2)
	snap := state.Snapshot{
		LocalID:  self,
		PeerKeys: []types.PeerKey{self, peer},
		Nodes: map[types.PeerKey]state.NodeView{
			peer: {
				IPs:       []string{"10.0.0.2"},
				LocalPort: 60611,
			},
		},
	}

	got := knownPeers(snap, newTestCache(t))

	require.Len(t, got, 1)
	require.Equal(t, peer, got[0].PeerID)
	require.Equal(t, []string{"10.0.0.2"}, got[0].IPs)
	require.Equal(t, uint32(60611), got[0].LocalPort)
}

func TestKnownPeersCacheOnly(t *testing.T) {
	self := testPeerKey(1)
	peer := testPeerKey(2)
	snap := state.Snapshot{
		LocalID:  self,
		PeerKeys: []types.PeerKey{self},
	}
	cache := newTestCache(t)
	cache.Upsert(peer, []string{"192.168.0.220:60611", "203.0.113.7:60611"}, time.Now())

	got := knownPeers(snap, cache)

	require.Len(t, got, 1)
	require.Equal(t, peer, got[0].PeerID)
	require.ElementsMatch(t, []string{"192.168.0.220", "203.0.113.7"}, got[0].IPs)
	require.Equal(t, uint32(60611), got[0].LocalPort)
	require.NotEmpty(t, got[0].LastAddr)
}

func TestKnownPeersStateAndCacheUnion(t *testing.T) {
	self := testPeerKey(1)
	peer := testPeerKey(2)
	snap := state.Snapshot{
		LocalID:  self,
		PeerKeys: []types.PeerKey{self, peer},
		Nodes: map[types.PeerKey]state.NodeView{
			peer: {
				IPs:       []string{"10.0.0.2"},
				LocalPort: 60611,
				LastAddr:  "203.0.113.7:60611",
			},
		},
	}
	cache := newTestCache(t)
	cache.Upsert(peer, []string{"192.168.0.220:60611", "10.0.0.2:60611"}, time.Now())

	got := knownPeers(snap, cache)

	require.Len(t, got, 1)
	require.ElementsMatch(t, []string{"10.0.0.2", "192.168.0.220"}, got[0].IPs)
	require.Equal(t, "203.0.113.7:60611", got[0].LastAddr, "state LastAddr wins over cache fallback")
}

func TestKnownPeersDeniedFiltered(t *testing.T) {
	self := testPeerKey(1)
	peer := testPeerKey(2)
	snap := state.Snapshot{
		LocalID:    self,
		PeerKeys:   []types.PeerKey{self},
		DeniedKeys: []types.PeerKey{peer},
	}
	cache := newTestCache(t)
	cache.Upsert(peer, []string{"192.168.0.220:60611"}, time.Now())

	got := knownPeers(snap, cache)

	require.Empty(t, got)
}

func TestKnownPeersStalePortDroppedFromCache(t *testing.T) {
	self := testPeerKey(1)
	peer := testPeerKey(2)
	snap := state.Snapshot{
		LocalID:  self,
		PeerKeys: []types.PeerKey{self, peer},
		Nodes: map[types.PeerKey]state.NodeView{
			peer: {
				IPs:       []string{"10.0.0.2"},
				LocalPort: 60611,
			},
		},
	}
	cache := newTestCache(t)
	cache.Upsert(peer, []string{"192.168.0.220:12345"}, time.Now())

	got := knownPeers(snap, cache)

	require.Len(t, got, 1)
	require.Equal(t, []string{"10.0.0.2"}, got[0].IPs, "cache addr on different port is stale and ignored")
}
