package topology

import (
	"math"
	"testing"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func peerKey(b byte) types.PeerKey {
	var pk types.PeerKey
	pk[0] = b
	return pk
}

func TestComputeTargetPeersEmpty(t *testing.T) {
	result := ComputeTargetPeers(peerKey(0), Coord{}, nil, DefaultParams(1))
	require.Empty(t, result)
}

func TestComputeTargetPeersSinglePeer(t *testing.T) {
	peers := []PeerInfo{{Key: peerKey(1), Coord: &Coord{X: 1}}}
	result := ComputeTargetPeers(peerKey(0), Coord{}, peers, DefaultParams(1))
	require.Len(t, result, 1)
	require.Equal(t, peerKey(1), result[0])
}

func TestComputeTargetPeersTwoPeers(t *testing.T) {
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}},
		{Key: peerKey(2), Coord: &Coord{X: 2}},
	}
	result := ComputeTargetPeers(peerKey(0), Coord{}, peers, DefaultParams(1))
	require.Len(t, result, 2)
}

func TestInfraSelection(t *testing.T) {
	peers := []PeerInfo{
		{Key: peerKey(1), PubliclyAccessible: true},
		{Key: peerKey(2), PubliclyAccessible: true},
		{Key: peerKey(3), PubliclyAccessible: true},
		{Key: peerKey(4), PubliclyAccessible: true},
		{Key: peerKey(5)},
	}
	result := selectInfra(peers, 3)
	require.Len(t, result, 3)
	// Should be sorted by PeerKey, so the three smallest.
	require.Equal(t, peerKey(1), result[0])
	require.Equal(t, peerKey(2), result[1])
	require.Equal(t, peerKey(3), result[2])
}

func TestInfraCapRespected(t *testing.T) {
	peers := []PeerInfo{
		{Key: peerKey(1), PubliclyAccessible: true},
		{Key: peerKey(2), PubliclyAccessible: true},
	}
	result := selectInfra(peers, 3)
	require.Len(t, result, 2)
}

func TestNearestByDistance(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 100, Y: 0}}, // dist ~100
		{Key: peerKey(2), Coord: &Coord{X: 10, Y: 0}},  // dist ~10
		{Key: peerKey(3), Coord: &Coord{X: 50, Y: 0}},  // dist ~50
		{Key: peerKey(4), Coord: &Coord{X: 1, Y: 0}},   // dist ~1
	}
	exclude := map[types.PeerKey]struct{}{}
	result := selectNearest(local, peers, exclude, 2)
	require.Len(t, result, 2)
	require.Equal(t, peerKey(4), result[0])
	require.Equal(t, peerKey(2), result[1])
}

func TestNearestNilCoordFallback(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: nil},                 // max distance
		{Key: peerKey(2), Coord: &Coord{X: 10, Y: 0}}, // dist ~10
		{Key: peerKey(3), Coord: nil},                 // max distance
	}
	exclude := map[types.PeerKey]struct{}{}
	result := selectNearest(local, peers, exclude, 3)
	require.Len(t, result, 3)
	require.Equal(t, peerKey(2), result[0])
}

func TestNearestLANDiversityCap(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	// 4 peers on LAN A, 3 on LAN B. K=4, cap=ceil(4/2)=2 per LAN.
	lanA := []string{"192.168.1.10"}
	lanB := []string{"192.168.2.10"}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}, IPs: lanA},
		{Key: peerKey(2), Coord: &Coord{X: 2}, IPs: lanA},
		{Key: peerKey(3), Coord: &Coord{X: 3}, IPs: lanA},
		{Key: peerKey(4), Coord: &Coord{X: 4}, IPs: lanA},
		{Key: peerKey(5), Coord: &Coord{X: 5}, IPs: lanB},
		{Key: peerKey(6), Coord: &Coord{X: 6}, IPs: lanB},
		{Key: peerKey(7), Coord: &Coord{X: 7}, IPs: lanB},
	}
	exclude := map[types.PeerKey]struct{}{}
	result := selectNearest(local, peers, exclude, 4)
	require.Len(t, result, 4)

	lanACount := 0
	for _, pk := range result {
		if pk == peerKey(1) || pk == peerKey(2) || pk == peerKey(3) || pk == peerKey(4) {
			lanACount++
		}
	}
	require.LessOrEqual(t, lanACount, 2, "at most ceil(k/2) from any single LAN")
}

func TestLongLinksDeterministic(t *testing.T) {
	local := peerKey(0)
	peers := []PeerInfo{
		{Key: peerKey(1)},
		{Key: peerKey(2)},
		{Key: peerKey(3)},
		{Key: peerKey(4)},
		{Key: peerKey(5)},
	}
	exclude := map[types.PeerKey]struct{}{}

	r1 := selectLongLinks(local, peers, exclude, 2, 1)
	r2 := selectLongLinks(local, peers, exclude, 2, 1)
	require.Equal(t, r1, r2)
}

func TestLongLinksEpochRotation(t *testing.T) {
	local := peerKey(0)
	peers := make([]PeerInfo, 20)
	for i := range peers {
		peers[i] = PeerInfo{Key: peerKey(byte(i + 1))}
	}
	exclude := map[types.PeerKey]struct{}{}

	r1 := selectLongLinks(local, peers, exclude, 2, 1)
	r2 := selectLongLinks(local, peers, exclude, 2, 2)
	// With 20 candidates and 2 selections, different epochs should usually
	// produce different selections.
	require.NotEqual(t, r1, r2, "long-links should change across epochs")
}

func TestNoCrossLayerDuplicates(t *testing.T) {
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}, PubliclyAccessible: true},
		{Key: peerKey(2), Coord: &Coord{X: 2}, PubliclyAccessible: true},
		{Key: peerKey(3), Coord: &Coord{X: 3}},
		{Key: peerKey(4), Coord: &Coord{X: 4}},
		{Key: peerKey(5), Coord: &Coord{X: 5}},
	}
	result := ComputeTargetPeers(peerKey(0), Coord{}, peers, DefaultParams(1))

	seen := make(map[types.PeerKey]struct{})
	for _, pk := range result {
		_, dup := seen[pk]
		require.False(t, dup, "duplicate peer in target set: %s", pk)
		seen[pk] = struct{}{}
	}
}

func TestAllNilCoordsMigration(t *testing.T) {
	// When all coords are nil, k-nearest degrades to deterministic key ordering.
	peers := []PeerInfo{
		{Key: peerKey(3)},
		{Key: peerKey(1)},
		{Key: peerKey(2)},
	}
	local := Coord{}
	exclude := map[types.PeerKey]struct{}{}
	result := selectNearest(local, peers, exclude, 3)
	require.Len(t, result, 3)
	// All have distance MaxFloat64, so sorted by PeerKey.
	require.Equal(t, peerKey(1), result[0])
	require.Equal(t, peerKey(2), result[1])
	require.Equal(t, peerKey(3), result[2])
}

func TestLanPrefix(t *testing.T) {
	tests := []struct {
		name  string
		ips   []string
		empty bool
	}{
		{"private ipv4", []string{"192.168.1.10"}, false},
		{"private ipv6", []string{"fd00::1"}, false},
		{"public ipv4", []string{"8.8.8.8"}, false},
		{"public ipv6", []string{"2001:db8::1"}, false},
		{"loopback", []string{"127.0.0.1"}, true},
		{"unspecified", []string{"0.0.0.0"}, true},
		{"no ips", nil, true},
		{"invalid", []string{"not-an-ip"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := lanPrefix(tt.ips)
			if tt.empty {
				require.Empty(t, prefix)
			} else {
				require.NotEmpty(t, prefix)
			}
		})
	}
}

func TestLanPrefixSameLAN(t *testing.T) {
	a := lanPrefix([]string{"192.168.1.10"})
	b := lanPrefix([]string{"192.168.1.20"})
	require.Equal(t, a, b, "same /24 should produce same prefix")

	c := lanPrefix([]string{"192.168.2.10"})
	require.NotEqual(t, a, c, "different /24 should produce different prefix")
}

func TestLanPrefixPublicSubnet(t *testing.T) {
	a := lanPrefix([]string{"35.200.1.10"})
	b := lanPrefix([]string{"35.200.1.20"})
	require.Equal(t, a, b, "same public /24 should produce same prefix")

	c := lanPrefix([]string{"35.200.2.10"})
	require.NotEqual(t, a, c, "different public /24 should produce different prefix")
}

func TestDistanceUsedForSorting(t *testing.T) {
	a := Coord{X: 10, Y: 0, Height: 0}
	b := Coord{X: 20, Y: 0, Height: 0}
	origin := Coord{}
	require.Less(t, Distance(origin, a), Distance(origin, b))
}

func TestNilVsZeroCoord(t *testing.T) {
	zero := &Coord{}
	origin := Coord{}
	d := Distance(origin, *zero)
	require.InDelta(t, 0.0, d, 1e-12, "zero coord should have zero distance from origin")

	// nil coord should be treated as max distance.
	require.Equal(t, math.MaxFloat64, func() float64 {
		var c *Coord
		if c != nil {
			return Distance(origin, *c)
		}
		return math.MaxFloat64
	}())
}
