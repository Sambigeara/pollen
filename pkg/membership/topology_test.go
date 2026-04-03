package membership

import (
	"math"
	"testing"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestComputeTargetPeersEmpty(t *testing.T) {
	result := ComputeTargetPeers(peerKey(0), coords.Coord{}, nil, DefaultParams(1))
	require.Empty(t, result)
}

func TestComputeTargetPeersSinglePeer(t *testing.T) {
	peers := []PeerInfo{{Key: peerKey(1), Coord: &coords.Coord{X: 1}}}
	params := DefaultParams(1)
	params.LocalNATType = nat.Easy
	result := ComputeTargetPeers(peerKey(0), coords.Coord{}, peers, params)
	require.Len(t, result, 1)
	require.Equal(t, peerKey(1), result[0])
}

func TestComputeTargetPeersTwoPeers(t *testing.T) {
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &coords.Coord{X: 1}},
		{Key: peerKey(2), Coord: &coords.Coord{X: 2}},
	}
	params := DefaultParams(1)
	params.LocalNATType = nat.Easy
	result := ComputeTargetPeers(peerKey(0), coords.Coord{}, peers, params)
	require.Len(t, result, 2)
}

func TestInfraSelection(t *testing.T) {
	local := peerKey(0)
	peers := []PeerInfo{
		{Key: peerKey(1), PubliclyAccessible: true},
		{Key: peerKey(2), PubliclyAccessible: true},
		{Key: peerKey(3), PubliclyAccessible: true},
		{Key: peerKey(4), PubliclyAccessible: true},
		{Key: peerKey(5)},
	}
	result := selectInfra(local, peers, 3)
	require.Equal(t, []types.PeerKey{peerKey(3), peerKey(4), peerKey(2)}, result)
}

func TestInfraCapRespected(t *testing.T) {
	local := peerKey(0)
	peers := []PeerInfo{
		{Key: peerKey(1), PubliclyAccessible: true},
		{Key: peerKey(2), PubliclyAccessible: true},
	}
	result := selectInfra(local, peers, 3)
	require.Len(t, result, 2)
}

func TestInfraDistribution(t *testing.T) {
	relays := []PeerInfo{
		{Key: peerKey(101), PubliclyAccessible: true},
		{Key: peerKey(102), PubliclyAccessible: true},
		{Key: peerKey(103), PubliclyAccessible: true},
		{Key: peerKey(104), PubliclyAccessible: true},
	}

	selectionCount := make(map[types.PeerKey]int)
	for i := byte(1); i <= 20; i++ {
		local := peerKey(i)
		result := selectInfra(local, relays, 2)
		for _, pk := range result {
			selectionCount[pk]++
		}
	}

	for _, relay := range relays {
		require.Less(t, selectionCount[relay.Key], 20, "relay %v selected by all 20 nodes — no distribution", relay.Key)
		require.Greater(t, selectionCount[relay.Key], 2, "relay %v selected by too few nodes — poor distribution", relay.Key)
	}
}

func TestNearestByDistance(t *testing.T) {
	local := coords.Coord{X: 0, Y: 0}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &coords.Coord{X: 100, Y: 0}},
		{Key: peerKey(2), Coord: &coords.Coord{X: 10, Y: 0}},
		{Key: peerKey(3), Coord: &coords.Coord{X: 50, Y: 0}},
		{Key: peerKey(4), Coord: &coords.Coord{X: 1, Y: 0}},
	}
	exclude := map[types.PeerKey]struct{}{}
	result := selectNearest(peerKey(0), local, peers, exclude, Params{NearestK: 2, LocalNATType: nat.Easy})
	require.Len(t, result, 2)
	require.Equal(t, peerKey(4), result[0])
	require.Equal(t, peerKey(2), result[1])
}

func TestNearestHysteresisKeepsIncumbent(t *testing.T) {
	local := coords.Coord{X: 0, Y: 0}
	incumbent := peerKey(1)
	challenger := peerKey(2)
	peers := []PeerInfo{
		{Key: incumbent, Coord: &coords.Coord{X: 50, Y: 0}},
		{Key: challenger, Coord: &coords.Coord{X: 45, Y: 0}},
	}
	outbound := map[types.PeerKey]struct{}{incumbent: {}}
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 1, CurrentOutbound: outbound, LocalNATType: nat.Easy})
	require.Len(t, result, 1)
	require.Equal(t, incumbent, result[0])
}

func TestNearestHysteresisAllowsDisplacement(t *testing.T) {
	local := coords.Coord{X: 0, Y: 0}
	incumbent := peerKey(1)
	challenger := peerKey(2)
	peers := []PeerInfo{
		{Key: incumbent, Coord: &coords.Coord{X: 50, Y: 0}},
		{Key: challenger, Coord: &coords.Coord{X: 35, Y: 0}},
	}
	outbound := map[types.PeerKey]struct{}{incumbent: {}}
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 1, CurrentOutbound: outbound, LocalNATType: nat.Easy})
	require.Len(t, result, 1)
	require.Equal(t, challenger, result[0])
}

func TestNearestHysteresisFloorForClosePeers(t *testing.T) {
	local := coords.Coord{X: 0, Y: 0}
	incumbent := peerKey(1)
	challenger := peerKey(2)
	peers := []PeerInfo{
		{Key: incumbent, Coord: &coords.Coord{X: 3, Y: 0}},
		{Key: challenger, Coord: &coords.Coord{X: 2, Y: 0}},
	}
	outbound := map[types.PeerKey]struct{}{incumbent: {}}
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 1, CurrentOutbound: outbound, LocalNATType: nat.Easy})
	require.Len(t, result, 1)
	require.Equal(t, incumbent, result[0])
}

func TestNearestNilCoordFallback(t *testing.T) {
	local := coords.Coord{X: 0, Y: 0}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: nil},
		{Key: peerKey(2), Coord: &coords.Coord{X: 10, Y: 0}},
		{Key: peerKey(3), Coord: nil},
	}
	exclude := map[types.PeerKey]struct{}{}
	result := selectNearest(peerKey(0), local, peers, exclude, Params{NearestK: 3, LocalNATType: nat.Easy})
	require.Len(t, result, 3)
	require.Equal(t, peerKey(2), result[0])
}

func TestNearestLANDiversityCap(t *testing.T) {
	local := coords.Coord{X: 0, Y: 0}
	lanA := []string{"192.168.1.10"}
	lanB := []string{"192.168.2.10"}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &coords.Coord{X: 1}, IPs: lanA},
		{Key: peerKey(2), Coord: &coords.Coord{X: 2}, IPs: lanA},
		{Key: peerKey(3), Coord: &coords.Coord{X: 3}, IPs: lanA},
		{Key: peerKey(4), Coord: &coords.Coord{X: 4}, IPs: lanA},
		{Key: peerKey(5), Coord: &coords.Coord{X: 5}, IPs: lanB},
		{Key: peerKey(6), Coord: &coords.Coord{X: 6}, IPs: lanB},
		{Key: peerKey(7), Coord: &coords.Coord{X: 7}, IPs: lanB},
	}
	exclude := map[types.PeerKey]struct{}{}
	result := selectNearest(peerKey(0), local, peers, exclude, Params{NearestK: 4, LocalNATType: nat.Easy})
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

	r1 := selectLongLinks(local, peers, exclude, Params{RandomR: 2, Epoch: 1, LocalNATType: nat.Easy})
	r2 := selectLongLinks(local, peers, exclude, Params{RandomR: 2, Epoch: 1, LocalNATType: nat.Easy})
	require.Equal(t, r1, r2)
}

func TestLongLinksEpochRotation(t *testing.T) {
	local := peerKey(0)
	peers := make([]PeerInfo, 20)
	for i := range peers {
		peers[i] = PeerInfo{Key: peerKey(byte(i + 1))}
	}
	exclude := map[types.PeerKey]struct{}{}

	r1 := selectLongLinks(local, peers, exclude, Params{RandomR: 2, Epoch: 1, LocalNATType: nat.Easy})
	r2 := selectLongLinks(local, peers, exclude, Params{RandomR: 2, Epoch: 2, LocalNATType: nat.Easy})
	require.NotEqual(t, r1, r2, "long-links should change across epochs")
}

func TestNoCrossLayerDuplicates(t *testing.T) {
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &coords.Coord{X: 1}, PubliclyAccessible: true},
		{Key: peerKey(2), Coord: &coords.Coord{X: 2}, PubliclyAccessible: true},
		{Key: peerKey(3), Coord: &coords.Coord{X: 3}},
		{Key: peerKey(4), Coord: &coords.Coord{X: 4}},
		{Key: peerKey(5), Coord: &coords.Coord{X: 5}},
	}
	params := DefaultParams(1)
	params.LocalNATType = nat.Easy
	result := ComputeTargetPeers(peerKey(0), coords.Coord{}, peers, params)

	seen := make(map[types.PeerKey]struct{})
	for _, pk := range result {
		_, dup := seen[pk]
		require.False(t, dup, "duplicate peer in target set: %s", pk)
		seen[pk] = struct{}{}
	}
}

func TestAllNilCoordsMigration(t *testing.T) {
	peers := []PeerInfo{{Key: peerKey(3)}, {Key: peerKey(1)}, {Key: peerKey(2)}}
	local := coords.Coord{}
	exclude := map[types.PeerKey]struct{}{}
	result := selectNearest(peerKey(0), local, peers, exclude, Params{NearestK: 3, LocalNATType: nat.Easy})
	require.Len(t, result, 3)
	require.Equal(t, peerKey(1), result[0])
	require.Equal(t, peerKey(2), result[1])
	require.Equal(t, peerKey(3), result[2])
}

func TestNearestHMACFallback(t *testing.T) {
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &coords.Coord{X: 1}},
		{Key: peerKey(2), Coord: &coords.Coord{X: 2}},
		{Key: peerKey(3), Coord: &coords.Coord{X: 100}},
		{Key: peerKey(4), Coord: &coords.Coord{X: 200}},
		{Key: peerKey(5), Coord: &coords.Coord{X: 300}},
	}
	local := coords.Coord{}
	exclude := map[types.PeerKey]struct{}{}

	distResult := selectNearest(peerKey(0), local, peers, exclude, Params{NearestK: 2, LocalNATType: nat.Easy})
	require.Len(t, distResult, 2)
	require.Equal(t, peerKey(1), distResult[0])
	require.Equal(t, peerKey(2), distResult[1])

	hmacResult := selectNearest(peerKey(0), local, peers, exclude, Params{NearestK: 2, UseHMACNearest: true, Epoch: 1, LocalNATType: nat.Easy})
	require.Len(t, hmacResult, 2)

	hmacResult2 := selectNearest(peerKey(0), local, peers, exclude, Params{NearestK: 2, UseHMACNearest: true, Epoch: 1, LocalNATType: nat.Easy})
	require.Equal(t, hmacResult, hmacResult2)
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
	a := coords.Coord{X: 10, Y: 0, Height: 0}
	b := coords.Coord{X: 20, Y: 0, Height: 0}
	origin := coords.Coord{}
	require.Less(t, coords.Distance(origin, a), coords.Distance(origin, b))
}

func TestNilVsZeroCoord(t *testing.T) {
	zero := &coords.Coord{}
	origin := coords.Coord{}
	d := coords.Distance(origin, *zero)
	require.InDelta(t, 0.0, d, 1e-12, "zero coord should have zero distance from origin")

	require.Equal(t, math.MaxFloat64, func() float64 {
		var c *coords.Coord
		if c != nil {
			return coords.Distance(origin, *c)
		}
		return math.MaxFloat64
	}())
}

func TestNearestSkipsHardHardPairs(t *testing.T) {
	local := coords.Coord{X: 0, Y: 0}
	localIPs := []string{"10.0.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &coords.Coord{X: 1}, IPs: []string{"10.0.2.10"}, NatType: nat.Hard},
		{Key: peerKey(2), Coord: &coords.Coord{X: 2}, IPs: []string{"10.0.2.20"}, NatType: nat.Unknown},
	}
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 2, LocalNATType: nat.Hard, LocalIPs: localIPs})
	require.Len(t, result, 1)
	require.Equal(t, peerKey(2), result[0])
}

func TestNearestUnknownLocalAllowsAll(t *testing.T) {
	local := coords.Coord{X: 0, Y: 0}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &coords.Coord{X: 1}, NatType: nat.Hard},
		{Key: peerKey(2), Coord: &coords.Coord{X: 2}, NatType: nat.Unknown},
	}
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 2})
	require.Len(t, result, 2)
}

func TestNearestEasyLocalKeepsAll(t *testing.T) {
	local := coords.Coord{X: 0, Y: 0}
	localIPs := []string{"10.0.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &coords.Coord{X: 1}, IPs: []string{"172.16.0.10"}, NatType: nat.Unknown},
		{Key: peerKey(2), Coord: &coords.Coord{X: 2}, IPs: []string{"172.17.0.20"}, NatType: nat.Hard},
		{Key: peerKey(3), Coord: &coords.Coord{X: 3}, IPs: []string{"172.18.0.30"}, NatType: nat.Easy},
	}
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 3, LocalNATType: nat.Easy, LocalIPs: localIPs})
	require.Len(t, result, 3)
}

func TestNearestHardLocalKeepsUnknownAndEasy(t *testing.T) {
	local := coords.Coord{X: 0, Y: 0}
	localIPs := []string{"10.0.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &coords.Coord{X: 1}, IPs: []string{"172.16.0.10"}, NatType: nat.Unknown},
		{Key: peerKey(2), Coord: &coords.Coord{X: 2}, IPs: []string{"172.17.0.20"}, NatType: nat.Easy},
	}
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 2, LocalNATType: nat.Hard, LocalIPs: localIPs})
	require.Len(t, result, 2)
}

func TestNearestKeepsHardHardLAN(t *testing.T) {
	local := coords.Coord{X: 0, Y: 0}
	localIPs := []string{"192.168.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &coords.Coord{X: 1}, IPs: []string{"192.168.1.10"}, NatType: nat.Hard},
	}
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 1, LocalNATType: nat.Hard, LocalIPs: localIPs})
	require.Len(t, result, 1)
	require.Equal(t, peerKey(1), result[0])
}

func TestNearestLANBypassesNATFilterBothUnknown(t *testing.T) {
	local := coords.Coord{X: 0, Y: 0}
	localIPs := []string{"192.168.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &coords.Coord{X: 1}, IPs: []string{"192.168.1.10"}, NatType: nat.Unknown},
	}
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 1, LocalNATType: nat.Unknown, LocalIPs: localIPs})
	require.Len(t, result, 1)
	require.Equal(t, peerKey(1), result[0])
}

func TestLongLinksSkipsNonEasyPairs(t *testing.T) {
	local := peerKey(0)
	localIPs := []string{"10.0.1.5"}
	peers := make([]PeerInfo, 5)
	for i := range peers {
		peers[i] = PeerInfo{Key: peerKey(byte(i + 1)), IPs: []string{"10.0.2.10"}, NatType: nat.Hard}
	}
	result := selectLongLinks(local, peers, nil, Params{RandomR: 2, Epoch: 1, LocalNATType: nat.Hard, LocalIPs: localIPs})
	require.Empty(t, result)
}

func TestLongLinksKeepsUnknownWhenLocalHard(t *testing.T) {
	local := peerKey(0)
	localIPs := []string{"10.0.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), IPs: []string{"10.0.2.10"}, NatType: nat.Unknown},
		{Key: peerKey(2), IPs: []string{"10.0.2.20"}, NatType: nat.Easy},
	}
	result := selectLongLinks(local, peers, nil, Params{RandomR: 2, Epoch: 1, LocalNATType: nat.Hard, LocalIPs: localIPs})
	require.Len(t, result, 2)
}

func TestLongLinksKeepsEasyPeers(t *testing.T) {
	local := peerKey(0)
	localIPs := []string{"10.0.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), IPs: []string{"10.0.2.10"}, NatType: nat.Easy},
		{Key: peerKey(2), IPs: []string{"10.0.2.20"}, NatType: nat.Hard},
	}
	result := selectLongLinks(local, peers, nil, Params{RandomR: 2, Epoch: 1, LocalNATType: nat.Hard, LocalIPs: localIPs})
	require.Len(t, result, 1)
	require.Equal(t, peerKey(1), result[0])
}

func TestLongLinksEasyLocalKeepsAll(t *testing.T) {
	local := peerKey(0)
	localIPs := []string{"10.0.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), IPs: []string{"10.0.2.10"}, NatType: nat.Unknown},
		{Key: peerKey(2), IPs: []string{"10.0.2.20"}, NatType: nat.Easy},
		{Key: peerKey(3), IPs: []string{"10.0.2.30"}, NatType: nat.Hard},
	}
	result := selectLongLinks(local, peers, nil, Params{RandomR: 3, Epoch: 1, LocalNATType: nat.Easy, LocalIPs: localIPs})
	require.Len(t, result, 3)
}

func TestComputeTargetPeersPreferFullMeshTargetsAllFeasiblePeers(t *testing.T) {
	local := peerKey(0)
	params := Params{
		LocalIPs:       []string{"81.108.176.99", "192.168.0.203"},
		LocalNATType:   nat.Unknown,
		PreferFullMesh: true,
	}
	peers := []PeerInfo{
		{Key: peerKey(1), IPs: []string{"18.135.80.64"}, PubliclyAccessible: true},
		{Key: peerKey(2), IPs: []string{"192.168.0.24"}},
		{Key: peerKey(3), IPs: []string{"192.168.0.220"}},
	}

	result := ComputeTargetPeers(local, coords.Coord{}, peers, params)
	require.Equal(t, []types.PeerKey{peerKey(1), peerKey(2), peerKey(3)}, result)
}

func TestComputeTargetPeersPreferFullMeshSkipsHardHardNonLAN(t *testing.T) {
	local := peerKey(0)
	params := Params{
		LocalIPs:       []string{"10.1.2.10"},
		LocalNATType:   nat.Hard,
		PreferFullMesh: true,
	}
	peers := []PeerInfo{
		{Key: peerKey(1), IPs: []string{"10.2.2.10"}, NatType: nat.Hard},
		{Key: peerKey(2), IPs: []string{"3.250.216.148"}, PubliclyAccessible: true},
	}

	result := ComputeTargetPeers(local, coords.Coord{}, peers, params)
	require.Equal(t, []types.PeerKey{peerKey(2)}, result)
}

func TestFullMeshAllowsUnknownUnknownNonLAN(t *testing.T) {
	local := peerKey(0)
	params := Params{
		LocalIPs:       []string{"10.1.2.10"},
		LocalNATType:   nat.Unknown,
		PreferFullMesh: true,
	}
	peers := []PeerInfo{
		{Key: peerKey(1), IPs: []string{"10.1.3.10"}, NatType: nat.Unknown},
	}

	result := ComputeTargetPeers(local, coords.Coord{}, peers, params)
	require.Equal(t, []types.PeerKey{peerKey(1)}, result)
}
