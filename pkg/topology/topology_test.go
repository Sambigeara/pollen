package topology

import (
	"math"
	"testing"

	"github.com/sambigeara/pollen/pkg/nat"
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
	params := DefaultParams(1)
	params.LocalNATType = nat.Easy
	result := ComputeTargetPeers(peerKey(0), Coord{}, peers, params)
	require.Len(t, result, 1)
	require.Equal(t, peerKey(1), result[0])
}

func TestComputeTargetPeersTwoPeers(t *testing.T) {
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}},
		{Key: peerKey(2), Coord: &Coord{X: 2}},
	}
	params := DefaultParams(1)
	params.LocalNATType = nat.Easy
	result := ComputeTargetPeers(peerKey(0), Coord{}, peers, params)
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
	// HMAC(peerKey(0), "infra", _) deterministic order: 3, 4, 2, 1.
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
		require.Less(t, selectionCount[relay.Key], 20,
			"relay %v selected by all 20 nodes — no distribution", relay.Key)
		require.Greater(t, selectionCount[relay.Key], 2,
			"relay %v selected by too few nodes — poor distribution", relay.Key)
	}
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
	result := selectNearest(peerKey(0), local, peers, exclude, Params{NearestK: 2, LocalNATType: nat.Easy})
	require.Len(t, result, 2)
	require.Equal(t, peerKey(4), result[0])
	require.Equal(t, peerKey(2), result[1])
}

func TestNearestHysteresisKeepsIncumbent(t *testing.T) {
	// Incumbent at distance 50, challenger at 45 (only 10% closer).
	// Discount = max(50*0.2, 5) = 10, so incumbent ranks as 40 — beats challenger.
	local := Coord{X: 0, Y: 0}
	incumbent := peerKey(1)
	challenger := peerKey(2)
	peers := []PeerInfo{
		{Key: incumbent, Coord: &Coord{X: 50, Y: 0}},
		{Key: challenger, Coord: &Coord{X: 45, Y: 0}},
	}
	outbound := map[types.PeerKey]struct{}{incumbent: {}}
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 1, CurrentOutbound: outbound, LocalNATType: nat.Easy})
	require.Len(t, result, 1)
	require.Equal(t, incumbent, result[0])
}

func TestNearestHysteresisAllowsDisplacement(t *testing.T) {
	// Incumbent at distance 50, challenger at 35 (30% closer).
	// Discount = max(50*0.2, 5) = 10, so incumbent ranks as 40 — challenger at 35 wins.
	local := Coord{X: 0, Y: 0}
	incumbent := peerKey(1)
	challenger := peerKey(2)
	peers := []PeerInfo{
		{Key: incumbent, Coord: &Coord{X: 50, Y: 0}},
		{Key: challenger, Coord: &Coord{X: 35, Y: 0}},
	}
	outbound := map[types.PeerKey]struct{}{incumbent: {}}
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 1, CurrentOutbound: outbound, LocalNATType: nat.Easy})
	require.Len(t, result, 1)
	require.Equal(t, challenger, result[0])
}

func TestNearestHysteresisFloorForClosePeers(t *testing.T) {
	// Two peers very close: incumbent at distance 3, challenger at 2.
	// Without floor: discount = 3*0.2 = 0.6, incumbent ranks as 2.4 — challenger wins (bad, causes churn).
	// With floor:    discount = max(0.6, 5) = 5, incumbent ranks as 0 — incumbent kept.
	local := Coord{X: 0, Y: 0}
	incumbent := peerKey(1)
	challenger := peerKey(2)
	peers := []PeerInfo{
		{Key: incumbent, Coord: &Coord{X: 3, Y: 0}},
		{Key: challenger, Coord: &Coord{X: 2, Y: 0}},
	}
	outbound := map[types.PeerKey]struct{}{incumbent: {}}
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 1, CurrentOutbound: outbound, LocalNATType: nat.Easy})
	require.Len(t, result, 1)
	require.Equal(t, incumbent, result[0])
}

func TestNearestNilCoordFallback(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: nil},                 // max distance
		{Key: peerKey(2), Coord: &Coord{X: 10, Y: 0}}, // dist ~10
		{Key: peerKey(3), Coord: nil},                 // max distance
	}
	exclude := map[types.PeerKey]struct{}{}
	result := selectNearest(peerKey(0), local, peers, exclude, Params{NearestK: 3, LocalNATType: nat.Easy})
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
	params := DefaultParams(1)
	params.LocalNATType = nat.Easy
	result := ComputeTargetPeers(peerKey(0), Coord{}, peers, params)

	seen := make(map[types.PeerKey]struct{})
	for _, pk := range result {
		_, dup := seen[pk]
		require.False(t, dup, "duplicate peer in target set: %s", pk)
		seen[pk] = struct{}{}
	}
}

func TestAllNilCoordsMigration(t *testing.T) {
	// When all coords are nil and coord error is low, k-nearest degrades to
	// deterministic key ordering (all at MaxFloat64 distance).
	peers := []PeerInfo{
		{Key: peerKey(3)},
		{Key: peerKey(1)},
		{Key: peerKey(2)},
	}
	local := Coord{}
	exclude := map[types.PeerKey]struct{}{}
	result := selectNearest(peerKey(0), local, peers, exclude, Params{NearestK: 3, LocalNATType: nat.Easy})
	require.Len(t, result, 3)
	require.Equal(t, peerKey(1), result[0])
	require.Equal(t, peerKey(2), result[1])
	require.Equal(t, peerKey(3), result[2])
}

func TestNearestHMACFallback(t *testing.T) {
	// With UseHMACNearest, selectNearest should use HMAC scoring instead of
	// distance, producing stable, spread-out selection.
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}},
		{Key: peerKey(2), Coord: &Coord{X: 2}},
		{Key: peerKey(3), Coord: &Coord{X: 100}},
		{Key: peerKey(4), Coord: &Coord{X: 200}},
		{Key: peerKey(5), Coord: &Coord{X: 300}},
	}
	local := Coord{}
	exclude := map[types.PeerKey]struct{}{}

	// Distance-based: peerKey(1) and peerKey(2) win.
	distResult := selectNearest(peerKey(0), local, peers, exclude, Params{NearestK: 2, LocalNATType: nat.Easy})
	require.Len(t, distResult, 2)
	require.Equal(t, peerKey(1), distResult[0])
	require.Equal(t, peerKey(2), distResult[1])

	// HMAC mode: result is deterministic but not necessarily distance-ordered.
	hmacResult := selectNearest(peerKey(0), local, peers, exclude, Params{NearestK: 2, UseHMACNearest: true, Epoch: 1, LocalNATType: nat.Easy})
	require.Len(t, hmacResult, 2)

	// Run again with same params — should be deterministic.
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

// --- NAT filter tests ---
// The NAT filter blocks only Hard+Hard for non-LAN pairs.

func TestNearestSkipsHardHardPairs(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	localIPs := []string{"10.0.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}, IPs: []string{"10.0.2.10"}, NatType: nat.Hard},
		{Key: peerKey(2), Coord: &Coord{X: 2}, IPs: []string{"10.0.2.20"}, NatType: nat.Unknown},
	}
	// Local is Hard — Hard+Hard skipped, but Hard+Unknown allowed (might work).
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 2, LocalNATType: nat.Hard, LocalIPs: localIPs})
	require.Len(t, result, 1)
	require.Equal(t, peerKey(2), result[0])
}

func TestNearestUnknownLocalAllowsAll(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}, NatType: nat.Hard},
		{Key: peerKey(2), Coord: &Coord{X: 2}, NatType: nat.Unknown},
	}
	// Local is Unknown — neither Unknown+Hard nor Unknown+Unknown is Hard+Hard, so both pass.
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 2})
	require.Len(t, result, 2)
}

func TestNearestEasyLocalKeepsAll(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	localIPs := []string{"10.0.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}, IPs: []string{"172.16.0.10"}, NatType: nat.Unknown},
		{Key: peerKey(2), Coord: &Coord{X: 2}, IPs: []string{"172.17.0.20"}, NatType: nat.Hard},
		{Key: peerKey(3), Coord: &Coord{X: 3}, IPs: []string{"172.18.0.30"}, NatType: nat.Easy},
	}
	// Local is Easy — all remotes kept (at least one side is Easy).
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 3, LocalNATType: nat.Easy, LocalIPs: localIPs})
	require.Len(t, result, 3)
}

func TestNearestHardLocalKeepsUnknownAndEasy(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	localIPs := []string{"10.0.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}, IPs: []string{"172.16.0.10"}, NatType: nat.Unknown},
		{Key: peerKey(2), Coord: &Coord{X: 2}, IPs: []string{"172.17.0.20"}, NatType: nat.Easy},
	}
	// Local is Hard: Unknown and Easy remotes both kept (only Hard+Hard is blocked).
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 2, LocalNATType: nat.Hard, LocalIPs: localIPs})
	require.Len(t, result, 2)
}

func TestNearestKeepsHardHardLAN(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	localIPs := []string{"192.168.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}, IPs: []string{"192.168.1.10"}, NatType: nat.Hard},
	}
	// Both Hard but same LAN prefix → NOT skipped.
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 1, LocalNATType: nat.Hard, LocalIPs: localIPs})
	require.Len(t, result, 1)
	require.Equal(t, peerKey(1), result[0])
}

func TestNearestLANBypassesNATFilterBothUnknown(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	localIPs := []string{"192.168.1.5"}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}, IPs: []string{"192.168.1.10"}, NatType: nat.Unknown},
	}
	// Both Unknown but same LAN → kept.
	result := selectNearest(peerKey(0), local, peers, nil, Params{NearestK: 1, LocalNATType: nat.Unknown, LocalIPs: localIPs})
	require.Len(t, result, 1)
	require.Equal(t, peerKey(1), result[0])
}

func TestNearestSuppressesRemotePrivateByDefault(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	params := Params{
		NearestK:                2,
		LocalNATType:            nat.Easy,
		LocalIPs:                []string{"10.1.2.10"},
		LocalObservedExternalIP: "52.204.52.130",
	}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}, IPs: []string{"10.2.2.10"}, NatType: nat.Hard, ObservedExternalIP: "34.252.188.39"},
		{Key: peerKey(2), Coord: &Coord{X: 2}, PubliclyAccessible: true, IPs: []string{"3.250.216.148"}},
	}

	result := selectNearest(peerKey(0), local, peers, nil, params)
	require.Equal(t, []types.PeerKey{peerKey(2)}, result)
}

func TestNearestKeepsSameSitePrivate(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	params := Params{
		NearestK:                2,
		LocalNATType:            nat.Easy,
		LocalIPs:                []string{"10.1.2.10"},
		LocalObservedExternalIP: "52.204.52.130",
	}
	peers := []PeerInfo{
		{Key: peerKey(1), Coord: &Coord{X: 1}, IPs: []string{"10.1.1.234"}, NatType: nat.Hard},
		{Key: peerKey(2), Coord: &Coord{X: 2}, IPs: []string{"10.2.2.10"}, NatType: nat.Hard, ObservedExternalIP: "34.252.188.39"},
	}

	result := selectNearest(peerKey(0), local, peers, nil, params)
	require.Equal(t, []types.PeerKey{peerKey(1)}, result)
}

func TestNearestKeepsSharedObservedEgressPrivate(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	params := Params{
		NearestK:                1,
		LocalNATType:            nat.Easy,
		LocalIPs:                []string{"10.1.2.10"},
		LocalObservedExternalIP: "52.204.52.130",
	}
	peers := []PeerInfo{{
		Key:                peerKey(1),
		Coord:              &Coord{X: 1},
		IPs:                []string{"10.9.9.10"},
		NatType:            nat.Hard,
		ObservedExternalIP: "52.204.52.130",
	}}

	result := selectNearest(peerKey(0), local, peers, nil, params)
	require.Equal(t, []types.PeerKey{peerKey(1)}, result)
}

func TestNearestDoesNotSuppressRemotePrivateWithoutObservedEgressSignal(t *testing.T) {
	local := Coord{X: 0, Y: 0}
	params := Params{
		NearestK:     1,
		LocalNATType: nat.Easy,
		LocalIPs:     []string{"10.1.2.10"},
	}
	peers := []PeerInfo{{
		Key:     peerKey(1),
		Coord:   &Coord{X: 1},
		IPs:     []string{"10.2.2.10"},
		NatType: nat.Hard,
	}}

	result := selectNearest(peerKey(0), local, peers, nil, params)
	require.Equal(t, []types.PeerKey{peerKey(1)}, result)
}

func TestLongLinksSkipsNonEasyPairs(t *testing.T) {
	local := peerKey(0)
	localIPs := []string{"10.0.1.5"}
	peers := make([]PeerInfo, 5)
	for i := range peers {
		peers[i] = PeerInfo{Key: peerKey(byte(i + 1)), IPs: []string{"10.0.2.10"}, NatType: nat.Hard}
	}
	// All peers are Hard+remote, local is Hard → all skipped.
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
	// Local is Hard: Unknown and Easy remotes both kept (only Hard+Hard is blocked).
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
	// Local is Hard. Peer 1 is Easy → kept. Peer 2 is Hard+remote → skipped.
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
	// Local is Easy: all remotes kept.
	result := selectLongLinks(local, peers, nil, Params{RandomR: 3, Epoch: 1, LocalNATType: nat.Easy, LocalIPs: localIPs})
	require.Len(t, result, 3)
}

func TestLongLinksSuppressRemotePrivateByDefault(t *testing.T) {
	local := peerKey(0)
	params := Params{
		RandomR:                 2,
		Epoch:                   1,
		LocalNATType:            nat.Easy,
		LocalIPs:                []string{"10.1.2.10"},
		LocalObservedExternalIP: "52.204.52.130",
	}
	peers := []PeerInfo{
		{Key: peerKey(1), IPs: []string{"10.2.2.10"}, NatType: nat.Hard, ObservedExternalIP: "34.252.188.39"},
		{Key: peerKey(2), IPs: []string{"3.250.216.148"}, PubliclyAccessible: true},
		{Key: peerKey(3), IPs: []string{"10.1.1.234"}, NatType: nat.Hard},
	}

	result := selectLongLinks(local, peers, nil, params)
	require.ElementsMatch(t, []types.PeerKey{peerKey(2), peerKey(3)}, result)
}

func TestComputeTargetPeersPreferFullMeshTargetsAllFeasiblePeers(t *testing.T) {
	local := peerKey(0)
	params := Params{
		LocalIPs:                []string{"81.108.176.99", "192.168.0.203"},
		LocalObservedExternalIP: "81.108.176.99",
		LocalNATType:            nat.Unknown,
		PreferFullMesh:          true,
	}
	peers := []PeerInfo{
		{Key: peerKey(1), IPs: []string{"18.135.80.64"}, PubliclyAccessible: true},
		{Key: peerKey(2), IPs: []string{"192.168.0.24"}},
		{Key: peerKey(3), IPs: []string{"192.168.0.220"}},
	}

	result := ComputeTargetPeers(local, Coord{}, peers, params)
	require.Equal(t, []types.PeerKey{peerKey(1), peerKey(2), peerKey(3)}, result)
}

func TestComputeTargetPeersPreferFullMeshSkipsInfeasibleRemotePrivate(t *testing.T) {
	local := peerKey(0)
	params := Params{
		LocalIPs:                []string{"10.1.2.10"},
		LocalObservedExternalIP: "52.204.52.130",
		LocalNATType:            nat.Hard,
		PreferFullMesh:          true,
	}
	peers := []PeerInfo{
		{Key: peerKey(1), IPs: []string{"10.2.2.10"}, NatType: nat.Hard, ObservedExternalIP: "34.252.188.39"},
		{Key: peerKey(2), IPs: []string{"3.250.216.148"}, PubliclyAccessible: true},
	}

	result := ComputeTargetPeers(local, Coord{}, peers, params)
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

	result := ComputeTargetPeers(local, Coord{}, peers, params)
	require.Equal(t, []types.PeerKey{peerKey(1)}, result)
}
