package membership

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"net"
	"sort"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

type PeerInfo struct {
	Coord              *coords.Coord
	ObservedExternalIP string
	IPs                []string
	NatType            nat.Type
	Key                types.PeerKey
	PubliclyAccessible bool
}

type Params struct {
	CurrentOutbound         map[types.PeerKey]struct{}
	LocalObservedExternalIP string
	LocalIPs                []string
	InfraMax                int
	NearestK                int
	RandomR                 int
	Epoch                   int64
	LocalNATType            nat.Type
	PreferFullMesh          bool
	UseHMACNearest          bool
}

const (
	DefaultInfraMax       = 2
	DefaultNearestK       = 4
	DefaultRandomR        = 2
	EpochSeconds          = 300  // 5 minutes
	NearestHysteresis     = 0.20 // incumbent distance discount (20%)
	MinHysteresisDistance = 5.0  // minimum absolute discount (ms) for close peers
)

func DefaultParams(epoch int64) Params {
	return Params{
		InfraMax: DefaultInfraMax,
		NearestK: DefaultNearestK,
		RandomR:  DefaultRandomR,
		Epoch:    epoch,
	}
}

// ComputeTargetPeers selects outbound targets in three layers: infrastructure
// backbone, Vivaldi k-nearest, and deterministic random long-links.
func ComputeTargetPeers(localKey types.PeerKey, localCoord coords.Coord, peers []PeerInfo, params Params) []types.PeerKey {
	if len(peers) == 0 {
		return nil
	}
	if params.PreferFullMesh {
		return selectFeasibleFullMesh(localKey, peers, params)
	}

	selected := make(map[types.PeerKey]struct{})

	for _, pk := range selectInfra(localKey, peers, params.InfraMax) {
		selected[pk] = struct{}{}
	}
	for _, pk := range selectNearest(localKey, localCoord, peers, selected, params) {
		selected[pk] = struct{}{}
	}
	for _, pk := range selectLongLinks(localKey, peers, selected, params) {
		selected[pk] = struct{}{}
	}

	result := make([]types.PeerKey, 0, len(selected))
	for pk := range selected {
		result = append(result, pk)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Less(result[j]) })
	return result
}

// TODO(saml): cap full-mesh fan-out (e.g. 16 targets) to bound the burst
// when a large cluster restarts with persisted state and 0 connections.
func selectFeasibleFullMesh(localKey types.PeerKey, peers []PeerInfo, params Params) []types.PeerKey {
	localPrefix := lanPrefix(params.LocalIPs)
	result := make([]types.PeerKey, 0, len(peers))
	for _, p := range peers {
		if p.Key == localKey {
			continue
		}
		isLAN := localPrefix != "" && lanPrefix(p.IPs) == localPrefix
		if natFiltered(isLAN, params.LocalNATType, p.NatType) {
			continue
		}
		result = append(result, p.Key)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Less(result[j]) })
	return result
}

type scored struct {
	key   types.PeerKey
	score [32]byte
}

func hmacScore(localKey types.PeerKey, parts ...[]byte) [32]byte {
	h := hmac.New(sha256.New, localKey.Bytes())
	for _, p := range parts {
		h.Write(p)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func topByScore(candidates []scored, limit int) []types.PeerKey {
	sort.Slice(candidates, func(i, j int) bool {
		ci, cj := candidates[i].score, candidates[j].score
		if ci != cj {
			return bytes.Compare(ci[:], cj[:]) < 0
		}
		return candidates[i].key.Less(candidates[j].key)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	result := make([]types.PeerKey, len(candidates))
	for i, c := range candidates {
		result[i] = c.key
	}
	return result
}

func selectInfra(localKey types.PeerKey, peers []PeerInfo, limit int) []types.PeerKey {
	var candidates []scored
	for _, p := range peers {
		if !p.PubliclyAccessible {
			continue
		}
		candidates = append(candidates, scored{
			key:   p.Key,
			score: hmacScore(localKey, []byte("infra"), p.Key.Bytes()),
		})
	}
	return topByScore(candidates, limit)
}

type nearestCandidate struct {
	ips     []string
	dist    float64
	key     types.PeerKey
	natType nat.Type
	lan     bool
}

// TODO(saml): peers behind the same NAT on different subnets (e.g., 10.2.2.x
// and 10.2.3.x) can direct-dial but aren't detected as LAN by /24 prefix
// matching. Gossiping each node's observed external IP and comparing would let
// us identify same-NAT peers and bypass the NAT filter for them.
func selectNearest(localKey types.PeerKey, localCoord coords.Coord, peers []PeerInfo, exclude map[types.PeerKey]struct{}, params Params) []types.PeerKey {
	localPrefix := lanPrefix(params.LocalIPs)

	var candidates []nearestCandidate
	for _, p := range peers {
		if _, ok := exclude[p.Key]; ok {
			continue
		}
		if suppressProactivePrivate(params, p) {
			continue
		}
		d := math.MaxFloat64
		if p.Coord != nil {
			d = coords.Distance(localCoord, *p.Coord)
		}
		if _, incumbent := params.CurrentOutbound[p.Key]; incumbent {
			d = math.Max(0, d-math.Max(d*NearestHysteresis, MinHysteresisDistance))
		}
		isLAN := localPrefix != "" && lanPrefix(p.IPs) == localPrefix
		candidates = append(candidates, nearestCandidate{key: p.Key, dist: d, ips: p.IPs, natType: p.NatType, lan: isLAN})
	}

	k := params.NearestK
	lanCap := (k + 1) / 2 //nolint:mnd
	if params.UseHMACNearest {
		return selectNearestHMAC(localKey, candidates, k, lanCap, params)
	}
	return selectNearestByDistance(candidates, k, lanCap, params)
}

func selectNearestByDistance(candidates []nearestCandidate, k, lanCap int, params Params) []types.PeerKey {
	lanCount := make(map[string]int)
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].dist != candidates[j].dist {
			return candidates[i].dist < candidates[j].dist
		}
		return candidates[i].key.Less(candidates[j].key)
	})

	var result []types.PeerKey
	for _, c := range candidates {
		if len(result) >= k {
			break
		}
		if natFiltered(c.lan, params.LocalNATType, c.natType) {
			continue
		}
		prefix := lanPrefix(c.ips)
		if prefix != "" && lanCount[prefix] >= lanCap {
			continue
		}
		result = append(result, c.key)
		if prefix != "" {
			lanCount[prefix]++
		}
	}
	return result
}

func selectNearestHMAC(localKey types.PeerKey, candidates []nearestCandidate, k, lanCap int, params Params) []types.PeerKey {
	lanCount := make(map[string]int)

	type hmacCandidate struct {
		ips   []string
		key   types.PeerKey
		score [32]byte
	}

	// Epoch excluded: during HMAC mode targets must stay stable so vivaldi
	// converges. Epoch rotation caused a churn → no-samples feedback loop.
	var scoredCandidates []hmacCandidate
	for _, c := range candidates {
		if natFiltered(c.lan, params.LocalNATType, c.natType) {
			continue
		}
		scoredCandidates = append(scoredCandidates, hmacCandidate{
			key:   c.key,
			score: hmacScore(localKey, []byte("nearest"), c.key.Bytes()),
			ips:   c.ips,
		})
	}

	sort.Slice(scoredCandidates, func(i, j int) bool {
		si, sj := scoredCandidates[i].score, scoredCandidates[j].score
		if si != sj {
			return bytes.Compare(si[:], sj[:]) < 0
		}
		return scoredCandidates[i].key.Less(scoredCandidates[j].key)
	})

	var result []types.PeerKey
	for _, sc := range scoredCandidates {
		if len(result) >= k {
			break
		}
		prefix := lanPrefix(sc.ips)
		if prefix != "" && lanCount[prefix] >= lanCap {
			continue
		}
		result = append(result, sc.key)
		if prefix != "" {
			lanCount[prefix]++
		}
	}
	return result
}

func selectLongLinks(localKey types.PeerKey, peers []PeerInfo, exclude map[types.PeerKey]struct{}, params Params) []types.PeerKey {
	var epochBuf [8]byte
	binary.BigEndian.PutUint64(epochBuf[:], uint64(params.Epoch))

	localPrefix := lanPrefix(params.LocalIPs)

	var candidates []scored
	for _, p := range peers {
		if _, ok := exclude[p.Key]; ok {
			continue
		}
		if suppressProactivePrivate(params, p) {
			continue
		}
		isLAN := localPrefix != "" && lanPrefix(p.IPs) == localPrefix
		if natFiltered(isLAN, params.LocalNATType, p.NatType) {
			continue
		}
		candidates = append(candidates, scored{
			key:   p.Key,
			score: hmacScore(localKey, []byte("long"), epochBuf[:], p.Key.Bytes()),
		})
	}
	return topByScore(candidates, params.RandomR)
}

func suppressProactivePrivate(params Params, peer PeerInfo) bool {
	if peer.PubliclyAccessible {
		return false
	}
	if InferPrivatelyRoutable(params.LocalIPs, peer.IPs) {
		return false
	}
	if params.LocalObservedExternalIP == "" || peer.ObservedExternalIP == "" {
		return false
	}
	return !SameObservedEgress(params.LocalObservedExternalIP, peer.ObservedExternalIP)
}

func SameObservedEgress(a, b string) bool {
	return a != "" && a == b
}

func natFiltered(isLAN bool, localNAT, remoteNAT nat.Type) bool {
	return !isLAN && localNAT == nat.Hard && remoteNAT == nat.Hard
}

func InferPrivatelyRoutable(localIPs, peerIPs []string) bool {
	localPrefixes := privateSitePrefixes(localIPs)
	if len(localPrefixes) == 0 {
		return false
	}
	for prefix := range privateSitePrefixes(peerIPs) {
		if _, ok := localPrefixes[prefix]; ok {
			return true
		}
	}
	return false
}

func privateSitePrefixes(ips []string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, s := range ips {
		ip := net.ParseIP(s)
		if prefix, ok := privateSitePrefix(ip); ok {
			out[prefix] = struct{}{}
		}
	}
	return out
}

func privateSitePrefix(ip net.IP) (string, bool) {
	if !isUsablePrivateIP(ip) {
		return "", false
	}
	if ip4 := ip.To4(); ip4 != nil {
		//nolint:mnd
		switch {
		case ip4[0] == 10:
			return string(ip4[:2]), true
		case ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31:
			return string(ip4[:2]), true
		case ip4[0] == 192 && ip4[1] == 168:
			return string(ip4[:3]), true
		}
		return "", false
	}
	if ip16 := ip.To16(); ip16 != nil {
		return string(ip16[:6]), true
	}
	return "", false
}

func isUsablePrivateIP(ip net.IP) bool {
	return ip != nil && ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsUnspecified()
}

func lanPrefix(ips []string) string {
	for _, s := range ips {
		ip := net.ParseIP(s)
		if ip == nil || ip.IsLoopback() || ip.IsUnspecified() {
			continue
		}
		if ip4 := ip.To4(); ip4 != nil {
			return string(ip4[:3])
		}
		if ip16 := ip.To16(); ip16 != nil {
			return string(ip16[:8])
		}
	}
	return ""
}
