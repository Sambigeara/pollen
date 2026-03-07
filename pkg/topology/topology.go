package topology

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"net"
	"sort"

	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

// PeerInfo is a topology-relevant snapshot of a peer.
type PeerInfo struct {
	Coord              *Coord
	ObservedExternalIP string
	IPs                []string
	NatType            nat.Type
	Key                types.PeerKey
	PubliclyAccessible bool
}

// Params controls the topology budget.
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

// Budget: 2 infra + 4 nearest + 2 random = 8 max targets.
const (
	DefaultInfraMax       = 2
	DefaultNearestK       = 4
	DefaultRandomR        = 2
	EpochSeconds          = 300  // 5 minutes
	NearestHysteresis     = 0.20 // incumbent distance discount (20%)
	MinHysteresisDistance = 5.0  // minimum absolute discount (ms) for close peers
	privateIPv4Ten        = 10
	privateIPv4One72      = 172
	privateIPv4Two192     = 192
	privateIPv4Two168     = 168
	private172Min         = 16
	private172Max         = 31
)

// DefaultParams returns Params with default budgets.
func DefaultParams(epoch int64) Params {
	return Params{
		InfraMax: DefaultInfraMax,
		NearestK: DefaultNearestK,
		RandomR:  DefaultRandomR,
		Epoch:    epoch,
	}
}

// ComputeTargetPeers selects a bounded set of outbound connection targets.
// Three layers: infrastructure backbone, Vivaldi k-nearest, random long-links.
// The result is deterministic given the same inputs.
func ComputeTargetPeers(localKey types.PeerKey, localCoord Coord, peers []PeerInfo, params Params) []types.PeerKey {
	if len(peers) == 0 {
		return nil
	}
	if params.PreferFullMesh {
		return selectFeasibleFullMesh(localKey, peers, params)
	}

	selected := make(map[types.PeerKey]struct{})

	// Layer 1: Infrastructure backbone.
	infra := selectInfra(localKey, peers, params.InfraMax)
	for _, pk := range infra {
		selected[pk] = struct{}{}
	}

	// Layer 2: K-nearest by Vivaldi distance with LAN diversity cap.
	nearest := selectNearest(localKey, localCoord, peers, selected, params)
	for _, pk := range nearest {
		selected[pk] = struct{}{}
	}

	// Layer 3: Deterministic random long-links.
	longLinks := selectLongLinks(localKey, peers, selected, params)
	for _, pk := range longLinks {
		selected[pk] = struct{}{}
	}

	result := make([]types.PeerKey, 0, len(selected))
	for pk := range selected {
		result = append(result, pk)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Less(result[j]) })
	return result
}

func selectFeasibleFullMesh(localKey types.PeerKey, peers []PeerInfo, params Params) []types.PeerKey {
	localPrefix := lanPrefix(params.LocalIPs)
	result := make([]types.PeerKey, 0, len(peers))
	for _, p := range peers {
		if p.Key == localKey {
			continue
		}
		if p.PubliclyAccessible || InferPrivatelyRoutable(params.LocalIPs, p.IPs) {
			result = append(result, p.Key)
			continue
		}
		if suppressProactivePrivate(params, p) {
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

// selectInfra picks up to limit PubliclyAccessible peers, scored by
// HMAC(localKey, relayKey) so different nodes spread across relays.
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

// selectNearest picks k closest peers by Vivaldi distance, excluding already
// selected peers. Applies a LAN diversity cap: at most ceil(k/2) peers from
// any single LAN prefix. Currently-connected outbound peers get a distance
// discount of NearestHysteresis to reduce connection churn.
//
// When UseHMACNearest is set, coordinates are too noisy for meaningful distance
// ordering. In that regime we fall back to HMAC-deterministic selection (same
// mechanism as long-links) so targets are stable and well-spread from T=0 until
// coordinates converge. The caller applies hysteresis on the toggle.
//
// TODO(saml): peers behind the same NAT on different subnets (e.g., 10.2.2.x
// and 10.2.3.x) can direct-dial but aren't detected as LAN by /24 prefix
// matching. Gossiping each node's observed external IP and comparing would let
// us identify same-NAT peers and bypass the NAT filter for them.
func selectNearest(localKey types.PeerKey, localCoord Coord, peers []PeerInfo, exclude map[types.PeerKey]struct{}, params Params) []types.PeerKey {
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
			d = Distance(localCoord, *p.Coord)
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
	var epochBuf [8]byte
	binary.BigEndian.PutUint64(epochBuf[:], uint64(params.Epoch))

	type hmacCandidate struct {
		ips   []string
		key   types.PeerKey
		score [32]byte
	}

	var scoredCandidates []hmacCandidate
	for _, c := range candidates {
		if natFiltered(c.lan, params.LocalNATType, c.natType) {
			continue
		}
		scoredCandidates = append(scoredCandidates, hmacCandidate{
			key:   c.key,
			score: hmacScore(localKey, []byte("nearest"), epochBuf[:], c.key.Bytes()),
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

// selectLongLinks picks r peers via deterministic HMAC-SHA256 permutation.
// Non-LAN pairs require at least one side to be Easy.
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

// natFiltered returns true when a non-LAN pair should be skipped because
// neither side has Easy NAT (making direct connectivity unlikely).
func natFiltered(isLAN bool, localNAT, remoteNAT nat.Type) bool {
	return !isLAN && localNAT != nat.Easy && remoteNAT != nat.Easy
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
		switch {
		case ip4[0] == privateIPv4Ten:
			return string(ip4[:2]), true
		case ip4[0] == privateIPv4One72 && ip4[1] >= private172Min && ip4[1] <= private172Max:
			return string(ip4[:2]), true
		case ip4[0] == privateIPv4Two192 && ip4[1] == privateIPv4Two168:
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

// lanPrefix returns a grouping key for the peer's subnet. It uses the first
// unicast IP found: /24 for IPv4, /64 for IPv6. Returns "" if no usable IP.
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
