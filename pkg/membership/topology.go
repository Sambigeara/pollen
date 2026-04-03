package membership

import (
	"bytes"
	"cmp"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"maps"
	"math"
	"net/netip"
	"slices"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

type PeerInfo struct {
	Coord              *coords.Coord
	IPs                []string
	NatType            nat.Type
	Key                types.PeerKey
	PubliclyAccessible bool
}

type Params struct {
	CurrentOutbound map[types.PeerKey]struct{}
	LocalIPs        []string
	InfraMax        int
	NearestK        int
	RandomR         int
	Epoch           int64
	LocalNATType    nat.Type
	PreferFullMesh  bool
	UseHMACNearest  bool
}

const (
	DefaultInfraMax       = 2
	DefaultNearestK       = 4
	DefaultRandomR        = 2
	EpochSeconds          = 300 // 5 minutes
	nearestHysteresis     = 0.20
	minHysteresisDistance = 5.0
)

func DefaultParams(epoch int64) Params {
	return Params{
		InfraMax: DefaultInfraMax,
		NearestK: DefaultNearestK,
		RandomR:  DefaultRandomR,
		Epoch:    epoch,
	}
}

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

	return slices.SortedFunc(maps.Keys(selected), func(a, b types.PeerKey) int { return a.Compare(b) })
}

func selectFeasibleFullMesh(localKey types.PeerKey, peers []PeerInfo, params Params) []types.PeerKey {
	localPrefix := lanPrefix(params.LocalIPs)
	result := make([]types.PeerKey, 0, len(peers))
	for _, p := range peers {
		if p.Key == localKey || peerNATFiltered(localPrefix, params.LocalNATType, p.NatType, p.IPs) {
			continue
		}
		result = append(result, p.Key)
	}
	slices.SortFunc(result, func(a, b types.PeerKey) int { return a.Compare(b) })
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
	slices.SortFunc(candidates, func(a, b scored) int {
		if c := bytes.Compare(a.score[:], b.score[:]); c != 0 {
			return c
		}
		return a.key.Compare(b.key)
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
		if p.PubliclyAccessible {
			candidates = append(candidates, scored{
				key:   p.Key,
				score: hmacScore(localKey, []byte("infra"), p.Key.Bytes()),
			})
		}
	}
	return topByScore(candidates, limit)
}

type nearestCandidate struct {
	ips   []string
	dist  float64
	key   types.PeerKey
	score [32]byte
}

func selectNearest(localKey types.PeerKey, localCoord coords.Coord, peers []PeerInfo, exclude map[types.PeerKey]struct{}, params Params) []types.PeerKey {
	localPrefix := lanPrefix(params.LocalIPs)

	var candidates []nearestCandidate
	for _, p := range peers {
		if _, ok := exclude[p.Key]; ok || peerNATFiltered(localPrefix, params.LocalNATType, p.NatType, p.IPs) {
			continue
		}
		d := math.MaxFloat64
		if p.Coord != nil {
			d = coords.Distance(localCoord, *p.Coord)
		}
		if _, incumbent := params.CurrentOutbound[p.Key]; incumbent {
			d = math.Max(0, d-math.Max(d*nearestHysteresis, minHysteresisDistance))
		}

		cand := nearestCandidate{key: p.Key, dist: d, ips: p.IPs}
		if params.UseHMACNearest {
			cand.score = hmacScore(localKey, []byte("nearest"), p.Key.Bytes())
		}
		candidates = append(candidates, cand)
	}

	k := params.NearestK
	lanCap := (k + 1) / 2 //nolint:mnd

	slices.SortStableFunc(candidates, func(a, b nearestCandidate) int {
		if params.UseHMACNearest {
			if c := bytes.Compare(a.score[:], b.score[:]); c != 0 {
				return c
			}
		} else if c := cmp.Compare(a.dist, b.dist); c != 0 {
			return c
		}
		return a.key.Compare(b.key)
	})
	return selectWithLANCap(candidates, k, lanCap)
}

func selectWithLANCap(candidates []nearestCandidate, k, lanCap int) []types.PeerKey {
	lanCount := make(map[string]int)
	var result []types.PeerKey
	for _, c := range candidates {
		if len(result) >= k {
			break
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

func selectLongLinks(localKey types.PeerKey, peers []PeerInfo, exclude map[types.PeerKey]struct{}, params Params) []types.PeerKey {
	var epochBuf [8]byte
	binary.BigEndian.PutUint64(epochBuf[:], uint64(params.Epoch))

	localPrefix := lanPrefix(params.LocalIPs)

	var candidates []scored
	for _, p := range peers {
		if _, ok := exclude[p.Key]; ok || peerNATFiltered(localPrefix, params.LocalNATType, p.NatType, p.IPs) {
			continue
		}
		candidates = append(candidates, scored{
			key:   p.Key,
			score: hmacScore(localKey, []byte("long"), epochBuf[:], p.Key.Bytes()),
		})
	}
	return topByScore(candidates, params.RandomR)
}

func SameObservedEgress(a, b string) bool {
	return a != "" && a == b
}

func peerNATFiltered(localPrefix string, localNAT, remoteNAT nat.Type, peerIPs []string) bool {
	isLAN := localPrefix != "" && lanPrefix(peerIPs) == localPrefix
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
		if ip, err := netip.ParseAddr(s); err == nil && isUsablePrivateIP(ip) {
			b := ip.As16()
			if ip.Is4() {
				out[string(b[12:14])] = struct{}{} // Store 16-bit subnet equivalent
			} else {
				out[string(b[:6])] = struct{}{}
			}
		}
	}
	return out
}

func isUsablePrivateIP(ip netip.Addr) bool {
	return ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsUnspecified()
}

func lanPrefix(ips []string) string {
	for _, s := range ips {
		if ip, err := netip.ParseAddr(s); err == nil && !ip.IsLoopback() && !ip.IsUnspecified() {
			b := ip.As16()
			if ip.Is4() {
				return string(b[12:15]) // /24 match
			}
			return string(b[:8]) // /64 match
		}
	}
	return ""
}
