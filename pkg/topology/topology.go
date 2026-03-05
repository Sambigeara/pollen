package topology

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"net"
	"sort"

	"github.com/sambigeara/pollen/pkg/types"
)

// PeerInfo is a topology-relevant snapshot of a peer.
type PeerInfo struct {
	Coord              *Coord
	IPs                []string
	Key                types.PeerKey
	PubliclyAccessible bool
}

// Params controls the topology budget.
type Params struct {
	CurrentOutbound map[types.PeerKey]struct{} // currently-connected outbound peers
	InfraMax        int                        // max infrastructure backbone peers
	NearestK        int                        // Vivaldi k-nearest neighbors
	RandomR         int                        // deterministic random long-links
	Epoch           int64                      // current epoch (time.Now().Unix() / EpochSeconds)
}

// Budget: 2 infra + 4 nearest + 2 random = 8 max targets.
const (
	DefaultInfraMax   = 2
	DefaultNearestK   = 4
	DefaultRandomR    = 2
	EpochSeconds      = 300  // 5 minutes
	NearestHysteresis = 0.20 // incumbent distance discount (20%)
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

	selected := make(map[types.PeerKey]struct{})

	// Layer 1: Infrastructure backbone.
	infra := selectInfra(localKey, peers, params.InfraMax)
	for _, pk := range infra {
		selected[pk] = struct{}{}
	}

	// Layer 2: K-nearest by Vivaldi distance with LAN diversity cap.
	nearest := selectNearest(localCoord, peers, selected, params.NearestK, params.CurrentOutbound)
	for _, pk := range nearest {
		selected[pk] = struct{}{}
	}

	// Layer 3: Deterministic random long-links.
	longLinks := selectLongLinks(localKey, peers, selected, params.RandomR, params.Epoch)
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

// selectNearest picks k closest peers by Vivaldi distance, excluding already
// selected peers. Applies a LAN diversity cap: at most ceil(k/2) peers from
// any single LAN prefix. Currently-connected outbound peers get a distance
// discount of NearestHysteresis to reduce connection churn.
func selectNearest(localCoord Coord, peers []PeerInfo, exclude map[types.PeerKey]struct{}, k int, currentOutbound map[types.PeerKey]struct{}) []types.PeerKey {
	type candidate struct {
		ips  []string
		dist float64
		key  types.PeerKey
	}

	var candidates []candidate
	for _, p := range peers {
		if _, ok := exclude[p.Key]; ok {
			continue
		}
		d := math.MaxFloat64
		if p.Coord != nil {
			d = Distance(localCoord, *p.Coord)
		}
		if _, incumbent := currentOutbound[p.Key]; incumbent {
			d *= (1 - NearestHysteresis)
		}
		candidates = append(candidates, candidate{key: p.Key, dist: d, ips: p.IPs})
	}

	// Stable sort: distance first, then PeerKey for deterministic tie-breaking.
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].dist != candidates[j].dist {
			return candidates[i].dist < candidates[j].dist
		}
		return candidates[i].key.Less(candidates[j].key)
	})

	lanCap := (k + 1) / 2 //nolint:mnd
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

// selectLongLinks picks r peers via deterministic HMAC-SHA256 permutation.
func selectLongLinks(localKey types.PeerKey, peers []PeerInfo, exclude map[types.PeerKey]struct{}, r int, epoch int64) []types.PeerKey {
	var epochBuf [8]byte
	binary.BigEndian.PutUint64(epochBuf[:], uint64(epoch))

	var candidates []scored
	for _, p := range peers {
		if _, ok := exclude[p.Key]; ok {
			continue
		}
		candidates = append(candidates, scored{
			key:   p.Key,
			score: hmacScore(localKey, []byte("long"), epochBuf[:], p.Key.Bytes()),
		})
	}
	return topByScore(candidates, r)
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
