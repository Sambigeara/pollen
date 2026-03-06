package node

import (
	"net"
	"sort"

	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/types"
)

type inferredReachability int

const (
	reachabilityUnknown inferredReachability = iota
	reachabilityPublicDirect
	reachabilitySameSitePrivate
)

const (
	privateIPv4Ten    = 10
	privateIPv4One72  = 172
	privateIPv4Two192 = 192
	privateIPv4Two168 = 168
	private172Min     = 16
	private172Max     = 31
)

func inferReachability(localIPs, peerIPs []string, publiclyAccessible bool) inferredReachability {
	if publiclyAccessible {
		return reachabilityPublicDirect
	}
	if inferPrivatelyRoutable(localIPs, peerIPs) {
		return reachabilitySameSitePrivate
	}
	return reachabilityUnknown
}

func inferPrivatelyRoutable(localIPs, peerIPs []string) bool {
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

func orderPeerAddrs(localIPs []string, peerIPs []net.IP, port, extPort int) []*net.UDPAddr {
	type scoredAddr struct {
		addr  *net.UDPAddr
		score int
	}

	scored := make([]scoredAddr, 0, len(peerIPs))
	for _, ip := range peerIPs {
		if ip == nil {
			continue
		}
		p := port
		if extPort != 0 && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
			p = extPort
		}

		score := 2
		switch {
		case inferPrivatelyRoutable(localIPs, []string{ip.String()}):
			score = 0
		case !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast():
			score = 1
		}

		scored = append(scored, scoredAddr{addr: &net.UDPAddr{IP: ip, Port: p}, score: score})
	}

	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score < scored[j].score
		}
		return scored[i].addr.String() < scored[j].addr.String()
	})

	addrs := make([]*net.UDPAddr, 0, len(scored))
	for _, item := range scored {
		addrs = append(addrs, item.addr)
	}
	return addrs
}

type coordinatorCandidate struct {
	key                types.PeerKey
	publiclyAccessible bool
}

func rankCoordinators(localIPs, targetIPs []string, target types.PeerKey, connectedPeers []types.PeerKey, state *store.Store) []types.PeerKey {
	candidates := make([]coordinatorCandidate, 0, len(connectedPeers))
	for _, key := range connectedPeers {
		candidateIPs := state.NodeIPs(key)
		if len(candidateIPs) == 0 || inferPrivatelyRoutable(localIPs, candidateIPs) || inferPrivatelyRoutable(targetIPs, candidateIPs) {
			continue
		}
		if !state.IsConnected(key, target) {
			continue
		}
		rec, ok := state.Get(key)
		if !ok {
			continue
		}
		candidates = append(candidates, coordinatorCandidate{
			key:                key,
			publiclyAccessible: rec.PubliclyAccessible,
		})
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].publiclyAccessible != candidates[j].publiclyAccessible {
			return candidates[i].publiclyAccessible
		}
		return candidates[i].key.Less(candidates[j].key)
	})

	out := make([]types.PeerKey, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, candidate.key)
	}
	return out
}
