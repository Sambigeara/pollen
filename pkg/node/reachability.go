package node

import (
	"cmp"
	"net"
	"slices"

	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
)

type inferredReachability int

const (
	reachabilityUnknown inferredReachability = iota
	reachabilityPublicDirect
	reachabilitySameSitePrivate
)

func inferReachability(localIPs, peerIPs []string, publiclyAccessible bool) inferredReachability {
	if publiclyAccessible {
		return reachabilityPublicDirect
	}
	if topology.InferPrivatelyRoutable(localIPs, peerIPs) {
		return reachabilitySameSitePrivate
	}
	return reachabilityUnknown
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
		case topology.InferPrivatelyRoutable(localIPs, []string{ip.String()}):
			score = 0
		case !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast():
			score = 1
		}

		scored = append(scored, scoredAddr{addr: &net.UDPAddr{IP: ip, Port: p}, score: score})
	}

	slices.SortStableFunc(scored, func(a, b scoredAddr) int {
		if c := cmp.Compare(a.score, b.score); c != 0 {
			return c
		}
		return cmp.Compare(a.addr.String(), b.addr.String())
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

func rankCoordinators(localIPs, targetIPs []string, target types.PeerKey, connectedPeers []types.PeerKey, snap store.Snapshot) []types.PeerKey {
	localNV := snap.Nodes[snap.LocalID]
	targetNV := snap.Nodes[target]
	candidates := make([]coordinatorCandidate, 0, len(connectedPeers))
	for _, key := range connectedPeers {
		nv, ok := snap.Nodes[key]
		if !ok {
			continue
		}
		candidateIPs := nv.IPs
		if len(candidateIPs) == 0 || topology.InferPrivatelyRoutable(localIPs, candidateIPs) || topology.InferPrivatelyRoutable(targetIPs, candidateIPs) {
			continue
		}
		if _, connected := nv.Reachable[target]; !connected {
			continue
		}
		if topology.SameObservedEgress(localNV.ObservedExternalIP, nv.ObservedExternalIP) || topology.SameObservedEgress(targetNV.ObservedExternalIP, nv.ObservedExternalIP) {
			continue
		}
		candidates = append(candidates, coordinatorCandidate{
			key:                key,
			publiclyAccessible: nv.PubliclyAccessible,
		})
	}

	slices.SortStableFunc(candidates, func(a, b coordinatorCandidate) int {
		if a.publiclyAccessible != b.publiclyAccessible {
			if a.publiclyAccessible {
				return -1
			}
			return 1
		}
		return a.key.Compare(b.key)
	})

	out := make([]types.PeerKey, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, candidate.key)
	}
	return out
}
