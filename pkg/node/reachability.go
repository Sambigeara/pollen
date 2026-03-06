package node

import (
	"net"
	"sort"

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
	localRec, _ := state.Get(state.LocalID)
	targetRec, _ := state.Get(target)
	candidates := make([]coordinatorCandidate, 0, len(connectedPeers))
	for _, key := range connectedPeers {
		candidateIPs := state.NodeIPs(key)
		if len(candidateIPs) == 0 || topology.InferPrivatelyRoutable(localIPs, candidateIPs) || topology.InferPrivatelyRoutable(targetIPs, candidateIPs) {
			continue
		}
		if !state.IsConnected(key, target) {
			continue
		}
		rec, ok := state.Get(key)
		if !ok {
			continue
		}
		if topology.SameObservedEgress(localRec.ObservedExternalIP, rec.ObservedExternalIP) || topology.SameObservedEgress(targetRec.ObservedExternalIP, rec.ObservedExternalIP) {
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
