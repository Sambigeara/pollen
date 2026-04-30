// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"net"
	"net/netip"
	"slices"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/peercache"
	"github.com/sambigeara/pollen/pkg/routing"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const (
	revokeStreakThreshold       = 3
	revokeStreakThresholdPublic = 30

	vivaldiEnterHMACThreshold = 0.6
	vivaldiExitHMACThreshold  = 0.35

	adaptiveMinClusterSize     = 8
	tinyClusterPeerThreshold   = 4
	publicRatioSparseThreshold = 0.75
	publicRatioMixedThreshold  = 0.5
)

type atomicRouter struct {
	table    routing.Table
	changeCh chan struct{}
	mu       sync.RWMutex
}

func newAtomicRouter() *atomicRouter {
	return &atomicRouter{changeCh: make(chan struct{})}
}

func (r *atomicRouter) NextHop(dest types.PeerKey) (types.PeerKey, bool) {
	r.mu.RLock()
	t := r.table
	r.mu.RUnlock()
	return t.NextHop(dest)
}

func (r *atomicRouter) Changed() <-chan struct{} {
	r.mu.RLock()
	ch := r.changeCh
	r.mu.RUnlock()
	return ch
}

func (r *atomicRouter) set(t routing.Table) {
	r.mu.Lock()
	r.table = t
	close(r.changeCh)
	r.changeCh = make(chan struct{})
	r.mu.Unlock()
}

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
	if membership.InferPrivatelyRoutable(localIPs, peerIPs) {
		return reachabilitySameSitePrivate
	}
	return reachabilityUnknown
}

type knownPeer struct {
	VivaldiCoord       *coords.Coord
	LastAddr           string
	ObservedExternalIP string
	IPs                []string
	NatType            nat.Type
	LocalPort          uint32
	ExternalPort       uint32
	PeerID             types.PeerKey
	PubliclyAccessible bool
}

// knownPeers returns the union of peers we should consider dialling: every
// live gossip peer, plus every peer in the durable cache that has not been
// explicitly denied. Cache entries supply addresses for peers we know about
// from a join token but have never spoken to, and supplement the IP set of
// peers already in gossip so transient route failures don't strand a LAN
// candidate forever.
func knownPeers(snap state.Snapshot, cache *peercache.Store) []knownPeer {
	live := make(map[types.PeerKey]struct{}, len(snap.PeerKeys))
	for _, pk := range snap.PeerKeys {
		live[pk] = struct{}{}
	}
	denied := make(map[types.PeerKey]struct{})
	for _, pk := range snap.DeniedPeers() {
		denied[pk] = struct{}{}
	}

	cacheByPeer := make(map[types.PeerKey][]netip.AddrPort)
	if cache != nil {
		for _, entry := range cache.Snapshot() {
			for _, s := range entry.Addrs {
				if ap, err := netip.ParseAddrPort(s); err == nil {
					cacheByPeer[entry.PeerKey] = append(cacheByPeer[entry.PeerKey], ap)
				}
			}
		}
	}

	candidates := make(map[types.PeerKey]struct{}, len(live)+len(cacheByPeer))
	for pk := range live {
		candidates[pk] = struct{}{}
	}
	for pk := range cacheByPeer {
		candidates[pk] = struct{}{}
	}
	delete(candidates, snap.LocalID)
	for pk := range denied {
		delete(candidates, pk)
	}

	known := make([]knownPeer, 0, len(candidates))
	for pk := range candidates {
		kp := knownPeer{PeerID: pk}
		if nv, ok := snap.Nodes[pk]; ok {
			kp.LocalPort = nv.LocalPort
			kp.ExternalPort = nv.ExternalPort
			kp.ObservedExternalIP = nv.ObservedExternalIP
			kp.NatType = nv.NatType
			kp.IPs = slices.Clone(nv.IPs)
			kp.LastAddr = nv.LastAddr
			kp.PubliclyAccessible = nv.PubliclyAccessible
			kp.VivaldiCoord = nv.VivaldiCoord
		}
		mergeCacheAddresses(&kp, cacheByPeer[pk])
		if !knownPeerHasUsableAddr(kp) {
			continue
		}
		known = append(known, kp)
	}
	slices.SortFunc(known, func(a, b knownPeer) int {
		return a.PeerID.Compare(b.PeerID)
	})
	return known
}

// mergeCacheAddresses augments a knownPeer with addresses from the persistent
// peer cache. If state didn't supply a listening port, the most common cached
// port stands in. Cached addresses on other ports are dropped — a peer binds
// one UDP socket, so divergent ports are stale.
func mergeCacheAddresses(kp *knownPeer, cached []netip.AddrPort) {
	if len(cached) == 0 {
		return
	}
	if kp.LocalPort == 0 {
		kp.LocalPort = mostCommonPort(cached)
	}
	if kp.LocalPort == 0 {
		return
	}

	existing := make(map[string]struct{}, len(kp.IPs))
	for _, ip := range kp.IPs {
		existing[ip] = struct{}{}
	}
	for _, ap := range cached {
		if uint32(ap.Port()) != kp.LocalPort {
			continue
		}
		ipStr := ap.Addr().String()
		if _, ok := existing[ipStr]; ok {
			continue
		}
		kp.IPs = append(kp.IPs, ipStr)
		existing[ipStr] = struct{}{}
	}
	if kp.LastAddr == "" {
		for _, ap := range cached {
			if uint32(ap.Port()) == kp.LocalPort {
				kp.LastAddr = ap.String()
				break
			}
		}
	}
}

func mostCommonPort(addrs []netip.AddrPort) uint32 {
	counts := make(map[uint16]int, len(addrs))
	for _, a := range addrs {
		counts[a.Port()]++
	}
	var bestPort uint16
	var bestCount int
	for p, c := range counts {
		if c > bestCount || (c == bestCount && (bestCount == 0 || p < bestPort)) {
			bestPort = p
			bestCount = c
		}
	}
	return uint32(bestPort)
}

func knownPeerHasUsableAddr(kp knownPeer) bool {
	if kp.LastAddr != "" {
		return true
	}
	if kp.ObservedExternalIP != "" && (kp.ExternalPort != 0 || kp.LocalPort != 0) {
		return true
	}
	return len(kp.IPs) > 0 && kp.LocalPort != 0
}

func (n *Supervisor) syncPeersFromState(_ context.Context, snap state.Snapshot) {
	known := knownPeers(snap, n.peerCache)

	peerInfos := make([]membership.PeerInfo, 0, len(known))
	peerMap := make(map[types.PeerKey]knownPeer, len(known))
	for _, kp := range known {
		peerMap[kp.PeerID] = kp
		peerInfos = append(peerInfos, membership.PeerInfo{
			Key:                kp.PeerID,
			Coord:              kp.VivaldiCoord,
			IPs:                kp.IPs,
			NatType:            kp.NatType,
			PubliclyAccessible: kp.PubliclyAccessible,
		})
	}

	connectedPeers := n.GetConnectedPeers()
	currentOutbound := make(map[types.PeerKey]struct{}, len(connectedPeers))
	for _, pk := range connectedPeers {
		if n.mesh.IsOutbound(pk) {
			currentOutbound[pk] = struct{}{}
		}
	}

	cm := n.membership.ControlMetrics()
	if n.useHMACNearest {
		if cm.SmoothedErr < vivaldiExitHMACThreshold {
			n.useHMACNearest = false
		}
	} else {
		if cm.SmoothedErr > vivaldiEnterHMACThreshold {
			n.useHMACNearest = true
		}
	}

	epoch := time.Now().Unix() / membership.EpochSeconds
	localNV := snap.Nodes[snap.LocalID]
	localIPs := localNV.IPs
	shape := summarizeTopologyShape(localIPs, known)
	params := adaptiveTopologyParams(epoch, shape)
	params.PreferFullMesh = len(known) <= tinyClusterPeerThreshold
	params.LocalIPs = localIPs
	params.CurrentOutbound = currentOutbound
	params.LocalNATType = n.natDetector.Type()
	params.UseHMACNearest = n.useHMACNearest
	targets := membership.ComputeTargetPeers(snap.LocalID, cm.LocalCoord, peerInfos, params)

	ctx := context.Background()
	n.topoMetrics.VivaldiError.Record(ctx, cm.SmoothedErr)
	var hmac float64
	if n.useHMACNearest {
		hmac = 1.0
	}
	n.topoMetrics.HMACNearestEnabled.Record(ctx, hmac)

	targetSet := buildTargetPeerSet(targets, n.tunneling.DesiredPeers())

	for pk := range targetSet {
		kp, ok := peerMap[pk]
		if !ok {
			continue
		}

		ips := make([]net.IP, 0, len(kp.IPs))
		for _, ipStr := range kp.IPs {
			if ip := net.ParseIP(ipStr); ip != nil {
				ips = append(ips, ip)
			}
		}

		var lastAddr *net.UDPAddr
		if kp.LastAddr != "" {
			if addr, err := net.ResolveUDPAddr("udp", kp.LastAddr); err == nil {
				lastAddr = addr
			} else {
				n.log.Debugw("invalid last addr", "peer", kp.PeerID.Short(), "addr", kp.LastAddr, zap.Error(err))
			}
		}
		if lastAddr == nil && kp.ObservedExternalIP != "" {
			port := kp.LocalPort
			if kp.ExternalPort != 0 {
				port = kp.ExternalPort
			}
			lastAddr = &net.UDPAddr{
				IP:   net.ParseIP(kp.ObservedExternalIP),
				Port: int(port),
			}
		}

		if len(ips) == 0 && lastAddr == nil {
			continue
		}

		reachability := inferReachability(params.LocalIPs, kp.IPs, kp.PubliclyAccessible)
		n.mesh.DiscoverPeer(kp.PeerID, ips, int(kp.LocalPort), lastAddr, reachability == reachabilitySameSitePrivate, kp.PubliclyAccessible)
	}

	for pk := range targetSet {
		delete(n.nonTargetStreak, pk)
	}

	// Prune only peers that gossip has confirmed live. Cache-only entries
	// belong to peers we haven't yet handshaken with; they get an FSM slot
	// from DiscoverPeer above and stay there until either gossip arrives or
	// the cache evicts them. Forgetting them here would just rediscover them
	// on the next tick.
	live := make(map[types.PeerKey]struct{}, len(snap.PeerKeys))
	for _, pk := range snap.PeerKeys {
		live[pk] = struct{}{}
	}
	for _, kp := range known {
		if _, targeted := targetSet[kp.PeerID]; targeted {
			continue
		}
		if _, isLive := live[kp.PeerID]; !isLive {
			continue
		}
		connected := n.mesh.IsPeerConnected(kp.PeerID)
		if connected && !n.mesh.IsOutbound(kp.PeerID) {
			continue
		}
		if connected {
			n.nonTargetStreak[kp.PeerID]++
			threshold := revokeStreakThreshold
			if kp.PubliclyAccessible {
				threshold = revokeStreakThresholdPublic
			}
			if n.nonTargetStreak[kp.PeerID] < threshold {
				continue
			}
			n.mesh.ClosePeerSession(kp.PeerID, transport.DisconnectTopologyPrune)
			n.topoMetrics.TopologyPrunes.Add(ctx, 1)
		}
		delete(n.nonTargetStreak, kp.PeerID)
		n.mesh.ForgetPeer(kp.PeerID)
	}
}

func buildTargetPeerSet(targets, desiredPeers []types.PeerKey) map[types.PeerKey]struct{} {
	out := make(map[types.PeerKey]struct{}, len(targets)+len(desiredPeers))
	for _, pk := range targets {
		out[pk] = struct{}{}
	}
	for _, pk := range desiredPeers {
		out[pk] = struct{}{}
	}
	return out
}

type topologyShape struct {
	totalCount           int
	publicCount          int
	sameSitePrivateCount int
	remotePrivateCount   int
}

func summarizeTopologyShape(localIPs []string, peers []knownPeer) topologyShape {
	var s topologyShape
	for _, kp := range peers {
		s.totalCount++
		switch {
		case kp.PubliclyAccessible:
			s.publicCount++
		case membership.InferPrivatelyRoutable(localIPs, kp.IPs):
			s.sameSitePrivateCount++
		default:
			s.remotePrivateCount++
		}
	}
	return s
}

func adaptiveTopologyParams(epoch int64, shape topologyShape) membership.Params {
	p := membership.DefaultParams(epoch)

	if shape.totalCount < adaptiveMinClusterSize {
		return p
	}

	publicRatio := float64(shape.publicCount) / float64(shape.totalCount)
	switch {
	case publicRatio >= publicRatioSparseThreshold:
		p.NearestK = 2
		p.RandomR = 1
	case publicRatio >= publicRatioMixedThreshold:
		p.NearestK = 3
		p.RandomR = 2
	}

	return p
}
