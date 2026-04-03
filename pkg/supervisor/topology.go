package supervisor

import (
	"context"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/nat"
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

// --- Atomic Router ---

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

// --- Topology Resolution ---

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

func knownPeersFromSnapshot(snap state.Snapshot) []knownPeer {
	live := make(map[types.PeerKey]struct{}, len(snap.PeerKeys))
	for _, pk := range snap.PeerKeys {
		live[pk] = struct{}{}
	}

	known := make([]knownPeer, 0, len(live))
	for pk, nv := range snap.Nodes {
		if pk == snap.LocalID {
			continue
		}
		if _, ok := live[pk]; !ok {
			continue
		}
		if nv.LastAddr == "" && (len(nv.IPs) == 0 || nv.LocalPort == 0) {
			continue
		}
		known = append(known, knownPeer{
			PeerID:             pk,
			LocalPort:          nv.LocalPort,
			ExternalPort:       nv.ExternalPort,
			ObservedExternalIP: nv.ObservedExternalIP,
			NatType:            nv.NatType,
			IPs:                nv.IPs,
			LastAddr:           nv.LastAddr,
			PubliclyAccessible: nv.PubliclyAccessible,
			VivaldiCoord:       nv.VivaldiCoord,
		})
	}
	slices.SortFunc(known, func(a, b knownPeer) int {
		return a.PeerID.Compare(b.PeerID)
	})
	return known
}

func (n *Supervisor) syncPeersFromState(_ context.Context, snap state.Snapshot) {
	knownPeers := knownPeersFromSnapshot(snap)

	peerInfos := make([]membership.PeerInfo, 0, len(knownPeers))
	peerMap := make(map[types.PeerKey]knownPeer, len(knownPeers))
	for _, kp := range knownPeers {
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
	shape := summarizeTopologyShape(localIPs, knownPeers)
	params := adaptiveTopologyParams(epoch, shape)
	params.PreferFullMesh = len(knownPeers) <= tinyClusterPeerThreshold
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

	for _, kp := range knownPeers {
		if _, targeted := targetSet[kp.PeerID]; targeted {
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
