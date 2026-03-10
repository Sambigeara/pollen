package node

import (
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/topology"
)

const (
	adaptiveMinClusterSize     = 8
	tinyClusterPeerThreshold   = 4
	publicRatioSparseThreshold = 0.75
	publicRatioMixedThreshold  = 0.5
)

// topologyShape summarises the composition of known peers by reachability class.
type topologyShape struct {
	totalCount           int
	publicCount          int
	sameSitePrivateCount int
	remotePrivateCount   int
}

// summarizeTopologyShape classifies known peers into public, same-site private,
// and remote private buckets using the same logic the topology engine uses for
// dial/suppression decisions.
func summarizeTopologyShape(localIPs []string, peers []store.KnownPeer) topologyShape {
	var s topologyShape
	for _, kp := range peers {
		s.totalCount++
		switch {
		case kp.PubliclyAccessible:
			s.publicCount++
		case topology.InferPrivatelyRoutable(localIPs, kp.IPs):
			s.sameSitePrivateCount++
		default:
			s.remotePrivateCount++
		}
	}
	return s
}

// adaptiveTopologyParams adjusts the NearestK and RandomR budgets based on the
// observed cluster composition. Public-heavy clusters get sparser budgets to
// reduce density and prune churn; mixed/private-heavy clusters keep defaults.
// InfraMax is intentionally left unchanged.
func adaptiveTopologyParams(epoch int64, shape topologyShape) topology.Params {
	p := topology.DefaultParams(epoch)

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
