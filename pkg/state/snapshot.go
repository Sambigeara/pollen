package state

import (
	"fmt"
	"maps"
	"slices"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

type Snapshot struct {
	Nodes      map[types.PeerKey]NodeView
	Specs      map[string]WorkloadSpecView
	Claims     map[string]map[types.PeerKey]struct{}
	Placements map[types.PeerKey]NodePlacementState
	Heatmaps   map[types.PeerKey]map[types.PeerKey]TrafficSnapshot
	digest     Digest
	PeerKeys   []types.PeerKey
	DeniedKeys []types.PeerKey
	LocalID    types.PeerKey
}

type Digest struct {
	proto *statev1.Digest
}

func (d Digest) Marshal() ([]byte, error) {
	if d.proto == nil {
		return (&statev1.Digest{}).MarshalVT()
	}
	return d.proto.MarshalVT()
}

func UnmarshalDigest(data []byte) (Digest, error) {
	pb := &statev1.Digest{}
	if len(data) > 0 {
		if err := pb.UnmarshalVT(data); err != nil {
			return Digest{}, fmt.Errorf("unmarshal digest: %w", err)
		}
	}
	return Digest{proto: pb}, nil
}

type Service struct {
	Name string
	Port uint32
}

type NodeView struct {
	Services           map[string]*Service
	WorkloadSpecs      map[string]*statev1.WorkloadSpecChange
	WorkloadClaims     map[string]struct{}
	VivaldiCoord       *coords.Coord
	TrafficRates       map[types.PeerKey]TrafficSnapshot
	Reachable          map[types.PeerKey]struct{}
	LastAddr           string
	ObservedExternalIP string
	PeerPub            []byte
	IPs                []string
	VivaldiErr         float64
	MemTotalBytes      uint64
	NatType            nat.Type
	CertExpiry         int64
	LocalPort          uint32
	ExternalPort       uint32
	CPUPercent         uint32
	MemPercent         uint32
	NumCPU             uint32
	PubliclyAccessible bool
}

func (nv NodeView) clone() NodeView {
	c := nv
	c.Services = maps.Clone(nv.Services)
	c.Reachable = maps.Clone(nv.Reachable)
	c.WorkloadSpecs = maps.Clone(nv.WorkloadSpecs)
	c.WorkloadClaims = maps.Clone(nv.WorkloadClaims)
	c.TrafficRates = maps.Clone(nv.TrafficRates)
	c.IPs = append([]string(nil), nv.IPs...)
	c.PeerPub = append([]byte(nil), nv.PeerPub...)
	if nv.VivaldiCoord != nil {
		coord := *nv.VivaldiCoord
		c.VivaldiCoord = &coord
	}
	return c
}

type PeerInfo struct {
	NodeView
	Key types.PeerKey
}

type ServiceInfo struct {
	Name string
	Port uint32
	Peer types.PeerKey
}

type WorkloadInfo struct {
	Hash    string
	Claimed []types.PeerKey
	Spec    WorkloadSpecView
}

type WorkloadSpecView struct {
	Spec      *statev1.WorkloadSpecChange
	Publisher types.PeerKey
}

type TrafficSnapshot struct {
	BytesIn  uint64
	BytesOut uint64
}

type NodePlacementState struct {
	Coord         *coords.Coord
	TrafficTo     map[types.PeerKey]uint64
	MemTotalBytes uint64
	CPUPercent    uint32
	MemPercent    uint32
	NumCPU        uint32
}

func (s Snapshot) Self() types.PeerKey {
	return s.LocalID
}

func (s Snapshot) Digest() Digest {
	return s.digest
}

func (s Snapshot) Peers() []PeerInfo {
	peers := make([]PeerInfo, 0, len(s.PeerKeys))
	for _, k := range s.PeerKeys {
		peers = append(peers, PeerInfo{Key: k, NodeView: s.Nodes[k]})
	}
	return peers
}

func (s Snapshot) Peer(key types.PeerKey) (PeerInfo, bool) {
	nv, ok := s.Nodes[key]
	if !ok {
		return PeerInfo{}, false
	}
	return PeerInfo{Key: key, NodeView: nv}, true
}

func (s Snapshot) Services() []ServiceInfo {
	var out []ServiceInfo
	for pk, nv := range s.Nodes {
		for name, svc := range nv.Services {
			out = append(out, ServiceInfo{
				Name: name,
				Port: svc.Port,
				Peer: pk,
			})
		}
	}
	return out
}

func (s Snapshot) Workloads() []WorkloadInfo {
	out := make([]WorkloadInfo, 0, len(s.Specs))
	for hash, spec := range s.Specs {
		claimants := s.Claims[hash]
		claimed := make([]types.PeerKey, 0, len(claimants))
		for pk := range claimants {
			claimed = append(claimed, pk)
		}
		out = append(out, WorkloadInfo{
			Hash:    hash,
			Spec:    spec,
			Claimed: claimed,
		})
	}
	return out
}

func (s Snapshot) DeniedPeers() []types.PeerKey {
	return s.DeniedKeys
}

func (s *store) snapshotLocked() Snapshot {
	valid := s.validNodesLocked()
	live := liveComponent(s.localID, valid)

	nodes := make(map[types.PeerKey]NodeView, len(valid))
	heatmaps := make(map[types.PeerKey]map[types.PeerKey]TrafficSnapshot, len(valid))
	for pk, rec := range valid {
		nodes[pk] = rec.NodeView
		if len(rec.TrafficRates) > 0 {
			heatmaps[pk] = rec.TrafficRates
		}
	}

	peerKeys := slices.Collect(maps.Keys(live))

	// Specs: conflict resolution — lowest PeerKey wins.
	specs := make(map[string]WorkloadSpecView)
	for peerID, rec := range valid {
		for hash, spec := range rec.WorkloadSpecs {
			existing, ok := specs[hash]
			if !ok || peerID.Compare(existing.Publisher) < 0 {
				specs[hash] = WorkloadSpecView{Spec: spec, Publisher: peerID}
			}
		}
	}

	// Claims: only from live-component nodes.
	claims := make(map[string]map[types.PeerKey]struct{})
	for peerID, rec := range valid {
		if _, ok := live[peerID]; !ok {
			continue
		}
		for hash := range rec.WorkloadClaims {
			if claims[hash] == nil {
				claims[hash] = make(map[types.PeerKey]struct{})
			}
			claims[hash][peerID] = struct{}{}
		}
	}

	placements := make(map[types.PeerKey]NodePlacementState, len(live))
	for pk := range live {
		nv := nodes[pk]
		nps := NodePlacementState{
			CPUPercent:    nv.CPUPercent,
			MemPercent:    nv.MemPercent,
			MemTotalBytes: nv.MemTotalBytes,
			NumCPU:        nv.NumCPU,
			Coord:         nv.VivaldiCoord,
		}
		if len(nv.TrafficRates) > 0 {
			nps.TrafficTo = make(map[types.PeerKey]uint64, len(nv.TrafficRates))
			for peer, rate := range nv.TrafficRates {
				nps.TrafficTo[peer] = rate.BytesIn + rate.BytesOut
			}
		}
		placements[pk] = nps
	}

	deniedKeys := slices.SortedFunc(maps.Keys(s.denied), func(a, b types.PeerKey) int { return a.Compare(b) })

	return Snapshot{
		LocalID:    s.localID,
		Nodes:      nodes,
		PeerKeys:   peerKeys,
		Specs:      specs,
		Claims:     claims,
		Placements: placements,
		Heatmaps:   heatmaps,
		DeniedKeys: deniedKeys,
		digest:     Digest{proto: s.digestLocked()},
	}
}

// liveComponent returns the set of valid peers reachable from the local node
// via BFS over the reachability graph.
func liveComponent(localID types.PeerKey, valid map[types.PeerKey]nodeRecord) map[types.PeerKey]struct{} {
	component := map[types.PeerKey]struct{}{localID: {}}
	queue := []types.PeerKey{localID}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		rec, ok := valid[cur]
		if !ok {
			continue
		}
		for neighbor := range rec.Reachable {
			if _, seen := component[neighbor]; seen {
				continue
			}
			if _, isValid := valid[neighbor]; !isValid {
				continue
			}
			component[neighbor] = struct{}{}
			queue = append(queue, neighbor)
		}
	}
	return component
}
