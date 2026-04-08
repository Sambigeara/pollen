package state

import (
	"fmt"
	"maps"
	"slices"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

type Snapshot struct {
	Nodes      map[types.PeerKey]NodeView
	Specs      map[string]WorkloadSpecView
	Claims     map[string]map[types.PeerKey]struct{}
	digest     Digest
	live       map[types.PeerKey]struct{}
	PeerKeys   []types.PeerKey
	DeniedKeys []types.PeerKey
	LocalID    types.PeerKey
}

type NodeView struct {
	LastEventAt        time.Time
	TrafficRates       map[types.PeerKey]TrafficSnapshot
	Reachable          map[types.PeerKey]struct{}
	Services           map[string]*Service
	SeedLoad           map[string]float32
	SeedDemand         map[string]float32
	VivaldiCoord       *coords.Coord
	ObservedExternalIP string
	LastAddr           string
	Name               string
	PeerPub            []byte
	IPs                []string
	NatType            nat.Type
	VivaldiErr         float64
	MemTotalBytes      uint64
	CertExpiry         int64
	CPUPercent         uint32
	MemPercent         uint32
	NumCPU             uint32
	CPUBudgetPercent   uint32
	MemBudgetPercent   uint32
	ExternalPort       uint32
	LocalPort          uint32
	PubliclyAccessible bool
	AdminCapable       bool
}

type WorkloadSpecView struct {
	Spec      *statev1.WorkloadSpecChange
	Publisher types.PeerKey
}

type TrafficSnapshot struct {
	BytesIn  uint64
	BytesOut uint64
}

type Service struct {
	Name     string
	Port     uint32
	Protocol statev1.ServiceProtocol
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

// TODO(saml): remove once all persisted state and in-flight gossip carry an explicit protocol.
// normaliseProtocol maps the zero value (UNSPECIFIED) to TCP for backward
// compatibility with gossip data that predates the protocol field.
func normaliseProtocol(p statev1.ServiceProtocol) statev1.ServiceProtocol {
	if p == statev1.ServiceProtocol_SERVICE_PROTOCOL_UNSPECIFIED {
		return statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP
	}
	return p
}

func (s Snapshot) Digest() Digest               { return s.digest }
func (s Snapshot) DeniedPeers() []types.PeerKey { return s.DeniedKeys }

func (s Snapshot) SpecByName(name string) (string, WorkloadSpecView, bool) {
	var bestHash string
	var bestView WorkloadSpecView
	found := false
	for hash, sv := range s.Specs {
		if sv.Spec.GetName() != name {
			continue
		}
		if !found || sv.Publisher.Compare(bestView.Publisher) < 0 {
			bestHash = hash
			bestView = sv
			found = true
		}
	}
	return bestHash, bestView, found
}

func (s Snapshot) LocalSpecByName(name string, localID types.PeerKey) (string, bool) {
	for hash, sv := range s.Specs {
		if sv.Spec.GetName() == name && sv.Publisher == localID {
			return hash, true
		}
	}
	return "", false
}

type ServiceInfo struct {
	Name     string
	Port     uint32
	Peer     types.PeerKey
	Protocol statev1.ServiceProtocol
}

func (s Snapshot) Services() []ServiceInfo {
	var out []ServiceInfo
	for pk, nv := range s.Nodes {
		for name, svc := range nv.Services {
			out = append(out, ServiceInfo{Name: name, Port: svc.Port, Peer: pk, Protocol: svc.Protocol})
		}
	}
	return out
}

func (s *store) buildSnapshot() Snapshot {
	now := s.nowFunc()
	valid := make(map[types.PeerKey]nodeRecord)

	for pk, rec := range s.nodes {
		if _, isDenied := s.denied[pk]; isDenied {
			continue
		}
		if ev, ok := rec.log[attrKey{kind: attrCertExpiry}]; ok && !ev.Deleted {
			if auth.IsExpiredAt(time.Unix(ev.GetCertExpiry().ExpiryUnix, 0), now) {
				continue
			}
		}
		valid[pk] = rec
	}

	nodes := make(map[types.PeerKey]NodeView)
	specs := make(map[string]WorkloadSpecView)
	claims := make(map[string]map[types.PeerKey]struct{})

	for pk, rec := range valid {
		nv, recSpecs, recClaims := buildNodeView(pk, rec)
		nodes[pk] = nv

		for hash, spec := range recSpecs {
			if existing, ok := specs[hash]; !ok || pk.Compare(existing.Publisher) < 0 {
				specs[hash] = WorkloadSpecView{Spec: spec, Publisher: pk}
			}
		}

		for hash := range recClaims {
			if claims[hash] == nil {
				claims[hash] = make(map[types.PeerKey]struct{})
			}
			claims[hash][pk] = struct{}{}
		}
	}

	live := s.calculateLiveComponent(nodes, now)

	peersDigest := make(map[string]*statev1.PeerDigest, len(live))
	for pk := range live {
		rec := s.nodes[pk]
		peersDigest[pk.String()] = &statev1.PeerDigest{
			MaxCounter: rec.maxCounter,
			StateHash:  s.computePeerHash(rec),
		}
	}

	denied := slices.SortedFunc(maps.Keys(s.denied), func(a, b types.PeerKey) int { return a.Compare(b) })

	// Only expose claims from live nodes
	filteredClaims := make(map[string]map[types.PeerKey]struct{})
	for hash, peerMap := range claims {
		for pk := range peerMap {
			if _, ok := live[pk]; ok {
				if filteredClaims[hash] == nil {
					filteredClaims[hash] = make(map[types.PeerKey]struct{})
				}
				filteredClaims[hash][pk] = struct{}{}
			}
		}
	}

	return Snapshot{
		LocalID:    s.localID,
		Nodes:      nodes,
		Specs:      specs,
		Claims:     filteredClaims,
		live:       live,
		PeerKeys:   slices.Collect(maps.Keys(live)),
		DeniedKeys: denied,
		digest:     Digest{proto: &statev1.Digest{Peers: peersDigest}},
	}
}

func buildNodeView(pk types.PeerKey, rec nodeRecord) (NodeView, map[string]*statev1.WorkloadSpecChange, map[string]struct{}) {
	nv := NodeView{
		PeerPub:      pk.Bytes(),
		Services:     make(map[string]*Service),
		Reachable:    make(map[types.PeerKey]struct{}),
		TrafficRates: make(map[types.PeerKey]TrafficSnapshot),
		SeedLoad:     make(map[string]float32),
		SeedDemand:   make(map[string]float32),
		LastAddr:     rec.LastAddr,
		LastEventAt:  rec.lastEventAt,
	}
	specs := make(map[string]*statev1.WorkloadSpecChange)
	claims := make(map[string]struct{})

	for key, ev := range rec.log {
		if ev.Deleted {
			continue
		}
		switch v := ev.Change.(type) {
		case *statev1.GossipEvent_Network:
			nv.IPs, nv.LocalPort = v.Network.Ips, v.Network.LocalPort
		case *statev1.GossipEvent_ObservedAddress:
			nv.ObservedExternalIP, nv.ExternalPort = v.ObservedAddress.Ip, v.ObservedAddress.Port
		case *statev1.GossipEvent_CertExpiry:
			nv.CertExpiry = v.CertExpiry.ExpiryUnix
		case *statev1.GossipEvent_Service:
			nv.Services[key.name] = &Service{Name: key.name, Port: v.Service.Port, Protocol: normaliseProtocol(v.Service.Protocol)}
		case *statev1.GossipEvent_Reachability:
			nv.Reachable[key.peer] = struct{}{}
		case *statev1.GossipEvent_PubliclyAccessible:
			nv.PubliclyAccessible = true
		case *statev1.GossipEvent_AdminCapable:
			nv.AdminCapable = true
		case *statev1.GossipEvent_Vivaldi:
			if v.Vivaldi != nil {
				nv.VivaldiCoord = &coords.Coord{X: v.Vivaldi.X, Y: v.Vivaldi.Y, Height: v.Vivaldi.Height}
				nv.VivaldiErr = v.Vivaldi.Error
			}
		case *statev1.GossipEvent_NatType:
			nv.NatType = nat.TypeFromUint32(v.NatType.NatType)
		case *statev1.GossipEvent_ResourceTelemetry:
			nv.CPUPercent, nv.MemPercent = v.ResourceTelemetry.CpuPercent, v.ResourceTelemetry.MemPercent
			nv.MemTotalBytes, nv.NumCPU = v.ResourceTelemetry.MemTotalBytes, v.ResourceTelemetry.NumCpu
			nv.CPUBudgetPercent, nv.MemBudgetPercent = v.ResourceTelemetry.CpuBudgetPercent, v.ResourceTelemetry.MemBudgetPercent
		case *statev1.GossipEvent_WorkloadSpec:
			specs[key.name] = v.WorkloadSpec
		case *statev1.GossipEvent_WorkloadClaim:
			claims[key.name] = struct{}{}
		case *statev1.GossipEvent_TrafficHeatmap:
			for _, r := range v.TrafficHeatmap.Rates {
				if peerPK, err := types.PeerKeyFromString(r.PeerId); err == nil {
					nv.TrafficRates[peerPK] = TrafficSnapshot{BytesIn: r.BytesIn, BytesOut: r.BytesOut}
				}
			}
		case *statev1.GossipEvent_SeedLoad:
			maps.Copy(nv.SeedLoad, v.SeedLoad.Rates)
		case *statev1.GossipEvent_SeedDemand:
			maps.Copy(nv.SeedDemand, v.SeedDemand.Rates)
		case *statev1.GossipEvent_Heartbeat:
		case *statev1.GossipEvent_NodeName:
			nv.Name = v.NodeName.Name
		}
	}
	return nv, specs, claims
}

const reachableMaxAge = 30 * time.Second

// calculateLiveComponent walks the reachability graph starting from the local
// node. Only reachability claims from peers whose events we've received within
// reachableMaxAge are trusted; stale peers' claims are excluded from the
// vouched set. The local node's own claims are always trusted.
func (s *store) calculateLiveComponent(nodes map[types.PeerKey]NodeView, now time.Time) map[types.PeerKey]struct{} {
	vouched := make(map[types.PeerKey]struct{})
	for pk, nv := range nodes {
		if pk != s.localID && now.Sub(nv.LastEventAt) > reachableMaxAge {
			continue
		}
		for peer := range nv.Reachable {
			vouched[peer] = struct{}{}
		}
	}

	live := map[types.PeerKey]struct{}{s.localID: {}}
	queue := []types.PeerKey{s.localID}

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		if curr != s.localID {
			if _, ok := vouched[curr]; !ok {
				continue
			}
		}

		nv, ok := nodes[curr]
		if !ok {
			continue
		}

		// Only traverse a node's reachability claims if fresh (or local).
		// A stale node may still be in the live set (vouched by a fresh peer),
		// but its own claims about who it can reach are not trusted.
		if curr != s.localID && now.Sub(nv.LastEventAt) > reachableMaxAge {
			continue
		}

		for neighbor := range nv.Reachable {
			if _, seen := live[neighbor]; !seen {
				// nodes is pre-filtered (expired/denied peers removed); only enqueue
				// neighbours present in it so live is a subset of nodes and downstream
				// consumers (peersDigest, filteredClaims, encodeDelta) never see excluded peers.
				if _, ok := nodes[neighbor]; ok {
					live[neighbor] = struct{}{}
					queue = append(queue, neighbor)
				}
			}
		}
	}
	return live
}
