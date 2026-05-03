// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
	"google.golang.org/protobuf/types/known/structpb"
)

type Snapshot struct {
	Nodes          map[types.PeerKey]NodeView
	Specs          map[string]WorkloadSpecView
	Claims         map[string]map[types.PeerKey]struct{}
	DrainingClaims map[string]map[types.PeerKey]struct{}
	StaticSpecs    map[string]StaticSpecView
	StaticClaims   map[string]map[types.PeerKey]struct{}
	BlobSpecs      map[string]BlobSpecView
	digest         Digest
	live           map[types.PeerKey]struct{}
	PeerKeys       []types.PeerKey
	DeniedKeys     []types.PeerKey
	LocalID        types.PeerKey
}

type StaticSpecView struct {
	Spec      StaticSpec
	Publisher types.PeerKey
}

type BlobSpecView struct {
	Spec      BlobSpec
	Publisher types.PeerKey
}

type NodeView struct {
	LastEventAt        time.Time
	BackoffExpiry      time.Time
	TrafficRates       map[types.PeerKey]TrafficSnapshot
	Reachable          map[types.PeerKey]struct{}
	Services           map[string]*Service
	CallCounts         map[string]uint64
	Blobs              map[string]struct{}
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
	MemPercent         uint32
	NumCPU             uint32
	CPUPercent         uint32
	ExternalPort       uint32
	LocalPort          uint32
	PubliclyAccessible bool
	AdminCapable       bool
	CanServeStatic     bool
}

type WorkloadSpecView struct {
	Spec      WorkloadSpec
	Publisher types.PeerKey
}

type TrafficSnapshot struct {
	RateIn  uint64
	RateOut uint64
}

type Service struct {
	Properties *structpb.Struct
	Name       string
	Port       uint32
	Protocol   statev1.ServiceProtocol
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

// Backward compatibility: gossip data that predates the protocol field
// carries UNSPECIFIED, which callers treat as TCP.
func NormaliseProtocol(p statev1.ServiceProtocol) statev1.ServiceProtocol {
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
		if sv.Spec.Name != name {
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
		if sv.Spec.Name == name && sv.Publisher == localID {
			return hash, true
		}
	}
	return "", false
}

type ServiceInfo struct {
	Properties *structpb.Struct
	Name       string
	Peer       types.PeerKey
	Port       uint32
	Protocol   statev1.ServiceProtocol
}

// PeersWithBlob returns live peers advertising hash. Stale BlobAvailability
// from offline peers persists in gossip until cert expiry; including them would
// direct fetches at unreachable nodes.
func (s Snapshot) PeersWithBlob(hash string) []types.PeerKey {
	live := make(map[types.PeerKey]struct{}, len(s.PeerKeys))
	for _, pk := range s.PeerKeys {
		live[pk] = struct{}{}
	}
	var out []types.PeerKey
	for pk, nv := range s.Nodes {
		if _, ok := live[pk]; !ok {
			continue
		}
		if _, ok := nv.Blobs[hash]; ok {
			out = append(out, pk)
		}
	}
	return out
}

func (s Snapshot) BlobByName(name string) (string, BlobSpecView, bool) {
	var bestDigest string
	var bestView BlobSpecView
	found := false
	for digest, view := range s.BlobSpecs {
		if view.Spec.Name != name {
			continue
		}
		if !found || view.Publisher.Compare(bestView.Publisher) < 0 {
			bestDigest = digest
			bestView = view
			found = true
		}
	}
	return bestDigest, bestView, found
}

func (s Snapshot) Services() []ServiceInfo {
	var out []ServiceInfo
	for pk, nv := range s.Nodes {
		for name, svc := range nv.Services {
			out = append(out, ServiceInfo{Name: name, Port: svc.Port, Peer: pk, Protocol: svc.Protocol, Properties: svc.Properties})
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
	claims := make(map[string]map[types.PeerKey]struct{})
	drainingClaims := make(map[string]map[types.PeerKey]struct{})
	staticClaims := make(map[string]map[types.PeerKey]struct{})

	for pk, rec := range valid {
		nv, recClaims, recStaticClaims := buildNodeView(pk, rec)
		nodes[pk] = nv

		for hash, draining := range recClaims {
			if claims[hash] == nil {
				claims[hash] = make(map[types.PeerKey]struct{})
			}
			claims[hash][pk] = struct{}{}
			if draining {
				if drainingClaims[hash] == nil {
					drainingClaims[hash] = make(map[types.PeerKey]struct{})
				}
				drainingClaims[hash][pk] = struct{}{}
			}
		}

		for name := range recStaticClaims {
			if staticClaims[name] == nil {
				staticClaims[name] = make(map[types.PeerKey]struct{})
			}
			staticClaims[name][pk] = struct{}{}
		}
	}

	specs := make(map[string]WorkloadSpecView)
	staticSpecs := make(map[string]StaticSpecView)
	blobSpecs := make(map[string]BlobSpecView)
	outranks := func(candidate, incumbent types.PeerKey) bool {
		_, candidateValid := valid[candidate]
		_, incumbentValid := valid[incumbent]
		if candidateValid != incumbentValid {
			return candidateValid
		}
		return candidate.Compare(incumbent) < 0
	}
	for pk, rec := range s.nodes {
		for key, ev := range rec.log {
			if ev.Deleted {
				continue
			}
			switch key.kind { //nolint:exhaustive
			case attrWorkloadSpec:
				if existing, ok := specs[key.name]; !ok || outranks(pk, existing.Publisher) {
					specs[key.name] = WorkloadSpecView{
						Spec:      workloadSpecFromProto(ev.GetWorkloadSpec()),
						Publisher: pk,
					}
				}
			case attrStaticSpec:
				if existing, ok := staticSpecs[key.name]; !ok || outranks(pk, existing.Publisher) {
					staticSpecs[key.name] = StaticSpecView{
						Spec:      staticSpecFromProto(ev.GetStaticSpec()),
						Publisher: pk,
					}
				}
			case attrBlobSpec:
				if existing, ok := blobSpecs[key.name]; !ok || outranks(pk, existing.Publisher) {
					blobSpecs[key.name] = BlobSpecView{
						Spec:      blobSpecFromProto(ev.GetBlobSpec()),
						Publisher: pk,
					}
				}
			}
		}
	}

	live := s.calculateLiveComponent(nodes, now)

	peersDigest := make(map[string]*statev1.PeerDigest, len(valid))
	for pk, rec := range valid {
		peersDigest[pk.String()] = &statev1.PeerDigest{
			MaxCounter: rec.maxCounter,
			StateHash:  s.computePeerHash(rec),
		}
	}

	denied := slices.SortedFunc(maps.Keys(s.denied), func(a, b types.PeerKey) int { return a.Compare(b) })

	filteredClaims := filterLive(claims, live)
	filteredDrainingClaims := filterLive(drainingClaims, live)
	filteredStaticClaims := filterLive(staticClaims, live)

	return Snapshot{
		LocalID:        s.localID,
		Nodes:          nodes,
		Specs:          specs,
		DrainingClaims: filteredDrainingClaims,
		Claims:         filteredClaims,
		StaticSpecs:    staticSpecs,
		StaticClaims:   filteredStaticClaims,
		BlobSpecs:      blobSpecs,
		live:           live,
		PeerKeys:       slices.SortedFunc(maps.Keys(live), types.PeerKey.Compare),
		DeniedKeys:     denied,
		digest:         Digest{proto: &statev1.Digest{Peers: peersDigest}},
	}
}

func filterLive(claims map[string]map[types.PeerKey]struct{}, live map[types.PeerKey]struct{}) map[string]map[types.PeerKey]struct{} {
	out := make(map[string]map[types.PeerKey]struct{})
	for key, peerMap := range claims {
		for pk := range peerMap {
			if _, ok := live[pk]; ok {
				if out[key] == nil {
					out[key] = make(map[types.PeerKey]struct{})
				}
				out[key][pk] = struct{}{}
			}
		}
	}
	return out
}

// claims bool value: true = draining (still claimant but scheduled for
// release), allowing peers to issue replacement claims for make-before-break.
func buildNodeView(pk types.PeerKey, rec nodeRecord) (NodeView, map[string]bool, map[string]struct{}) {
	nv := NodeView{
		PeerPub:      pk.Bytes(),
		Services:     make(map[string]*Service),
		Reachable:    make(map[types.PeerKey]struct{}),
		TrafficRates: make(map[types.PeerKey]TrafficSnapshot),
		CallCounts:   make(map[string]uint64),
		Blobs:        make(map[string]struct{}),
		LastAddr:     rec.LastAddr,
		LastEventAt:  rec.lastEventAt,
	}
	claims := make(map[string]bool)
	staticClaims := make(map[string]struct{})

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
			nv.Services[key.name] = &Service{Name: key.name, Port: v.Service.Port, Protocol: NormaliseProtocol(v.Service.Protocol), Properties: v.Service.Properties}
		case *statev1.GossipEvent_Reachability:
			nv.Reachable[key.peer] = struct{}{}
		case *statev1.GossipEvent_PubliclyAccessible:
			nv.PubliclyAccessible = true
		case *statev1.GossipEvent_AdminCapable:
			nv.AdminCapable = true
		case *statev1.GossipEvent_StaticCapable:
			nv.CanServeStatic = true
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
		case *statev1.GossipEvent_WorkloadClaim:
			claims[key.name] = v.WorkloadClaim.GetDraining()
		case *statev1.GossipEvent_TrafficHeatmap:
			for _, r := range v.TrafficHeatmap.Rates {
				if peerPK, err := types.PeerKeyFromString(r.PeerId); err == nil {
					nv.TrafficRates[peerPK] = TrafficSnapshot{RateIn: r.RateIn, RateOut: r.RateOut}
				}
			}
		case *statev1.GossipEvent_BackoffTtl:
			if v.BackoffTtl != nil {
				nv.BackoffExpiry = time.UnixMilli(v.BackoffTtl.ExpiresAtUnixMs)
			}
		case *statev1.GossipEvent_PerSeedCallCounts:
			if v.PerSeedCallCounts != nil {
				maps.Copy(nv.CallCounts, v.PerSeedCallCounts.Counts)
			}
		case *statev1.GossipEvent_BlobAvailability:
			for _, d := range v.BlobAvailability.GetDigests() {
				nv.Blobs[hex.EncodeToString(d)] = struct{}{}
			}
		case *statev1.GossipEvent_Heartbeat:
		case *statev1.GossipEvent_NodeName:
			nv.Name = v.NodeName.Name
		case *statev1.GossipEvent_StaticClaim:
			staticClaims[key.name] = struct{}{}
		case *statev1.GossipEvent_WorkloadSpec, *statev1.GossipEvent_StaticSpec, *statev1.GossipEvent_BlobSpec:
		}
	}
	return nv, claims, staticClaims
}

func staticSpecFromProto(p *statev1.StaticSpecChange) StaticSpec {
	return StaticSpec{
		Name:           p.GetName(),
		ManifestDigest: hex.EncodeToString(p.GetManifestDigest()),
	}
}

func blobSpecFromProto(p *statev1.BlobSpecChange) BlobSpec {
	return BlobSpec{
		Name:   p.GetName(),
		Digest: hex.EncodeToString(p.GetDigest()),
	}
}

const reachableMaxAge = 30 * time.Second

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

		// A stale node may be live (vouched by others) but its own
		// reachability claims are not trusted.
		if curr != s.localID && now.Sub(nv.LastEventAt) > reachableMaxAge {
			continue
		}

		for neighbor := range nv.Reachable {
			if _, seen := live[neighbor]; !seen {
				if _, ok := nodes[neighbor]; ok {
					live[neighbor] = struct{}{}
					queue = append(queue, neighbor)
				}
			}
		}
	}
	return live
}
