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
)

type Snapshot struct {
	Nodes        map[types.PeerKey]NodeView
	Specs        map[string]WorkloadSpecView
	Claims       map[string]map[types.PeerKey]struct{}
	StaticSpecs  map[string]StaticSpecView
	StaticClaims map[string]map[types.PeerKey]struct{}
	BlobSpecs    map[string]BlobSpecView
	digest       Digest
	live         map[types.PeerKey]struct{}
	PeerKeys     []types.PeerKey
	DeniedKeys   []types.PeerKey
	LocalID      types.PeerKey
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
	TrafficRates       map[types.PeerKey]TrafficSnapshot
	Reachable          map[types.PeerKey]struct{}
	Services           map[string]*Service
	SeedMetrics        map[string]SeedMetrics
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
	CPUPercent         uint32
	MemPercent         uint32
	NumCPU             uint32
	CPUBudgetPercent   uint32
	MemBudgetPercent   uint32
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
	Name     string
	Port     uint32
	Peer     types.PeerKey
	Protocol statev1.ServiceProtocol
}

// PeersWithBlob returns live peers advertising hash. Stale BlobAvailability
// from offline peers persists in gossip until cert expiry; including them
// would direct fetches at unreachable nodes.
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

// BlobByName returns the lowest-peer-key publisher's spec when multiple
// peers publish the same name.
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
	claims := make(map[string]map[types.PeerKey]struct{})
	staticClaims := make(map[string]map[types.PeerKey]struct{})

	for pk, rec := range valid {
		nv, recClaims, recStaticClaims := buildNodeView(pk, rec)
		nodes[pk] = nv

		for hash := range recClaims {
			if claims[hash] == nil {
				claims[hash] = make(map[types.PeerKey]struct{})
			}
			claims[hash][pk] = struct{}{}
		}

		for name := range recStaticClaims {
			if staticClaims[name] == nil {
				staticClaims[name] = make(map[types.PeerKey]struct{})
			}
			staticClaims[name][pk] = struct{}{}
		}
	}

	// Specs are cluster-scoped admin intent: collected from every peer's
	// log so they survive publisher departure — cert expiry, deny,
	// liveness. Valid publishers outrank invalid ones so a fresh live
	// seed supersedes a departed peer's stale spec; within the same tier
	// the lower peer key wins.
	//
	// TODO(saml): O(n_peers * n_attrs) on every buildSnapshot call. Acceptable
	// while specs are rare and buildNodeView already walks every valid peer's
	// log in the same shape. If this becomes a hot path, memoise via a
	// cluster-scoped index maintained on ingest — at the cost of bookkeeping
	// across every log-mutation path.
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
	filteredStaticClaims := filterLive(staticClaims, live)

	return Snapshot{
		LocalID:      s.localID,
		Nodes:        nodes,
		Specs:        specs,
		Claims:       filteredClaims,
		StaticSpecs:  staticSpecs,
		StaticClaims: filteredStaticClaims,
		BlobSpecs:    blobSpecs,
		live:         live,
		PeerKeys:     slices.Collect(maps.Keys(live)),
		DeniedKeys:   denied,
		digest:       Digest{proto: &statev1.Digest{Peers: peersDigest}},
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

// buildNodeView projects a peer's log into its peer-local NodeView plus
// the per-peer claim sets (workload and static). Spec declarations are
// cluster-scoped and live in s.specs; they are not surfaced here.
func buildNodeView(pk types.PeerKey, rec nodeRecord) (NodeView, map[string]struct{}, map[string]struct{}) {
	nv := NodeView{
		PeerPub:      pk.Bytes(),
		Services:     make(map[string]*Service),
		Reachable:    make(map[types.PeerKey]struct{}),
		TrafficRates: make(map[types.PeerKey]TrafficSnapshot),
		SeedMetrics:  make(map[string]SeedMetrics),
		Blobs:        make(map[string]struct{}),
		LastAddr:     rec.LastAddr,
		LastEventAt:  rec.lastEventAt,
	}
	claims := make(map[string]struct{})
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
			nv.Services[key.name] = &Service{Name: key.name, Port: v.Service.Port, Protocol: NormaliseProtocol(v.Service.Protocol)}
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
			nv.CPUBudgetPercent, nv.MemBudgetPercent = v.ResourceTelemetry.CpuBudgetPercent, v.ResourceTelemetry.MemBudgetPercent
		case *statev1.GossipEvent_WorkloadClaim:
			claims[key.name] = struct{}{}
		case *statev1.GossipEvent_TrafficHeatmap:
			for _, r := range v.TrafficHeatmap.Rates {
				if peerPK, err := types.PeerKeyFromString(r.PeerId); err == nil {
					nv.TrafficRates[peerPK] = TrafficSnapshot{RateIn: r.RateIn, RateOut: r.RateOut}
				}
			}
		case *statev1.GossipEvent_SeedMetrics:
			maps.Copy(nv.SeedMetrics, seedMetricsFromProto(v.SeedMetrics))
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
		MinReplicas:    p.GetMinReplicas(),
		Claim:          claimFromProto(p.GetPublisherClaim()),
	}
}

func blobSpecFromProto(p *statev1.BlobSpecChange) BlobSpec {
	return BlobSpec{
		Name:   p.GetName(),
		Digest: hex.EncodeToString(p.GetDigest()),
		Claim:  claimFromProto(p.GetPublisherClaim()),
	}
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
