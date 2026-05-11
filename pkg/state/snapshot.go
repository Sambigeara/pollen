// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

type Snapshot struct {
	Nodes          map[types.PeerKey]NodeView
	Specs          map[string]WorkloadSpecView
	Claims         map[string]map[types.PeerKey]struct{}
	DrainingClaims map[string]map[types.PeerKey]struct{}
	StaticSpecs    map[string]StaticSpecView
	StaticClaims   map[string]map[types.PeerKey]struct{}
	BlobSpecs      map[string]BlobSpecView
	Wrappings      map[string]map[types.PeerKey]*statev1.BlobWrappingChange
	digest         Digest
	live           map[types.PeerKey]struct{}
	PeerKeys       []types.PeerKey
	DeniedKeys     []types.PeerKey
	LocalID        types.PeerKey
}

type StaticSpecView struct {
	Auth      *admissionv1.SpecAuth
	Spec      StaticSpec
	Publisher types.PeerKey
}

type BlobSpecView struct {
	Auth      *admissionv1.SpecAuth
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
	Cert               *admissionv1.DelegationCert
	VivaldiCoord       *coords.Coord
	ObservedExternalIP string
	LastAddr           string
	Name               string
	PeerPub            []byte
	IPs                []string
	NatType            nat.Type
	VivaldiErr         float64
	MemTotalBytes      uint64
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
	Auth      *admissionv1.SpecAuth
	Spec      WorkloadSpec
	Publisher types.PeerKey
}

type TrafficSnapshot struct {
	RateIn  uint64
	RateOut uint64
}

type Service struct {
	Auth     *admissionv1.SpecAuth
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

// LocalCert returns the local node's delegation cert as published into
// gossip, or nil if the local node hasn't published one yet (the
// cluster-bootstrap window before SetLocalDelegationCert fires).
func (s Snapshot) LocalCert() *admissionv1.DelegationCert {
	nv, ok := s.Nodes[s.LocalID]
	if !ok {
		return nil
	}
	return nv.Cert
}

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
	Auth     *admissionv1.SpecAuth
	Name     string
	Peer     types.PeerKey
	Port     uint32
	Protocol statev1.ServiceProtocol
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

// WrappingFor returns the wrapping addressed to recipient for
// blobHash, or false when no peer has gossiped one. Callers can trust
// the wrapper identity without re-verifying because admission already
// validated the chain and signature.
func (s Snapshot) WrappingFor(blobHash string, recipient types.PeerKey) (*statev1.BlobWrappingChange, bool) {
	byRecipient, ok := s.Wrappings[blobHash]
	if !ok {
		return nil, false
	}
	w, ok := byRecipient[recipient]
	return w, ok
}

// ManifestPaths resolves a static-manifest digest to the set of file
// content-addressed digests it references. Implementations read the
// manifest blob from local CAS; missing or not-yet-fetched manifests
// return (nil, false).
type ManifestPaths interface {
	ManifestPaths(digest string) (map[string]struct{}, bool)
}

// BlobEntitlements returns every spec auth that references hash, either
// directly (workload-spec hash, blob-spec digest, static-spec manifest
// digest) or indirectly via a locally-readable static manifest's path
// list. Each returned auth is a candidate entitlement: a node may hold
// or serve the bytes if any cert satisfies one of them. The set is
// union, not intersection: revoking one publisher's entitlement only
// matters if it was the last reference standing.
//
// Pass a nil mp to skip nested-manifest resolution; callers that don't
// have a CAS handle (e.g. snapshot-only tests) still get correct
// answers for the direct cases.
func (s Snapshot) BlobEntitlements(hash string, mp ManifestPaths) []*admissionv1.SpecAuth {
	var out []*admissionv1.SpecAuth
	if sv, ok := s.Specs[hash]; ok && sv.Auth != nil {
		out = append(out, sv.Auth)
	}
	if bv, ok := s.BlobSpecs[hash]; ok && bv.Auth != nil {
		out = append(out, bv.Auth)
	}
	for _, sv := range s.StaticSpecs {
		if sv.Auth == nil {
			continue
		}
		if sv.Spec.ManifestDigest == hash {
			out = append(out, sv.Auth)
			continue
		}
		if mp == nil {
			continue
		}
		paths, ok := mp.ManifestPaths(sv.Spec.ManifestDigest)
		if !ok {
			continue
		}
		if _, hit := paths[hash]; hit {
			out = append(out, sv.Auth)
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
			out = append(out, ServiceInfo{Name: name, Port: svc.Port, Peer: pk, Protocol: svc.Protocol, Auth: svc.Auth})
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
	wrappings := make(map[string]map[types.PeerKey]*statev1.BlobWrappingChange)
	wrapperBy := make(map[string]map[types.PeerKey]types.PeerKey)
	// Iterating valid (not s.nodes) means specs published only by a
	// denied peer drop out of the snapshot. Their gossip events stay in
	// the log so deny scoping can still reason about them, but
	// gate.Invoke/Fetch/Connect should not surface a resource whose only
	// publisher has lost authority.
	outranks := func(candidate, incumbent types.PeerKey) bool {
		return candidate.Compare(incumbent) < 0
	}
	for pk, rec := range valid {
		for key, ev := range rec.log {
			if ev.Deleted {
				continue
			}
			switch key.kind { //nolint:exhaustive
			case attrWorkloadSpec:
				if existing, ok := specs[key.name]; !ok || outranks(pk, existing.Publisher) {
					sc := ev.GetSpecChange()
					specs[key.name] = WorkloadSpecView{
						Spec:      workloadSpecFromProto(sc.GetWorkload()),
						Auth:      sc.GetAuth(),
						Publisher: pk,
					}
				}
			case attrStaticSpec:
				if existing, ok := staticSpecs[key.name]; !ok || outranks(pk, existing.Publisher) {
					sc := ev.GetSpecChange()
					staticSpecs[key.name] = StaticSpecView{
						Spec:      staticSpecFromProto(sc.GetStatic()),
						Auth:      sc.GetAuth(),
						Publisher: pk,
					}
				}
			case attrBlobSpec:
				if existing, ok := blobSpecs[key.name]; !ok || outranks(pk, existing.Publisher) {
					sc := ev.GetSpecChange()
					blobSpecs[key.name] = BlobSpecView{
						Spec:      blobSpecFromProto(sc.GetBlob()),
						Auth:      sc.GetAuth(),
						Publisher: pk,
					}
				}
			case attrBlobWrapping:
				w := ev.GetBlobWrapping()
				if w == nil {
					continue
				}
				if existingPub, ok := wrapperBy[key.name][key.peer]; ok && !outranks(pk, existingPub) {
					continue
				}
				byRecipient, ok := wrappings[key.name]
				if !ok {
					byRecipient = make(map[types.PeerKey]*statev1.BlobWrappingChange)
					wrappings[key.name] = byRecipient
				}
				byPub, ok := wrapperBy[key.name]
				if !ok {
					byPub = make(map[types.PeerKey]types.PeerKey)
					wrapperBy[key.name] = byPub
				}
				byRecipient[key.peer] = w
				byPub[key.peer] = pk
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
		Wrappings:      wrappings,
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
		case *statev1.GossipEvent_SpecChange:
			if svc := v.SpecChange.GetService(); svc != nil {
				nv.Services[key.name] = &Service{Name: key.name, Port: svc.Port, Protocol: NormaliseProtocol(svc.Protocol), Auth: v.SpecChange.GetAuth()}
			}
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
		case *statev1.GossipEvent_DelegationCert:
			nv.Cert = v.DelegationCert.GetCert()
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
