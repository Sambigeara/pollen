// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"cmp"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"time"

	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"
)

type Snapshot struct {
	Nodes                map[types.PeerKey]NodeView
	Specs                map[string]WorkloadSpecView
	Claims               map[string]map[types.PeerKey]struct{}
	DrainingClaims       map[string]map[types.PeerKey]struct{}
	StaticSpecs          map[string]StaticSpecView
	StaticClaims         map[StaticClaimKey]map[types.PeerKey]struct{}
	BlobSpecs            map[string]BlobSpecView
	Wrappings            map[string]map[types.PeerKey]*factv1.BlobWrapping
	WorkloadStoringPeers map[string]map[types.PeerKey]struct{}
	StaticStoringPeers   map[string]map[types.PeerKey]struct{}
	BlobStoringPeers     map[string]map[types.PeerKey]struct{}
	// SpecsAll/StaticSpecsAll/BlobSpecsAll hold one entry per (authority
	// Principal, logical name) without dedupe. The deduped Specs map
	// keys on the wasm content-hash and BlobSpecs on the blob digest
	// (the artefact identity, deliberately shared when two tenants
	// publish identical bytes), and StaticSpecs keys on name. Those are
	// the runtime/serving views. Ownership, visibility and accounting
	// are publication concerns and must iterate these per-authority
	// slices, because a tenant whose artefact or name collides with
	// another's is otherwise invisible in the deduped maps.
	SpecsAll       []WorkloadSpecView
	StaticSpecsAll []StaticSpecView
	BlobSpecsAll   []BlobSpecView
	digest         Digest
	live           map[types.PeerKey]struct{}
	PeerKeys       []types.PeerKey
	DeniedKeys     []types.PeerKey
	LocalID        types.PeerKey
}

type StaticSpecView struct {
	Fact      *factv1.Fact
	Spec      StaticSpec
	Publisher types.PeerKey
}

type BlobSpecView struct {
	Fact      *factv1.Fact
	Spec      BlobSpec
	Publisher types.PeerKey
}

// StaticClaimKey identifies a static-site claim by publishing authority
// and logical name. Two tenants naming a site the same would share one
// name-only claim register; keying by (authority, name) keeps each
// tenant's serving peers distinct.
type StaticClaimKey struct {
	Name      string
	Authority types.PeerKey
}

type NodeView struct {
	LastEventAt        time.Time
	BackoffExpiry      time.Time
	TrafficRates       map[types.PeerKey]TrafficSnapshot
	Reachable          map[types.PeerKey]struct{}
	Services           map[string]*Service
	CallCounts         map[string]uint64
	Blobs              map[string]struct{}
	Grant              *identityv1.Grant
	VivaldiCoord       *coords.Coord
	ObservedExternalIP string
	ControlAddr        string
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
	Fact      *factv1.Fact
	Spec      WorkloadSpec
	Publisher types.PeerKey
}

type TrafficSnapshot struct {
	RateIn  uint64
	RateOut uint64
}

type Service struct {
	Fact     *factv1.Fact
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

func (s Snapshot) IsDenied(peer types.PeerKey) bool {
	return slices.Contains(s.DeniedKeys, peer)
}

// DenyChecker adapts this snapshot's chain-aware deny set to the
// identity.DenyChecker shape used by durable-authority verification.
// The snapshot is captured by value, so the checker is a stable view
// safe to call from any goroutine.
func (s Snapshot) DenyChecker() identity.DenyChecker {
	return func(subjectPub []byte) bool {
		return s.IsDenied(types.PeerKeyFromBytes(subjectPub))
	}
}

// LocalGrant returns the local node's grant as published into gossip,
// or nil if the local node hasn't published one yet (the
// cluster-bootstrap window before SetLocalGrant fires).
func (s Snapshot) LocalGrant() *identityv1.Grant {
	nv, ok := s.Nodes[s.LocalID]
	if !ok {
		return nil
	}
	return nv.Grant
}

// GrantFor returns the grant gossiped for the node whose subject is
// authorityPub, or nil if that authority has not gossiped a grant. This
// is how a Fact's named authority is resolved to its durable Grant at
// admission and runtime, mirroring the store's grantForPeerLocked.
func (s Snapshot) GrantFor(authorityPub []byte) *identityv1.Grant {
	nv, ok := s.Nodes[types.PeerKeyFromBytes(authorityPub)]
	if !ok {
		return nil
	}
	return nv.Grant
}

// AuthorityUsage is the set of resource names an authority currently
// has live in cluster state, partitioned by budgeted kind. The account
// stage counts these against the authority's Budget; set membership
// keeps the check idempotent under gossip replay, since re-admitting a
// resource the authority already holds does not grow the set.
type AuthorityUsage struct {
	FunctionNames map[string]struct{}
	BlobNames     map[string]struct{}
	SiteNames     map[string]struct{}
}

// UsageByAuthority projects the snapshot to the resource names the
// authority owns, by budgeted kind. All three kinds come from the
// un-deduped per-(authority,name) sources, so a tenant's resource is
// counted even when another tenant's same-named or same-content
// resource wins the deduped runtime view.
func (s Snapshot) UsageByAuthority(authorityPub []byte) AuthorityUsage {
	authority := types.PeerKeyFromBytes(authorityPub)
	u := AuthorityUsage{
		FunctionNames: make(map[string]struct{}),
		BlobNames:     make(map[string]struct{}),
		SiteNames:     make(map[string]struct{}),
	}
	for _, sv := range s.SpecsAll {
		if sv.Publisher == authority {
			u.FunctionNames[sv.Spec.Name] = struct{}{}
		}
	}
	for _, bv := range s.BlobSpecsAll {
		if bv.Publisher == authority {
			u.BlobNames[bv.Spec.Name] = struct{}{}
		}
	}
	for _, sv := range s.StaticSpecsAll {
		if sv.Publisher == authority {
			u.SiteNames[sv.Spec.Name] = struct{}{}
		}
	}
	return u
}

// SpecByName resolves a workload logical name within a single authority.
// Publication identity is (authority, name), so a name resolves to at
// most one spec per authority and there is no cross-tenant tie-break:
// two tenants' identically named workloads are distinct registers.
func (s Snapshot) SpecByName(name string, authority types.PeerKey) (string, WorkloadSpecView, bool) {
	for _, sv := range s.SpecsAll {
		if sv.Spec.Name == name && sv.Publisher == authority {
			return sv.Spec.Hash, sv, true
		}
	}
	return "", WorkloadSpecView{}, false
}

// LocalSpecByName resolves a workload name published by this node. It
// is SpecByName scoped to the local authority: resolving through
// SpecsAll, not the deduped Specs map, is what stops a remote tenant's
// byte-identical content from hiding this node's own spec.
func (s Snapshot) LocalSpecByName(name string, localID types.PeerKey) (string, bool) {
	hash, _, ok := s.SpecByName(name, localID)
	return hash, ok
}

// LocalPublishesWorkload reports whether localID published a workload
// under hash. Like LocalSpecByName it scans SpecsAll, not the deduped
// Specs map.
func (s Snapshot) LocalPublishesWorkload(hash string, localID types.PeerKey) bool {
	for _, sv := range s.SpecsAll {
		if sv.Spec.Hash == hash && sv.Publisher == localID {
			return true
		}
	}
	return false
}

// LocalPublishesStatic reports whether localID published a static site
// under name; see LocalPublishesWorkload.
func (s Snapshot) LocalPublishesStatic(name string, localID types.PeerKey) bool {
	for _, sv := range s.StaticSpecsAll {
		if sv.Spec.Name == name && sv.Publisher == localID {
			return true
		}
	}
	return false
}

// LocalPublishesBlob reports whether localID published a named blob
// under digest; see LocalPublishesWorkload.
func (s Snapshot) LocalPublishesBlob(digest string, localID types.PeerKey) bool {
	for _, bv := range s.BlobSpecsAll {
		if bv.Spec.Digest == digest && bv.Publisher == localID {
			return true
		}
	}
	return false
}

type ServiceInfo struct {
	Fact     *factv1.Fact
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

// PeersWithWorkloadSpec returns live peers whose log carries a
// non-deleted workload-spec entry for hash. With signed-event relay,
// this is distinct from the spec's Publisher (authority): a peer can
// store and gossip a spec it didn't sign. Fetch routing for the
// workload binary consults this set, not Publisher.
func (s Snapshot) PeersWithWorkloadSpec(hash string) []types.PeerKey {
	return sortedPeerSet(s.WorkloadStoringPeers[hash])
}

// PeersWithStaticSpec returns live peers whose log carries a
// non-deleted static-spec entry for name. See PeersWithWorkloadSpec.
func (s Snapshot) PeersWithStaticSpec(name string) []types.PeerKey {
	return sortedPeerSet(s.StaticStoringPeers[name])
}

// PeersWithBlobSpec returns live peers whose log carries a
// non-deleted blob-spec entry for digest. See PeersWithWorkloadSpec.
func (s Snapshot) PeersWithBlobSpec(digest string) []types.PeerKey {
	return sortedPeerSet(s.BlobStoringPeers[digest])
}

func sortedPeerSet(set map[types.PeerKey]struct{}) []types.PeerKey {
	if len(set) == 0 {
		return nil
	}
	return slices.SortedFunc(maps.Keys(set), types.PeerKey.Compare)
}

// WrappingFor returns the wrapping addressed to recipient for
// blobHash, or false when no peer has gossiped one. Callers can trust
// the wrapper identity without re-verifying because admission already
// validated the chain and signature.
func (s Snapshot) WrappingFor(blobHash string, recipient types.PeerKey) (*factv1.BlobWrapping, bool) {
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

// BlobEntitlements returns every Fact that references hash directly
// (workload-spec hash, blob-spec digest, static-spec manifest digest)
// or indirectly via a locally-readable static manifest's path list. A
// node may hold or serve the bytes if any one Fact satisfies the
// caller; the set is a union, not an intersection, so revoking one
// publisher's entitlement only matters if it was the last reference
// standing. Like WorkloadEntitlements it reads the per-(authority,name)
// publication sources, never the deduped runtime maps.
//
// Pass a nil mp to skip nested-manifest resolution; callers without a
// CAS handle (e.g. snapshot-only tests) still get correct answers for
// the direct cases.
func (s Snapshot) BlobEntitlements(hash string, mp ManifestPaths) []*factv1.Fact {
	out := s.WorkloadEntitlements(hash)
	for _, bv := range s.BlobSpecsAll {
		if bv.Spec.Digest == hash && bv.Fact != nil {
			out = append(out, bv.Fact)
		}
	}
	for _, sv := range s.StaticSpecsAll {
		if sv.Fact == nil {
			continue
		}
		if sv.Spec.ManifestDigest == hash {
			out = append(out, sv.Fact)
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
			out = append(out, sv.Fact)
		}
	}
	return out
}

// WorkloadEntitlements returns every workload Fact whose content hash
// is hash, one per publishing authority. Invoke and host decisions
// over shared bytes are a union: two tenants on identical bytes are
// distinct publications with their own policies, so a caller that
// holds no specific (authority, name) authorises against the set and
// is admitted if any member allows it. The deduped Specs map is
// deliberately not consulted; it would collapse the set to one
// arbitrary co-publisher.
func (s Snapshot) WorkloadEntitlements(hash string) []*factv1.Fact {
	var out []*factv1.Fact
	for _, sv := range s.SpecsAll {
		if sv.Spec.Hash == hash && sv.Fact != nil {
			out = append(out, sv.Fact)
		}
	}
	return out
}

func (s Snapshot) Services() []ServiceInfo {
	var out []ServiceInfo
	for pk, nv := range s.Nodes {
		for name, svc := range nv.Services {
			out = append(out, ServiceInfo{Name: name, Port: svc.Port, Peer: pk, Protocol: svc.Protocol, Fact: svc.Fact})
		}
	}
	return out
}

// tombstoneBodyHashLen is the fixed width of a Fact's body hash (sha256
// today). The width is part of the tombstoneKey struct so the key
// stays comparable as a map key; if a future Fact swaps in a
// different digest algorithm this size needs to grow with it.
const tombstoneBodyHashLen = 32

// tombstoneKey identifies the (kind, identifier, publisher, body_hash)
// tuple whose presence in any peer's log suppresses live specs that
// match exactly across the cluster. publisher carries cross-slot
// suppression (Phase 3f); body_hash discriminates so a tombstone for
// an older revision of a name doesn't permanently kill future
// re-publishes under different content.
type tombstoneKey struct {
	name      string
	publisher types.PeerKey
	kind      attrKind
	bodyHash  [tombstoneBodyHashLen]byte
}

func newTombstoneKey(kind attrKind, name string, pub types.PeerKey, bodyHash []byte) tombstoneKey {
	k := tombstoneKey{kind: kind, name: name, publisher: pub}
	copy(k.bodyHash[:], bodyHash)
	return k
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
	staticClaims := make(map[StaticClaimKey]map[types.PeerKey]struct{})

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

		for ck := range recStaticClaims {
			if staticClaims[ck] == nil {
				staticClaims[ck] = make(map[types.PeerKey]struct{})
			}
			staticClaims[ck][pk] = struct{}{}
		}
	}

	specs := make(map[string]WorkloadSpecView)
	specsByPub := make(map[types.PeerKey]map[string]WorkloadSpecView)
	staticSpecs := make(map[string]StaticSpecView)
	staticSpecsByPub := make(map[types.PeerKey]map[string]StaticSpecView)
	blobSpecs := make(map[string]BlobSpecView)
	blobSpecsByPub := make(map[types.PeerKey]map[string]BlobSpecView)
	specStoring := make(map[string]map[types.PeerKey]struct{})
	staticStoring := make(map[string]map[types.PeerKey]struct{})
	blobStoring := make(map[string]map[types.PeerKey]struct{})
	wrappings := make(map[string]map[types.PeerKey]*factv1.BlobWrapping)
	wrapperBy := make(map[string]map[types.PeerKey]types.PeerKey)
	// Pre-pass: collect every publisher-signed tombstone keyed by
	// (kind, name, publisher). A tombstone in any peer's slot kills
	// every live spec by the same publisher with the same (kind, name)
	// across the cluster; this is what makes wire-mode unseeds work
	// from an edge node that didn't originally accept the seed.
	tombstones := make(map[tombstoneKey]struct{})
	for _, rec := range valid {
		for key, ev := range rec.log {
			if !ev.Deleted || !isSpecKind(key.kind) {
				continue
			}
			auth := ev.GetSpecChange().GetFact()
			pub := types.PeerKeyFromBytes(auth.GetAuthorityPub())
			tombstones[newTombstoneKey(key.kind, key.name, pub, auth.GetBodyHash())] = struct{}{}
		}
	}
	// Iterating valid (not s.nodes) means specs published only by a
	// denied peer drop out of the snapshot. Their gossip events stay in
	// the log so deny scoping can still reason about them, but
	// admission Invoke/Fetch/Connect should not surface a resource whose
	// only publisher has lost authority.
	outranks := func(candidate, incumbent types.PeerKey) bool {
		return candidate.Compare(incumbent) < 0
	}
	for pk, rec := range valid {
		for key, ev := range rec.log {
			if ev.Deleted {
				continue
			}
			var publisher types.PeerKey
			if isSpecKind(key.kind) {
				auth := ev.GetSpecChange().GetFact()
				publisher = types.PeerKeyFromBytes(auth.GetAuthorityPub())
				if _, killed := tombstones[newTombstoneKey(key.kind, key.name, publisher, auth.GetBodyHash())]; killed {
					continue
				}
			}
			switch key.kind { //nolint:exhaustive
			case attrWorkloadSpec:
				sc := ev.GetSpecChange()
				view := WorkloadSpecView{
					Spec:      workloadSpecFromProto(sc.GetWorkload()),
					Fact:      sc.GetFact(),
					Publisher: publisher,
				}
				// Runtime/fetch views key on the wasm content-hash: two
				// tenants on identical bytes share one artefact, claim
				// set and replica pool by design. outranks only makes
				// the shared entry deterministic, it is not ownership.
				if specStoring[view.Spec.Hash] == nil {
					specStoring[view.Spec.Hash] = make(map[types.PeerKey]struct{})
				}
				specStoring[view.Spec.Hash][pk] = struct{}{}
				if existing, ok := specs[view.Spec.Hash]; !ok || outranks(publisher, existing.Publisher) {
					specs[view.Spec.Hash] = view
				}
				// Publication view: one entry per (authority, name) so a
				// tenant sees its own workload even when its bytes or
				// name collide with another tenant's.
				if specsByPub[publisher] == nil {
					specsByPub[publisher] = make(map[string]WorkloadSpecView)
				}
				specsByPub[publisher][key.name] = view
			case attrStaticSpec:
				sc := ev.GetSpecChange()
				if staticStoring[key.name] == nil {
					staticStoring[key.name] = make(map[types.PeerKey]struct{})
				}
				staticStoring[key.name][pk] = struct{}{}
				view := StaticSpecView{
					Spec:      staticSpecFromProto(sc.GetStatic()),
					Fact:      sc.GetFact(),
					Publisher: publisher,
				}
				if existing, ok := staticSpecs[key.name]; !ok || outranks(publisher, existing.Publisher) {
					staticSpecs[key.name] = view
				}
				// Per-publisher view: dedupe by (publisher, name)
				// across every peer's log so a wire-mode tenant's
				// spec is visible even when their daemon, if any,
				// is offline and the spec is only carried by edge
				// relays.
				if staticSpecsByPub[publisher] == nil {
					staticSpecsByPub[publisher] = make(map[string]StaticSpecView)
				}
				staticSpecsByPub[publisher][key.name] = view
			case attrBlobSpec:
				sc := ev.GetSpecChange()
				view := BlobSpecView{
					Spec:      blobSpecFromProto(sc.GetBlob()),
					Fact:      sc.GetFact(),
					Publisher: publisher,
				}
				// Runtime/fetch views key on the content digest, shared
				// across tenants by design; outranks only makes the
				// shared entry deterministic.
				if blobStoring[view.Spec.Digest] == nil {
					blobStoring[view.Spec.Digest] = make(map[types.PeerKey]struct{})
				}
				blobStoring[view.Spec.Digest][pk] = struct{}{}
				if existing, ok := blobSpecs[view.Spec.Digest]; !ok || outranks(publisher, existing.Publisher) {
					blobSpecs[view.Spec.Digest] = view
				}
				// Publication view: one entry per (authority, name).
				if blobSpecsByPub[publisher] == nil {
					blobSpecsByPub[publisher] = make(map[string]BlobSpecView)
				}
				blobSpecsByPub[publisher][key.name] = view
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
					byRecipient = make(map[types.PeerKey]*factv1.BlobWrapping)
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
	filteredSpecStoring := filterLive(specStoring, live)
	filteredStaticStoring := filterLive(staticStoring, live)
	filteredBlobStoring := filterLive(blobStoring, live)

	return Snapshot{
		LocalID:              s.localID,
		Nodes:                nodes,
		Specs:                specs,
		DrainingClaims:       filteredDrainingClaims,
		Claims:               filteredClaims,
		SpecsAll:             flattenSpecsByPub(specsByPub, func(v WorkloadSpecView) (types.PeerKey, string) { return v.Publisher, v.Spec.Name }),
		StaticSpecs:          staticSpecs,
		StaticSpecsAll:       flattenSpecsByPub(staticSpecsByPub, func(v StaticSpecView) (types.PeerKey, string) { return v.Publisher, v.Spec.Name }),
		StaticClaims:         filteredStaticClaims,
		BlobSpecs:            blobSpecs,
		BlobSpecsAll:         flattenSpecsByPub(blobSpecsByPub, func(v BlobSpecView) (types.PeerKey, string) { return v.Publisher, v.Spec.Name }),
		Wrappings:            wrappings,
		WorkloadStoringPeers: filteredSpecStoring,
		StaticStoringPeers:   filteredStaticStoring,
		BlobStoringPeers:     filteredBlobStoring,
		live:                 live,
		PeerKeys:             slices.SortedFunc(maps.Keys(live), types.PeerKey.Compare),
		DeniedKeys:           denied,
		digest:               Digest{proto: &statev1.Digest{Peers: peersDigest}},
	}
}

func filterLive[K comparable](claims map[K]map[types.PeerKey]struct{}, live map[types.PeerKey]struct{}) map[K]map[types.PeerKey]struct{} {
	out := make(map[K]map[types.PeerKey]struct{})
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
func buildNodeView(pk types.PeerKey, rec nodeRecord) (NodeView, map[string]bool, map[StaticClaimKey]struct{}) {
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
	staticClaims := make(map[StaticClaimKey]struct{})

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
				nv.Services[key.name] = &Service{Name: key.name, Port: svc.Port, Protocol: NormaliseProtocol(svc.Protocol), Fact: v.SpecChange.GetFact()}
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
		case *statev1.GossipEvent_ControlAddr:
			nv.ControlAddr = v.ControlAddr.Addr
		case *statev1.GossipEvent_StaticClaim:
			staticClaims[StaticClaimKey{Authority: key.peer, Name: key.name}] = struct{}{}
		case *statev1.GossipEvent_Grant:
			nv.Grant = v.Grant.GetGrant()
		}
	}
	return nv, claims, staticClaims
}

// flattenSpecsByPub returns every (authority, name) spec view in a
// stable (publisher, name) order so cluster-wide views and test
// fixtures stay reproducible across daemon restarts. key extracts the
// sort tuple from a view.
func flattenSpecsByPub[V any](byPub map[types.PeerKey]map[string]V, key func(V) (types.PeerKey, string)) []V {
	if len(byPub) == 0 {
		return nil
	}
	out := make([]V, 0, len(byPub))
	for _, named := range byPub {
		for _, v := range named {
			out = append(out, v)
		}
	}
	slices.SortFunc(out, func(a, b V) int {
		ap, an := key(a)
		bp, bn := key(b)
		if c := ap.Compare(bp); c != 0 {
			return c
		}
		return cmp.Compare(an, bn)
	})
	return out
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
