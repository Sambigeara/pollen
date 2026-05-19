// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"maps"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

// Lens is the authority a snapshot is read through: the resolved
// Principal of the caller, never the serving node's own identity. It is
// an alias rather than a parallel type so the read lens, control scoping
// and admission all interpret one resolved authority. The zero Lens is
// the unidentified caller and permits nothing, so a misconfigured daemon
// defaults to leaking nothing rather than everything.
type Lens = identity.Principal

// LensFor derives the read and ownership lens from an already-verified
// caller Grant. A nil grant yields the default-deny zero Lens.
func LensFor(grant *identityv1.Grant) Lens {
	return identity.PrincipalFromGrant(grant)
}

// ScopedView is a snapshot projected through a Lens. The spec slices
// hold one entry per (authority, logical name) the lens may see, so a
// tenant whose artefact or name collides with another's still appears
// and an admin sees every colliding tenant distinctly. Nodes is the set
// of nodes the lens may observe: every node for an admin, and for a
// tenant only the nodes that hold at least one of the tenant's own facts
// (its functions, sites or blobs), so node visibility stays coupled to
// the caller's own authority.
type ScopedView struct {
	Nodes     map[types.PeerKey]state.NodeView
	Workloads []state.WorkloadSpecView
	Statics   []state.StaticSpecView
	Blobs     []state.BlobSpecView
	Lens      Lens
}

// Project filters snap through lens. Resource visibility is by authority
// equality; node visibility for a tenant is the union of the peers that
// store or run the tenant's own facts, which is exactly what the
// snapshot's storing-peer and claim indices already track.
func Project(snap state.Snapshot, lens Lens) ScopedView {
	sv := ScopedView{Lens: lens}
	// Iterate the un-deduped per-(authority,name) publication sources,
	// not the deduped runtime maps: the deduped Specs/BlobSpecs key on
	// artefact content and StaticSpecs on name, so a tenant whose bytes
	// or name collide with another's is dropped before the lens runs.
	// Visibility is a publication concern and reads the publication
	// source. Each (authority, name) is carried through verbatim, so an
	// admin lens sees every colliding tenant and a tenant sees only its
	// own; (authority, name) is already unique in the source, so there
	// is no tie-break.
	for _, w := range snap.SpecsAll {
		if lens.Permits(w.Publisher) {
			sv.Workloads = append(sv.Workloads, w)
		}
	}
	for _, st := range snap.StaticSpecsAll {
		if lens.Permits(st.Publisher) {
			sv.Statics = append(sv.Statics, st)
		}
	}
	for _, b := range snap.BlobSpecsAll {
		if lens.Permits(b.Publisher) {
			sv.Blobs = append(sv.Blobs, b)
		}
	}

	if lens.Admin() {
		sv.Nodes = make(map[types.PeerKey]state.NodeView, len(snap.Nodes))
		maps.Copy(sv.Nodes, snap.Nodes)
		return sv
	}

	holders := make(map[types.PeerKey]struct{})
	addPeers := func(set map[types.PeerKey]struct{}) {
		for pk := range set {
			holders[pk] = struct{}{}
		}
	}
	// Holder lookups stay on the content/name runtime indices: a
	// shared artefact's storing peers and claims are deliberately
	// cross-tenant, but the static-claim register is now per-authority.
	for _, w := range sv.Workloads {
		addPeers(snap.WorkloadStoringPeers[w.Spec.Hash])
		addPeers(snap.Claims[w.Spec.Hash])
		addPeers(snap.DrainingClaims[w.Spec.Hash])
	}
	for _, st := range sv.Statics {
		addPeers(snap.StaticStoringPeers[st.Spec.Name])
		addPeers(snap.StaticClaims[state.StaticClaimKey{Authority: st.Publisher, Name: st.Spec.Name}])
	}
	for _, b := range sv.Blobs {
		addPeers(snap.BlobStoringPeers[b.Spec.Digest])
	}

	sv.Nodes = make(map[types.PeerKey]state.NodeView, len(holders))
	for pk := range holders {
		if n, ok := snap.Nodes[pk]; ok {
			sv.Nodes[pk] = n
		}
	}
	return sv
}
