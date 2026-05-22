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
// of nodes the lens may observe, computed by Project as a union of
// structural visibility (Permits over each node's grant) and the hosts
// of any visible resource.
type ScopedView struct {
	Nodes     map[types.PeerKey]state.NodeView
	Workloads []state.WorkloadSpecView
	Statics   []state.StaticSpecView
	Blobs     []state.BlobSpecView
	Lens      Lens
}

// Project filters snap through lens. Resource visibility is by the
// cluster visibility rule (see Permits). Node visibility is the union
// of two sets: nodes whose grant the lens sees structurally (chain,
// subtree, workspace peers), plus nodes that store or run any
// visible resource. The first set keeps every member of the lens's
// workspace visible even before any deployment lands; the second
// keeps any host of a visible workload in view even when that host
// sits outside the workspace.
func Project(snap state.Snapshot, lens Lens) ScopedView {
	sv := ScopedView{Lens: lens}
	// Iterate the un-deduped per-(authority,name) publication sources,
	// not the deduped runtime maps: the deduped maps key on artefact
	// content or name, so a tenant whose bytes or name collide with
	// another's would be dropped before the lens runs. Visibility is a
	// publication concern, so it reads the publication source.
	for _, w := range snap.SpecsAll {
		if Permits(lens, w.Publisher, snap) {
			sv.Workloads = append(sv.Workloads, w)
		}
	}
	for _, st := range snap.StaticSpecsAll {
		if Permits(lens, st.Publisher, snap) {
			sv.Statics = append(sv.Statics, st)
		}
	}
	for _, b := range snap.BlobSpecsAll {
		if Permits(lens, b.Publisher, snap) {
			sv.Blobs = append(sv.Blobs, b)
		}
	}

	if lens.Admin() {
		sv.Nodes = make(map[types.PeerKey]state.NodeView, len(snap.Nodes))
		maps.Copy(sv.Nodes, snap.Nodes)
		return sv
	}

	sv.Nodes = make(map[types.PeerKey]state.NodeView)
	// Structural visibility: every node whose grant the lens sees.
	for pk, nv := range snap.Nodes {
		if Permits(lens, pk, snap) {
			sv.Nodes[pk] = nv
		}
	}
	// Plus the hosts of any visible resource, so a workload running on
	// a peer outside the lens's workspace is still locatable. Holder
	// lookups stay on the content/name runtime indices: a shared
	// artefact's storing peers and claims are deliberately cross-tenant,
	// but the static-claim register is per-authority.
	addNode := func(pk types.PeerKey) {
		if _, already := sv.Nodes[pk]; already {
			return
		}
		if n, ok := snap.Nodes[pk]; ok {
			sv.Nodes[pk] = n
		}
	}
	addPeers := func(set map[types.PeerKey]struct{}) {
		for pk := range set {
			addNode(pk)
		}
	}
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
	return sv
}
