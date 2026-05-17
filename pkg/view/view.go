// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"maps"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

// Lens is the authority a snapshot is read through. It is derived from
// the calling Grant, never from the serving node's own identity. The
// zero Lens is the unidentified caller and permits nothing, so a
// misconfigured daemon defaults to leaking nothing rather than
// everything.
type Lens struct {
	admin   bool
	subject types.PeerKey
	valid   bool
}

// LensFor derives the read and ownership lens from a caller Grant. A nil
// Grant yields the default-deny zero Lens. Admin authority is the
// can_admit capability; the subject is the Grant's own key, so a tenant
// is scoped to facts it published rather than to whatever node happens
// to be serving the request.
func LensFor(grant *identityv1.Grant) Lens {
	if grant == nil {
		return Lens{}
	}
	return Lens{
		admin:   grant.GetClaims().GetCapabilities().GetCanAdmit(),
		subject: types.PeerKeyFromBytes(grant.GetClaims().GetSubjectPub()),
		valid:   true,
	}
}

func (l Lens) Admin() bool { return l.admin }

func (l Lens) Subject() types.PeerKey { return l.subject }

// Permits reports whether this lens may see or mutate a resource whose
// authority is publisher. An admin sees the whole cluster; a tenant
// sees only its own facts; an unidentified caller sees nothing.
func (l Lens) Permits(publisher types.PeerKey) bool {
	if !l.valid {
		return false
	}
	return l.admin || publisher == l.subject
}

// ScopedView is a snapshot projected through a Lens. The spec maps are
// keyed exactly as the snapshot keys them so callers iterate them in
// place. Nodes is the set of nodes the lens may observe: every node for
// an admin, and for a tenant only the nodes that hold at least one of
// the tenant's own facts (its functions, sites or blobs), so node
// visibility stays coupled to the caller's own authority.
type ScopedView struct {
	Nodes     map[types.PeerKey]state.NodeView
	Workloads map[string]state.WorkloadSpecView
	Statics   map[string]state.StaticSpecView
	Blobs     map[string]state.BlobSpecView
	Lens      Lens
}

// Project filters snap through lens. Resource visibility is by authority
// equality; node visibility for a tenant is the union of the peers that
// store or run the tenant's own facts, which is exactly what the
// snapshot's storing-peer and claim indices already track.
func Project(snap state.Snapshot, lens Lens) ScopedView {
	sv := ScopedView{
		Lens:      lens,
		Workloads: make(map[string]state.WorkloadSpecView),
		Statics:   make(map[string]state.StaticSpecView),
		Blobs:     make(map[string]state.BlobSpecView),
	}
	for hash, w := range snap.Specs {
		if lens.Permits(w.Publisher) {
			sv.Workloads[hash] = w
		}
	}
	for name, st := range snap.StaticSpecs {
		if lens.Permits(st.Publisher) {
			sv.Statics[name] = st
		}
	}
	for digest, b := range snap.BlobSpecs {
		if lens.Permits(b.Publisher) {
			sv.Blobs[digest] = b
		}
	}

	if lens.admin {
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
	for hash := range sv.Workloads {
		addPeers(snap.WorkloadStoringPeers[hash])
		addPeers(snap.Claims[hash])
		addPeers(snap.DrainingClaims[hash])
	}
	for name := range sv.Statics {
		addPeers(snap.StaticStoringPeers[name])
		addPeers(snap.StaticClaims[name])
	}
	for digest := range sv.Blobs {
		addPeers(snap.BlobStoringPeers[digest])
	}

	sv.Nodes = make(map[types.PeerKey]state.NodeView, len(holders))
	for pk := range holders {
		if n, ok := snap.Nodes[pk]; ok {
			sv.Nodes[pk] = n
		}
	}
	return sv
}
