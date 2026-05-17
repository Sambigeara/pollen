// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package admission

import (
	"errors"
	"fmt"
	"time"

	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"google.golang.org/protobuf/proto"
)

// authenticate proves the Fact and resolves the authority's Grant.
//
// The authority Grant is sourced by origin. A Fact whose authority is
// this node is locally self-signed: its Grant may not have gossiped
// yet, so it resolves from snap.LocalGrant() and inherits MayPublish's
// bootstrap tolerance (a nil policy before the local Grant is published
// is permitted; a non-nil policy is not). Every other Fact (gossip
// relay, presigned wire publish whose authority is the tenant) resolves
// the authority Grant from cluster state and must verify against it.
//
// The returned Grant is nil only in the tolerated bootstrap window;
// authorise and AccountCheck both treat a nil authority Grant as the
// unrestricted bootstrap case, consistent with authenticate permitting
// it here.
func (p *Pipeline) authenticate(snap state.Snapshot, sc *statev1.SpecChange) (*identityv1.Grant, error) {
	body, expected, err := decodeSpecChange(sc)
	if err != nil {
		return nil, err
	}
	f := sc.GetFact()
	if f == nil {
		return nil, errors.New("admission: spec change missing fact")
	}
	if !proto.Equal(f.GetResource(), expected) {
		return nil, errors.New("admission: fact resource mismatch")
	}

	authGrant, err := p.resolveAndVerify(snap, f, body)
	if err != nil {
		return nil, err
	}

	// A spec that carries both public=true and inline clauses looks
	// gated to a casual reader but admits anyone at runtime (decide
	// short-circuits on public). Rejecting it here closes the door
	// against tampered or hand-crafted facts.
	policy := f.GetPolicy()
	if policy.GetPublic() && policy.GetInline() != nil {
		return nil, errors.New("admission: predicate has both public=true and inline clauses")
	}
	// Defence-in-depth against an authority whose slug grinds against a
	// live mesh peer's slug. The slug is 60 bits, so a real collision is
	// statistically unreachable; the check guarantees canonical-URL
	// routing stays unambiguous against active peers.
	authority := types.PeerKeyFromBytes(f.GetAuthorityPub())
	slug := authority.Slug()
	for peer := range snap.Nodes {
		if peer != authority && peer.Slug() == slug {
			return nil, fmt.Errorf("admission: authority slug %q collides with existing peer %s", slug, peer.Short())
		}
	}
	return authGrant, nil
}

// resolveAndVerify resolves the authority Grant by origin and verifies
// the Fact against it. A self-authored Fact (authority is this node)
// resolves from the held local Grant and inherits MayPublish's
// bootstrap tolerance: before that Grant has gossiped a nil policy is
// permitted (a nil Grant is returned, treated downstream as the
// unrestricted bootstrap case) and a non-nil policy is not. Every other
// Fact resolves from cluster state and must verify against it.
func (p *Pipeline) resolveAndVerify(snap state.Snapshot, f *factv1.Fact, body fact.Body) (*identityv1.Grant, error) {
	if types.PeerKeyFromBytes(f.GetAuthorityPub()) == snap.LocalID {
		g := snap.LocalGrant()
		if g == nil {
			if f.GetPolicy() != nil {
				return nil, errLocalGrantUnpublished
			}
			return nil, nil //nolint:nilnil
		}
		if err := fact.VerifyFact(f, body, g, p.rootPub, time.Now(), snap.DenyChecker()); err != nil {
			return nil, err
		}
		return g, nil
	}
	g := snap.GrantFor(f.GetAuthorityPub())
	if g == nil {
		return nil, errors.New("admission: fact authority grant not in cluster state")
	}
	if err := fact.VerifyFact(f, body, g, p.rootPub, time.Now(), snap.DenyChecker()); err != nil {
		return nil, err
	}
	return g, nil
}

// authorise enforces the authority Grant's per-kind publish capability
// for the resource kind and the publisher's own policy attributes. A
// nil authGrant is the tolerated bootstrap window (authenticate already
// permitted it), so there is nothing to enforce yet.
func authorise(sc *statev1.SpecChange, authGrant *identityv1.Grant) error {
	if authGrant == nil {
		return nil
	}
	pub := authGrant.GetClaims().GetCapabilities().GetPublish()
	var (
		allowed bool
		kind    string
	)
	switch sc.GetBody().(type) {
	case *statev1.SpecChange_Workload:
		allowed, kind = pub.GetFunctions(), "functions"
	case *statev1.SpecChange_Blob:
		allowed, kind = pub.GetBlobs(), "blobs"
	case *statev1.SpecChange_Static:
		allowed, kind = pub.GetSites(), "sites"
	case *statev1.SpecChange_Service:
		allowed, kind = pub.GetServices(), "services"
	}
	if !allowed {
		return fmt.Errorf("admission: authority grant lacks publish capability for %s", kind)
	}
	return checkPolicyClauses(authGrant, sc.GetFact().GetPolicy())
}
