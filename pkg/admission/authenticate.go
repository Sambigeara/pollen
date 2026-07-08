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
// The authority Grant is sourced by origin. A Fact whose authority is this
// node is self-signed and its Grant may not have gossiped yet, so it
// resolves from snap.LocalGrant() and inherits MayPublish's bootstrap
// tolerance. Every other Fact resolves the authority Grant from cluster
// state and must verify against it.
//
// The returned Grant is nil only in the tolerated bootstrap window;
// authorise and AccountCheck both treat a nil authority Grant as
// unrestricted.
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

	// decide short-circuits on public, so a spec carrying both public=true
	// and inline clauses looks gated but admits anyone. Reject it here.
	policy := f.GetPolicy()
	if policy.GetPublic() && policy.GetInline() != nil {
		return nil, errors.New("admission: predicate has both public=true and inline clauses")
	}
	// Slugs are 60 bits, so a collision is statistically unreachable; this
	// check keeps canonical-URL routing unambiguous against active peers.
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
// the Fact against it. See authenticate for the self-authored versus
// cluster-sourced split and the bootstrap-tolerance nil-Grant case.
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

// authorise enforces the authority Grant's per-kind publish capability and
// the publisher's policy attributes. A nil authGrant is the bootstrap window
// authenticate already tolerated, so there is nothing to enforce.
//
// Signed tombstones bypass the cap check: a publisher who has lost (e.g.)
// publish:functions must still retire their own Facts, else a cap-shrink
// strands old publications on remote peers. Safe because the tombstone is
// signed by the publisher (verified in authenticate) and a fully denied
// publisher fails the chain check upstream.
func authorise(sc *statev1.SpecChange, authGrant *identityv1.Grant) error {
	if authGrant == nil {
		return nil
	}
	if sc.GetFact().GetDeleted() {
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
		return fmt.Errorf("authority grant lacks publish capability for %s", kind)
	}
	return checkPolicyClauses(authGrant, sc.GetFact().GetPolicy())
}
