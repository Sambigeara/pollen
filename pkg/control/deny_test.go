// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubState exposes a fixed grant map as the snapshot: only Nodes[k].Grant
// is populated, which is what GrantFor (and the authority check) reads.
type stubState struct {
	grants map[types.PeerKey]*identityv1.Grant
}

func (s *stubState) Snapshot() state.Snapshot {
	nodes := make(map[types.PeerKey]state.NodeView, len(s.grants))
	for k, g := range s.grants {
		nodes[k] = state.NodeView{Grant: g}
	}
	return state.Snapshot{Nodes: nodes}
}

func ed25519Pair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

func issuePrincipalGrant(t *testing.T, signerPriv ed25519.PrivateKey, parent *identityv1.Grant, subjectPub ed25519.PublicKey, caps *identityv1.Capabilities) *identityv1.Grant {
	t.Helper()
	g, err := identity.IssueGrant(signerPriv, parent, subjectPub, caps, identity.UnlimitedBudget(),
		time.Now().Add(-time.Hour), time.Time{}, false)
	require.NoError(t, err)
	return g
}

func callerCtx(grant *identityv1.Grant) context.Context {
	return auth.WithCaller(context.Background(), identity.PrincipalFromGrant(grant))
}

// newAuthorityService wires a Service from the components a control
// handler reads: membership for the underlying primitive, state for
// snapshot lookups, delivery for grant-offer dispatch. Mirrors
// NewService's invariant that state is non-nil; an empty stubState
// stands in when callers do not exercise the snapshot path.
func newAuthorityService(t *testing.T, mem *stubMembership, st StateReader, delivery PeerDelivery) *Service {
	t.Helper()
	if st == nil {
		st = &stubState{}
	}
	return &Service{
		membership: mem,
		state:      st,
		delivery:   delivery,
		log:        zap.NewNop().Sugar(),
	}
}

func TestDenyPeer(t *testing.T) {
	rootPub, rootPriv := ed25519Pair(t)
	rootGrant := issuePrincipalGrant(t, rootPriv, nil, rootPub, identity.FullCapabilities())

	wsPub, wsPriv := ed25519Pair(t)
	wsGrant := issuePrincipalGrant(t, rootPriv, rootGrant, wsPub, identity.WorkspaceCapabilities())

	tenantPub, _ := ed25519Pair(t)
	tenantGrant := issuePrincipalGrant(t, wsPriv, wsGrant, tenantPub, identity.LeafCapabilities())
	tenantKey := types.PeerKeyFromBytes(tenantPub)

	otherWSPub, otherWSPriv := ed25519Pair(t)
	otherWSGrant := issuePrincipalGrant(t, rootPriv, rootGrant, otherWSPub, identity.WorkspaceCapabilities())
	otherPub, _ := ed25519Pair(t)
	otherGrant := issuePrincipalGrant(t, otherWSPriv, otherWSGrant, otherPub, identity.LeafCapabilities())
	otherKey := types.PeerKeyFromBytes(otherPub)

	leafPub, _ := ed25519Pair(t)
	leafGrant := issuePrincipalGrant(t, wsPriv, wsGrant, leafPub, identity.LeafCapabilities())

	// A delegated cluster-admin (holds can_admit, signed by root) and a
	// sibling cluster-admin under root. Neither is an ancestor of the
	// other, so can_admit alone must not let one deny the other.
	adminAPub, _ := ed25519Pair(t)
	adminAGrant := issuePrincipalGrant(t, rootPriv, rootGrant, adminAPub, identity.FullCapabilities())
	siblingAdminPub, _ := ed25519Pair(t)
	siblingAdminGrant := issuePrincipalGrant(t, rootPriv, rootGrant, siblingAdminPub, identity.FullCapabilities())
	siblingAdminKey := types.PeerKeyFromBytes(siblingAdminPub)

	snap := &stubState{grants: map[types.PeerKey]*identityv1.Grant{
		tenantKey:       tenantGrant,
		otherKey:        otherGrant,
		siblingAdminKey: siblingAdminGrant,
	}}

	t.Run("root denies any peer", func(t *testing.T) {
		mem := &stubMembership{rootPriv: rootPriv}
		svc := newAuthorityService(t, mem, snap, nil)
		_, err := svc.DenyPeer(callerCtx(rootGrant), &controlv1.DenyPeerRequest{PeerPub: tenantPub})
		require.NoError(t, err)
		require.Equal(t, tenantKey, mem.lastDenied)
	})

	t.Run("workspace-admin denies peer in own subtree", func(t *testing.T) {
		mem := &stubMembership{rootPriv: rootPriv}
		svc := newAuthorityService(t, mem, snap, nil)
		_, err := svc.DenyPeer(callerCtx(wsGrant), &controlv1.DenyPeerRequest{PeerPub: tenantPub})
		require.NoError(t, err)
		require.Equal(t, tenantKey, mem.lastDenied)
	})

	t.Run("workspace-admin cannot deny across workspaces", func(t *testing.T) {
		mem := &stubMembership{rootPriv: rootPriv}
		svc := newAuthorityService(t, mem, snap, nil)
		_, err := svc.DenyPeer(callerCtx(wsGrant), &controlv1.DenyPeerRequest{PeerPub: otherPub})
		require.Error(t, err)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.Equal(t, types.PeerKey{}, mem.lastDenied, "membership primitive must not be reached")
	})

	t.Run("delegated cluster-admin cannot deny a sibling admin", func(t *testing.T) {
		mem := &stubMembership{rootPriv: rootPriv}
		svc := newAuthorityService(t, mem, snap, nil)
		_, err := svc.DenyPeer(callerCtx(adminAGrant), &controlv1.DenyPeerRequest{PeerPub: siblingAdminPub})
		require.Error(t, err)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.Equal(t, types.PeerKey{}, mem.lastDenied, "can_admit is not a lateral bypass; membership primitive must not be reached")
	})

	t.Run("leaf cannot deny anyone", func(t *testing.T) {
		mem := &stubMembership{rootPriv: rootPriv}
		svc := newAuthorityService(t, mem, snap, nil)
		_, err := svc.DenyPeer(callerCtx(leafGrant), &controlv1.DenyPeerRequest{PeerPub: tenantPub})
		require.Error(t, err)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.Equal(t, types.PeerKey{}, mem.lastDenied, "membership primitive must not be reached")
	})

	t.Run("self-target is rejected", func(t *testing.T) {
		mem := &stubMembership{rootPriv: rootPriv}
		svc := newAuthorityService(t, mem, snap, nil)
		_, err := svc.DenyPeer(callerCtx(wsGrant), &controlv1.DenyPeerRequest{PeerPub: wsPub})
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Equal(t, types.PeerKey{}, mem.lastDenied, "membership primitive must not be reached")
	})

	t.Run("unknown target fails closed", func(t *testing.T) {
		unknownPub, _ := ed25519Pair(t)
		mem := &stubMembership{rootPriv: rootPriv}
		svc := newAuthorityService(t, mem, snap, nil)
		_, err := svc.DenyPeer(callerCtx(wsGrant), &controlv1.DenyPeerRequest{PeerPub: unknownPub})
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Equal(t, types.PeerKey{}, mem.lastDenied, "membership primitive must not be reached")
	})

	t.Run("unauthenticated caller is rejected", func(t *testing.T) {
		mem := &stubMembership{rootPriv: rootPriv}
		svc := newAuthorityService(t, mem, snap, nil)
		_, err := svc.DenyPeer(context.Background(), &controlv1.DenyPeerRequest{PeerPub: tenantPub})
		require.Error(t, err)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.Equal(t, types.PeerKey{}, mem.lastDenied, "membership primitive must not be reached")
	})
}
