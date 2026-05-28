// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestUpgradePeer_AuthorityScope pins the adoption rule: a workspace-admin
// may upgrade peers in its own subtree, but may not reach across into a
// sibling workspace, absorb a sibling workspace-admin, or capture the root.
// An admit-capable caller may invite a peer with no known grant; a non-admit
// caller fails closed.
func TestUpgradePeer_AuthorityScope(t *testing.T) {
	rootPub, rootPriv := ed25519Pair(t)
	rootGrant := issuePrincipalGrant(t, rootPriv, nil, rootPub, identity.FullCapabilities())

	wsPub, wsPriv := ed25519Pair(t)
	wsGrant := issuePrincipalGrant(t, rootPriv, rootGrant, wsPub, identity.WorkspaceCapabilities())

	tenantPub, _ := ed25519Pair(t)
	tenantGrant := issuePrincipalGrant(t, wsPriv, wsGrant, tenantPub, identity.LeafCapabilities())
	tenantKey := types.PeerKeyFromBytes(tenantPub)

	otherWSPub, otherWSPriv := ed25519Pair(t)
	otherWSGrant := issuePrincipalGrant(t, rootPriv, rootGrant, otherWSPub, identity.WorkspaceCapabilities())
	otherWSKey := types.PeerKeyFromBytes(otherWSPub)
	otherPub, _ := ed25519Pair(t)
	otherGrant := issuePrincipalGrant(t, otherWSPriv, otherWSGrant, otherPub, identity.LeafCapabilities())
	otherKey := types.PeerKeyFromBytes(otherPub)

	rootKey := types.PeerKeyFromBytes(rootPub)

	snap := &stubState{grants: map[types.PeerKey]*identityv1.Grant{
		tenantKey:  tenantGrant,
		otherKey:   otherGrant,
		otherWSKey: otherWSGrant,
		rootKey:    rootGrant,
	}}

	t.Run("workspace-admin upgrades peer in own subtree", func(t *testing.T) {
		delivery := &stubDelivery{resp: &meshv1.GrantOfferResponse{Accepted: true}}
		svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, snap, delivery)
		resp, err := svc.UpgradePeer(callerCtx(wsGrant), &controlv1.UpgradePeerRequest{
			PeerPub:      tenantPub,
			Capabilities: identity.PublisherCapabilities(),
		})
		require.NoError(t, err)
		require.True(t, resp.GetDelivered())
		require.True(t, delivery.called, "grant must be dispatched to the target")
		require.Equal(t, tenantKey, delivery.lastTo)
	})

	t.Run("workspace-admin cannot hijack across workspaces", func(t *testing.T) {
		delivery := &stubDelivery{resp: &meshv1.GrantOfferResponse{Accepted: true}}
		svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, snap, delivery)
		_, err := svc.UpgradePeer(callerCtx(wsGrant), &controlv1.UpgradePeerRequest{
			PeerPub:      otherPub,
			Capabilities: identity.PublisherCapabilities(),
		})
		require.Error(t, err)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.Contains(t, status.Convert(err).Message(), "outside caller's authority",
			"the hijack guard must fire, not the ceiling check")
		require.False(t, delivery.called, "must reject before minting or dispatching")
	})

	t.Run("workspace-admin cannot absorb a sibling workspace-admin", func(t *testing.T) {
		delivery := &stubDelivery{resp: &meshv1.GrantOfferResponse{Accepted: true}}
		svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, snap, delivery)
		_, err := svc.UpgradePeer(callerCtx(wsGrant), &controlv1.UpgradePeerRequest{
			PeerPub:      otherWSPub,
			Capabilities: identity.PublisherCapabilities(),
		})
		require.Error(t, err)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.False(t, delivery.called)
	})

	t.Run("workspace-admin cannot capture the root", func(t *testing.T) {
		delivery := &stubDelivery{resp: &meshv1.GrantOfferResponse{Accepted: true}}
		svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, snap, delivery)
		_, err := svc.UpgradePeer(callerCtx(wsGrant), &controlv1.UpgradePeerRequest{
			PeerPub:      rootPub,
			Capabilities: identity.LeafCapabilities(),
		})
		require.Error(t, err)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.False(t, delivery.called)
	})

	t.Run("admit-capable caller may invite a peer with no known grant", func(t *testing.T) {
		strangerPub, _ := ed25519Pair(t)
		delivery := &stubDelivery{err: transport.ErrPeerOffline}
		svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, snap, delivery)
		_, err := svc.UpgradePeer(callerCtx(rootGrant), &controlv1.UpgradePeerRequest{
			PeerPub:      strangerPub,
			Capabilities: identity.LeafCapabilities(),
		})
		require.Error(t, err)
		require.Equal(t, codes.Unavailable, status.Code(err),
			"an unknown target must reach delivery and surface the offline fallback signal")
		require.True(t, delivery.called, "guard must let an admit-capable caller through to dispatch")
	})

	t.Run("non-admit caller targeting an unknown peer fails closed", func(t *testing.T) {
		strangerPub, _ := ed25519Pair(t)
		delivery := &stubDelivery{resp: &meshv1.GrantOfferResponse{Accepted: true}}
		svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, snap, delivery)
		_, err := svc.UpgradePeer(callerCtx(wsGrant), &controlv1.UpgradePeerRequest{
			PeerPub:      strangerPub,
			Capabilities: identity.PublisherCapabilities(),
		})
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.False(t, delivery.called, "a missed-gossip target must not default-allow")
	})
}

// stubMembership mints child grants under a fixed root, returning the
// grant the handler receives but never gossiping anything. It mirrors
// the membership.IssueGrant primitive without standing up a Service.
// lastDenied records the most recent DenyPeer target so positive tests
// can prove the handler reached the underlying primitive (zero value
// means the handler short-circuited before calling DenyPeer).
type stubMembership struct {
	rootPriv   ed25519.PrivateKey
	lastDenied types.PeerKey
}

func (m *stubMembership) DenyPeer(key types.PeerKey) error {
	m.lastDenied = key
	return nil
}

func (m *stubMembership) IssueGrant(_ context.Context, peerKey types.PeerKey, caps *identityv1.Capabilities, budget *identityv1.Budget, _ bool) (*identityv1.Grant, error) {
	if budget == nil {
		budget = identity.UnlimitedBudget()
	}
	now := time.Now()
	return identity.IssueGrant(m.rootPriv, nil, ed25519.PublicKey(peerKey.Bytes()), caps, budget, now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
}
func (m *stubMembership) RegisterPeerGrant(types.PeerKey, *identityv1.Grant, []byte) {}
func (m *stubMembership) RenewalFailing() bool                                       { return false }

// stubDelivery records the last grant offered and replies with a
// configurable outcome. err takes precedence over resp.
type stubDelivery struct {
	resp    *meshv1.GrantOfferResponse
	err     error
	called  bool
	lastTo  types.PeerKey
	offered *identityv1.Grant
}

func (d *stubDelivery) SendGrantOffer(_ context.Context, peer types.PeerKey, grant *identityv1.Grant) (*meshv1.GrantOfferResponse, error) {
	d.called = true
	d.lastTo = peer
	d.offered = grant
	if d.err != nil {
		return nil, d.err
	}
	return d.resp, nil
}

// TestUpgradePeer pins the issuer-side handler's contract: caller must
// hold CanDelegate, capabilities must be supplied, offline peers raise
// codes.Unavailable so the CLI can fall back to a subject-pinned token,
// recipient rejections surface as Delivered=false with the recipient's
// reason, and the happy path returns Delivered=true after the grant
// has been pushed. The caller is root, the universal authority, so the
// hijack guard always passes and these cases exercise delivery alone.
func TestUpgradePeer(t *testing.T) {
	rootPub, rootPriv := ed25519Pair(t)
	rootGrant := issuePrincipalGrant(t, rootPriv, nil, rootPub, identity.FullCapabilities())

	subjectPub, _ := ed25519Pair(t)
	subjectKey := types.PeerKeyFromBytes(subjectPub)
	subjectGrant := issuePrincipalGrant(t, rootPriv, rootGrant, subjectPub, identity.LeafCapabilities())
	snap := &stubState{grants: map[types.PeerKey]*identityv1.Grant{subjectKey: subjectGrant}}

	t.Run("non-delegating caller is rejected with PermissionDenied", func(t *testing.T) {
		leafPub, _ := ed25519Pair(t)
		leaf := issuePrincipalGrant(t, rootPriv, rootGrant, leafPub, identity.LeafCapabilities())
		svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, snap, &stubDelivery{})

		_, err := svc.UpgradePeer(callerCtx(leaf), &controlv1.UpgradePeerRequest{
			PeerPub:      subjectPub,
			Capabilities: identity.FullCapabilities(),
		})
		require.Error(t, err)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	})

	t.Run("missing capabilities is InvalidArgument", func(t *testing.T) {
		svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, snap, &stubDelivery{})

		_, err := svc.UpgradePeer(callerCtx(rootGrant), &controlv1.UpgradePeerRequest{PeerPub: subjectPub})
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("happy path returns Delivered=true and offers the minted grant to the peer", func(t *testing.T) {
		delivery := &stubDelivery{resp: &meshv1.GrantOfferResponse{Accepted: true}}
		svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, snap, delivery)

		resp, err := svc.UpgradePeer(callerCtx(rootGrant), &controlv1.UpgradePeerRequest{
			PeerPub:      subjectPub,
			Capabilities: identity.FullCapabilities(),
		})
		require.NoError(t, err)
		require.True(t, resp.GetDelivered())
		require.True(t, delivery.called)
		require.Equal(t, subjectKey, delivery.lastTo)
		require.NotNil(t, delivery.offered)
	})

	t.Run("offline peer surfaces codes.Unavailable for CLI fallback", func(t *testing.T) {
		delivery := &stubDelivery{err: transport.ErrPeerOffline}
		svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, snap, delivery)

		_, err := svc.UpgradePeer(callerCtx(rootGrant), &controlv1.UpgradePeerRequest{
			PeerPub:      subjectPub,
			Capabilities: identity.FullCapabilities(),
		})
		require.Error(t, err)
		require.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("recipient rejection returns Delivered=false with the reason verbatim", func(t *testing.T) {
		delivery := &stubDelivery{resp: &meshv1.GrantOfferResponse{Reason: "denylisted"}}
		svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, snap, delivery)

		resp, err := svc.UpgradePeer(callerCtx(rootGrant), &controlv1.UpgradePeerRequest{
			PeerPub:      subjectPub,
			Capabilities: identity.FullCapabilities(),
		})
		require.NoError(t, err)
		require.False(t, resp.GetDelivered())
		require.Equal(t, "denylisted", resp.GetReason())
	})

	t.Run("dispatch error other than offline surfaces as Internal", func(t *testing.T) {
		delivery := &stubDelivery{err: errors.New("write deadline exceeded")}
		svc := newAuthorityService(t, &stubMembership{rootPriv: rootPriv}, snap, delivery)

		_, err := svc.UpgradePeer(callerCtx(rootGrant), &controlv1.UpgradePeerRequest{
			PeerPub:      subjectPub,
			Capabilities: identity.FullCapabilities(),
		})
		require.Error(t, err)
		require.Equal(t, codes.Internal, status.Code(err))
	})
}
