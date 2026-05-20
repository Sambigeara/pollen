// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubMembership mints child grants under a fixed root, returning the
// grant the handler receives but never gossiping anything. It mirrors
// the membership.IssueGrant primitive without standing up a Service.
type stubMembership struct {
	rootPriv ed25519.PrivateKey
}

func (m *stubMembership) DenyPeer(types.PeerKey) error { return nil }
func (m *stubMembership) IssueGrant(_ context.Context, peerKey types.PeerKey, caps *identityv1.Capabilities, budget *identityv1.Budget) (*identityv1.Grant, error) {
	if budget == nil {
		budget = identity.UnlimitedBudget()
	}
	now := time.Now()
	return identity.IssueGrant(m.rootPriv, nil, ed25519.PublicKey(peerKey.Bytes()), caps, budget, now.Add(-time.Hour), now.Add(30*24*time.Hour))
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

// adminCaller builds an admin caller context for handler tests: full
// capabilities, root-signed grant. CanDelegate is what UpgradePeer's
// permission gate requires.
func adminCaller(t *testing.T, rootPriv ed25519.PrivateKey) context.Context {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	grant, err := identity.IssueGrant(rootPriv, nil, pub, identity.FullCapabilities(), identity.UnlimitedBudget(),
		time.Now().Add(-time.Hour), time.Time{})
	require.NoError(t, err)
	return auth.WithCaller(context.Background(), identity.PrincipalFromGrant(grant))
}

func newUpgradeService(t *testing.T, rootPriv ed25519.PrivateKey, delivery PeerDelivery) *Service {
	t.Helper()
	return &Service{
		membership: &stubMembership{rootPriv: rootPriv},
		delivery:   delivery,
		log:        zap.NewNop().Sugar(),
	}
}

// TestUpgradePeer pins the issuer-side handler's contract: caller must
// hold CanDelegate, capabilities must be supplied, offline peers raise
// codes.Unavailable so the CLI can fall back to a subject-pinned token,
// recipient rejections surface as Delivered=false with the recipient's
// reason, and the happy path returns Delivered=true after the grant
// has been pushed.
func TestUpgradePeer(t *testing.T) {
	_, rootPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	subjectKey := types.PeerKeyFromBytes(subjectPub)

	t.Run("non-delegating caller is rejected with PermissionDenied", func(t *testing.T) {
		leaf, err := identity.IssueGrant(rootPriv, nil, subjectPub, identity.LeafCapabilities(), identity.UnlimitedBudget(),
			time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour))
		require.NoError(t, err)
		ctx := auth.WithCaller(context.Background(), identity.PrincipalFromGrant(leaf))
		svc := newUpgradeService(t, rootPriv, &stubDelivery{})

		_, err = svc.UpgradePeer(ctx, &controlv1.UpgradePeerRequest{
			PeerPub:      subjectPub,
			Capabilities: identity.FullCapabilities(),
		})
		require.Error(t, err)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	})

	t.Run("missing capabilities is InvalidArgument", func(t *testing.T) {
		ctx := adminCaller(t, rootPriv)
		svc := newUpgradeService(t, rootPriv, &stubDelivery{})

		_, err := svc.UpgradePeer(ctx, &controlv1.UpgradePeerRequest{PeerPub: subjectPub})
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("happy path returns Delivered=true and offers the minted grant to the peer", func(t *testing.T) {
		ctx := adminCaller(t, rootPriv)
		delivery := &stubDelivery{resp: &meshv1.GrantOfferResponse{Accepted: true}}
		svc := newUpgradeService(t, rootPriv, delivery)

		resp, err := svc.UpgradePeer(ctx, &controlv1.UpgradePeerRequest{
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
		ctx := adminCaller(t, rootPriv)
		delivery := &stubDelivery{err: transport.ErrPeerOffline}
		svc := newUpgradeService(t, rootPriv, delivery)

		_, err := svc.UpgradePeer(ctx, &controlv1.UpgradePeerRequest{
			PeerPub:      subjectPub,
			Capabilities: identity.FullCapabilities(),
		})
		require.Error(t, err)
		require.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("recipient rejection returns Delivered=false with the reason verbatim", func(t *testing.T) {
		ctx := adminCaller(t, rootPriv)
		delivery := &stubDelivery{resp: &meshv1.GrantOfferResponse{Reason: "denylisted"}}
		svc := newUpgradeService(t, rootPriv, delivery)

		resp, err := svc.UpgradePeer(ctx, &controlv1.UpgradePeerRequest{
			PeerPub:      subjectPub,
			Capabilities: identity.FullCapabilities(),
		})
		require.NoError(t, err)
		require.False(t, resp.GetDelivered())
		require.Equal(t, "denylisted", resp.GetReason())
	})

	t.Run("dispatch error other than offline surfaces as Internal", func(t *testing.T) {
		ctx := adminCaller(t, rootPriv)
		delivery := &stubDelivery{err: errors.New("write deadline exceeded")}
		svc := newUpgradeService(t, rootPriv, delivery)

		_, err := svc.UpgradePeer(ctx, &controlv1.UpgradePeerRequest{
			PeerPub:      subjectPub,
			Capabilities: identity.FullCapabilities(),
		})
		require.Error(t, err)
		require.Equal(t, codes.Internal, status.Code(err))
	})
}
