// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package membership

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// upgradeHarness builds a real publisher Service against a live store
// with the production admission pipeline wired as the mutation
// validator, so the recipient's verify-adopt-revoke-gossip pipeline
// exercises the same authorise gate inbound gossip does.
type upgradeHarness struct {
	rootPub  ed25519.PublicKey
	rootPriv ed25519.PrivateKey
	sPub     ed25519.PublicKey
	sPriv    ed25519.PrivateKey
	self     types.PeerKey
	store    state.StateStore
	creds    *identity.Credentials
	svc      *Service
}

func newUpgradeHarness(t *testing.T) *upgradeHarness {
	t.Helper()
	rootPub, rootPriv := ed25519KP(t)
	sPub, sPriv := ed25519KP(t)
	self := types.PeerKeyFromBytes(sPub)

	seed, err := identity.IssueGrant(rootPriv, nil, sPub,
		identity.PublisherCapabilities(), identity.UnlimitedBudget(),
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour), false)
	require.NoError(t, err)

	creds := identity.NewCredentials(rootPub, sPriv, seed)
	st := state.New(self, rootPub)
	st.SetMutationValidator(admission.New(rootPub, st).Admit)
	st.SetLocalSigner(fact.NewSigner(sPriv))
	sig, err := identity.SignGrantSubject(seed, sPriv)
	require.NoError(t, err)
	st.SetLocalGrant(seed, sig)

	svc := &Service{
		creds:    creds,
		store:    st,
		signPriv: sPriv,
		log:      zap.NewNop().Sugar(),
		events:   make(chan state.Event, eventBufSize),
		localID:  self,
	}
	return &upgradeHarness{
		rootPub: rootPub, rootPriv: rootPriv,
		sPub: sPub, sPriv: sPriv, self: self,
		store: st, creds: creds, svc: svc,
	}
}

func (h *upgradeHarness) mintFor(t *testing.T, subject ed25519.PublicKey, caps *identityv1.Capabilities) *identityv1.Grant {
	t.Helper()
	g, err := identity.IssueGrant(h.rootPriv, nil, subject, caps, identity.UnlimitedBudget(),
		time.Now().Add(-time.Hour), time.Now().Add(30*24*time.Hour), false)
	require.NoError(t, err)
	return g
}

func ed25519KP(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

func upgradeTestHash(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	raw[0] = 0xcd
	return hex.EncodeToString(raw)
}

// TestReceiveGrantOffer pins the recipient's verify-adopt-revoke
// pipeline behaviour: a valid same-subject offer swaps the in-memory
// grant, rewrites the local gossiped Principal entry, and tombstones
// only the kinds the new caps no longer authorise; any verification
// failure leaves both credentials and store untouched.
func TestReceiveGrantOffer(t *testing.T) {
	t.Run("admin upgrade adopts the new grant and gossips it", func(t *testing.T) {
		h := newUpgradeHarness(t)
		before := h.creds.Grant()
		offer := h.mintFor(t, h.sPub, identity.FullCapabilities())

		resp := h.svc.ReceiveGrantOffer(&meshv1.GrantOfferRequest{Grant: offer})
		require.True(t, resp.GetAccepted())
		require.Empty(t, resp.GetReason())

		got := h.creds.Grant()
		require.NotSame(t, before, got, "in-memory grant must be swapped")
		require.True(t, got.GetClaims().GetCapabilities().GetCanAdmit(), "upgraded grant must carry CanAdmit")
		require.True(t, got.GetClaims().GetCapabilities().GetCanDelegate(), "upgraded grant must carry CanDelegate")

		nv := h.store.Snapshot().Nodes[h.self]
		require.True(t, bytes.Equal(nv.Grant.GetClaims().GetSubjectPub(), h.sPub),
			"local Principal entry must still name our subject")
		require.True(t, nv.Grant.GetClaims().GetCapabilities().GetCanAdmit(),
			"local Principal entry must reflect the new caps")
	})

	t.Run("nil grant is rejected without state change", func(t *testing.T) {
		h := newUpgradeHarness(t)
		before := h.creds.Grant()

		resp := h.svc.ReceiveGrantOffer(&meshv1.GrantOfferRequest{})
		require.False(t, resp.GetAccepted())
		require.NotEmpty(t, resp.GetReason())
		require.Same(t, before, h.creds.Grant())
	})

	t.Run("wrong subject is rejected without state change", func(t *testing.T) {
		h := newUpgradeHarness(t)
		otherPub, _ := ed25519KP(t)
		offer := h.mintFor(t, otherPub, identity.FullCapabilities())
		before := h.creds.Grant()

		resp := h.svc.ReceiveGrantOffer(&meshv1.GrantOfferRequest{Grant: offer})
		require.False(t, resp.GetAccepted())
		require.Contains(t, resp.GetReason(), "subject")
		require.Same(t, before, h.creds.Grant())
	})

	t.Run("expired grant is rejected without state change", func(t *testing.T) {
		h := newUpgradeHarness(t)
		expired, err := identity.IssueGrant(h.rootPriv, nil, h.sPub,
			identity.FullCapabilities(), identity.UnlimitedBudget(),
			time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour), false)
		require.NoError(t, err)
		before := h.creds.Grant()

		resp := h.svc.ReceiveGrantOffer(&meshv1.GrantOfferRequest{Grant: expired})
		require.False(t, resp.GetAccepted())
		require.Same(t, before, h.creds.Grant())
	})

	t.Run("cap-shrink tombstones only the kinds that lost authority", func(t *testing.T) {
		h := newUpgradeHarness(t)
		hash := upgradeTestHash(t)
		_, err := h.store.PublishWorkload(state.WorkloadSpec{Hash: hash, Name: "echo", MinReplicas: 1}, nil)
		require.NoError(t, err)
		_, present := h.store.Snapshot().Specs[hash]
		require.True(t, present, "workload spec must be present before downgrade")

		downgrade := &identityv1.Capabilities{
			Publish: &identityv1.PublishCapability{Sites: true},
		}
		offer := h.mintFor(t, h.sPub, downgrade)

		resp := h.svc.ReceiveGrantOffer(&meshv1.GrantOfferRequest{Grant: offer})
		require.True(t, resp.GetAccepted())

		snap := h.store.Snapshot()
		_, stillThere := snap.Specs[hash]
		require.False(t, stillThere, "spec for a kind no longer authorised must be tombstoned")
		require.False(t, snap.Nodes[h.self].Grant.GetClaims().GetCapabilities().GetPublish().GetFunctions(),
			"local Principal entry must reflect dropped publish:functions")
	})

	t.Run("revoke failure rolls in-memory grant back", func(t *testing.T) {
		h := newUpgradeHarness(t)
		failErr := errors.New("simulated store fault")
		h.svc.store = &failingRevokeStore{ClusterState: h.store, err: failErr}
		before := h.creds.Grant()
		offer := h.mintFor(t, h.sPub, identity.FullCapabilities())

		resp := h.svc.ReceiveGrantOffer(&meshv1.GrantOfferRequest{Grant: offer})
		require.False(t, resp.GetAccepted())
		require.Contains(t, resp.GetReason(), "simulated store fault")
		require.Same(t, before, h.creds.Grant(),
			"in-memory grant must be restored when RevokeOwnSpecs fails")
	})
}

// failingRevokeStore proxies every ClusterState method through to a
// real store but forces RevokeOwnSpecs to fail, exercising the
// in-memory grant rollback when a post-adopt store write errors.
type failingRevokeStore struct {
	ClusterState
	err error
}

func (f *failingRevokeStore) RevokeOwnSpecs(*identityv1.Capabilities) ([]state.Event, error) {
	return nil, f.err
}
