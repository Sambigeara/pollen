// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package admission

import (
	"crypto/ed25519"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

// grantCaps root-signs an authority grant. Root self-issuance (parent nil)
// skips the child-subset check, so any capability shape is valid and chains
// to adminPub as the root.
func grantCaps(t *testing.T, now time.Time, caps *identityv1.Capabilities) (rootPub, authPub ed25519.PublicKey, authPriv ed25519.PrivateKey, g *identityv1.Grant) {
	t.Helper()
	adminPub, adminPriv := newKeyPair(t)
	authPub, authPriv = newKeyPair(t)
	g, err := identity.IssueGrant(adminPriv, nil, authPub, caps, identity.UnlimitedBudget(), now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	return adminPub, authPub, authPriv, g
}

func staticSpecChange(name string) (*statev1.StaticSpecChange, *admissionv1.ResourceID) {
	digest := []byte("digest-bytes-32-aaaaaaaaaaaaaaaa")
	return &statev1.StaticSpecChange{Name: name, ManifestDigest: digest},
		&admissionv1.ResourceID{Body: &admissionv1.ResourceID_Static{Static: &admissionv1.StaticID{Name: name, ManifestDigest: digest}}}
}

// The grant authenticates either way (well-formed, chains to root); the
// rejection for a missing per-kind publish capability is specifically at
// authorise.
func TestAuthoriseRejectsMissingPublishBit(t *testing.T) {
	now := time.Now()

	t.Run("workload rejected without Functions", func(t *testing.T) {
		rootPub, authPub, authPriv, grant := grantCaps(t, now, identity.LeafCapabilities())
		body, res := seedBodyResource("echo", "a")
		f, err := fact.IssueFact(authPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		g := New(rootPub, fakeStore{snap: state.Snapshot{Nodes: nodes(authPub, grant)}})
		err = g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}})
		require.ErrorContains(t, err, "lacks publish capability for functions")
	})

	t.Run("static rejected when caps omit Sites", func(t *testing.T) {
		caps := &identityv1.Capabilities{Publish: &identityv1.PublishCapability{Functions: true, Blobs: true}}
		rootPub, authPub, authPriv, grant := grantCaps(t, now, caps)
		sb, sres := staticSpecChange("site")
		f, err := fact.IssueFact(authPriv, sres, sb, nil, 1, false)
		require.NoError(t, err)
		g := New(rootPub, fakeStore{snap: state.Snapshot{Nodes: nodes(authPub, grant)}})
		err = g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Static{Static: sb}})
		require.ErrorContains(t, err, "lacks publish capability for sites")
	})

	t.Run("admitted with the bit set", func(t *testing.T) {
		rootPub, authPub, authPriv, grant := grantCaps(t, now, identity.PublisherCapabilities())
		body, res := seedBodyResource("echo", "a")
		f, err := fact.IssueFact(authPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		g := New(rootPub, fakeStore{snap: state.Snapshot{Nodes: nodes(authPub, grant)}})
		require.NoError(t, g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}}))
	})
}

func TestAuthoriseEnforcesPublisherAttributes(t *testing.T) {
	now := time.Now()
	policy := &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{{Key: "team", Equals: "core"}}}}

	t.Run("publisher missing the attribute is rejected", func(t *testing.T) {
		rootPub, authPub, authPriv, grant := grantCaps(t, now, identity.PublisherCapabilities())
		body, res := seedBodyResource("echo", "a")
		f, err := fact.IssueFact(authPriv, res, body, policy, 1, false)
		require.NoError(t, err)
		g := New(rootPub, fakeStore{snap: state.Snapshot{Nodes: nodes(authPub, grant)}})
		err = g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}})
		require.ErrorContains(t, err, `missing prop "team"`)
	})

	t.Run("publisher carrying the attribute is admitted", func(t *testing.T) {
		attrs, err := structpb.NewStruct(map[string]any{"team": "core"})
		require.NoError(t, err)
		caps := identity.PublisherCapabilities()
		caps.Attributes = attrs
		rootPub, authPub, authPriv, grant := grantCaps(t, now, caps)
		body, res := seedBodyResource("echo", "a")
		f, err := fact.IssueFact(authPriv, res, body, policy, 1, false)
		require.NoError(t, err)
		g := New(rootPub, fakeStore{snap: state.Snapshot{Nodes: nodes(authPub, grant)}})
		require.NoError(t, g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}}))
	})
}

// Local-source bootstrap window: a self-authored Fact whose Grant has not
// yet gossiped is admitted only with a nil policy, rejected the moment it
// carries one.
func TestAuthenticateLocalBootstrapTolerance(t *testing.T) {
	now := time.Now()
	rootPub, authPub, authPriv, _ := grantCaps(t, now, identity.PublisherCapabilities())
	local := types.PeerKeyFromBytes(authPub)
	body, res := seedBodyResource("echo", "a")

	t.Run("nil policy before grant gossips is admitted", func(t *testing.T) {
		f, err := fact.IssueFact(authPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		g := New(rootPub, fakeStore{snap: state.Snapshot{LocalID: local}})
		require.NoError(t, g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}}))
	})

	t.Run("policy before grant gossips is rejected", func(t *testing.T) {
		pol := &admissionv1.Predicate{Public: true}
		f, err := fact.IssueFact(authPriv, res, body, pol, 1, false)
		require.NoError(t, err)
		g := New(rootPub, fakeStore{snap: state.Snapshot{LocalID: local}})
		err = g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}})
		require.ErrorContains(t, err, "local grant is not yet published")
	})
}

// A Fact must admit identically whether its authority resolves as the local
// node (LocalGrant) or a gossiped peer (GrantFor).
func TestLocalGossipParity(t *testing.T) {
	now := time.Now()
	rootPub, authPub, authPriv, grant := grantCaps(t, now, identity.PublisherCapabilities())
	body, res := seedBodyResource("echo", "a")
	f, err := fact.IssueFact(authPriv, res, body, nil, 1, false)
	require.NoError(t, err)
	good := &statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}}

	authKey := types.PeerKeyFromBytes(authPub)
	gossip := New(rootPub, fakeStore{snap: state.Snapshot{
		LocalID: types.PeerKeyFromBytes([]byte("a-different-local-nodekey-32byte")),
		Nodes:   nodes(authPub, grant),
	}})
	localView := New(rootPub, fakeStore{snap: state.Snapshot{
		LocalID: authKey,
		Nodes:   nodes(authPub, grant),
	}})

	require.NoError(t, gossip.Admit(good), "gossip-resolved authority admitted")
	require.NoError(t, localView.Admit(good), "local-resolved authority admitted")

	tampered := &statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{
		Workload: &statev1.WorkloadSpecChange{Hash: body.GetHash(), Name: "tampered", MinReplicas: 1},
	}}
	require.Error(t, gossip.Admit(tampered), "gossip path rejects mismatched body")
	require.Error(t, localView.Admit(tampered), "local path rejects mismatched body")
}

// A publisher who has lost a publish capability can still tombstone their
// previously authorised Facts of that kind: authenticate proves the
// tombstone is publisher-signed, and authorise exempts tombstones from the
// per-kind check so an admin cap-shrink does not strand old publications.
// The per-kind check stays active on every non-tombstone Fact.
func TestTombstoneBypassesPublishCap(t *testing.T) {
	now := time.Now()
	caps := &identityv1.Capabilities{Publish: &identityv1.PublishCapability{Functions: true}}
	rootPub, authPub, authPriv, grant := grantCaps(t, now, caps)
	sb, sres := staticSpecChange("site")
	tombstone, err := fact.IssueFact(authPriv, sres, sb, nil, 2, true)
	require.NoError(t, err)
	g := New(rootPub, fakeStore{snap: state.Snapshot{Nodes: nodes(authPub, grant)}})
	require.NoError(t, g.Admit(&statev1.SpecChange{Fact: tombstone, Body: &statev1.SpecChange_Static{Static: sb}}))

	create, err := fact.IssueFact(authPriv, sres, sb, nil, 3, false)
	require.NoError(t, err)
	err = g.Admit(&statev1.SpecChange{Fact: create, Body: &statev1.SpecChange_Static{Static: sb}})
	require.ErrorContains(t, err, "lacks publish capability for sites",
		"a non-tombstone create still requires the per-kind cap")
}
