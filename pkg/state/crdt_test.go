// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func keyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

func workloadHash(b byte) string {
	return hex.EncodeToString(bytes.Repeat([]byte{b}, 32))
}

// TestGossipConvergesGrantAndFact proves a grant and a spec fact
// published on one store reach another store over a single gossip
// exchange, and that the spec stays resolvable there with no further
// liveness from the publisher (durable survival at the snapshot layer).
func TestGossipConvergesGrantAndFact(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := keyPair(t)
	pPub, pPriv := keyPair(t)
	pKey := types.PeerKeyFromBytes(pPub)

	grantP, err := identity.IssueGrant(rootPriv, nil, pPub,
		identity.PublisherCapabilities(), &identityv1.Budget{},
		now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	sigP, err := identity.SignGrantSubject(grantP, pPriv)
	require.NoError(t, err)

	a := New(pKey, rootPub)
	a.SetLocalSigner(fact.NewSigner(pPriv))
	a.SetLocalGrant(grantP, sigP)
	spec := WorkloadSpec{Hash: workloadHash(0xaa), Name: "echo", MinReplicas: 1}
	_, err = a.PublishWorkload(spec, nil)
	require.NoError(t, err)

	b := New(types.PeerKeyFromBytes([]byte{0x09}), rootPub)
	_, _, err = b.ApplyDelta(a.EncodeFull())
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap.GrantFor(pPub), "grant converged to B")
	require.Equal(t,
		grantP.GetClaims().GetSerial(),
		snap.Nodes[pKey].Grant.GetClaims().GetSerial())
	sv, ok := snap.Specs[spec.Hash]
	require.True(t, ok, "spec converged to B")
	require.Equal(t, pKey, sv.Publisher)
	require.NotNil(t, sv.Fact)
	require.False(t, snap.IsDenied(pKey))
}

// TestRootDenyPoisonsSubtree proves a root-issued deny of an
// intermediate authority transitively removes a descendant publisher's
// specs from the rebuilt snapshot.
func TestRootDenyPoisonsSubtree(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := keyPair(t)
	rootKey := types.PeerKeyFromBytes(rootPub)
	iPub, iPriv := keyPair(t)
	iKey := types.PeerKeyFromBytes(iPub)
	pPub, pPriv := keyPair(t)
	pKey := types.PeerKeyFromBytes(pPub)

	grantI, err := identity.IssueGrant(rootPriv, nil, iPub,
		identity.FullCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	sigI, err := identity.SignGrantSubject(grantI, iPriv)
	require.NoError(t, err)

	grantP, err := identity.IssueGrant(iPriv, grantI, pPub,
		identity.PublisherCapabilities(), &identityv1.Budget{},
		now.Add(-time.Minute), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	sigP, err := identity.SignGrantSubject(grantP, pPriv)
	require.NoError(t, err)

	iStore := New(iKey, rootPub)
	iStore.SetLocalGrant(grantI, sigI)

	pStore := New(pKey, rootPub)
	pStore.SetLocalSigner(fact.NewSigner(pPriv))
	pStore.SetLocalGrant(grantP, sigP)
	spec := WorkloadSpec{Hash: workloadHash(0xbb), Name: "site", MinReplicas: 1}
	_, err = pStore.PublishWorkload(spec, nil)
	require.NoError(t, err)

	root := New(rootKey, rootPub)
	_, _, err = root.ApplyDelta(iStore.EncodeFull())
	require.NoError(t, err)
	_, _, err = root.ApplyDelta(pStore.EncodeFull())
	require.NoError(t, err)

	pre := root.Snapshot()
	_, ok := pre.Specs[spec.Hash]
	require.True(t, ok, "publisher spec present before deny")
	require.False(t, pre.IsDenied(pKey))

	// Root denies the intermediate, not the publisher itself.
	root.DenyPeer(iKey)

	post := root.Snapshot()
	require.True(t, post.IsDenied(iKey), "intermediate denied")
	require.True(t, post.IsDenied(pKey), "descendant poisoned transitively")
	_, ok = post.Specs[spec.Hash]
	require.False(t, ok, "denied subtree's specs vanish from the rebuilt snapshot")
}

// TestWireTombstoneReplayRejected proves the gossip-apply path rejects
// a captured published spec event whose envelope Deleted bit has been
// flipped: the signed fact says create, the envelope says tombstone, so
// acceptableSpecEventLocked refuses it.
func TestWireTombstoneReplayRejected(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := keyPair(t)
	pPub, pPriv := keyPair(t)
	pKey := types.PeerKeyFromBytes(pPub)

	grantP, err := identity.IssueGrant(rootPriv, nil, pPub,
		identity.PublisherCapabilities(), &identityv1.Budget{},
		now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	sigP, err := identity.SignGrantSubject(grantP, pPriv)
	require.NoError(t, err)

	a := New(pKey, rootPub)
	a.SetLocalSigner(fact.NewSigner(pPriv))
	a.SetLocalGrant(grantP, sigP)
	spec := WorkloadSpec{Hash: workloadHash(0xcd), Name: "echo", MinReplicas: 1}
	_, err = a.PublishWorkload(spec, nil)
	require.NoError(t, err)
	data := a.EncodeFull()

	// Control: the untampered delta admits the spec.
	clean := New(types.PeerKeyFromBytes([]byte{0x08}), rootPub)
	_, _, err = clean.ApplyDelta(data)
	require.NoError(t, err)
	_, ok := clean.Snapshot().Specs[spec.Hash]
	require.True(t, ok, "control: clean delta admits the spec")

	// Flip the spec event's envelope Deleted bit (signed fact stays
	// create) and re-apply: it must be refused.
	batch := &statev1.GossipEventBatch{}
	require.NoError(t, batch.UnmarshalVT(data))
	flipped := false
	for _, ev := range batch.GetEvents() {
		if ev.GetSpecChange() != nil {
			ev.Deleted = !ev.Deleted
			flipped = true
		}
	}
	require.True(t, flipped, "spec event present in the batch")
	tampered, err := batch.MarshalVT()
	require.NoError(t, err)

	b := New(types.PeerKeyFromBytes([]byte{0x09}), rootPub)
	_, _, err = b.ApplyDelta(tampered)
	require.NoError(t, err)
	_, ok = b.Snapshot().Specs[spec.Hash]
	require.False(t, ok, "tombstone-replay of a published fact is rejected on the wire")
}

// TestDenyBeforeGrantBecomesEffectiveOnArrival proves a delegated
// admin's deny lodged before the subject's grant has gossiped stays
// pending (the admin's authority over the subject cannot yet be
// established), then takes effect once the subject's grant arrives and
// shows the admin in its chain.
func TestDenyBeforeGrantBecomesEffectiveOnArrival(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := keyPair(t)
	adminPub, adminPriv := keyPair(t)
	adminKey := types.PeerKeyFromBytes(adminPub)
	xPub, xPriv := keyPair(t)
	xKey := types.PeerKeyFromBytes(xPub)

	adminGrant, err := identity.IssueGrant(rootPriv, nil, adminPub,
		identity.FullCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	sigAdmin, err := identity.SignGrantSubject(adminGrant, adminPriv)
	require.NoError(t, err)

	grantX, err := identity.IssueGrant(adminPriv, adminGrant, xPub,
		identity.PublisherCapabilities(), &identityv1.Budget{},
		now.Add(-time.Minute), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	sigX, err := identity.SignGrantSubject(grantX, xPriv)
	require.NoError(t, err)

	xStore := New(xKey, rootPub)
	xStore.SetLocalSigner(fact.NewSigner(xPriv))
	xStore.SetLocalGrant(grantX, sigX)
	spec := WorkloadSpec{Hash: workloadHash(0xde), Name: "site", MinReplicas: 1}
	_, err = xStore.PublishWorkload(spec, nil)
	require.NoError(t, err)

	// The delegated admin denies X before it has seen X's grant.
	admin := New(adminKey, rootPub)
	admin.SetLocalGrant(adminGrant, sigAdmin)
	admin.DenyPeer(xKey)
	require.False(t, admin.Snapshot().IsDenied(xKey),
		"deny stays pending while X's grant (and so the admin's authority over X) is unknown")

	_, _, err = admin.ApplyDelta(xStore.EncodeFull())
	require.NoError(t, err)

	post := admin.Snapshot()
	require.True(t, post.IsDenied(xKey),
		"deny takes effect once X's grant arrives showing the admin in its chain")
	_, ok := post.Specs[spec.Hash]
	require.False(t, ok, "denied publisher's spec is excluded from the rebuilt snapshot")
}

// TestPublishedFactCannotBeReplayedAsTombstone proves a create fact
// (deleted=false) is refused on the tombstone path, so a relayed
// published fact cannot be turned into an unseed.
func TestPublishedFactCannotBeReplayedAsTombstone(t *testing.T) {
	_, pPriv := keyPair(t)
	st := New(types.PeerKeyFromBytes([]byte{0x01}), bytes.Repeat([]byte{0x02}, 32))

	hash := workloadHash(0xab)
	body := &statev1.WorkloadSpecChange{Hash: hash, Name: "echo", MinReplicas: 1}
	res := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{
		Name: body.GetName(),
		Hash: bytes.Repeat([]byte{0xab}, 32),
	}}}
	createFact, err := fact.IssueFact(pPriv, res, body, nil, 1, false)
	require.NoError(t, err)

	_, err = st.DeleteWorkloadSpecPresigned(hash, createFact)
	require.ErrorContains(t, err, "must have Deleted=true")
}

// TestRegisterPeerGrantAdmitsDaemonlessPublisher pins the substrate fix
// for the wire-tenant publish path. A wire publisher runs no daemon, so
// it never SetLocalGrants its own grant; before the serving node relays
// that grant in, snap.GrantFor(publisher) is nil and the admission
// pipeline rejects every presigned Fact with "fact authority grant not
// in cluster state". RegisterPeerGrant makes the grant resolvable while
// reusing the identical proof-of-possession gate a gossiped grant must
// clear, so it introduces no new trust, and the relayed grant converges
// so peers that never saw the publisher's session admit its Facts too.
func TestRegisterPeerGrantAdmitsDaemonlessPublisher(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := keyPair(t)
	pPub, pPriv := keyPair(t)
	pKey := types.PeerKeyFromBytes(pPub)
	srvKey := types.PeerKeyFromBytes([]byte{0x07})

	grantP, err := identity.IssueGrant(rootPriv, nil, pPub,
		identity.PublisherCapabilities(), &identityv1.Budget{},
		now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	sigP, err := identity.SignGrantSubject(grantP, pPriv)
	require.NoError(t, err)

	t.Run("absent until relayed, then resolvable", func(t *testing.T) {
		srv := New(srvKey, rootPub)
		require.Nil(t, srv.Snapshot().GrantFor(pPub),
			"daemonless publisher's grant is not in cluster state")

		events := srv.RegisterPeerGrant(pKey, grantP, sigP)
		require.Contains(t, events, GrantChanged{Peer: pKey})

		got := srv.Snapshot().GrantFor(pPub)
		require.NotNil(t, got, "relayed grant now resolves for admission")
		require.Equal(t, grantP.GetClaims().GetSerial(), got.GetClaims().GetSerial())

		require.Empty(t, srv.RegisterPeerGrant(pKey, grantP, sigP),
			"re-registering identical content is a no-op, no slot churn")
	})

	t.Run("proof-of-possession gate still enforced", func(t *testing.T) {
		srv := New(srvKey, rootPub)
		require.Empty(t, srv.RegisterPeerGrant(pKey, grantP, nil))
		require.Empty(t, srv.RegisterPeerGrant(pKey, grantP, bytes.Repeat([]byte{0x01}, 64)))
		require.Nil(t, srv.Snapshot().GrantFor(pPub),
			"a missing or forged subject proof is rejected, exactly as for gossip")

		require.Empty(t, srv.RegisterPeerGrant(types.PeerKeyFromBytes([]byte{0x08}), grantP, sigP))
		require.Nil(t, srv.Snapshot().GrantFor(pPub),
			"a grant whose subject is not the slot peer is rejected")
	})

	t.Run("converges so non-publisher nodes admit the fact", func(t *testing.T) {
		srv := New(srvKey, rootPub)
		srv.RegisterPeerGrant(pKey, grantP, sigP)

		b := New(types.PeerKeyFromBytes([]byte{0x09}), rootPub)
		_, _, err := b.ApplyDelta(srv.EncodeFull())
		require.NoError(t, err)
		require.NotNil(t, b.Snapshot().GrantFor(pPub),
			"relayed grant reaches a node that never saw the publisher's session")
	})
}
