// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"crypto/ed25519"
	"encoding/hex"
	"testing"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestMultiTenantSpecIsolation is P1's core-guarantee proof: two
// distinct principals publishing byte-identical workload content under
// the same logical name occupy distinct (authority, name) registers and
// are both visible per-authority, with no cross-publisher conflict
// rejection. publisherFullState fixes the hash and name, so two calls
// differ only by principal, exactly the collision the old global
// content-hash key conflated.
func TestMultiTenantSpecIsolation(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	pkA, pubA, hash, dataA := publisherFullState(t, rootPriv, rootPub)
	pkB, pubB, hashB, dataB := publisherFullState(t, rootPriv, rootPub)
	require.Equal(t, hash, hashB, "fixture publishes identical content")
	require.NotEqual(t, pkA, pkB)

	b := validatedStore(t, types.PeerKeyFromBytes([]byte{0x09}), rootPub)
	_, _, err := b.ApplyDelta(dataA)
	require.NoError(t, err)
	_, _, err = b.ApplyDelta(dataB)
	require.NoError(t, err)
	snap := b.Snapshot()

	// Both publications coexist in the per-(authority,name) source: the
	// second publisher is not rejected as a conflicting owner and not
	// dropped by a dedupe tie-break.
	byAuthority := map[types.PeerKey]state.WorkloadSpecView{}
	for _, sv := range snap.SpecsAll {
		require.Equal(t, "echo", sv.Spec.Name)
		require.Equal(t, hash, sv.Spec.Hash)
		byAuthority[sv.Publisher] = sv
	}
	require.Contains(t, byAuthority, pkA)
	require.Contains(t, byAuthority, pkB)

	// The deduped runtime map collapses identical bytes to one shared
	// artefact entry, deterministically the lowest publisher, matching
	// buildSnapshot's outranks so the runtime and listing planes never
	// disagree on a colliding key.
	require.Len(t, snap.Specs, 1)
	lower := pkA
	if pkB.Compare(pkA) < 0 {
		lower = pkB
	}
	require.Equal(t, lower, snap.Specs[hash].Publisher)

	// Accounting is per-authority: each principal is charged its own
	// publication only, never the other tenant's identical-content one.
	require.Equal(t, map[string]struct{}{"echo": {}}, snap.UsageByAuthority(pubA).FunctionNames)
	require.Equal(t, map[string]struct{}{"echo": {}}, snap.UsageByAuthority(pubB).FunctionNames)

	// Denied-principal exclusion is independent of the retired conflict
	// scan: it lives in buildSnapshot's valid-record filter and
	// recomputeDeniedLocked, both untouched by P1 and exercised by the
	// existing deny/transitive-revocation suite.
}

// TestRevokeOwnSpecsRetainsAuthorisedKinds proves the cap-aware filter:
// a retain capability set covering the kind keeps the matching spec
// in cluster state; a retain set without the kind tombstones it.
// Exercises the contract that a partial cap-shrink (e.g. publish:sites
// drops while publish:functions stays) does not collapse the publisher's
// entire surface: only the kinds that lost authority go.
func TestRevokeOwnSpecsRetainsAuthorisedKinds(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	now := time.Now()

	makeStore := func(t *testing.T) (state.StateStore, string) {
		t.Helper()
		pPub, pPriv := keyPair(t)
		pKey := types.PeerKeyFromBytes(pPub)
		st := validatedStore(t, pKey, rootPub)
		grant, err := identity.IssueGrant(rootPriv, nil, pPub,
			identity.PublisherCapabilities(), &identityv1.Budget{},
			now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
		require.NoError(t, err)
		sig, err := identity.SignGrantSubject(grant, pPriv)
		require.NoError(t, err)
		st.SetLocalSigner(fact.NewSigner(pPriv))
		st.SetLocalGrant(grant, sig)

		hash := validHash(t)
		_, err = st.PublishWorkload(state.WorkloadSpec{Hash: hash, Name: "echo", MinReplicas: 1}, nil)
		require.NoError(t, err)
		return st, hash
	}

	t.Run("retain functions keeps the workload", func(t *testing.T) {
		st, hash := makeStore(t)
		retain := &identityv1.Capabilities{Publish: &identityv1.PublishCapability{Functions: true}}
		_, err := st.RevokeOwnSpecs(retain)
		require.NoError(t, err)
		require.Contains(t, st.Snapshot().Specs, hash, "kind retained by caps must not be tombstoned")
	})

	t.Run("retain sites without functions tombstones the workload", func(t *testing.T) {
		st, hash := makeStore(t)
		retain := &identityv1.Capabilities{Publish: &identityv1.PublishCapability{Sites: true}}
		_, err := st.RevokeOwnSpecs(retain)
		require.NoError(t, err)
		_, present := st.Snapshot().Specs[hash]
		require.False(t, present, "kind without retain authority must be tombstoned")
	})
}

// TestRevokeOwnSpecsIgnoresDedupeWinner is the regression for the
// cycle-2 CRITICAL: RevokeOwnSpecs must tombstone this node's own spec
// even when a colliding remote tenant publishing byte-identical content
// won the deduped snap.Specs map. A deduped-map scan would skip the
// local spec whenever the remote's PeerKey sorts lower, leaving a
// cap-downgraded principal serving a spec it has lost authority over.
func TestRevokeOwnSpecsIgnoresDedupeWinner(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	remotePK, _, hash, remoteData := publisherFullState(t, rootPriv, rootPub)

	// Force the adverse ordering: the remote tenant must sort lower so
	// outranks makes it win the deduped snap.Specs. That is exactly the
	// case the old deduped-map RevokeOwnSpecs skipped this node's own
	// spec, so the regression now bites deterministically rather than
	// on a coin-flip of random key order.
	var bPub ed25519.PublicKey
	var bPriv ed25519.PrivateKey
	var bKey types.PeerKey
	for {
		bPub, bPriv = keyPair(t)
		bKey = types.PeerKeyFromBytes(bPub)
		if bKey.Compare(remotePK) > 0 {
			break
		}
	}
	b := validatedStore(t, bKey, rootPub)
	now := time.Now()
	bGrant, err := identity.IssueGrant(rootPriv, nil, bPub,
		identity.PublisherCapabilities(), &identityv1.Budget{},
		now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	bSig, err := identity.SignGrantSubject(bGrant, bPriv)
	require.NoError(t, err)
	b.SetLocalSigner(fact.NewSigner(bPriv))
	b.SetLocalGrant(bGrant, bSig)

	_, err = b.PublishWorkload(state.WorkloadSpec{Hash: hash, Name: "echo", MinReplicas: 1}, nil)
	require.NoError(t, err)
	_, _, err = b.ApplyDelta(remoteData)
	require.NoError(t, err)

	// Precondition: identical bytes+name collapse to one shared runtime
	// entry, and by the forced ordering the remote tenant owns it. A
	// deduped-map RevokeOwnSpecs would now skip this node's own spec.
	pre := b.Snapshot()
	require.Len(t, pre.Specs, 1)
	require.Equal(t, remotePK, pre.Specs[hash].Publisher)

	_, err = b.RevokeOwnSpecs(nil)
	require.NoError(t, err)

	after := b.Snapshot()
	remoteStillPresent := false
	for _, sv := range after.SpecsAll {
		require.NotEqual(t, bKey, sv.Publisher, "local node's own spec must be revoked regardless of dedupe winner")
		if sv.Publisher == remotePK {
			remoteStillPresent = true
		}
	}
	require.True(t, remoteStillPresent, "revoking own specs must not touch another tenant's identical-content spec")
}

// TestCapShrinkConvergesOnRemotePeer pins the end-to-end convergence
// shape an admin-initiated cap-shrink upgrade depends on: the recipient
// queues tombstones for the kinds they have lost, then gossips the new
// (shrunken) grant in the same batch. The apply path admits the new
// grant in the first pass before the second-pass spec tombstones, so a
// remote peer's authorise stage sees the recipient's NEW caps when
// admitting the tombstones. The tombstone exemption in authorise lets
// those tombstones land regardless, and B drops the spec; without it,
// the spec would be stranded on every peer except the publisher.
func TestCapShrinkConvergesOnRemotePeer(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	now := time.Now()

	// A: publisher with publish:functions. Publishes a workload and
	// gossips full state to B.
	aPub, aPriv := keyPair(t)
	aKey := types.PeerKeyFromBytes(aPub)
	a := validatedStore(t, aKey, rootPub)
	aGrant, err := identity.IssueGrant(rootPriv, nil, aPub,
		identity.PublisherCapabilities(), &identityv1.Budget{},
		now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	aSig, err := identity.SignGrantSubject(aGrant, aPriv)
	require.NoError(t, err)
	a.SetLocalSigner(fact.NewSigner(aPriv))
	a.SetLocalGrant(aGrant, aSig)
	hash := validHash(t)
	_, err = a.PublishWorkload(state.WorkloadSpec{Hash: hash, Name: "echo", MinReplicas: 1}, nil)
	require.NoError(t, err)

	bPub, _ := keyPair(t)
	bKey := types.PeerKeyFromBytes(bPub)
	b := validatedStore(t, bKey, rootPub)
	_, _, err = b.ApplyDelta(a.EncodeFull())
	require.NoError(t, err)
	require.Contains(t, b.Snapshot().Specs, hash, "B observes A's workload before the upgrade")

	// A receives an admin-initiated cap-shrink: drops publish:functions.
	// RevokeOwnSpecs queues the tombstone (signed under A's still-current
	// signing key, which is unchanged), then SetLocalGrant queues the
	// new grant event. FlushPendingGossip drains both into one batch.
	shrunken := &identityv1.Capabilities{
		Publish: &identityv1.PublishCapability{Sites: true},
	}
	_, err = a.RevokeOwnSpecs(shrunken)
	require.NoError(t, err)
	newGrant, err := identity.IssueGrant(rootPriv, nil, aPub,
		shrunken, &identityv1.Budget{},
		now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	newSig, err := identity.SignGrantSubject(newGrant, aPriv)
	require.NoError(t, err)
	a.SetLocalGrant(newGrant, newSig)

	// Send A's full state to B (the FlushPendingGossip equivalent for
	// the test: a single delivery covering tombstone + new grant).
	_, _, err = b.ApplyDelta(a.EncodeFull())
	require.NoError(t, err)

	snap := b.Snapshot()
	_, stillThere := snap.Specs[hash]
	require.False(t, stillThere,
		"B must drop A's workload once A has tombstoned it and gossiped the cap-shrunken grant")
	require.False(t, snap.Nodes[aKey].Grant.GetClaims().GetCapabilities().GetPublish().GetFunctions(),
		"B must observe A's new (shrunken) caps")
}

func validHash(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	raw[0] = 0xcd
	return hex.EncodeToString(raw)
}

// TestSpecPublishRequiresName proves the fail-closed name guard on
// every create path. The proto's min_len:1 stops a nameless workload at
// the wire, but an in-process local-signer publish never round-trips
// through buf.validate, so the store enforces it directly: without a
// name there is no (authority, name) register to occupy.
func TestSpecPublishRequiresName(t *testing.T) {
	rootPub, _ := keyPair(t)
	st := state.New(types.PeerKeyFromBytes([]byte{0x09}), rootPub)
	h := validHash(t)

	_, err := st.PublishWorkload(state.WorkloadSpec{Hash: h, Name: "", MinReplicas: 1}, nil)
	require.ErrorIs(t, err, state.ErrMissingName)

	_, err = st.SetStaticSpec(state.StaticSpec{Name: "", ManifestDigest: h}, nil)
	require.ErrorIs(t, err, state.ErrMissingName)

	_, err = st.SetBlobSpec(state.BlobSpec{Name: "", Digest: h}, nil)
	require.ErrorIs(t, err, state.ErrMissingName)
}

// TestSpecByNameResolvesWithinAuthority proves P2's core resolver
// guarantee: a workload name resolves to at most one spec per authority
// and never crosses tenants. Principals A and B publish byte-identical
// content under the same logical name "echo"; each authority's lookup
// returns only its own publication. A third real, provisioned tenant C
// that published a different name "ping" still cannot resolve A's or
// B's "echo": resolution is per-authority, not "any authority that has
// published something".
func TestSpecByNameResolvesWithinAuthority(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	pkA, _, hash, dataA := publisherFullState(t, rootPriv, rootPub)
	pkB, _, hashB, dataB := publisherFullState(t, rootPriv, rootPub)
	require.Equal(t, hash, hashB, "fixture publishes identical content")
	require.NotEqual(t, pkA, pkB)

	b := validatedStore(t, types.PeerKeyFromBytes([]byte{0x09}), rootPub)
	_, _, err := b.ApplyDelta(dataA)
	require.NoError(t, err)
	_, _, err = b.ApplyDelta(dataB)
	require.NoError(t, err)
	snap := b.Snapshot()

	hA, svA, okA := snap.SpecByName("echo", pkA)
	require.True(t, okA)
	require.Equal(t, hash, hA)
	require.Equal(t, pkA, svA.Publisher)

	hB, svB, okB := snap.SpecByName("echo", pkB)
	require.True(t, okB)
	require.Equal(t, hash, hB)
	require.Equal(t, pkB, svB.Publisher)

	// A real, provisioned third tenant that published a different name
	// is still confined to its own authority: it resolves its own
	// "ping" but never A's or B's "echo".
	cPub, cPriv := keyPair(t)
	pkC := types.PeerKeyFromBytes(cPub)
	now := time.Now()
	cGrant, err := identity.IssueGrant(rootPriv, nil, cPub,
		identity.PublisherCapabilities(), &identityv1.Budget{},
		now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	cSig, err := identity.SignGrantSubject(cGrant, cPriv)
	require.NoError(t, err)
	c := state.New(pkC, rootPub)
	c.SetLocalSigner(fact.NewSigner(cPriv))
	c.SetLocalGrant(cGrant, cSig)
	_, err = c.PublishWorkload(state.WorkloadSpec{Hash: validHash(t), Name: "ping", MinReplicas: 1}, nil)
	require.NoError(t, err)
	_, _, err = b.ApplyDelta(c.EncodeFull())
	require.NoError(t, err)
	snap = b.Snapshot()

	_, _, okPing := snap.SpecByName("ping", pkC)
	require.True(t, okPing, "C resolves its own published name")
	_, _, okEchoAsC := snap.SpecByName("echo", pkC)
	require.False(t, okEchoAsC, "a real tenant cannot resolve another tenant's name")
}

// TestStaticClaimAuthorityIsolation proves the StaticClaimChange
// authority field end to end through the real CRDT path: one node
// claiming the same site name for two distinct authorities occupies two
// independent registers, and releasing one authority's claim leaves the
// other's serving commitment intact.
func TestStaticClaimAuthorityIsolation(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	localPub, localPriv := keyPair(t)
	local := types.PeerKeyFromBytes(localPub)
	now := time.Now()
	g, err := identity.IssueGrant(rootPriv, nil, localPub,
		identity.PublisherCapabilities(), &identityv1.Budget{},
		now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
	require.NoError(t, err)
	sig, err := identity.SignGrantSubject(g, localPriv)
	require.NoError(t, err)
	st := validatedStore(t, local, rootPub)
	st.SetLocalSigner(fact.NewSigner(localPriv))
	st.SetLocalGrant(g, sig)

	authA := types.PeerKeyFromBytes([]byte{0x01})
	authB := types.PeerKeyFromBytes([]byte{0x02})
	st.ClaimStatic("blog", authA)
	st.ClaimStatic("blog", authB)

	snap := st.Snapshot()
	keyA := state.StaticClaimKey{Authority: authA, Name: "blog"}
	keyB := state.StaticClaimKey{Authority: authB, Name: "blog"}
	require.Contains(t, snap.StaticClaims[keyA], local)
	require.Contains(t, snap.StaticClaims[keyB], local)

	st.ReleaseStatic("blog", authA)
	snap = st.Snapshot()
	require.NotContains(t, snap.StaticClaims, keyA)
	require.Contains(t, snap.StaticClaims[keyB], local, "releasing one authority's claim must not drop another's")
}

// TestLocalPublishesScopesByAuthority pins both conjuncts of the three
// ownership pre-check predicates: the key (hash/name/digest) match and
// the publisher match. An equality flip in either is an ownership-bypass
// (a dedupe-winner suppressing the loser's unseed authorisation), so
// each predicate is asserted true for the owner, false for a wrong key,
// and false for a co-publisher of the same key.
func TestLocalPublishesScopesByAuthority(t *testing.T) {
	owner := types.PeerKey{0xaa}
	other := types.PeerKey{0xbb}
	snap := state.Snapshot{
		SpecsAll:       []state.WorkloadSpecView{{Spec: state.WorkloadSpec{Hash: "wasmX", Name: "fn"}, Publisher: owner}},
		StaticSpecsAll: []state.StaticSpecView{{Spec: state.StaticSpec{Name: "site"}, Publisher: owner}},
		BlobSpecsAll:   []state.BlobSpecView{{Spec: state.BlobSpec{Name: "b", Digest: "blobX"}, Publisher: owner}},
	}

	require.True(t, snap.LocalPublishesWorkload("wasmX", owner))
	require.False(t, snap.LocalPublishesWorkload("other", owner), "wrong hash must not match")
	require.False(t, snap.LocalPublishesWorkload("wasmX", other), "co-publisher of identical bytes is not the owner")

	require.True(t, snap.LocalPublishesStatic("site", owner))
	require.False(t, snap.LocalPublishesStatic("other", owner), "wrong name must not match")
	require.False(t, snap.LocalPublishesStatic("site", other), "co-publisher of same name is not the owner")

	require.True(t, snap.LocalPublishesBlob("blobX", owner))
	require.False(t, snap.LocalPublishesBlob("other", owner), "wrong digest must not match")
	require.False(t, snap.LocalPublishesBlob("blobX", other), "co-publisher of identical bytes is not the owner")
}

// TestUsageByAuthorityScopesByPublisher pins the publisher filter on
// each of the three per-authority usage arms. Asymmetric ownership (A
// owns two of each kind, B one) is deliberate: a symmetric fixture
// cannot distinguish the publisher-equality flip that drives per-tenant
// budget accounting, and a flip is a cross-tenant budget-attribution
// bug.
func TestUsageByAuthorityScopesByPublisher(t *testing.T) {
	pubA := make([]byte, 32)
	pubA[0] = 0xaa
	pubB := make([]byte, 32)
	pubB[0] = 0xbb
	keyA := types.PeerKeyFromBytes(pubA)
	keyB := types.PeerKeyFromBytes(pubB)

	snap := state.Snapshot{
		SpecsAll: []state.WorkloadSpecView{
			{Spec: state.WorkloadSpec{Name: "a-fn-1"}, Publisher: keyA},
			{Spec: state.WorkloadSpec{Name: "a-fn-2"}, Publisher: keyA},
			{Spec: state.WorkloadSpec{Name: "b-fn"}, Publisher: keyB},
		},
		BlobSpecsAll: []state.BlobSpecView{
			{Spec: state.BlobSpec{Name: "a-blob-1"}, Publisher: keyA},
			{Spec: state.BlobSpec{Name: "a-blob-2"}, Publisher: keyA},
			{Spec: state.BlobSpec{Name: "b-blob"}, Publisher: keyB},
		},
		StaticSpecsAll: []state.StaticSpecView{
			{Spec: state.StaticSpec{Name: "a-site-1"}, Publisher: keyA},
			{Spec: state.StaticSpec{Name: "a-site-2"}, Publisher: keyA},
			{Spec: state.StaticSpec{Name: "b-site"}, Publisher: keyB},
		},
	}

	a := snap.UsageByAuthority(pubA)
	require.Equal(t, map[string]struct{}{"a-fn-1": {}, "a-fn-2": {}}, a.FunctionNames)
	require.Equal(t, map[string]struct{}{"a-blob-1": {}, "a-blob-2": {}}, a.BlobNames)
	require.Equal(t, map[string]struct{}{"a-site-1": {}, "a-site-2": {}}, a.SiteNames)

	b := snap.UsageByAuthority(pubB)
	require.Equal(t, map[string]struct{}{"b-fn": {}}, b.FunctionNames)
	require.Equal(t, map[string]struct{}{"b-blob": {}}, b.BlobNames)
	require.Equal(t, map[string]struct{}{"b-site": {}}, b.SiteNames)
}
