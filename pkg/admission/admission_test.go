// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package admission

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

type fakeStore struct{ snap state.Snapshot }

func (f fakeStore) Snapshot() state.Snapshot { return f.snap }

func newKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

// authority builds a root-signed publisher grant. attrs, when set,
// become the grant's capability attributes for inline-clause tests.
func authority(t *testing.T, now, deadline time.Time, attrs map[string]any) (rootPub, authPub ed25519.PublicKey, authPriv ed25519.PrivateKey, grant *identityv1.Grant) {
	t.Helper()
	adminPub, adminPriv := newKeyPair(t)
	authPub, authPriv = newKeyPair(t)
	caps := identity.PublisherCapabilities()
	if attrs != nil {
		s, err := structpb.NewStruct(attrs)
		require.NoError(t, err)
		caps.Attributes = s
	}
	grant, err := identity.IssueGrant(adminPriv, nil, authPub, caps, identity.UnlimitedBudget(), now.Add(-time.Hour), deadline, false)
	require.NoError(t, err)
	return adminPub, authPub, authPriv, grant
}

// rootedAuthority issues a publisher grant under a shared admin key, so
// several authorities chain to one cluster root. Use it where more than one
// publisher must coexist under the root the Pipeline verifies against
// (issuerGrantValid checks each issuer's grant chains to that root).
func rootedAuthority(t *testing.T, adminPriv ed25519.PrivateKey, now, deadline time.Time) (pub ed25519.PublicKey, priv ed25519.PrivateKey, grant *identityv1.Grant) {
	t.Helper()
	pub, priv = newKeyPair(t)
	grant, err := identity.IssueGrant(adminPriv, nil, pub, identity.PublisherCapabilities(), identity.UnlimitedBudget(), now.Add(-time.Hour), deadline, false)
	require.NoError(t, err)
	return pub, priv, grant
}

func seedBodyResource(name, hexByte string) (*statev1.WorkloadSpecChange, *admissionv1.ResourceID) {
	body := &statev1.WorkloadSpecChange{Hash: strings.Repeat(hexByte, 64), Name: name, MinReplicas: 1}
	hb, _ := hex.DecodeString(body.GetHash())
	return body, &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: name, Hash: hb}}}
}

func blobBodyResource(name, hexByte string) (*statev1.BlobSpecChange, *admissionv1.ResourceID) {
	digest, _ := hex.DecodeString(strings.Repeat(hexByte, 64))
	body := &statev1.BlobSpecChange{Name: name, Digest: digest}
	return body, &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{Name: name, Digest: digest}}}
}

func nodes(authPub ed25519.PublicKey, grant *identityv1.Grant) map[types.PeerKey]state.NodeView {
	return map[types.PeerKey]state.NodeView{
		types.PeerKeyFromBytes(authPub): {Grant: grant},
	}
}

func TestAdmit(t *testing.T) {
	now := time.Now()

	t.Run("accepts a well-formed workload fact", func(t *testing.T) {
		rootPub, authPub, authPriv, grant := authority(t, now, now.Add(30*24*time.Hour), nil)
		body, res := seedBodyResource("echo", "a")
		f, err := fact.IssueFact(authPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		sc := &statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}}
		g := New(rootPub, fakeStore{snap: state.Snapshot{Nodes: nodes(authPub, grant)}})
		require.NoError(t, g.Admit(sc))
	})

	t.Run("accepts static and blob facts", func(t *testing.T) {
		rootPub, authPub, authPriv, grant := authority(t, now, now.Add(30*24*time.Hour), nil)
		store := fakeStore{snap: state.Snapshot{Nodes: nodes(authPub, grant)}}
		g := New(rootPub, store)

		sb := &statev1.StaticSpecChange{Name: "site", ManifestDigest: []byte("digest-bytes-32-aaaaaaaaaaaaaaaa")}
		sres := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Static{Static: &admissionv1.StaticID{Name: "site", ManifestDigest: sb.GetManifestDigest()}}}
		sf, err := fact.IssueFact(authPriv, sres, sb, nil, 1, false)
		require.NoError(t, err)
		require.NoError(t, g.Admit(&statev1.SpecChange{Fact: sf, Body: &statev1.SpecChange_Static{Static: sb}}))

		bb := &statev1.BlobSpecChange{Name: "blob", Digest: []byte("digest-bytes-32-bbbbbbbbbbbbbbbb")}
		bres := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{Name: "blob", Digest: bb.GetDigest()}}}
		bf, err := fact.IssueFact(authPriv, bres, bb, nil, 1, false)
		require.NoError(t, err)
		require.NoError(t, g.Admit(&statev1.SpecChange{Fact: bf, Body: &statev1.SpecChange_Blob{Blob: bb}}))
	})

	t.Run("fail-closed rejections", func(t *testing.T) {
		rootPub, authPub, authPriv, grant := authority(t, now, now.Add(30*24*time.Hour), nil)
		body, res := seedBodyResource("echo", "a")
		goodFact := func() *factv1.Fact {
			f, err := fact.IssueFact(authPriv, res, body, nil, 1, false)
			require.NoError(t, err)
			return f
		}

		t.Run("missing fact", func(t *testing.T) {
			g := New(rootPub, fakeStore{snap: state.Snapshot{Nodes: nodes(authPub, grant)}})
			err := g.Admit(&statev1.SpecChange{Body: &statev1.SpecChange_Workload{Workload: body}})
			require.Error(t, err)
		})

		t.Run("resource does not match body", func(t *testing.T) {
			f := goodFact()
			tampered := &statev1.WorkloadSpecChange{Hash: body.GetHash(), Name: "tampered", MinReplicas: 1}
			g := New(rootPub, fakeStore{snap: state.Snapshot{Nodes: nodes(authPub, grant)}})
			err := g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: tampered}})
			require.Error(t, err)
		})

		t.Run("authority grant not in cluster state", func(t *testing.T) {
			g := New(rootPub, fakeStore{snap: state.Snapshot{}})
			err := g.Admit(&statev1.SpecChange{Fact: goodFact(), Body: &statev1.SpecChange_Workload{Workload: body}})
			require.ErrorContains(t, err, "authority grant not in cluster state")
		})

		t.Run("expired authority grant", func(t *testing.T) {
			rp, ap, apriv, expg := authority(t, now.Add(-48*time.Hour), now.Add(-time.Hour), nil)
			f, err := fact.IssueFact(apriv, res, body, nil, 1, false)
			require.NoError(t, err)
			g := New(rp, fakeStore{snap: state.Snapshot{Nodes: nodes(ap, expg)}})
			err = g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}})
			require.ErrorContains(t, err, "expired")
		})

		t.Run("denied authority", func(t *testing.T) {
			snap := state.Snapshot{
				Nodes:      nodes(authPub, grant),
				DeniedKeys: []types.PeerKey{types.PeerKeyFromBytes(authPub)},
			}
			g := New(rootPub, fakeStore{snap: snap})
			err := g.Admit(&statev1.SpecChange{Fact: goodFact(), Body: &statev1.SpecChange_Workload{Workload: body}})
			require.ErrorContains(t, err, "revoked")
		})

		t.Run("public plus inline contradiction", func(t *testing.T) {
			pol := &admissionv1.Predicate{Public: true, Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{{Key: "team", Equals: "core"}}}}
			f, err := fact.IssueFact(authPriv, res, body, pol, 1, false)
			require.NoError(t, err)
			g := New(rootPub, fakeStore{snap: state.Snapshot{Nodes: nodes(authPub, grant)}})
			err = g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}})
			require.ErrorContains(t, err, "public=true and inline")
		})
	})
}

// Authorise and account rejections surface through Admit as ErrRejected with
// their reason byte-identical to what the daemon logs.
func TestAdmitWrapsRejectionsAsErrRejected(t *testing.T) {
	now := time.Now()

	t.Run("authorise: missing publish capability", func(t *testing.T) {
		rootPub, authPub, authPriv, grant := grantCaps(t, now, identity.LeafCapabilities())
		body, res := seedBodyResource("echo", "a")
		f, err := fact.IssueFact(authPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		g := New(rootPub, fakeStore{snap: state.Snapshot{Nodes: nodes(authPub, grant)}})
		err = g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}})
		require.ErrorIs(t, err, ErrRejected)
		require.EqualError(t, err, "admission: authority grant lacks publish capability for functions")
	})

	t.Run("account: count budget exhausted", func(t *testing.T) {
		adminPub, adminPriv := newKeyPair(t)
		authPub, authPriv := newKeyPair(t)
		grant, err := identity.IssueGrant(adminPriv, nil, authPub,
			identity.PublisherCapabilities(), &identityv1.Budget{MaxFunctions: 1},
			now.Add(-time.Hour), now.Add(30*24*time.Hour), false)
		require.NoError(t, err)

		snap := state.Snapshot{
			Nodes: nodes(authPub, grant),
			SpecsAll: []state.WorkloadSpecView{
				{Spec: state.WorkloadSpec{Name: "first"}, Publisher: types.PeerKeyFromBytes(authPub)},
			},
		}
		body, res := seedBodyResource("second", "b")
		f, err := fact.IssueFact(authPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		g := New(adminPub, fakeStore{snap: snap})
		err = g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}})
		require.ErrorIs(t, err, ErrRejected)
		require.EqualError(t, err, "admission: functions budget exhausted: authority holds 1, limit 1")
	})
}

func TestDecideFailClosed(t *testing.T) {
	now := time.Now()
	rootPub, _, _, grant := authority(t, now, now.Add(30*24*time.Hour), map[string]any{"team": "core"})
	g := New(rootPub, fakeStore{})
	denied := func([]byte) bool { return false }

	pub := &factv1.Fact{Policy: &admissionv1.Predicate{Public: true}}
	inline := &factv1.Fact{Policy: &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{{Key: "team", Equals: "core"}}}}}
	inlineMiss := &factv1.Fact{Policy: &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{{Key: "team", Equals: "wrong"}}}}}

	require.NoError(t, g.decide(nil, pub, now, denied), "public admits anonymous")
	require.NoError(t, g.decide(grant, pub, now, denied), "public admits grant-bearing")
	require.ErrorIs(t, g.decide(nil, inline, now, denied), wasm.ErrTargetNotFound, "nil grant on gated spec")
	require.NoError(t, g.decide(grant, inline, now, denied), "matching attribute admitted")
	require.ErrorIs(t, g.decide(grant, inlineMiss, now, denied), wasm.ErrTargetNotFound, "attribute mismatch rejected")

	deniedSubject := func(p []byte) bool { return string(p) == string(grant.GetClaims().GetSubjectPub()) }
	require.ErrorIs(t, g.decide(grant, inline, now, deniedSubject), wasm.ErrTargetNotFound, "denied grant rejected")

	_, _, _, expired := authority(t, now.Add(-48*time.Hour), now.Add(-time.Hour), map[string]any{"team": "core"})
	require.ErrorIs(t, g.decide(expired, inline, now, denied), wasm.ErrTargetNotFound, "expired grant rejected")
}

func TestConnectAuthorises(t *testing.T) {
	now := time.Now()
	rootPub, authPub, authPriv, grant := authority(t, now, now.Add(30*24*time.Hour), nil)
	body, res := seedBodyResource("svc", "c")
	svcFact, err := fact.IssueFact(authPriv, res, body, nil, 1, false)
	require.NoError(t, err)

	host := types.PeerKeyFromBytes(authPub)
	const port = 8443
	mkSnap := func(denied []types.PeerKey) state.Snapshot {
		return state.Snapshot{
			Nodes: map[types.PeerKey]state.NodeView{
				host: {Grant: grant, Services: map[string]*state.Service{
					"svc": {Name: "svc", Port: port, Fact: svcFact},
				}},
			},
			DeniedKeys: denied,
		}
	}

	g := New(rootPub, fakeStore{snap: mkSnap(nil)})
	require.NoError(t, g.Connect(grant, host, port), "valid grant reaches the service")
	require.ErrorIs(t, g.Connect(grant, host, 9999), wasm.ErrTargetNotFound, "no service on that port")

	gd := New(rootPub, fakeStore{snap: mkSnap([]types.PeerKey{types.PeerKeyFromBytes(authPub)})})
	require.ErrorIs(t, gd.Connect(grant, host, port), wasm.ErrTargetNotFound, "denied caller refused")
}

func TestRuntimeMethodsFailClosed(t *testing.T) {
	now := time.Now()
	rootPub, authPub, authPriv, grant := authority(t, now, now.Add(30*24*time.Hour), nil)

	body, res := seedBodyResource("echo", "a")
	gatedFact, err := fact.IssueFact(authPriv, res, body, &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{{Key: "team", Equals: "core"}}}}, 1, false)
	require.NoError(t, err)
	publicBody, publicRes := seedBodyResource("open", "b")
	publicFact, err := fact.IssueFact(authPriv, publicRes, publicBody, &admissionv1.Predicate{Public: true}, 1, false)
	require.NoError(t, err)

	authPK := types.PeerKeyFromBytes(authPub)
	views := []state.WorkloadSpecView{
		{Fact: gatedFact, Spec: state.WorkloadSpec{Name: "echo", Hash: body.GetHash()}, Publisher: authPK},
		{Fact: publicFact, Spec: state.WorkloadSpec{Name: "open", Hash: publicBody.GetHash()}, Publisher: authPK},
	}
	// Mirror buildSnapshot: the deduped runtime view and the
	// per-(authority, name) publication view are both populated and
	// consistent. Invoke reads the publication view; Fetch reads the
	// deduped one.
	snap := state.Snapshot{
		Nodes: nodes(authPub, grant),
		Specs: map[string]state.WorkloadSpecView{
			body.GetHash():       views[0],
			publicBody.GetHash(): views[1],
		},
		SpecsAll: views,
	}
	g := New(rootPub, fakeStore{snap: snap})

	t.Run("Invoke unknown target", func(t *testing.T) {
		_, err := g.Invoke(grant, nil, "deadbeef")
		require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	})
	t.Run("Invoke gated spec with nil caller", func(t *testing.T) {
		_, err := g.Invoke(nil, nil, body.GetHash())
		require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	})
	t.Run("Invoke public spec with nil caller", func(t *testing.T) {
		_, err := g.Invoke(nil, nil, publicBody.GetHash())
		require.NoError(t, err)
	})
	t.Run("Fetch with no entitlement", func(t *testing.T) {
		require.ErrorIs(t, g.Fetch(grant, "unreferenced"), wasm.ErrTargetNotFound)
	})
	t.Run("Fetch public blob with nil caller", func(t *testing.T) {
		require.NoError(t, g.Fetch(nil, publicBody.GetHash()))
	})
	t.Run("Connect unknown service", func(t *testing.T) {
		require.ErrorIs(t, g.Connect(grant, types.PeerKeyFromBytes(authPub), 9999), wasm.ErrTargetNotFound)
	})
	t.Run("MayHost nil grant or fact", func(t *testing.T) {
		require.ErrorIs(t, g.MayHost(nil, gatedFact), wasm.ErrTargetNotFound)
		require.ErrorIs(t, g.MayHost(grant, nil), wasm.ErrTargetNotFound)
	})
	t.Run("MayHost public spec", func(t *testing.T) {
		require.NoError(t, g.MayHost(grant, publicFact))
	})
	t.Run("MayPublish", func(t *testing.T) {
		require.NoError(t, g.MayPublish(grant, nil), "nil policy always permitted")
		require.NoError(t, g.MayPublish(grant, &admissionv1.Predicate{Public: true}), "valid grant may publish a public spec")
		require.Error(t, g.MayPublish(nil, &admissionv1.Predicate{Public: true}), "nil grant with policy rejected")
		_, _, _, expired := authority(t, now.Add(-48*time.Hour), now.Add(-time.Hour), nil)
		require.Error(t, g.MayPublish(expired, &admissionv1.Predicate{Public: true}))
	})
	t.Run("AllowAnonymous", func(t *testing.T) {
		require.NoError(t, g.AllowAnonymous(publicFact))
		require.ErrorIs(t, g.AllowAnonymous(gatedFact), wasm.ErrTargetNotFound)
	})
}

// Two authorities publish byte-identical workloads under different names and
// policies; the deduped artefact winner is the gated one. Invoke and
// MayHostByHash must decide against the addressed publication or the union,
// never the deduped winner.
func TestInvokeHostPublicationScopedMultiPublisher(t *testing.T) {
	now := time.Now()
	rootA, aPub, aPriv, aGrant := authority(t, now, now.Add(30*24*time.Hour), nil)
	_, bPub, bPriv, bGrant := authority(t, now, now.Add(30*24*time.Hour), nil)

	// Identical bytes (same hex byte → same 64-char hash) under two
	// distinct publications: A's is public, B's is gated.
	bodyA, resA := seedBodyResource("echo", "a")
	publicFact, err := fact.IssueFact(aPriv, resA, bodyA, &admissionv1.Predicate{Public: true}, 1, false)
	require.NoError(t, err)
	bodyB, resB := seedBodyResource("secret", "a")
	gatedFact, err := fact.IssueFact(bPriv, resB, bodyB,
		&admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{{Key: "team", Equals: "core"}}}}, 1, false)
	require.NoError(t, err)

	hash := bodyA.GetHash()
	require.Equal(t, hash, bodyB.GetHash(), "fixture must publish identical bytes")
	aPK, bPK := types.PeerKeyFromBytes(aPub), types.PeerKeyFromBytes(bPub)

	snap := state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{aPK: {Grant: aGrant}, bPK: {Grant: bGrant}},
		// Deduped winner is the GATED publication: the shape that denied a
		// legitimately-public publication on the live cluster.
		Specs: map[string]state.WorkloadSpecView{
			hash: {Fact: gatedFact, Spec: state.WorkloadSpec{Name: "secret", Hash: hash}, Publisher: bPK},
		},
		SpecsAll: []state.WorkloadSpecView{
			{Fact: publicFact, Spec: state.WorkloadSpec{Name: "echo", Hash: hash}, Publisher: aPK},
			{Fact: gatedFact, Spec: state.WorkloadSpec{Name: "secret", Hash: hash}, Publisher: bPK},
		},
	}
	g := New(rootA, fakeStore{snap: snap})

	t.Run("addressed public publication reachable despite gated dedupe winner", func(t *testing.T) {
		_, err := g.Invoke(nil, &Publication{AuthorityPub: aPub, Name: "echo"}, hash)
		require.NoError(t, err)
	})
	t.Run("addressed gated publication denied (no leak via identical public bytes)", func(t *testing.T) {
		_, err := g.Invoke(nil, &Publication{AuthorityPub: bPub, Name: "secret"}, hash)
		require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	})
	t.Run("selector bound to dispatched hash (forged selector cannot borrow a policy)", func(t *testing.T) {
		_, err := g.Invoke(nil, &Publication{AuthorityPub: aPub, Name: "echo"}, strings.Repeat("f", 64))
		require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	})
	t.Run("unknown name under a real authority denied", func(t *testing.T) {
		_, err := g.Invoke(nil, &Publication{AuthorityPub: aPub, Name: "nope"}, hash)
		require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	})
	t.Run("bare-hash union admits via the public co-publication", func(t *testing.T) {
		_, err := g.Invoke(nil, nil, hash)
		require.NoError(t, err)
	})
	t.Run("MayHostByHash union admits because a co-publication is public", func(t *testing.T) {
		require.NoError(t, g.MayHostByHash(bGrant, hash))
	})
	t.Run("MayHostByHash nil grant fails closed", func(t *testing.T) {
		require.ErrorIs(t, g.MayHostByHash(nil, hash), wasm.ErrTargetNotFound)
	})
	t.Run("MayHostByHash denies when every co-publication is gated and unmet", func(t *testing.T) {
		gatedOnly := state.Snapshot{
			Nodes:    map[types.PeerKey]state.NodeView{bPK: {Grant: bGrant}},
			SpecsAll: []state.WorkloadSpecView{{Fact: gatedFact, Spec: state.WorkloadSpec{Name: "secret", Hash: hash}, Publisher: bPK}},
		}
		require.ErrorIs(t, New(rootA, fakeStore{snap: gatedOnly}).MayHostByHash(bGrant, hash), wasm.ErrTargetNotFound)
	})
}

// A publisher's access token must authorise invoking its own workload even
// when the publication is gated, mirroring FetchByToken. Routing through
// decide(nil, ...) wrongly demanded public=true.
func TestInvokeByTokenAuthorisesGatedWorkload(t *testing.T) {
	now := time.Now()
	adminPub, adminPriv := newKeyPair(t)
	bPub, bPriv, bGrant := rootedAuthority(t, adminPriv, now, now.Add(30*24*time.Hour))

	body, res := seedBodyResource("secret", "a")
	gatedFact, err := fact.IssueFact(bPriv, res, body,
		&admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{{Key: "team", Equals: "core"}}}}, 1, false)
	require.NoError(t, err)
	hash := body.GetHash()
	bPK := types.PeerKeyFromBytes(bPub)

	g := New(adminPub, fakeStore{snap: state.Snapshot{
		Nodes:    map[types.PeerKey]state.NodeView{bPK: {Grant: bGrant}},
		SpecsAll: []state.WorkloadSpecView{{Fact: gatedFact, Spec: state.WorkloadSpec{Name: "secret", Hash: hash}, Publisher: bPK}},
	}})

	mintToken := func(priv ed25519.PrivateKey, r *admissionv1.ResourceID) *admissionv1.AccessToken {
		tok, err := auth.SignAccessToken(priv, r, now, time.Hour)
		require.NoError(t, err)
		return tok
	}

	t.Run("publisher's token authorises invoking its own gated workload", func(t *testing.T) {
		_, err := g.InvokeByToken(mintToken(bPriv, res), hash)
		require.NoError(t, err)
	})
	t.Run("token bound to its hash: a forged hash cannot borrow the entitlement", func(t *testing.T) {
		_, err := g.InvokeByToken(mintToken(bPriv, res), strings.Repeat("f", 64))
		require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	})
	t.Run("wrong-issuer token fails closed: only the publication authority's token admits", func(t *testing.T) {
		_, otherPriv := newKeyPair(t)
		_, err := g.InvokeByToken(mintToken(otherPriv, res), hash)
		require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	})
}

// Blob analogue of TestInvokeHostPublicationScopedMultiPublisher. Fetch
// (bare-hash union) and FetchByToken (exact issuer+resource) must decide
// over every co-publication, never an arbitrary deduped-map winner. The bug
// bites under opposite winners, so each path is locked against its own
// adversarial winner.
func TestFetchBlobPublicationScopedMultiPublisher(t *testing.T) {
	now := time.Now()
	adminPub, adminPriv := newKeyPair(t)
	aPub, aPriv, aGrant := rootedAuthority(t, adminPriv, now, now.Add(30*24*time.Hour))
	bPub, bPriv, bGrant := rootedAuthority(t, adminPriv, now, now.Add(30*24*time.Hour))

	bodyA, resA := blobBodyResource("pubdata", "a")
	publicFact, err := fact.IssueFact(aPriv, resA, bodyA, &admissionv1.Predicate{Public: true}, 1, false)
	require.NoError(t, err)
	bodyB, resB := blobBodyResource("secret", "a")
	gatedFact, err := fact.IssueFact(bPriv, resB, bodyB,
		&admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{{Key: "team", Equals: "core"}}}}, 1, false)
	require.NoError(t, err)

	hash := strings.Repeat("a", 64)
	aPK, bPK := types.PeerKeyFromBytes(aPub), types.PeerKeyFromBytes(bPub)
	bAll := []state.BlobSpecView{
		{Fact: publicFact, Spec: state.BlobSpec{Name: "pubdata", Digest: hash}, Publisher: aPK},
		{Fact: gatedFact, Spec: state.BlobSpec{Name: "secret", Digest: hash}, Publisher: bPK},
	}
	meshNodes := map[types.PeerKey]state.NodeView{aPK: {Grant: aGrant}, bPK: {Grant: bGrant}}

	// Deduped winner is the GATED publication. Fetch must still resolve the
	// public co-publication, else an anonymous read of bytes a tenant
	// published publicly is denied.
	gatedWinner := New(adminPub, fakeStore{snap: state.Snapshot{
		Nodes:        meshNodes,
		BlobSpecs:    map[string]state.BlobSpecView{hash: {Fact: gatedFact, Spec: state.BlobSpec{Name: "secret", Digest: hash}, Publisher: bPK}},
		BlobSpecsAll: bAll,
	}})
	// Deduped winner is the PUBLIC publication: when Fetch drops the
	// gated co-publication, a legitimate share-link holder for a gated
	// blob would be denied because someone else's identical bytes
	// happened to be public.
	publicWinner := New(adminPub, fakeStore{snap: state.Snapshot{
		Nodes:        meshNodes,
		BlobSpecs:    map[string]state.BlobSpecView{hash: {Fact: publicFact, Spec: state.BlobSpec{Name: "pubdata", Digest: hash}, Publisher: aPK}},
		BlobSpecsAll: bAll,
	}})

	mintToken := func(priv ed25519.PrivateKey, res *admissionv1.ResourceID) *admissionv1.AccessToken {
		tok, err := auth.SignAccessToken(priv, res, now, time.Hour)
		require.NoError(t, err)
		return tok
	}

	t.Run("anonymous fetch admitted via the public co-publication despite gated dedupe winner", func(t *testing.T) {
		require.NoError(t, gatedWinner.Fetch(nil, hash))
	})
	t.Run("gated-blob token holder authorised despite public dedupe winner", func(t *testing.T) {
		require.NoError(t, publicWinner.FetchByToken(mintToken(bPriv, resB), hash))
	})
	t.Run("public-blob token holder authorised despite gated dedupe winner", func(t *testing.T) {
		require.NoError(t, gatedWinner.FetchByToken(mintToken(aPriv, resA), hash))
	})
	t.Run("token bound to its hash: a forged hash cannot borrow the entitlement", func(t *testing.T) {
		require.ErrorIs(t, publicWinner.FetchByToken(mintToken(bPriv, resB), strings.Repeat("f", 64)), wasm.ErrTargetNotFound)
	})
	t.Run("token resource mismatch fails closed: un-dedup does not widen token auth", func(t *testing.T) {
		_, otherRes := blobBodyResource("elsewhere", "a")
		require.ErrorIs(t, publicWinner.FetchByToken(mintToken(bPriv, otherRes), hash), wasm.ErrTargetNotFound)
	})
	t.Run("anonymous fetch denied when every co-publication is gated and unmet", func(t *testing.T) {
		gatedOnly := New(adminPub, fakeStore{snap: state.Snapshot{
			Nodes:        map[types.PeerKey]state.NodeView{bPK: {Grant: bGrant}},
			BlobSpecsAll: []state.BlobSpecView{{Fact: gatedFact, Spec: state.BlobSpec{Name: "secret", Digest: hash}, Publisher: bPK}},
		}})
		require.ErrorIs(t, gatedOnly.Fetch(nil, hash), wasm.ErrTargetNotFound)
	})
}

// The token gate (issuerGrantValid) must refuse a token once the issuer's
// Grant has expired or been denied, not merely when the token's own TTL
// lapses.
func TestTokenBindsToIssuerGrantHorizon(t *testing.T) {
	now := time.Now()
	body, res := seedBodyResource("secret", "a")
	hash := body.GetHash()

	mintToken := func(priv ed25519.PrivateKey) *admissionv1.AccessToken {
		tok, err := auth.SignAccessToken(priv, res, now, time.Hour) // token itself stays valid
		require.NoError(t, err)
		return tok
	}
	specView := func(priv ed25519.PrivateKey) state.WorkloadSpecView {
		f, err := fact.IssueFact(priv, res, body,
			&admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{{Key: "team", Equals: "core"}}}}, 1, false)
		require.NoError(t, err)
		return state.WorkloadSpecView{Fact: f, Spec: state.WorkloadSpec{Name: "secret", Hash: hash}, Publisher: types.PeerKeyFromBytes(priv.Public().(ed25519.PublicKey))}
	}

	t.Run("denied when the issuer grant has expired", func(t *testing.T) {
		adminPub, adminPriv := newKeyPair(t)
		bPub, bPriv, bGrant := rootedAuthority(t, adminPriv, now, now.Add(-time.Minute))
		bPK := types.PeerKeyFromBytes(bPub)
		g := New(adminPub, fakeStore{snap: state.Snapshot{
			Nodes:    map[types.PeerKey]state.NodeView{bPK: {Grant: bGrant}},
			SpecsAll: []state.WorkloadSpecView{specView(bPriv)},
		}})
		_, err := g.InvokeByToken(mintToken(bPriv), hash)
		require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	})

	t.Run("denied when the issuer is on the denylist", func(t *testing.T) {
		adminPub, adminPriv := newKeyPair(t)
		bPub, bPriv, bGrant := rootedAuthority(t, adminPriv, now, now.Add(30*24*time.Hour))
		bPK := types.PeerKeyFromBytes(bPub)
		g := New(adminPub, fakeStore{snap: state.Snapshot{
			Nodes:      map[types.PeerKey]state.NodeView{bPK: {Grant: bGrant}},
			DeniedKeys: []types.PeerKey{bPK},
			SpecsAll:   []state.WorkloadSpecView{specView(bPriv)},
		}})
		_, err := g.InvokeByToken(mintToken(bPriv), hash)
		require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	})

	t.Run("admitted while the issuer grant is live", func(t *testing.T) {
		adminPub, adminPriv := newKeyPair(t)
		bPub, bPriv, bGrant := rootedAuthority(t, adminPriv, now, now.Add(30*24*time.Hour))
		bPK := types.PeerKeyFromBytes(bPub)
		g := New(adminPub, fakeStore{snap: state.Snapshot{
			Nodes:    map[types.PeerKey]state.NodeView{bPK: {Grant: bGrant}},
			SpecsAll: []state.WorkloadSpecView{specView(bPriv)},
		}})
		_, err := g.InvokeByToken(mintToken(bPriv), hash)
		require.NoError(t, err)
	})
}
