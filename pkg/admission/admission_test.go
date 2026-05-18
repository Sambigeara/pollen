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
	grant, err := identity.IssueGrant(adminPriv, nil, authPub, caps, identity.UnlimitedBudget(), now.Add(-time.Hour), deadline)
	require.NoError(t, err)
	return adminPub, authPub, authPriv, grant
}

func seedBodyResource(name, hexByte string) (*statev1.WorkloadSpecChange, *admissionv1.ResourceID) {
	body := &statev1.WorkloadSpecChange{Hash: strings.Repeat(hexByte, 64), Name: name, MinReplicas: 1}
	hb, _ := hex.DecodeString(body.GetHash())
	return body, &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: name, Hash: hb}}}
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

// TestAdmitWrapsRejectionsAsErrRejected proves the authorise and
// account verdicts surface through Admit as ErrRejected with their
// reason verbatim (the control layer maps that to FailedPrecondition),
// while the message stays byte-identical to what the daemon logs. The
// expected strings are the exact text the operator-facing docs quote.
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
			now.Add(-time.Hour), now.Add(30*24*time.Hour))
		require.NoError(t, err)

		snap := state.Snapshot{
			Nodes: nodes(authPub, grant),
			Specs: map[string]state.WorkloadSpecView{
				"deadbeef": {Spec: state.WorkloadSpec{Name: "first"}, Publisher: types.PeerKeyFromBytes(authPub)},
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

	snap := state.Snapshot{
		Nodes: nodes(authPub, grant),
		Specs: map[string]state.WorkloadSpecView{
			body.GetHash():       {Fact: gatedFact, Spec: state.WorkloadSpec{Name: "echo"}},
			publicBody.GetHash(): {Fact: publicFact, Spec: state.WorkloadSpec{Name: "open"}},
		},
	}
	g := New(rootPub, fakeStore{snap: snap})

	t.Run("Invoke unknown target", func(t *testing.T) {
		_, err := g.Invoke(grant, "deadbeef")
		require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	})
	t.Run("Invoke gated spec with nil caller", func(t *testing.T) {
		_, err := g.Invoke(nil, body.GetHash())
		require.ErrorIs(t, err, wasm.ErrTargetNotFound)
	})
	t.Run("Invoke public spec with nil caller", func(t *testing.T) {
		_, err := g.Invoke(nil, publicBody.GetHash())
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
