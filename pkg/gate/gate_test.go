// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package gate

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
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

func testCert(t *testing.T, priv ed25519.PrivateKey, pub ed25519.PublicKey, attrs map[string]any, now time.Time) *admissionv1.DelegationCert {
	t.Helper()
	var sattrs *structpb.Struct
	if attrs != nil {
		var err error
		sattrs, err = structpb.NewStruct(attrs)
		require.NoError(t, err)
	}
	caps := auth.FullCapabilities()
	caps.Attributes = sattrs
	cert, err := auth.IssueDelegationCert(priv, nil, pub, caps, now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	require.NoError(t, err)
	return cert
}

func TestAdmitVerifiesSpecAuthBody(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)
	publisher := testCert(t, rootPriv, rootPub, nil, now)
	body := &statev1.WorkloadSpecChange{Hash: strings.Repeat("a", 64), Name: "echo", MinReplicas: 1}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: "echo", Hash: bytesOf(0xaa)}}}
	specAuth, err := auth.IssueSpecAuth(rootPriv, publisher, resource, body, nil, false)
	require.NoError(t, err)
	sc := &statev1.SpecChange{Auth: specAuth, Body: &statev1.SpecChange_Workload{Workload: body}}

	g := New(rootPub, fakeStore{})
	require.NoError(t, g.Admit(sc))

	sc.GetWorkload().Name = "tampered"
	require.Error(t, g.Admit(sc))
}

func TestInvokeUsesCallerCertAttributes(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)
	callerPub, callerPriv := newKeyPair(t)
	callerCert := testCert(t, callerPriv, callerPub, map[string]any{"role": "worker"}, now)
	publisher := testCert(t, rootPriv, rootPub, nil, now)
	body := &statev1.WorkloadSpecChange{Hash: strings.Repeat("b", 64), Name: "echo", MinReplicas: 1}
	policy := &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{
		{Key: "role", Equals: "worker"},
	}}}
	specAuth, err := auth.IssueSpecAuth(rootPriv, publisher, &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: "echo", Hash: bytesOf(0xbb)}}}, body, policy, false)
	require.NoError(t, err)
	callerKey := types.PeerKeyFromBytes(callerPub)
	snap := state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{callerKey: {Cert: callerCert}},
		Specs: map[string]state.WorkloadSpecView{body.GetHash(): {Spec: state.WorkloadSpec{Hash: body.GetHash(), Name: body.GetName()}, Auth: specAuth}},
	}

	info, err := New(rootPub, fakeStore{snap: snap}).Invoke(callerKey, body.GetHash())
	require.NoError(t, err)
	require.Equal(t, "worker", info.Attributes["role"])
}

func TestInvokeDeniesPolicyMismatch(t *testing.T) {
	now := time.Now()
	g, callerKey, hash := newWorkloadFixture(t, now,
		map[string]any{"role": "operator"},
		clauseEquals("role", "worker"),
	)
	_, err := g.Invoke(callerKey, hash)
	require.ErrorIs(t, err, wasm.ErrTargetNotFound)
}

func TestInvokeAllowsOpenPolicy(t *testing.T) {
	now := time.Now()
	g, callerKey, hash := newWorkloadFixture(t, now, nil, nil)
	info, err := g.Invoke(callerKey, hash)
	require.NoError(t, err)
	require.NotNil(t, info)
}

func TestInvokeDeniesExpiredCallerCert(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)
	callerPub, callerPriv := newKeyPair(t)
	publisher := testCert(t, rootPriv, rootPub, nil, now)
	expired, err := auth.IssueDelegationCert(callerPriv, nil, callerPub, auth.FullCapabilities(),
		now.Add(-2*time.Hour), now.Add(-time.Hour), time.Time{})
	require.NoError(t, err)
	body := &statev1.WorkloadSpecChange{Hash: strings.Repeat("e", 64), Name: "echo", MinReplicas: 1}
	specAuth, err := auth.IssueSpecAuth(rootPriv, publisher,
		&admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: body.GetName(), Hash: bytesOf(0xee)}}},
		body, nil, false)
	require.NoError(t, err)
	callerKey := types.PeerKeyFromBytes(callerPub)
	snap := state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{callerKey: {Cert: expired}},
		Specs: map[string]state.WorkloadSpecView{body.GetHash(): {Spec: state.WorkloadSpec{Hash: body.GetHash(), Name: body.GetName()}, Auth: specAuth}},
	}
	g := New(rootPub, fakeStore{snap: snap})
	_, err = g.Invoke(callerKey, body.GetHash())
	require.ErrorIs(t, err, wasm.ErrTargetNotFound)
}

func TestInvokeRejectsPolicyWithoutInline(t *testing.T) {
	now := time.Now()
	g, callerKey, hash := newWorkloadFixture(t, now, nil, &admissionv1.Predicate{})
	_, err := g.Invoke(callerKey, hash)
	require.ErrorIs(t, err, wasm.ErrTargetNotFound, "non-Inline Predicate must fail closed even though Inline is currently the only variant")
}

func TestFetchHonoursPolicy(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)
	callerPub, callerPriv := newKeyPair(t)
	publisher := testCert(t, rootPriv, rootPub, nil, now)
	allow := testCert(t, callerPriv, callerPub, map[string]any{"role": "reader"}, now)
	deny := testCert(t, callerPriv, callerPub, map[string]any{"role": "intern"}, now)

	body := &statev1.BlobSpecChange{Name: "manifest", Digest: bytesOf(0xcc)}
	specAuth, err := auth.IssueSpecAuth(rootPriv, publisher,
		&admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{Name: body.GetName(), Digest: body.GetDigest()}}},
		body, clauseEquals("role", "reader"), false)
	require.NoError(t, err)

	digest := bytesAsHex(body.GetDigest())
	callerKey := types.PeerKeyFromBytes(callerPub)
	snap := state.Snapshot{
		Nodes:     map[types.PeerKey]state.NodeView{callerKey: {Cert: allow}},
		BlobSpecs: map[string]state.BlobSpecView{digest: {Spec: state.BlobSpec{Name: body.GetName(), Digest: digest}, Auth: specAuth}},
	}
	g := New(rootPub, fakeStore{snap: snap})
	require.NoError(t, g.Fetch(callerKey, digest))

	snap.Nodes[callerKey] = state.NodeView{Cert: deny}
	g.store = fakeStore{snap: snap}
	require.ErrorIs(t, g.Fetch(callerKey, digest), wasm.ErrTargetNotFound)
}

func TestFetchAuthorisesWorkloadBinary(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)
	callerPub, callerPriv := newKeyPair(t)
	publisher := testCert(t, rootPriv, rootPub, nil, now)
	allow := testCert(t, callerPriv, callerPub, map[string]any{"role": "admin"}, now)
	deny := testCert(t, callerPriv, callerPub, map[string]any{"role": "intern"}, now)

	body := &statev1.WorkloadSpecChange{Name: "echo", Hash: bytesAsHex(bytesOf(0xda))}
	specAuth, err := auth.IssueSpecAuth(rootPriv, publisher,
		&admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: body.GetName(), Hash: bytesOf(0xda)}}},
		body, clauseEquals("role", "admin"), false)
	require.NoError(t, err)

	callerKey := types.PeerKeyFromBytes(callerPub)
	snap := state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{callerKey: {Cert: allow}},
		Specs: map[string]state.WorkloadSpecView{body.GetHash(): {Spec: state.WorkloadSpec{Hash: body.GetHash(), Name: body.GetName()}, Auth: specAuth}},
	}
	g := New(rootPub, fakeStore{snap: snap})
	require.NoError(t, g.Fetch(callerKey, body.GetHash()), "caller with admin role must fetch workload binary")

	snap.Nodes[callerKey] = state.NodeView{Cert: deny}
	g.store = fakeStore{snap: snap}
	require.ErrorIs(t, g.Fetch(callerKey, body.GetHash()), wasm.ErrTargetNotFound, "caller without admin role must be denied")
}

func TestConnectHonoursPolicy(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)
	callerPub, callerPriv := newKeyPair(t)
	localPub, _ := newKeyPair(t)
	publisher := testCert(t, rootPriv, rootPub, nil, now)
	caller := testCert(t, callerPriv, callerPub, map[string]any{"team": "blue"}, now)

	body := &statev1.ServiceChange{Name: "api", Port: 8080}
	specAuth, err := auth.IssueSpecAuth(rootPriv, publisher,
		&admissionv1.ResourceID{Body: &admissionv1.ResourceID_Service{Service: &admissionv1.ServiceID{Name: body.GetName()}}},
		body, clauseEquals("team", "blue"), false)
	require.NoError(t, err)

	callerKey := types.PeerKeyFromBytes(callerPub)
	localKey := types.PeerKeyFromBytes(localPub)
	svc := &state.Service{Name: "api", Port: 8080, Auth: specAuth}
	snap := state.Snapshot{
		LocalID: localKey,
		Nodes: map[types.PeerKey]state.NodeView{
			callerKey: {Cert: caller},
			localKey:  {Services: map[string]*state.Service{"api": svc}},
		},
	}
	g := New(rootPub, fakeStore{snap: snap})
	require.NoError(t, g.Connect(callerKey, localKey, 8080))

	caller2 := testCert(t, callerPriv, callerPub, map[string]any{"team": "red"}, now)
	snap.Nodes[callerKey] = state.NodeView{Cert: caller2}
	g.store = fakeStore{snap: snap}
	require.ErrorIs(t, g.Connect(callerKey, localKey, 8080), wasm.ErrTargetNotFound)
}

func TestConnectAuthorsesAgainstChosenHost(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)
	callerPub, callerPriv := newKeyPair(t)
	publisher := testCert(t, rootPriv, rootPub, nil, now)
	caller := testCert(t, callerPriv, callerPub, map[string]any{"team": "blue"}, now)

	strict, err := auth.IssueSpecAuth(rootPriv, publisher,
		&admissionv1.ResourceID{Body: &admissionv1.ResourceID_Service{Service: &admissionv1.ServiceID{Name: "api"}}},
		&statev1.ServiceChange{Name: "api", Port: 8080}, clauseEquals("team", "red"), false)
	require.NoError(t, err)
	loose, err := auth.IssueSpecAuth(rootPriv, publisher,
		&admissionv1.ResourceID{Body: &admissionv1.ResourceID_Service{Service: &admissionv1.ServiceID{Name: "api"}}},
		&statev1.ServiceChange{Name: "api", Port: 8081}, clauseEquals("team", "blue"), false)
	require.NoError(t, err)

	callerKey := types.PeerKeyFromBytes(callerPub)
	strictHostKey, _ := newKeyPair(t)
	looseHostKey, _ := newKeyPair(t)
	strictKey := types.PeerKeyFromBytes(strictHostKey)
	looseKey := types.PeerKeyFromBytes(looseHostKey)

	snap := state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{
			callerKey: {Cert: caller},
			strictKey: {Services: map[string]*state.Service{"api": {Name: "api", Port: 8080, Auth: strict}}},
			looseKey:  {Services: map[string]*state.Service{"api": {Name: "api", Port: 8081, Auth: loose}}},
		},
	}
	g := New(rootPub, fakeStore{snap: snap})
	require.ErrorIs(t, g.Connect(callerKey, strictKey, 8080), wasm.ErrTargetNotFound, "strict host's red-only policy must reject blue caller")
	require.NoError(t, g.Connect(callerKey, looseKey, 8081), "loose host's blue policy must accept blue caller")
}

func TestMayHostAcceptsMatchingHost(t *testing.T) {
	now := time.Now()
	g, hostCert, specAuth := newHostFixture(t, now,
		map[string]any{"team": "blue"},
		clauseEquals("team", "blue"),
	)
	require.NoError(t, g.MayHost(hostCert, specAuth))
}

func TestMayHostRejectsMismatchedHost(t *testing.T) {
	now := time.Now()
	g, hostCert, specAuth := newHostFixture(t, now,
		map[string]any{"team": "red"},
		clauseEquals("team", "blue"),
	)
	require.ErrorIs(t, g.MayHost(hostCert, specAuth), wasm.ErrTargetNotFound)
}

func TestMayHostAllowsOpenPolicy(t *testing.T) {
	now := time.Now()
	g, hostCert, specAuth := newHostFixture(t, now, nil, nil)
	require.NoError(t, g.MayHost(hostCert, specAuth))
}

func TestMayHostRejectsExpiredHost(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)
	hostPub, hostPriv := newKeyPair(t)
	publisher := testCert(t, rootPriv, rootPub, nil, now)
	expired, err := auth.IssueDelegationCert(hostPriv, nil, hostPub, auth.FullCapabilities(),
		now.Add(-2*time.Hour), now.Add(-time.Hour), time.Time{})
	require.NoError(t, err)
	body := &statev1.WorkloadSpecChange{Hash: strings.Repeat("9", 64), Name: "echo", MinReplicas: 1}
	specAuth, err := auth.IssueSpecAuth(rootPriv, publisher,
		&admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: body.GetName(), Hash: bytesOf(0x99)}}},
		body, nil, false)
	require.NoError(t, err)
	g := New(rootPub, fakeStore{})
	require.ErrorIs(t, g.MayHost(expired, specAuth), wasm.ErrTargetNotFound)
}

func TestMayHostRejectsNilInputs(t *testing.T) {
	g := New(nil, fakeStore{})
	require.ErrorIs(t, g.MayHost(nil, &admissionv1.SpecAuth{}), wasm.ErrTargetNotFound)
	require.ErrorIs(t, g.MayHost(&admissionv1.DelegationCert{}, nil), wasm.ErrTargetNotFound)
}

func TestMayPublishAcceptsMatchingCert(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)
	cert := testCert(t, rootPriv, rootPub, map[string]any{"role": "admin"}, now)
	policy := &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{
		{Key: "role", Equals: "admin"},
	}}}
	require.NoError(t, New(rootPub, fakeStore{}).MayPublish(cert, policy))
}

func TestMayPublishAcceptsNoPolicy(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)
	cert := testCert(t, rootPriv, rootPub, nil, now)
	require.NoError(t, New(rootPub, fakeStore{}).MayPublish(cert, nil))
}

func TestMayPublishReportsMissingProp(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)
	cert := testCert(t, rootPriv, rootPub, map[string]any{"team": "blue"}, now)
	policy := &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{
		{Key: "role", Equals: "admin"},
	}}}
	err := New(rootPub, fakeStore{}).MayPublish(cert, policy)
	require.Error(t, err)
	require.Contains(t, err.Error(), `missing prop "role"`)
	require.Contains(t, err.Error(), `"admin"`)
}

func TestMayPublishReportsWrongValue(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKeyPair(t)
	cert := testCert(t, rootPriv, rootPub, map[string]any{"role": "worker"}, now)
	policy := &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{
		{Key: "role", Equals: "admin"},
	}}}
	err := New(rootPub, fakeStore{}).MayPublish(cert, policy)
	require.Error(t, err)
	require.Contains(t, err.Error(), `"worker"`)
	require.Contains(t, err.Error(), `"admin"`)
}

func TestMayPublishRejectsExpiredCert(t *testing.T) {
	now := time.Now()
	pub, priv := newKeyPair(t)
	expired, err := auth.IssueDelegationCert(priv, nil, pub, auth.FullCapabilities(),
		now.Add(-2*time.Hour), now.Add(-time.Hour), time.Time{})
	require.NoError(t, err)
	policy := &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{
		{Key: "role", Equals: "admin"},
	}}}
	require.ErrorContains(t, New(nil, fakeStore{}).MayPublish(expired, policy), "expired")
}

func TestMayPublishAllowsNilCertWithoutPolicy(t *testing.T) {
	// Bootstrap window: cert isn't in gossip yet, but a no-policy
	// publish must still succeed so the user can ship work without
	// waiting for the cert to round-trip.
	require.NoError(t, New(nil, fakeStore{}).MayPublish(nil, nil))
}

func TestMayPublishRejectsNilCertWithPolicy(t *testing.T) {
	policy := &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{
		{Key: "role", Equals: "admin"},
	}}}
	require.ErrorContains(t, New(nil, fakeStore{}).MayPublish(nil, policy), "not yet published")
}

func newHostFixture(t *testing.T, now time.Time, hostAttrs map[string]any, policy *admissionv1.Predicate) (*Gate, *admissionv1.DelegationCert, *admissionv1.SpecAuth) {
	t.Helper()
	rootPub, rootPriv := newKeyPair(t)
	hostPub, hostPriv := newKeyPair(t)
	hostCert := testCert(t, hostPriv, hostPub, hostAttrs, now)
	publisher := testCert(t, rootPriv, rootPub, nil, now)
	body := &statev1.WorkloadSpecChange{Hash: strings.Repeat("c", 64), Name: "echo", MinReplicas: 1}
	specAuth, err := auth.IssueSpecAuth(rootPriv, publisher,
		&admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: body.GetName(), Hash: bytesOf(0xcc)}}},
		body, policy, false)
	require.NoError(t, err)
	return New(rootPub, fakeStore{}), hostCert, specAuth
}

func bytesOf(v byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = v
	}
	return out
}

func bytesAsHex(b []byte) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = hexdigits[c>>4]
		out[i*2+1] = hexdigits[c&0x0f]
	}
	return string(out)
}

func clauseEquals(key, value string) *admissionv1.Predicate {
	return &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: []*admissionv1.Clause{
		{Key: key, Equals: value},
	}}}
}

func newWorkloadFixture(t *testing.T, now time.Time, callerAttrs map[string]any, policy *admissionv1.Predicate) (*Gate, types.PeerKey, string) {
	t.Helper()
	rootPub, rootPriv := newKeyPair(t)
	callerPub, callerPriv := newKeyPair(t)
	callerCert := testCert(t, callerPriv, callerPub, callerAttrs, now)
	publisher := testCert(t, rootPriv, rootPub, nil, now)
	body := &statev1.WorkloadSpecChange{Hash: strings.Repeat("d", 64), Name: "echo", MinReplicas: 1}
	specAuth, err := auth.IssueSpecAuth(rootPriv, publisher,
		&admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: body.GetName(), Hash: bytesOf(0xdd)}}},
		body, policy, false)
	require.NoError(t, err)
	callerKey := types.PeerKeyFromBytes(callerPub)
	snap := state.Snapshot{
		Nodes: map[types.PeerKey]state.NodeView{callerKey: {Cert: callerCert}},
		Specs: map[string]state.WorkloadSpecView{body.GetHash(): {Spec: state.WorkloadSpec{Hash: body.GetHash(), Name: body.GetName()}, Auth: specAuth}},
	}
	g := New(rootPub, fakeStore{snap: snap})
	return g, callerKey, body.GetHash()
}
