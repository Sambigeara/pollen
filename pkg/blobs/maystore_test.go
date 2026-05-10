// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type fakeState struct {
	mu   sync.Mutex
	snap state.Snapshot
}

func (f *fakeState) Snapshot() state.Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap
}

func (f *fakeState) SetLocalBlobs([]string) []state.Event { return nil }
func (f *fakeState) SetBlobSpec(state.BlobSpec, *admissionv1.Predicate) ([]state.Event, error) {
	return nil, nil
}
func (f *fakeState) DeleteBlobSpec(string) ([]state.Event, error)              { return nil, nil }
func (f *fakeState) SetBlobWrapping(*statev1.BlobWrappingChange) []state.Event { return nil }

type fakeGate struct {
	deny func(*admissionv1.SpecAuth) error
}

func (g *fakeGate) MayHost(_ *admissionv1.DelegationCert, sa *admissionv1.SpecAuth) error {
	if g.deny == nil {
		return nil
	}
	return g.deny(sa)
}

func dummyCert() *admissionv1.DelegationCert {
	return &admissionv1.DelegationCert{Claims: &admissionv1.DelegationCertClaims{}}
}

func authWithName(name string) *admissionv1.SpecAuth {
	return &admissionv1.SpecAuth{Resource: &admissionv1.ResourceID{
		Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: name}},
	}}
}

func snapWithLocalCert(local types.PeerKey, cert *admissionv1.DelegationCert) state.Snapshot {
	return state.Snapshot{
		LocalID: local,
		Nodes:   map[types.PeerKey]state.NodeView{local: {Cert: cert}},
	}
}

func TestMayStore_NilGateIsPermissive(t *testing.T) {
	svc := &Service{}
	require.NoError(t, svc.MayStore("abcd"))
}

func TestMayStore_DeniesWhenNoLocalCert(t *testing.T) {
	local := peerKey(1)
	st := &fakeState{snap: state.Snapshot{LocalID: local}}
	svc := &Service{state: st, gate: &fakeGate{}}
	require.ErrorIs(t, svc.MayStore("abcd"), ErrNotEntitled)
}

func TestMayStore_DeniesUnreferencedBlob(t *testing.T) {
	local := peerKey(1)
	st := &fakeState{snap: snapWithLocalCert(local, dummyCert())}
	svc := &Service{state: st, gate: &fakeGate{}}
	require.ErrorIs(t, svc.MayStore("orphan-hash"), ErrNotEntitled)
}

func TestMayStore_AllowsViaWorkloadSpec(t *testing.T) {
	local := peerKey(1)
	hash := strings.Repeat("a", 64)
	snap := snapWithLocalCert(local, dummyCert())
	snap.Specs = map[string]state.WorkloadSpecView{
		hash: {Spec: state.WorkloadSpec{Hash: hash}, Auth: authWithName("w")},
	}
	st := &fakeState{snap: snap}
	svc := &Service{state: st, gate: &fakeGate{}}
	require.NoError(t, svc.MayStore(hash))
}

func TestMayStore_AllowsViaBlobSpec(t *testing.T) {
	local := peerKey(1)
	digest := strings.Repeat("b", 64)
	snap := snapWithLocalCert(local, dummyCert())
	snap.BlobSpecs = map[string]state.BlobSpecView{
		digest: {Spec: state.BlobSpec{Digest: digest}, Auth: authWithName("b")},
	}
	st := &fakeState{snap: snap}
	svc := &Service{state: st, gate: &fakeGate{}}
	require.NoError(t, svc.MayStore(digest))
}

func TestMayStore_AllowsViaStaticManifest(t *testing.T) {
	local := peerKey(1)
	manifestDigest := strings.Repeat("c", 64)
	snap := snapWithLocalCert(local, dummyCert())
	snap.StaticSpecs = map[string]state.StaticSpecView{
		"site": {Spec: state.StaticSpec{Name: "site", ManifestDigest: manifestDigest}, Auth: authWithName("s")},
	}
	st := &fakeState{snap: snap}
	svc := &Service{state: st, gate: &fakeGate{}}
	require.NoError(t, svc.MayStore(manifestDigest))
}

func TestMayStore_AllowsViaNestedManifestPath(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	local := types.PeerKeyFromBytes(pub)
	dir := t.TempDir()
	store, err := cas.New(dir)
	require.NoError(t, err)

	pathDigest := []byte("file-digest-32-bytes-padding-xx!")
	require.Len(t, pathDigest, 32)
	manifest := &statev1.StaticManifest{Paths: []*statev1.StaticPath{{Path: "/index.html", Digest: pathDigest}}}
	manifestBytes, err := manifest.MarshalVT()
	require.NoError(t, err)

	dek, err := cas.GenerateDEK()
	require.NoError(t, err)
	manifestDigest, err := store.Put(strings.NewReader(string(manifestBytes)), dek)
	require.NoError(t, err)

	wrapped, err := cas.WrapDEK(dek, pub)
	require.NoError(t, err)
	manifestHashBytes, err := hex.DecodeString(manifestDigest)
	require.NoError(t, err)
	wrapping := &statev1.BlobWrappingChange{
		BlobHash:        manifestHashBytes,
		RecipientPubkey: pub,
		WrappedDek:      wrapped,
	}

	snap := snapWithLocalCert(local, dummyCert())
	snap.StaticSpecs = map[string]state.StaticSpecView{
		"site": {Spec: state.StaticSpec{Name: "site", ManifestDigest: manifestDigest}, Auth: authWithName("s")},
	}
	snap.Wrappings = map[string]map[types.PeerKey]*statev1.BlobWrappingChange{
		manifestDigest: {local: wrapping},
	}
	st := &fakeState{snap: snap}
	svc := &Service{
		store:          store,
		state:          st,
		gate:           &fakeGate{},
		signPriv:       priv,
		signPub:        pub,
		self:           local,
		parsedManifest: map[string]map[string]struct{}{},
		dekCache:       map[string][]byte{},
	}

	require.NoError(t, svc.MayStore(hex.EncodeToString(pathDigest)), "nested path digest must be entitled by the static spec")
}

func TestMayStore_DeniesNestedPathWhenManifestNotLocal(t *testing.T) {
	local := peerKey(1)
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)

	missingManifest := strings.Repeat("d", 64)
	pathDigest := strings.Repeat("e", 64)

	snap := snapWithLocalCert(local, dummyCert())
	snap.StaticSpecs = map[string]state.StaticSpecView{
		"site": {Spec: state.StaticSpec{Name: "site", ManifestDigest: missingManifest}, Auth: authWithName("s")},
	}
	st := &fakeState{snap: snap}
	svc := &Service{store: store, state: st, gate: &fakeGate{}, parsedManifest: map[string]map[string]struct{}{}}

	require.ErrorIs(t, svc.MayStore(pathDigest), ErrNotEntitled, "missing manifest cannot retrospectively entitle a nested path")
}

func TestMayStore_AllowsWhenAnyEntitlementPasses(t *testing.T) {
	local := peerKey(1)
	hash := strings.Repeat("f", 64)
	allowAuth := authWithName("allow")
	denyAuth := authWithName("deny")
	snap := snapWithLocalCert(local, dummyCert())
	snap.Specs = map[string]state.WorkloadSpecView{hash: {Spec: state.WorkloadSpec{Hash: hash}, Auth: allowAuth}}
	snap.BlobSpecs = map[string]state.BlobSpecView{hash: {Spec: state.BlobSpec{Digest: hash}, Auth: denyAuth}}
	st := &fakeState{snap: snap}
	gate := &fakeGate{deny: func(sa *admissionv1.SpecAuth) error {
		if sa == denyAuth {
			return errors.New("deny")
		}
		return nil
	}}
	svc := &Service{state: st, gate: gate}
	require.NoError(t, svc.MayStore(hash), "passing on any single entitlement is enough")
}

func TestMayStore_DeniesWhenAllEntitlementsFail(t *testing.T) {
	local := peerKey(1)
	hash := strings.Repeat("9", 64)
	snap := snapWithLocalCert(local, dummyCert())
	snap.Specs = map[string]state.WorkloadSpecView{hash: {Spec: state.WorkloadSpec{Hash: hash}, Auth: authWithName("a")}}
	snap.BlobSpecs = map[string]state.BlobSpecView{hash: {Spec: state.BlobSpec{Digest: hash}, Auth: authWithName("b")}}
	st := &fakeState{snap: snap}
	svc := &Service{state: st, gate: &fakeGate{deny: func(*admissionv1.SpecAuth) error { return errors.New("nope") }}}
	require.ErrorIs(t, svc.MayStore(hash), ErrNotEntitled)
}

func TestPrune_DropsPolicyDeniedFromKeep(t *testing.T) {
	dir := t.TempDir()
	store, err := cas.New(dir)
	require.NoError(t, err)

	keptHash := putAged(t, store, dir, "kept-policy-pass")
	deniedHash := putAged(t, store, dir, "kept-policy-deny")
	orphanHash := putAged(t, store, dir, "orphan")

	local := peerKey(1)
	snap := snapWithLocalCert(local, dummyCert())
	allowAuth := authWithName("allow")
	denyAuth := authWithName("deny")
	snap.Specs = map[string]state.WorkloadSpecView{
		keptHash:   {Spec: state.WorkloadSpec{Hash: keptHash}, Auth: allowAuth},
		deniedHash: {Spec: state.WorkloadSpec{Hash: deniedHash}, Auth: denyAuth},
	}
	st := &fakeState{snap: snap}
	gate := &fakeGate{deny: func(sa *admissionv1.SpecAuth) error {
		if sa == denyAuth {
			return errors.New("policy denies")
		}
		return nil
	}}
	svc := &Service{
		store:          store,
		state:          st,
		gate:           gate,
		local:          map[string]struct{}{keptHash: {}, deniedHash: {}, orphanHash: {}},
		parsedManifest: map[string]map[string]struct{}{},
	}

	keep := map[string]struct{}{keptHash: {}, deniedHash: {}}
	removed, err := svc.Prune(keep, 5*time.Minute)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{deniedHash, orphanHash}, removed, "policy-denied entries must evict alongside orphans")
	require.True(t, store.Has(keptHash))
	require.False(t, store.Has(deniedHash))
	require.False(t, store.Has(orphanHash))
}

func TestFetchFrom_DeniesUnentitledBlob(t *testing.T) {
	hash := strings.Repeat("ab", 32)
	local := peerKey(1)
	snap := snapWithLocalCert(local, dummyCert())
	st := &fakeState{snap: snap}

	rec := &recordingStream{}
	opener := &recordingOpener{stream: rec, status: statusOK}

	svc := &Service{
		store:          &fakeStore{},
		state:          st,
		gate:           &fakeGate{},
		mesh:           opener,
		timeout:        time.Second,
		parsedManifest: map[string]map[string]struct{}{},
	}
	err := svc.fetchFrom(context.Background(), hash, peerKey(2))
	require.Error(t, err)
	require.Contains(t, err.Error(), "not entitled")
}

type recordingStream struct {
	writes [][]byte
	reads  []byte
	closed bool
}

func (s *recordingStream) Read(p []byte) (int, error) {
	if len(s.reads) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.reads)
	s.reads = s.reads[n:]
	return n, nil
}

func (s *recordingStream) Write(p []byte) (int, error) {
	s.writes = append(s.writes, append([]byte(nil), p...))
	return len(p), nil
}

func (s *recordingStream) Close() error { s.closed = true; return nil }

type recordingOpener struct {
	stream *recordingStream
	status byte
}

func (o *recordingOpener) OpenStream(_ context.Context, _ types.PeerKey, _ transport.StreamType) (io.ReadWriteCloser, error) {
	o.stream.reads = []byte{o.status}
	return o.stream, nil
}
