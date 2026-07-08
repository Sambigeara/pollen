// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package static

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func pk(b byte) types.PeerKey {
	raw := make([]byte, 32)
	raw[0] = b
	return types.PeerKeyFromBytes(raw)
}

type fakeStateStore struct {
	stateStore
	snap         state.Snapshot
	staticSpecs  []state.StaticSpec
	presignedSet []state.StaticSpec
}

func (f *fakeStateStore) Snapshot() state.Snapshot { return f.snap }

func (f *fakeStateStore) SetStaticSpec(spec state.StaticSpec, _ *admissionv1.Predicate) ([]state.Event, error) {
	f.staticSpecs = append(f.staticSpecs, spec)
	return nil, nil
}

func (f *fakeStateStore) SetStaticSpecPresigned(spec state.StaticSpec, _ *factv1.Fact) ([]state.Event, error) {
	f.presignedSet = append(f.presignedSet, spec)
	return nil, nil
}

type wrapCall struct {
	hashes     []string
	recipients []types.PeerKey
}

// fakeBlobStore exercises only the methods SeedStatic touches:
// ManifestPaths (for fanout) and IssueWrappingsFor (the side effect
// the tests assert). Has/Get/Fetch on the embedded interface stay nil
// and would panic if reached; SeedStatic must not reach them.
type fakeBlobStore struct {
	blobStore
	manifestPaths map[string]map[string]struct{}
	wrapCalls     []wrapCall
	wrapErr       error
}

func (f *fakeBlobStore) ManifestPaths(digest string) (map[string]struct{}, bool) {
	p, ok := f.manifestPaths[digest]
	return p, ok
}

func (f *fakeBlobStore) IssueWrappingsFor(hashes []string, recipients []types.PeerKey) error {
	f.wrapCalls = append(f.wrapCalls, wrapCall{hashes: hashes, recipients: recipients})
	return f.wrapErr
}

// manifestDigestOf serialises a StaticManifest the same way the CLI
// does (cmd/pln/seed.go) and returns its sha256 hex, so tests pin the
// digest that fanout will look up against the same byte shape the
// production publisher emits.
func manifestDigestOf(t *testing.T, files map[string][]byte) string {
	t.Helper()
	paths := make([]*statev1.StaticPath, 0, len(files))
	for p, d := range files {
		paths = append(paths, &statev1.StaticPath{Path: p, Digest: d})
	}
	raw, err := (&statev1.StaticManifest{Paths: paths}).MarshalVT()
	require.NoError(t, err)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func mkDigest(b byte) []byte {
	d := make([]byte, digestSize)
	d[0] = b
	return d
}

// pathsOf returns the set of file digest hexes for the given file map,
// matching the shape blobs.ManifestPaths returns in production.
func pathsOf(files map[string][]byte) map[string]struct{} {
	out := make(map[string]struct{}, len(files))
	for _, d := range files {
		out[hex.EncodeToString(d)] = struct{}{}
	}
	return out
}

// SeedStatic must pre-position a wrapping for every serving peer
// against the manifest and every file digest it references, as part of
// the same call that publishes the spec (so daemon-up does not race
// lazy-wrap).
func TestSeedStatic_FanoutWrappingsToServingSet(t *testing.T) {
	self := pk(1)
	serveA := pk(2)
	serveB := pk(3)
	files := map[string][]byte{"/index.html": mkDigest(0xaa), "/about.html": mkDigest(0xbb)}
	manifestHex := manifestDigestOf(t, files)
	manifestDigest, err := hex.DecodeString(manifestHex)
	require.NoError(t, err)

	store := &fakeStateStore{snap: state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
		self:   {CanServeStatic: true},
		serveA: {CanServeStatic: true},
		serveB: {CanServeStatic: true},
		pk(4):  {CanServeStatic: false},
	}}}
	blobs := &fakeBlobStore{manifestPaths: map[string]map[string]struct{}{manifestHex: pathsOf(files)}}
	svc := New(self, store, blobs, true, zap.NewNop().Sugar())

	require.NoError(t, svc.SeedStatic("docs", manifestDigest, nil))
	require.Len(t, store.staticSpecs, 1)
	require.Equal(t, manifestHex, store.staticSpecs[0].ManifestDigest)

	require.Len(t, blobs.wrapCalls, 1, "fanout fires exactly once per seed")
	call := blobs.wrapCalls[0]
	require.ElementsMatch(t,
		[]string{manifestHex, hex.EncodeToString(mkDigest(0xaa)), hex.EncodeToString(mkDigest(0xbb))},
		call.hashes)
	require.ElementsMatch(t, []types.PeerKey{self, serveA, serveB}, call.recipients)
}

// SeedStaticPresigned shares the fanout path with the daemon-up
// variant: the two transports must converge on the same end state so
// the user-visible bug ("wire works, daemon-up doesn't") cannot
// regress in either direction.
func TestSeedStaticPresigned_FanoutWrappingsToServingSet(t *testing.T) {
	gateway := pk(1)
	other := pk(2)
	files := map[string][]byte{"/x.html": mkDigest(0xcc)}
	manifestHex := manifestDigestOf(t, files)
	manifestDigest, err := hex.DecodeString(manifestHex)
	require.NoError(t, err)

	store := &fakeStateStore{snap: state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
		gateway: {CanServeStatic: true},
		other:   {CanServeStatic: true},
	}}}
	blobs := &fakeBlobStore{manifestPaths: map[string]map[string]struct{}{manifestHex: pathsOf(files)}}
	svc := New(gateway, store, blobs, true, zap.NewNop().Sugar())

	tenantPub := pk(9).Bytes()
	require.NoError(t, svc.SeedStaticPresigned("site", manifestDigest, &factv1.Fact{AuthorityPub: tenantPub}))
	require.Len(t, store.presignedSet, 1)
	require.Len(t, blobs.wrapCalls, 1)
	require.ElementsMatch(t, []types.PeerKey{gateway, other}, blobs.wrapCalls[0].recipients)
}

// An empty serving set is a hard error returned before the spec is
// published; today a missing --static-addr lands a spec that nobody
// will ever claim, which is the silent-failure mode this guards.
func TestSeedStatic_RejectsEmptyServingSet(t *testing.T) {
	store := &fakeStateStore{snap: state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
		pk(1): {CanServeStatic: false},
	}}}
	blobs := &fakeBlobStore{}
	svc := New(pk(1), store, blobs, false, zap.NewNop().Sugar())

	err := svc.SeedStatic("docs", mkDigest(0x01), nil)
	require.ErrorIs(t, err, ErrNoServingCapacity)
	require.Empty(t, store.staticSpecs, "spec must not land when serving set is empty")
	require.Empty(t, blobs.wrapCalls)
}

// Same guarantee on the presigned path; the user's symptom did not
// hinge on transport, and neither must this gate.
func TestSeedStaticPresigned_RejectsEmptyServingSet(t *testing.T) {
	store := &fakeStateStore{snap: state.Snapshot{}}
	blobs := &fakeBlobStore{}
	svc := New(pk(1), store, blobs, false, zap.NewNop().Sugar())

	err := svc.SeedStaticPresigned("site", mkDigest(0x02), &factv1.Fact{AuthorityPub: pk(9).Bytes()})
	require.ErrorIs(t, err, ErrNoServingCapacity)
	require.Empty(t, store.presignedSet)
	require.Empty(t, blobs.wrapCalls)
}

// A failing fanout must not roll back the published spec: lazy-wrap on
// Serve still covers the tail, so undoing a successful publication
// would only leave the cluster worse off.
func TestSeedStatic_FanoutFailureDoesNotRollback(t *testing.T) {
	self := pk(1)
	server := pk(2)
	files := map[string][]byte{"/y": mkDigest(0xdd)}
	manifestHex := manifestDigestOf(t, files)
	manifestDigest, _ := hex.DecodeString(manifestHex)

	store := &fakeStateStore{snap: state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
		self:   {CanServeStatic: true},
		server: {CanServeStatic: true},
	}}}
	blobs := &fakeBlobStore{
		manifestPaths: map[string]map[string]struct{}{manifestHex: pathsOf(files)},
		wrapErr:       errors.New("simulated DEK lookup failure"),
	}
	svc := New(self, store, blobs, true, zap.NewNop().Sugar())

	require.NoError(t, svc.SeedStatic("docs", manifestDigest, nil), "fanout failure must be swallowed")
	require.Len(t, store.staticSpecs, 1, "spec stays published even when fanout failed")
}

// A manifest the daemon cannot decrypt yet (ManifestPaths returns
// false) must not block the spec from landing; reconcile and lazy-wrap
// together still converge.
func TestSeedStatic_ManifestUnreadableStillPublishes(t *testing.T) {
	self := pk(1)
	server := pk(2)
	manifestDigest := mkDigest(0xee)

	store := &fakeStateStore{snap: state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
		self:   {CanServeStatic: true},
		server: {CanServeStatic: true},
	}}}
	blobs := &fakeBlobStore{} // no manifestPaths entry: ManifestPaths returns false
	svc := New(self, store, blobs, true, zap.NewNop().Sugar())

	require.NoError(t, svc.SeedStatic("docs", manifestDigest, nil))
	require.Len(t, store.staticSpecs, 1)
	require.Empty(t, blobs.wrapCalls, "fanout was skipped because the manifest was unreadable")
}
