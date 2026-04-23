// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package static

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"testing"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/claims"
	"github.com/sambigeara/pollen/pkg/evaluator"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type fakeStore struct {
	snap    state.Snapshot
	claimed []string
	mu      sync.Mutex
}

func (f *fakeStore) Snapshot() state.Snapshot                              { return f.snap }
func (f *fakeStore) SetStaticSpec(state.StaticSpec) ([]state.Event, error) { return nil, nil }
func (f *fakeStore) DeleteStaticSpec(string) []state.Event                 { return nil }
func (f *fakeStore) ReleaseStatic(string) []state.Event                    { return nil }
func (f *fakeStore) ClaimStatic(name string) []state.Event {
	f.mu.Lock()
	f.claimed = append(f.claimed, name)
	f.mu.Unlock()
	return nil
}

type fakeBlobs struct {
	have    map[string][]byte
	fetched []string
	mu      sync.Mutex
}

func newFakeBlobs() *fakeBlobs { return &fakeBlobs{have: make(map[string][]byte)} }

func (f *fakeBlobs) Has(hash string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.have[hash]
	return ok
}

func (f *fakeBlobs) Get(hash string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.have[hash]
	if !ok {
		return nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeBlobs) Fetch(_ context.Context, hash string, _ []types.PeerKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetched = append(f.fetched, hash)
	f.have[hash] = []byte("fetched-" + hash)
	return nil
}

// seedManifest marshals a manifest referencing fileDigests (hex strings),
// stores it in the fake blob store, and returns its digest.
func seedManifest(t *testing.T, blobs *fakeBlobs, fileDigests ...string) string {
	t.Helper()
	paths := make([]*statev1.StaticPath, len(fileDigests))
	for i, hexDigest := range fileDigests {
		raw, err := hex.DecodeString(hexDigest)
		require.NoError(t, err)
		paths[i] = &statev1.StaticPath{Path: "/file" + hexDigest[:4], Digest: raw}
	}
	m := &statev1.StaticManifest{Paths: paths}
	data, err := m.MarshalVT()
	require.NoError(t, err)
	digest := "ab" + hex.EncodeToString(data[:31])
	blobs.have[digest] = data
	return digest
}

func snapshotWith(manifestDigest string, publisher types.PeerKey) state.Snapshot {
	return state.Snapshot{
		StaticSpecs: map[string]state.StaticSpecView{
			"home.local": {
				Spec:      state.StaticSpec{Name: "home.local", ManifestDigest: manifestDigest, MinReplicas: 1},
				Publisher: publisher,
			},
		},
		StaticClaims: map[string]map[types.PeerKey]struct{}{},
		Nodes: map[types.PeerKey]state.NodeView{
			publisher: {Blobs: map[string]struct{}{manifestDigest: {}}},
		},
		PeerKeys: []types.PeerKey{publisher},
	}
}

func TestEnsureReplicated_NonServingPeer_FetchesManifestButNotFilesOrClaim(t *testing.T) {
	publisher := types.PeerKey{1}
	self := types.PeerKey{2}

	blobs := newFakeBlobs()
	fileA := "cc" + hex.EncodeToString(bytes.Repeat([]byte{0xa}, 31))
	manifestDigest := seedManifest(t, blobs, fileA)

	snap := snapshotWith(manifestDigest, publisher)
	snap.Nodes[publisher].Blobs[fileA] = struct{}{}

	st := &fakeStore{snap: snap}
	svc := New(self, st, blobs, false, nil, zap.NewNop().Sugar())

	err := svc.ensureReplicated(t.Context(), snap, "home.local", snap.StaticSpecs["home.local"].Spec)
	require.NoError(t, err)

	require.NotContains(t, blobs.fetched, fileA, "non-serving peer must not fetch file blobs")
	require.Empty(t, st.claimed, "non-serving peer must not claim the site")
	// The manifest was already in the fake store (publisher-seeded), so no
	// fetch was needed; Has() returned true.
	require.True(t, blobs.Has(manifestDigest))
}

func TestEnsureReplicated_ServingPeer_FetchesFilesAndClaims(t *testing.T) {
	publisher := types.PeerKey{1}
	self := types.PeerKey{2}

	blobs := newFakeBlobs()
	fileA := "cc" + hex.EncodeToString(bytes.Repeat([]byte{0xa}, 31))
	manifestDigest := seedManifest(t, blobs, fileA)

	snap := snapshotWith(manifestDigest, publisher)
	snap.Nodes[publisher].Blobs[fileA] = struct{}{}

	st := &fakeStore{snap: snap}
	svc := New(self, st, blobs, true, nil, zap.NewNop().Sugar())

	err := svc.ensureReplicated(t.Context(), snap, "home.local", snap.StaticSpecs["home.local"].Spec)
	require.NoError(t, err)

	require.Contains(t, blobs.fetched, fileA, "serving peer must fetch file blobs")
	require.Equal(t, []string{"home.local"}, st.claimed)
}

func TestEnsureReplicated_DeniedBySeedPlacementGate(t *testing.T) {
	publisher := types.PeerKey{1}
	self := types.PeerKey{2}

	blobs := newFakeBlobs()
	fileA := "cc" + hex.EncodeToString(bytes.Repeat([]byte{0xa}, 31))
	manifestDigest := seedManifest(t, blobs, fileA)

	snap := snapshotWith(manifestDigest, publisher)
	snap.Nodes[publisher].Blobs[fileA] = struct{}{}

	st := &fakeStore{snap: snap}
	router, err := evaluator.NewRouter(
		evaluator.Config{Default: "deny_all"},
		evaluator.WithFactory("deny_all", func(string) (evaluator.Evaluator, error) {
			return denyAllEval{}, nil
		}),
	)
	require.NoError(t, err)
	svc := New(self, st, blobs, true, router, zap.NewNop().Sugar())

	err = svc.ensureReplicated(t.Context(), snap, "home.local", snap.StaticSpecs["home.local"].Spec)
	require.NoError(t, err, "gate denial must not surface as an error — just skip the claim")
	require.Empty(t, st.claimed, "denied seed_placement must not claim the site")
}

type denyAllEval struct{}

func (denyAllEval) Allow(context.Context, evaluator.Request) (evaluator.Decision, error) {
	return evaluator.Decision{Decision: false, Context: map[string]any{"reason_user": "blocked"}}, nil
}

// captureDenyEval records the last request it was asked to evaluate and
// always denies. Denial keeps the service from claiming so the test
// stays focused on the gate request shape.
type captureDenyEval struct {
	last evaluator.Request
}

func (c *captureDenyEval) Allow(_ context.Context, req evaluator.Request) (evaluator.Decision, error) {
	c.last = req
	return evaluator.Decision{Decision: false}, nil
}

func TestEnsureReplicated_GatePipesClaimProperties(t *testing.T) {
	publisher := types.PeerKey{1}
	self := types.PeerKey{2}

	blobs := newFakeBlobs()
	fileA := "cc" + hex.EncodeToString(bytes.Repeat([]byte{0xa}, 31))
	manifestDigest := seedManifest(t, blobs, fileA)

	snap := snapshotWith(manifestDigest, publisher)
	snap.Nodes[publisher].Blobs[fileA] = struct{}{}
	sv := snap.StaticSpecs["home.local"]
	sv.Spec.Claim = claims.New(map[string]any{"tier": "gold"}, []byte("sig"))
	snap.StaticSpecs["home.local"] = sv

	st := &fakeStore{snap: snap}
	spy := &captureDenyEval{}
	router, err := evaluator.NewRouter(
		evaluator.Config{Default: "capture"},
		evaluator.WithFactory("capture", func(string) (evaluator.Evaluator, error) { return spy, nil }),
	)
	require.NoError(t, err)
	svc := New(self, st, blobs, true, router, zap.NewNop().Sugar())

	err = svc.ensureReplicated(t.Context(), snap, "home.local", snap.StaticSpecs["home.local"].Spec)
	require.NoError(t, err)

	require.Equal(t, "gold", spy.last.Resource.Properties["tier"], "publisher claim properties must reach the seed_placement gate")
	require.Equal(t, "home.local", spy.last.Resource.ID)
	require.Equal(t, evaluator.ResourceStatic, spy.last.Resource.Type)
}

func TestStaticBlobs_IncludesManifestFilesWhenManifestLocal(t *testing.T) {
	publisher := types.PeerKey{1}
	self := types.PeerKey{2}

	blobs := newFakeBlobs()
	fileA := "cc" + hex.EncodeToString(bytes.Repeat([]byte{0xa}, 31))
	manifestDigest := seedManifest(t, blobs, fileA)

	snap := snapshotWith(manifestDigest, publisher)
	st := &fakeStore{snap: snap}
	svc := New(self, st, blobs, false, nil, zap.NewNop().Sugar())

	got := svc.StaticBlobs()
	require.Contains(t, got, manifestDigest)
	require.Contains(t, got, fileA, "file digests must appear once the manifest is loadable locally")
}
