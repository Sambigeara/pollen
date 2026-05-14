// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/gate"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type stubBlobs struct {
	body []byte
	err  error
}

func (s *stubBlobs) FetchPlaintext(_ context.Context, _ string) (io.ReadCloser, error) {
	if s.err != nil {
		return nil, s.err
	}
	return io.NopCloser(bytes.NewReader(s.body)), nil
}

type stubInvoker struct {
	output []byte
	err    error
}

func (s *stubInvoker) Call(_ context.Context, _, _ string, _ []byte) ([]byte, error) {
	return s.output, s.err
}

type fakeSnapshotter struct{ snap state.Snapshot }

func (f fakeSnapshotter) Snapshot() state.Snapshot { return f.snap }

func newKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

func newBlobFixture(t *testing.T) (rootPub []byte, rootPriv ed25519.PrivateKey, resource *admissionv1.ResourceID, hash string) {
	t.Helper()
	rootPub, rootPriv = newKey(t)
	digest := bytes.Repeat([]byte{0xa1}, 32)
	resource = &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{Name: "payload", Digest: digest}}}
	hash = bytesAsHex(digest)
	return rootPub, rootPriv, resource, hash
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

func newHandler(t *testing.T, snap state.Snapshot, rootPub []byte, b gatewayBlobReader, rt gatewayWorkloadInvoker) *gatewayHandler {
	t.Helper()
	snapshotter := fakeSnapshotter{snap: snap}
	g := gate.New(rootPub, snapshotter)
	return newGatewayHandler(g, snapshotter, b, rt, zap.NewNop().Sugar())
}

type gatewayResp struct {
	header http.Header
	body   []byte
	code   int
}

func doRequest(t *testing.T, h *gatewayHandler, host, path, body string) gatewayResp {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Host = host
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	resp := rec.Result()
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return gatewayResp{code: resp.StatusCode, body: b, header: resp.Header}
}

func TestGatewayFetchBlobStreams(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv, resource, hash := newBlobFixture(t)
	publisher, _ := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	body := &statev1.BlobSpecChange{Name: "payload", Digest: bytes.Repeat([]byte{0xa1}, 32)}
	specAuth, _ := auth.IssueSpecAuth(rootPriv, publisher, resource, body, nil, false)
	snap := state.Snapshot{
		BlobSpecs: map[string]state.BlobSpecView{hash: {Spec: state.BlobSpec{Name: body.GetName(), Digest: hash}, Auth: specAuth}},
	}

	want := []byte("hello world")
	h := newHandler(t, snap, rootPub, &stubBlobs{body: want}, nil)

	token, err := auth.SignAccessToken(rootPriv, resource, now, time.Hour)
	require.NoError(t, err)
	encoded, err := auth.EncodeAccessToken(token)
	require.NoError(t, err)

	resp := doRequest(t, h, "blob.pln.sh", "/_/"+encoded, "")
	require.Equal(t, http.StatusOK, resp.code)
	require.Equal(t, want, resp.body)
}

func TestGatewayRejectsTamperedToken(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv, resource, hash := newBlobFixture(t)
	publisher, _ := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	body := &statev1.BlobSpecChange{Name: "payload", Digest: bytes.Repeat([]byte{0xa1}, 32)}
	specAuth, _ := auth.IssueSpecAuth(rootPriv, publisher, resource, body, nil, false)
	snap := state.Snapshot{
		BlobSpecs: map[string]state.BlobSpecView{hash: {Spec: state.BlobSpec{Name: body.GetName(), Digest: hash}, Auth: specAuth}},
	}
	h := newHandler(t, snap, rootPub, &stubBlobs{body: []byte("never")}, nil)

	token, _ := auth.SignAccessToken(rootPriv, resource, now, time.Hour)
	token.Signature[0] ^= 0xff
	encoded, _ := auth.EncodeAccessToken(token)

	resp := doRequest(t, h, "blob.pln.sh", "/_/"+encoded, "")
	require.Equal(t, http.StatusForbidden, resp.code)
}

func TestGatewayRejectsExpiredToken(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv, resource, hash := newBlobFixture(t)
	publisher, _ := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	body := &statev1.BlobSpecChange{Name: "payload", Digest: bytes.Repeat([]byte{0xa1}, 32)}
	specAuth, _ := auth.IssueSpecAuth(rootPriv, publisher, resource, body, nil, false)
	snap := state.Snapshot{
		BlobSpecs: map[string]state.BlobSpecView{hash: {Spec: state.BlobSpec{Name: body.GetName(), Digest: hash}, Auth: specAuth}},
	}
	h := newHandler(t, snap, rootPub, &stubBlobs{body: []byte("never")}, nil)

	token, _ := auth.SignAccessToken(rootPriv, resource, now.Add(-2*time.Hour), time.Hour)
	encoded, _ := auth.EncodeAccessToken(token)

	resp := doRequest(t, h, "blob.pln.sh", "/_/"+encoded, "")
	require.Equal(t, http.StatusGone, resp.code)
}

func TestGatewayRejectsBlobTokenAtFnSubdomain(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv, resource, _ := newBlobFixture(t)
	snap := state.Snapshot{}
	h := newHandler(t, snap, rootPub, &stubBlobs{}, &stubInvoker{})

	token, _ := auth.SignAccessToken(rootPriv, resource, now, time.Hour)
	encoded, _ := auth.EncodeAccessToken(token)

	resp := doRequest(t, h, "fn.pln.sh", "/_/"+encoded, "")
	require.Equal(t, http.StatusForbidden, resp.code)
}

func TestGatewayInvokeRunsWorkload(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKey(t)
	publisher, _ := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	seedHash := bytes.Repeat([]byte{0xc1}, 32)
	hash := bytesAsHex(seedHash)
	body := &statev1.WorkloadSpecChange{Hash: hash, Name: "echo", MinReplicas: 1}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: body.GetName(), Hash: seedHash}}}
	specAuth, _ := auth.IssueSpecAuth(rootPriv, publisher, resource, body, nil, false)
	snap := state.Snapshot{
		Specs: map[string]state.WorkloadSpecView{hash: {Spec: state.WorkloadSpec{Hash: hash, Name: body.GetName()}, Auth: specAuth}},
	}
	want := []byte("output")
	h := newHandler(t, snap, rootPub, nil, &stubInvoker{output: want})

	token, _ := auth.SignAccessToken(rootPriv, resource, now, time.Hour)
	encoded, _ := auth.EncodeAccessToken(token)

	resp := doRequest(t, h, "fn.pln.sh", "/_/"+encoded+"?fn=main", "input")
	require.Equal(t, http.StatusOK, resp.code)
	require.Equal(t, want, resp.body)
}

func TestGatewayInvokeReportsPlacementFailure(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv := newKey(t)
	publisher, _ := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	seedHash := bytes.Repeat([]byte{0xc2}, 32)
	hash := bytesAsHex(seedHash)
	body := &statev1.WorkloadSpecChange{Hash: hash, Name: "echo", MinReplicas: 1}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: body.GetName(), Hash: seedHash}}}
	specAuth, _ := auth.IssueSpecAuth(rootPriv, publisher, resource, body, nil, false)
	snap := state.Snapshot{
		Specs: map[string]state.WorkloadSpecView{hash: {Spec: state.WorkloadSpec{Hash: hash, Name: body.GetName()}, Auth: specAuth}},
	}
	h := newHandler(t, snap, rootPub, nil, &stubInvoker{err: errors.New("no replica claims workload")})

	token, _ := auth.SignAccessToken(rootPriv, resource, now, time.Hour)
	encoded, _ := auth.EncodeAccessToken(token)

	resp := doRequest(t, h, "fn.pln.sh", "/_/"+encoded, "")
	require.Equal(t, http.StatusServiceUnavailable, resp.code)
}

func TestGatewayUnknownSubdomain(t *testing.T) {
	rootPub, _ := newKey(t)
	h := newHandler(t, state.Snapshot{}, rootPub, &stubBlobs{}, &stubInvoker{})
	resp := doRequest(t, h, "site.pln.sh", "/tok", "")
	require.Equal(t, http.StatusNotFound, resp.code)
}

func TestGatewayMissingRoute(t *testing.T) {
	rootPub, _ := newKey(t)
	h := newHandler(t, state.Snapshot{}, rootPub, &stubBlobs{}, &stubInvoker{})
	resp := doRequest(t, h, "blob.pln.sh", "/", "")
	require.Equal(t, http.StatusBadRequest, resp.code)
}

func TestGatewayMissingTokenAfterReservedSlug(t *testing.T) {
	rootPub, _ := newKey(t)
	h := newHandler(t, state.Snapshot{}, rootPub, &stubBlobs{}, &stubInvoker{})
	resp := doRequest(t, h, "blob.pln.sh", "/_/", "")
	require.Equal(t, http.StatusBadRequest, resp.code)
}

func TestGatewayUnknownSlugNotFound(t *testing.T) {
	rootPub, _ := newKey(t)
	h := newHandler(t, state.Snapshot{}, rootPub, &stubBlobs{}, &stubInvoker{})
	resp := doRequest(t, h, "fn.pln.sh", "/abc123abc123/echo", "")
	require.Equal(t, http.StatusNotFound, resp.code)
}

func TestGatewayMalformedSlugNotFound(t *testing.T) {
	rootPub, _ := newKey(t)
	h := newHandler(t, state.Snapshot{}, rootPub, &stubBlobs{}, &stubInvoker{})
	for _, path := range []string{
		"/ABC123ABC123/echo", // uppercase rejected
		"/short/echo",        // wrong length
		"/iiiiiiiiiiii/echo", // i excluded from Crockford alphabet
	} {
		resp := doRequest(t, h, "fn.pln.sh", path, "")
		require.Equal(t, http.StatusNotFound, resp.code, path)
	}
}

func newPublishedBlob(t *testing.T, publicSpec bool) (rootPub []byte, snap state.Snapshot, slug, name string) {
	t.Helper()
	now := time.Now()
	var rootPriv ed25519.PrivateKey
	rootPub, rootPriv = newKey(t)
	publisher, _ := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	digest := bytes.Repeat([]byte{0xa1}, 32)
	body := &statev1.BlobSpecChange{Name: "payload", Digest: digest}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{Name: body.GetName(), Digest: digest}}}
	var policy *admissionv1.Predicate
	if publicSpec {
		policy = &admissionv1.Predicate{Public: true}
	}
	specAuth, _ := auth.IssueSpecAuth(rootPriv, publisher, resource, body, policy, false)
	hash := bytesAsHex(digest)
	publisherKey := types.PeerKeyFromBytes(rootPub)
	snap = state.Snapshot{
		BlobSpecs: map[string]state.BlobSpecView{hash: {
			Spec:      state.BlobSpec{Name: body.GetName(), Digest: hash},
			Auth:      specAuth,
			Publisher: publisherKey,
		}},
	}
	return rootPub, snap, publisherKey.Slug(), body.GetName()
}

func TestGatewayNamedFetchPublic(t *testing.T) {
	want := []byte("hello world")
	rootPub, snap, slug, name := newPublishedBlob(t, true)
	h := newHandler(t, snap, rootPub, &stubBlobs{body: want}, nil)
	resp := doRequest(t, h, "blob.pln.sh", "/"+slug+"/"+name, "")
	require.Equal(t, http.StatusOK, resp.code)
	require.Equal(t, want, resp.body)
}

func TestGatewayNamedFetchPrivateIsNotFound(t *testing.T) {
	rootPub, snap, slug, name := newPublishedBlob(t, false)
	h := newHandler(t, snap, rootPub, &stubBlobs{body: []byte("never")}, nil)
	resp := doRequest(t, h, "blob.pln.sh", "/"+slug+"/"+name, "")
	require.Equal(t, http.StatusNotFound, resp.code)
}

func newPublishedWorkload(t *testing.T, publicSpec bool, output []byte) (rootPub []byte, snap state.Snapshot, slug, name string, invoker *stubInvoker) {
	t.Helper()
	now := time.Now()
	var rootPriv ed25519.PrivateKey
	rootPub, rootPriv = newKey(t)
	publisher, _ := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	seedHash := bytes.Repeat([]byte{0xc1}, 32)
	hash := bytesAsHex(seedHash)
	body := &statev1.WorkloadSpecChange{Hash: hash, Name: "echo", MinReplicas: 1}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{Name: body.GetName(), Hash: seedHash}}}
	var policy *admissionv1.Predicate
	if publicSpec {
		policy = &admissionv1.Predicate{Public: true}
	}
	specAuth, _ := auth.IssueSpecAuth(rootPriv, publisher, resource, body, policy, false)
	publisherKey := types.PeerKeyFromBytes(rootPub)
	snap = state.Snapshot{
		Specs: map[string]state.WorkloadSpecView{hash: {
			Spec:      state.WorkloadSpec{Hash: hash, Name: body.GetName()},
			Auth:      specAuth,
			Publisher: publisherKey,
		}},
	}
	invoker = &stubInvoker{output: output}
	return rootPub, snap, publisherKey.Slug(), body.GetName(), invoker
}

func TestGatewayNamedInvokePublic(t *testing.T) {
	want := []byte("output")
	rootPub, snap, slug, name, invoker := newPublishedWorkload(t, true, want)
	h := newHandler(t, snap, rootPub, nil, invoker)
	resp := doRequest(t, h, "fn.pln.sh", "/"+slug+"/"+name+"/greet", "input")
	require.Equal(t, http.StatusOK, resp.code)
	require.Equal(t, want, resp.body)
}

func TestGatewayNamedInvokeDefaultsToMain(t *testing.T) {
	rootPub, snap, slug, name, invoker := newPublishedWorkload(t, true, []byte("ok"))
	h := newHandler(t, snap, rootPub, nil, invoker)
	resp := doRequest(t, h, "fn.pln.sh", "/"+slug+"/"+name, "")
	require.Equal(t, http.StatusOK, resp.code)
}

func TestGatewayNamedInvokePrivateIsNotFound(t *testing.T) {
	rootPub, snap, slug, name, invoker := newPublishedWorkload(t, false, []byte("never"))
	h := newHandler(t, snap, rootPub, nil, invoker)
	resp := doRequest(t, h, "fn.pln.sh", "/"+slug+"/"+name, "")
	require.Equal(t, http.StatusNotFound, resp.code)
}

func TestTokenLimiter_ExhaustsAndRefills(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	l := newTokenLimiter(10, 5, 100)
	l.now = func() time.Time { return now }
	key := [32]byte{1}

	for i := range 5 {
		ok, _ := l.allow(key)
		require.True(t, ok, "burst tokens %d", i)
	}
	ok, retry := l.allow(key)
	require.False(t, ok)
	require.Greater(t, retry, time.Duration(0))

	// Advance the clock by 1s; should refill 10 tokens (capped at burst=5).
	now = now.Add(time.Second)
	ok, _ = l.allow(key)
	require.True(t, ok)
}

func TestTokenLimiter_DistinctKeysHaveIndependentBuckets(t *testing.T) {
	l := newTokenLimiter(10, 2, 100)
	a, b := [32]byte{1}, [32]byte{2}
	for range 2 {
		ok, _ := l.allow(a)
		require.True(t, ok)
	}
	ok, _ := l.allow(a)
	require.False(t, ok, "a's bucket exhausted")
	ok, _ = l.allow(b)
	require.True(t, ok, "b's bucket independent")
}

func TestTokenLimiter_EvictsAtCapacity(t *testing.T) {
	l := newTokenLimiter(10, 5, 3)
	for i := range 5 {
		var k [32]byte
		k[0] = byte(i)
		l.allow(k) //nolint:errcheck
	}
	require.LessOrEqual(t, len(l.buckets), 3, "buckets must respect capacity")
}

func TestGatewayReturns429WhenRateLimited(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv, resource, hash := newBlobFixture(t)
	publisher, _ := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	body := &statev1.BlobSpecChange{Name: "payload", Digest: bytes.Repeat([]byte{0xa1}, 32)}
	specAuth, _ := auth.IssueSpecAuth(rootPriv, publisher, resource, body, nil, false)
	snap := state.Snapshot{
		BlobSpecs: map[string]state.BlobSpecView{hash: {Spec: state.BlobSpec{Name: body.GetName(), Digest: hash}, Auth: specAuth}},
	}
	h := newHandler(t, snap, rootPub, &stubBlobs{body: []byte("x")}, &stubInvoker{})

	token, _ := auth.SignAccessToken(rootPriv, resource, now, time.Hour)
	encoded, _ := auth.EncodeAccessToken(token)

	// Burst is 20; one extra hit must trip 429 with a Retry-After header.
	for i := range gatewayTokenBurst {
		resp := doRequest(t, h, "blob.pln.sh", "/_/"+encoded, "")
		require.Equal(t, http.StatusOK, resp.code, "hit %d should pass within burst", i)
	}
	resp := doRequest(t, h, "blob.pln.sh", "/_/"+encoded, "")
	require.Equal(t, http.StatusTooManyRequests, resp.code)
	require.NotEmpty(t, resp.header.Get("Retry-After"))
}

func TestGatewayRateLimitImmuneToPathChurn(t *testing.T) {
	// An attacker hitting random invalid paths must not pollute the
	// limiter cache. Both named-route garbage and undecodable bearer
	// tokens have to bounce before the limiter sees them.
	rootPub, _ := newKey(t)
	h := newHandler(t, state.Snapshot{}, rootPub, &stubBlobs{}, &stubInvoker{})
	for range gatewayLimiterCapacity * 2 {
		doRequest(t, h, "blob.pln.sh", "/garbage-"+strings.Repeat("x", 32), "")
		doRequest(t, h, "blob.pln.sh", "/_/garbage-"+strings.Repeat("x", 32), "")
	}
	require.Empty(t, h.limiter.buckets, "garbage paths must not populate the rate limiter")
}
