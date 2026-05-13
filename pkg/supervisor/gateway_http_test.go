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

func newBlobFixture(t *testing.T, now time.Time) (rootPub []byte, rootPriv ed25519.PrivateKey, resource *admissionv1.ResourceID, hash string) {
	t.Helper()
	rootPub, rootPriv = newKey(t)
	publisher, err := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	require.NoError(t, err)
	digest := bytes.Repeat([]byte{0xa1}, 32)
	body := &statev1.BlobSpecChange{Name: "payload", Digest: digest}
	resource = &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Blob{Blob: &admissionv1.BlobID{Name: body.GetName(), Digest: digest}}}
	specAuth, err := auth.IssueSpecAuth(rootPriv, publisher, resource, body, nil, false)
	require.NoError(t, err)
	hash = bytesAsHex(digest)
	g := gate.New(rootPub, fakeSnapshotter{snap: state.Snapshot{
		BlobSpecs: map[string]state.BlobSpecView{hash: {Spec: state.BlobSpec{Name: body.GetName(), Digest: hash}, Auth: specAuth}},
	}})
	t.Cleanup(func() { _ = g })
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
	g := gate.New(rootPub, fakeSnapshotter{snap: snap})
	return newGatewayHandler(g, b, rt, zap.NewNop().Sugar())
}

type gatewayResp struct {
	code int
	body []byte
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
	return gatewayResp{code: resp.StatusCode, body: b}
}

func TestGatewayFetchBlobStreams(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv, resource, hash := newBlobFixture(t, now)
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

	resp := doRequest(t, h, "blob.pln.sh", "/"+encoded, "")
	require.Equal(t, http.StatusOK, resp.code)
	require.Equal(t, want, resp.body)
}

func TestGatewayRejectsTamperedToken(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv, resource, hash := newBlobFixture(t, now)
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

	resp := doRequest(t, h, "blob.pln.sh", "/"+encoded, "")
	require.Equal(t, http.StatusForbidden, resp.code)
}

func TestGatewayRejectsExpiredToken(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv, resource, hash := newBlobFixture(t, now)
	publisher, _ := auth.IssueDelegationCert(rootPriv, nil, rootPub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	body := &statev1.BlobSpecChange{Name: "payload", Digest: bytes.Repeat([]byte{0xa1}, 32)}
	specAuth, _ := auth.IssueSpecAuth(rootPriv, publisher, resource, body, nil, false)
	snap := state.Snapshot{
		BlobSpecs: map[string]state.BlobSpecView{hash: {Spec: state.BlobSpec{Name: body.GetName(), Digest: hash}, Auth: specAuth}},
	}
	h := newHandler(t, snap, rootPub, &stubBlobs{body: []byte("never")}, nil)

	token, _ := auth.SignAccessToken(rootPriv, resource, now.Add(-2*time.Hour), time.Hour)
	encoded, _ := auth.EncodeAccessToken(token)

	resp := doRequest(t, h, "blob.pln.sh", "/"+encoded, "")
	require.Equal(t, http.StatusGone, resp.code)
}

func TestGatewayRejectsBlobTokenAtFnSubdomain(t *testing.T) {
	now := time.Now()
	rootPub, rootPriv, resource, _ := newBlobFixture(t, now)
	snap := state.Snapshot{}
	h := newHandler(t, snap, rootPub, &stubBlobs{}, &stubInvoker{})

	token, _ := auth.SignAccessToken(rootPriv, resource, now, time.Hour)
	encoded, _ := auth.EncodeAccessToken(token)

	resp := doRequest(t, h, "fn.pln.sh", "/"+encoded, "")
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

	resp := doRequest(t, h, "fn.pln.sh", "/"+encoded+"?fn=main", "input")
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

	resp := doRequest(t, h, "fn.pln.sh", "/"+encoded, "")
	require.Equal(t, http.StatusServiceUnavailable, resp.code)
}

func TestGatewayUnknownSubdomain(t *testing.T) {
	rootPub, _ := newKey(t)
	h := newHandler(t, state.Snapshot{}, rootPub, &stubBlobs{}, &stubInvoker{})
	resp := doRequest(t, h, "site.pln.sh", "/tok", "")
	require.Equal(t, http.StatusNotFound, resp.code)
}

func TestGatewayMissingToken(t *testing.T) {
	rootPub, _ := newKey(t)
	h := newHandler(t, state.Snapshot{}, rootPub, &stubBlobs{}, &stubInvoker{})
	resp := doRequest(t, h, "blob.pln.sh", "/", "")
	require.Equal(t, http.StatusBadRequest, resp.code)
}
