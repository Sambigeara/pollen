// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type recordingState struct {
	mu        sync.Mutex
	wrappings []*statev1.BlobWrappingChange
	snap      state.Snapshot
}

func (r *recordingState) Snapshot() state.Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snap
}

func (r *recordingState) SetLocalBlobs([]string) []state.Event { return nil }
func (r *recordingState) SetBlobSpec(state.BlobSpec, *admissionv1.Predicate) ([]state.Event, error) {
	return nil, nil
}
func (r *recordingState) DeleteBlobSpec(string) ([]state.Event, error) { return nil, nil }
func (r *recordingState) SetBlobWrapping(w *statev1.BlobWrappingChange) []state.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wrappings = append(r.wrappings, w)
	if r.snap.Wrappings == nil {
		r.snap.Wrappings = make(map[string]map[types.PeerKey]*statev1.BlobWrappingChange)
	}
	hashHex := hex.EncodeToString(w.GetBlobHash())
	if r.snap.Wrappings[hashHex] == nil {
		r.snap.Wrappings[hashHex] = make(map[types.PeerKey]*statev1.BlobWrappingChange)
	}
	r.snap.Wrappings[hashHex][types.PeerKeyFromBytes(w.GetRecipientPubkey())] = w
	return nil
}

type staticCert struct{ cert *admissionv1.DelegationCert }

func (s staticCert) Cert() *admissionv1.DelegationCert { return s.cert }

func newTestService(t *testing.T) (*Service, *recordingState) {
	t.Helper()
	return newTestServiceAt(t, t.TempDir())
}

func TestService_PutGetEncryptsAndDecrypts(t *testing.T) {
	svc, rs := newTestService(t)

	plaintext := []byte("encrypted round-trip payload")
	hash, err := svc.Put(strings.NewReader(string(plaintext)))
	require.NoError(t, err)

	require.Len(t, rs.wrappings, 1, "Put must publish a self-addressed wrapping")
	require.Equal(t, hash, hex.EncodeToString(rs.wrappings[0].GetBlobHash()))

	rc, err := svc.Get(hash)
	require.NoError(t, err)
	t.Cleanup(func() { rc.Close() })
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestService_PutEnvelopeIsEncryptedOnDisk(t *testing.T) {
	dir := t.TempDir()
	svc, _ := newTestServiceAt(t, dir)

	plaintext := []byte("must not appear on disk in clear")
	hash, err := svc.Put(strings.NewReader(string(plaintext)))
	require.NoError(t, err)

	onDisk, err := os.ReadFile(filepath.Join(dir, "cas", hash[:2], hash))
	require.NoError(t, err)
	require.NotContains(t, string(onDisk), string(plaintext), "Service.Put must encrypt before writing")
}

// TestLocalDEKReturnsCopyDecoupledFromCache pins the invariant that
// evictDEK zeroising the cache backing in place cannot poison a
// concurrent Get whose AEAD construction is mid-flight on the previous
// return value. Without the copy, a Remove racing a decrypt surfaces
// as a tag-mismatch error to the reader.
func TestLocalDEKReturnsCopyDecoupledFromCache(t *testing.T) {
	svc, _ := newTestService(t)
	const hash = "abcd1234"
	svc.cacheDEK(hash, []byte{0x01, 0x02, 0x03, 0x04})

	out, err := svc.localDEK(hash)
	require.NoError(t, err)
	require.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, out)

	svc.evictDEK(hash)
	require.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, out,
		"localDEK must return a copy independent of the cache backing")
}

func TestService_RemoveEvictsDEK(t *testing.T) {
	svc, _ := newTestService(t)

	hash, err := svc.Put(strings.NewReader("payload"))
	require.NoError(t, err)
	require.NotEmpty(t, svc.dekCache[hash])

	require.NoError(t, svc.Remove(hash))
	require.Empty(t, svc.dekCache[hash], "DEK must be evicted on Remove")
}

func TestService_ServeIssuesWrappingForRequester(t *testing.T) {
	svc, rs := newTestService(t)

	hash, err := svc.Put(strings.NewReader("payload"))
	require.NoError(t, err)
	rs.mu.Lock()
	rs.wrappings = nil
	rs.mu.Unlock()

	requesterPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	requester := types.PeerKeyFromBytes(requesterPub)

	server, _ := newPipe()
	t.Cleanup(func() { server.Close() })
	svc.Serve(server, hash, requester)

	rs.mu.Lock()
	wrappings := append([]*statev1.BlobWrappingChange(nil), rs.wrappings...)
	rs.mu.Unlock()

	require.Len(t, wrappings, 1, "Serve must issue exactly one wrapping for the requester")
	require.Equal(t, []byte(requesterPub), wrappings[0].GetRecipientPubkey())
	require.Equal(t, hash, hex.EncodeToString(wrappings[0].GetBlobHash()))
}

func TestService_ServeSkipsWrappingForSelf(t *testing.T) {
	svc, rs := newTestService(t)

	hash, err := svc.Put(strings.NewReader("payload"))
	require.NoError(t, err)
	rs.mu.Lock()
	rs.wrappings = nil
	rs.mu.Unlock()

	server, _ := newPipe()
	t.Cleanup(func() { server.Close() })
	svc.Serve(server, hash, svc.self)

	rs.mu.Lock()
	count := len(rs.wrappings)
	rs.mu.Unlock()
	require.Zero(t, count, "Serve to self must not emit a fresh wrapping; the publisher self-wrap from Put already covers it")
}

func TestFetchPlaintext_SelfPublisher_ReadsLocalCAS(t *testing.T) {
	svc, rs := newTestService(t)

	plaintext := []byte("export me")
	hash, err := svc.Put(strings.NewReader(string(plaintext)))
	require.NoError(t, err)

	rs.mu.Lock()
	if rs.snap.BlobSpecs == nil {
		rs.snap.BlobSpecs = map[string]state.BlobSpecView{}
	}
	rs.snap.BlobSpecs[hash] = state.BlobSpecView{
		Spec:      state.BlobSpec{Name: "named", Digest: hash},
		Publisher: svc.self,
	}
	rs.mu.Unlock()

	rc, err := svc.FetchPlaintext(t.Context(), hash)
	require.NoError(t, err)
	t.Cleanup(func() { rc.Close() })
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestFetchPlaintext_NoPublisher(t *testing.T) {
	svc, _ := newTestService(t)
	hash := strings.Repeat("ab", 32)
	_, err := svc.FetchPlaintext(t.Context(), hash)
	require.ErrorIs(t, err, ErrNoPublisher)
}

func TestFetchPlaintext_ResolvesViaWorkloadSpec(t *testing.T) {
	svc, rs := newTestService(t)

	plaintext := []byte("wasm bytes here")
	hash, err := svc.Put(strings.NewReader(string(plaintext)))
	require.NoError(t, err)

	rs.mu.Lock()
	if rs.snap.Specs == nil {
		rs.snap.Specs = map[string]state.WorkloadSpecView{}
	}
	rs.snap.Specs[hash] = state.WorkloadSpecView{
		Spec:      state.WorkloadSpec{Name: "hello", Hash: hash},
		Publisher: svc.self,
	}
	rs.mu.Unlock()

	rc, err := svc.FetchPlaintext(t.Context(), hash)
	require.NoError(t, err)
	t.Cleanup(func() { rc.Close() })
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

// helpers below avoid pulling in nettest just for a one-shot pipe.

type discardPipe struct{ closed bool }

func (d *discardPipe) Read(p []byte) (int, error)  { return 0, io.EOF }
func (d *discardPipe) Write(p []byte) (int, error) { return len(p), nil }
func (d *discardPipe) Close() error                { d.closed = true; return nil }

func newPipe() (*discardPipe, *discardPipe) {
	return &discardPipe{}, &discardPipe{}
}

func newTestServiceAt(t *testing.T, dir string) (*Service, *recordingState) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Now()
	cert, err := auth.IssueDelegationCert(priv, nil, pub, auth.FullCapabilities(), now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	require.NoError(t, err)
	rs := &recordingState{snap: state.Snapshot{LocalID: types.PeerKeyFromBytes(pub), Nodes: map[types.PeerKey]state.NodeView{
		types.PeerKeyFromBytes(pub): {Cert: cert},
	}}}
	svc, err := New(dir, types.PeerKeyFromBytes(pub), nil, rs, nil, staticCert{cert: cert}, priv)
	require.NoError(t, err)
	return svc, rs
}
