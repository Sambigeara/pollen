// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type hangingStream struct {
	ch     chan struct{}
	mu     sync.Mutex
	closed bool
}

func newHangingStream() *hangingStream {
	return &hangingStream{ch: make(chan struct{})}
}

func (s *hangingStream) Read(p []byte) (int, error) {
	<-s.ch
	return 0, io.ErrClosedPipe
}

func (s *hangingStream) Write(p []byte) (int, error) {
	return len(p), nil
}

func (s *hangingStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
	return nil
}

type hangingOpener struct {
	stream *hangingStream
}

func (o *hangingOpener) OpenStream(_ context.Context, _ types.PeerKey, _ transport.StreamType) (io.ReadWriteCloser, error) {
	return o.stream, nil
}

type fakeStore struct{}

func (f *fakeStore) Put(r io.Reader, _ []byte) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (f *fakeStore) PutCiphertext(_ string, r io.Reader) error {
	_, err := io.Copy(io.Discard, r)
	return err
}
func (f *fakeStore) Get(string, []byte) (io.ReadCloser, error)   { return nil, nil }
func (f *fakeStore) GetCiphertext(string) (io.ReadCloser, error) { return nil, nil }
func (f *fakeStore) Has(string) bool                             { return false }
func (f *fakeStore) Remove(string) error                         { return nil }
func (f *fakeStore) Entries() ([]cas.Entry, error)               { return nil, nil }

func peerKey(b byte) types.PeerKey {
	var k types.PeerKey
	k[0] = b
	return k
}

func TestFetchFrom_TimeoutCoversEntireFetch(t *testing.T) {
	hs := newHangingStream()
	svc := &Service{store: &fakeStore{}, mesh: &hangingOpener{stream: hs}, timeout: 500 * time.Millisecond}

	hash := strings.Repeat("ab", 32)
	start := time.Now()
	err := svc.fetchFrom(t.Context(), hash, peerKey(1))
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(t, elapsed, 2*time.Second, "fetch should complete within the timeout, not hang indefinitely")
	require.Greater(t, elapsed, 400*time.Millisecond, "fetch should wait for the timeout before failing")
}

func TestFetch_AllPeersFail_ReturnsError(t *testing.T) {
	hs := newHangingStream()
	svc := &Service{store: &fakeStore{}, mesh: &hangingOpener{stream: hs}, timeout: 200 * time.Millisecond}

	hash := strings.Repeat("ab", 32)
	err := svc.Fetch(t.Context(), hash, []types.PeerKey{peerKey(1), peerKey(2)})
	require.Error(t, err)
	require.Contains(t, err.Error(), "fetch blob")
}
