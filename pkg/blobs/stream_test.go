// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"bytes"
	"context"
	"errors"
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

func (f *fakeStore) Put(r io.Reader) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
func (f *fakeStore) Get(string) (io.ReadCloser, error) { return nil, nil }
func (f *fakeStore) Has(string) bool                   { return false }
func (f *fakeStore) Remove(string) error               { return nil }
func (f *fakeStore) Entries() ([]cas.Entry, error)     { return nil, nil }

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

// scriptedStream replays a fixed status byte (and optional body) for a
// single fetchFrom attempt — enough to drive the per-peer outcomes in
// TestFetch_DenyTrumpsTransportFailure without spinning a real mesh.
type scriptedStream struct{ read *bytes.Reader }

func newScriptedStream(status byte, body []byte) *scriptedStream {
	buf := append([]byte{status}, body...)
	return &scriptedStream{read: bytes.NewReader(buf)}
}

func (s *scriptedStream) Read(p []byte) (int, error)  { return s.read.Read(p) }
func (s *scriptedStream) Write(p []byte) (int, error) { return len(p), nil }
func (*scriptedStream) Close() error                  { return nil }

type scriptedOpener struct {
	streams map[types.PeerKey]io.ReadWriteCloser
	errs    map[types.PeerKey]error
}

func (o *scriptedOpener) OpenStream(_ context.Context, pk types.PeerKey, _ transport.StreamType) (io.ReadWriteCloser, error) {
	if err, ok := o.errs[pk]; ok {
		return nil, err
	}
	if s, ok := o.streams[pk]; ok {
		return s, nil
	}
	return nil, errors.New("no scripted stream for peer")
}

// TestFetch_DenyTrumpsTransportFailure regresses the Codex M3 finding:
// a denied holder followed by an unreachable peer must surface as
// ErrUnauthorized — otherwise control's PermissionDenied mapping at
// the boundary is lost behind a NotFound.
func TestFetch_DenyTrumpsTransportFailure(t *testing.T) {
	denyer := peerKey(1)
	dead := peerKey(2)
	svc := &Service{
		store: &fakeStore{},
		mesh: &scriptedOpener{
			streams: map[types.PeerKey]io.ReadWriteCloser{
				denyer: newScriptedStream(statusUnauthorized, nil),
			},
			errs: map[types.PeerKey]error{
				dead: errors.New("connection refused"),
			},
		},
		timeout: time.Second,
	}

	hash := strings.Repeat("ab", 32)
	err := svc.Fetch(t.Context(), hash, []types.PeerKey{denyer, dead})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnauthorized, "deny from any holder must surface even when later peers fail differently")
}
