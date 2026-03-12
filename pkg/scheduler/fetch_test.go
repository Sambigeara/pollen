package scheduler

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// hangingStream accepts the connection but never responds, simulating a stalled peer.
type hangingStream struct {
	mu     sync.Mutex
	closed bool
	ch     chan struct{}
}

func newHangingStream() *hangingStream {
	return &hangingStream{ch: make(chan struct{})}
}

func (s *hangingStream) Read(p []byte) (int, error) {
	<-s.ch
	return 0, io.ErrClosedPipe
}

func (s *hangingStream) Write(p []byte) (int, error) {
	// Accept the write (hash bytes) to move past the write phase,
	// then block on Read.
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

func (o *hangingOpener) OpenArtifactStream(_ context.Context, _ types.PeerKey) (io.ReadWriteCloser, error) {
	return o.stream, nil
}

type fakeCASWriter struct{}

func (f *fakeCASWriter) Put(r io.Reader) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func TestFetchFrom_TimeoutCoversEntireFetch(t *testing.T) {
	hs := newHangingStream()
	opener := &hangingOpener{stream: hs}
	cas := &fakeCASWriter{}
	fetcher := &meshFetcher{mesh: opener, cas: cas, timeout: 500 * time.Millisecond}

	// Use a valid 64-char hex hash.
	hash := strings.Repeat("ab", 32)
	peer := peerKey(1)

	ctx := t.Context()

	start := time.Now()
	err := fetcher.fetchFrom(ctx, hash, peer)
	elapsed := time.Since(start)

	// Should fail (stream closed by timeout goroutine) within the timeout window.
	require.Error(t, err)
	require.Less(t, elapsed, 2*time.Second,
		"fetch should complete within the timeout, not hang indefinitely")
	require.Greater(t, elapsed, 400*time.Millisecond,
		"fetch should wait for the timeout before failing")
}

func TestFetch_AllPeersFail_ReturnsError(t *testing.T) {
	hs := newHangingStream()
	opener := &hangingOpener{stream: hs}
	cas := &fakeCASWriter{}
	fetcher := &meshFetcher{mesh: opener, cas: cas, timeout: 200 * time.Millisecond}

	hash := strings.Repeat("ab", 32)
	peers := []types.PeerKey{peerKey(1), peerKey(2)}

	ctx := t.Context()

	err := fetcher.Fetch(ctx, hash, peers)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fetch artifact")
}
