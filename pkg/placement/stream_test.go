package placement

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

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

type fakeCASWriter struct{}

func (f *fakeCASWriter) Put(r io.Reader) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

type mockInvoker struct {
	callFn func(ctx context.Context, hash, function string, input []byte) ([]byte, error)
}

func (m *mockInvoker) Call(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	return m.callFn(ctx, hash, function, input)
}

func pipePair(t *testing.T) (io.ReadWriteCloser, io.ReadWriteCloser) {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() {
		server.Close()
		client.Close()
	})
	return server, client
}

func TestFetchFrom_TimeoutCoversEntireFetch(t *testing.T) {
	hs := newHangingStream()
	opener := &hangingOpener{stream: hs}
	cas := &fakeCASWriter{}
	fetcher := &meshFetcher{mesh: opener, cas: cas, timeout: 500 * time.Millisecond}

	hash := strings.Repeat("ab", 32)
	peer := peerKey(1)

	ctx := t.Context()
	start := time.Now()
	err := fetcher.fetchFrom(ctx, hash, peer)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(t, elapsed, 2*time.Second, "fetch should complete within the timeout, not hang indefinitely")
	require.Greater(t, elapsed, 400*time.Millisecond, "fetch should wait for the timeout before failing")
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

func TestHandleAndInvokeRoundTrip(t *testing.T) {
	hash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	invoker := &mockInvoker{
		callFn: func(_ context.Context, h, fn string, input []byte) ([]byte, error) {
			require.Equal(t, hash, h)
			require.Equal(t, "handle", fn)
			return append([]byte("echo:"), input...), nil
		},
	}

	ctx := context.Background()
	server, client := pipePair(t)

	// Test goroutine does not matter, but production needs tracking.
	go handleWorkloadStream(ctx, server, invoker, 10*time.Second)

	output, err := invokeOverStream(ctx, client, hash, "handle", []byte("hello"))
	require.NoError(t, err)
	require.Equal(t, []byte("echo:hello"), output)
}

func TestHandleWorkloadStream_Error(t *testing.T) {
	hash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	invoker := &mockInvoker{
		callFn: func(_ context.Context, _, _ string, _ []byte) ([]byte, error) {
			return nil, fmt.Errorf("not compiled")
		},
	}

	ctx := context.Background()
	server, client := pipePair(t)
	go handleWorkloadStream(ctx, server, invoker, 10*time.Second)

	_, err := invokeOverStream(ctx, client, hash, "run", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not compiled")
}

func TestHandleWorkloadStream_EmptyInput(t *testing.T) {
	hash := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	invoker := &mockInvoker{
		callFn: func(_ context.Context, _, _ string, input []byte) ([]byte, error) {
			return []byte("ok"), nil
		},
	}

	ctx := context.Background()
	server, client := pipePair(t)
	go handleWorkloadStream(ctx, server, invoker, 10*time.Second)

	output, err := invokeOverStream(ctx, client, hash, "ping", nil)
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), output)
}
