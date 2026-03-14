package mesh

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// halfClosePipe is a bidirectional pipe with independent read/write halves,
// supporting CloseWrite for half-close semantics (like a QUIC stream).
type halfClosePipe struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func (p *halfClosePipe) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p *halfClosePipe) Write(b []byte) (int, error) { return p.w.Write(b) }
func (p *halfClosePipe) CloseWrite() error           { return p.w.Close() }

func (p *halfClosePipe) Close() error {
	_ = p.r.Close()
	return p.w.Close()
}

// pipeConn returns two connected halfClosePipes. Data written to one is
// readable from the other. CloseWrite on one side delivers EOF to the
// other's Read without affecting the reverse direction.
func pipeConn() (*halfClosePipe, *halfClosePipe) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()
	return &halfClosePipe{r: r1, w: w2}, &halfClosePipe{r: r2, w: w1}
}

func TestBridgeStreamsHalfClose(t *testing.T) {
	appA, bridgeA := pipeConn()
	bridgeB, appB := pipeConn()

	t.Cleanup(func() {
		_ = appA.Close()
		_ = appB.Close()
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		bridgeStreams(bridgeA, bridgeB)
	}()

	// A writes a request then half-closes its write side.
	_, err := appA.Write([]byte("request"))
	require.NoError(t, err)
	require.NoError(t, appA.CloseWrite())

	// B reads the request.
	buf := make([]byte, 64)
	n, err := io.ReadFull(appB, buf[:7])
	require.NoError(t, err)
	require.Equal(t, "request", string(buf[:n]))

	// B writes a response then half-closes.
	_, err = appB.Write([]byte("response"))
	require.NoError(t, err)
	require.NoError(t, appB.CloseWrite())

	// A reads the response — this is what broke before the fix because the
	// eager once.Do(teardown) killed both streams when the first copy finished.
	n, err = io.ReadFull(appA, buf[:8])
	require.NoError(t, err)
	require.Equal(t, "response", string(buf[:n]))

	// Bridge goroutine should exit cleanly.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("bridgeStreams did not exit")
	}
}
