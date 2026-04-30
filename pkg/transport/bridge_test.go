// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

	_, err := appA.Write([]byte("request"))
	require.NoError(t, err)
	require.NoError(t, appA.CloseWrite())

	buf := make([]byte, 64)
	n, err := io.ReadFull(appB, buf[:7])
	require.NoError(t, err)
	require.Equal(t, "request", string(buf[:n]))

	_, err = appB.Write([]byte("response"))
	require.NoError(t, err)
	require.NoError(t, appB.CloseWrite())

	n, err = io.ReadFull(appA, buf[:8])
	require.NoError(t, err)
	require.Equal(t, "response", string(buf[:n]))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("bridgeStreams did not exit")
	}
}
