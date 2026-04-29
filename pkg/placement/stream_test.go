// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/stretchr/testify/require"
)

func serveWithHeaderRead(ctx context.Context, stream io.ReadWriteCloser, peer types.PeerKey, invoker workloadInvoker) {
	info, hash, function, err := ReadHeader(stream, peer)
	if err != nil {
		stream.Close()
		return
	}
	handleWorkloadStream(ctx, stream, info, hash, function, invoker, 10*time.Second)
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
	go serveWithHeaderRead(ctx, server, types.PeerKey{}, invoker)

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
	go serveWithHeaderRead(ctx, server, types.PeerKey{}, invoker)

	_, err := invokeOverStream(ctx, client, hash, "run", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not compiled")
}

func TestHandleAndInvokeRoundTrip_CallerInfo(t *testing.T) {
	hash := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	wirePK := types.PeerKeyFromBytes([]byte("01234567890123456789012345678901"))
	authPK := types.PeerKeyFromBytes([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ012345"))
	var gotCtx context.Context
	invoker := &mockInvoker{
		callFn: func(ctx context.Context, _, _ string, input []byte) ([]byte, error) {
			gotCtx = ctx
			return input, nil
		},
	}

	// Client sends wirePK in the CallerInfo JSON.
	info := wasm.CallerInfo{PeerKey: wirePK, Attributes: map[string]any{"role": "relay"}}
	ctx := wasm.WithCallerInfo(context.Background(), info)

	server, client := pipePair(t)
	// Server uses authPK as the transport-authenticated peer.
	go serveWithHeaderRead(context.Background(), server, authPK, invoker)

	output, err := invokeOverStream(ctx, client, hash, "handle", []byte("x"))
	require.NoError(t, err)
	require.Equal(t, []byte("x"), output)

	got, ok := wasm.CallerInfoFromContext(gotCtx)
	require.True(t, ok)
	// PeerKey must be the transport-authenticated peer, not the wire value.
	require.Equal(t, authPK, got.PeerKey)
	require.Equal(t, "relay", got.Attributes["role"])
}

func TestHandleWorkloadStream_StampsCallChainForRecursionGuard(t *testing.T) {
	// The forwarded-call handler must stamp the hash into the local
	// call chain before invoking the workload, so a recursive
	// pollen_request from this seed back into itself fails fast with
	// ErrCycle instead of starving its own gate.
	hash := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	invoker := &mockInvoker{
		callFn: func(ctx context.Context, _, _ string, _ []byte) ([]byte, error) {
			if chainContains(ctx, hash) {
				return nil, fmt.Errorf("cycle: %w", ErrCycle)
			}
			return nil, fmt.Errorf("guard absent: chain did not contain %s", hash)
		},
	}

	ctx := context.Background()
	server, client := pipePair(t)
	go serveWithHeaderRead(ctx, server, types.PeerKey{}, invoker)

	_, err := invokeOverStream(ctx, client, hash, "handle", nil)
	require.ErrorIs(t, err, ErrCycle)
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
	go serveWithHeaderRead(ctx, server, types.PeerKey{}, invoker)

	output, err := invokeOverStream(ctx, client, hash, "ping", nil)
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), output)
}

// TestOverload_WireRoundTrip pins the wire contract for backpressure:
// writeOverload's encoding must decode back to an *OverloadError that
// Call's fallback loop treats as retryable rather than as a terminal
// workload failure. The admission gate that triggers this reply lives
// in Service.Serve / Service.callLocal; this test isolates the wire
// shape.
func TestOverload_WireRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	writeOverload(&buf, statusOverloaded, "node memory budget exhausted")

	var status [1]byte
	_, err := io.ReadFull(&buf, status[:])
	require.NoError(t, err)
	require.Equal(t, statusOverloaded, status[0])

	var bodyLen [4]byte
	_, err = io.ReadFull(&buf, bodyLen[:])
	require.NoError(t, err)

	body := make([]byte, binary.BigEndian.Uint32(bodyLen[:]))
	_, err = io.ReadFull(&buf, body)
	require.NoError(t, err)

	decoded := decodeOverload(body, ErrOverloaded)
	require.ErrorIs(t, decoded, ErrOverloaded)
	require.False(t, errors.Is(decoded, ErrWorkloadFailed), "must not collapse into ErrWorkloadFailed; Call's fallback loop treats that as terminal")

	var ovl *OverloadError
	require.ErrorAs(t, decoded, &ovl)
	require.Equal(t, "node memory budget exhausted", ovl.Reason)
}
