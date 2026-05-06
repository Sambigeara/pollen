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
	"strings"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/stretchr/testify/require"
)

func serveWithHeaderRead(ctx context.Context, stream io.ReadWriteCloser, peer types.PeerKey, invoker workloadInvoker) {
	info, chain, hash, function, err := ReadHeader(stream, peer)
	if err != nil {
		stream.Close()
		return
	}
	ctx = withChainSnapshot(ctx, chain)
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

type bufferStream struct{ bytes.Buffer }

func (*bufferStream) Close() error { return nil }

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

func TestHandleWorkloadStream_Overload(t *testing.T) {
	hash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	invoker := &mockInvoker{
		callFn: func(_ context.Context, _, _ string, _ []byte) ([]byte, error) {
			return nil, newOverload(ErrOverloaded, "node memory budget exhausted")
		},
	}

	ctx := context.Background()
	server, client := pipePair(t)
	go serveWithHeaderRead(ctx, server, types.PeerKey{}, invoker)

	_, err := invokeOverStream(ctx, client, hash, "run", nil)
	require.ErrorIs(t, err, ErrOverloaded)
	require.False(t, errors.Is(err, ErrWorkloadFailed))
}

func TestHandleWorkloadStream_ExpiredDeadlineSkipsInvocation(t *testing.T) {
	hash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	called := false
	invoker := &mockInvoker{
		callFn: func(context.Context, string, string, []byte) ([]byte, error) {
			called = true
			return []byte("late"), nil
		},
	}

	info := wasm.CallerInfo{DeadlineUnixMs: time.Now().Add(-time.Second).UnixMilli()}
	ctx := wasm.WithCallerInfo(context.Background(), info)
	server, client := pipePair(t)
	go serveWithHeaderRead(context.Background(), server, types.PeerKey{}, invoker)

	_, err := invokeOverStream(ctx, client, hash, "run", nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, called)
}

func TestInvokeOverStreamRejectsOversizedCallerEnvelope(t *testing.T) {
	hash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	ctx := wasm.WithCallerInfo(context.Background(), wasm.CallerInfo{
		Attributes: map[string]any{"oversized": strings.Repeat("x", 1<<16)},
	})

	_, err := invokeOverStream(ctx, &bufferStream{}, hash, "run", nil)

	require.ErrorContains(t, err, "caller metadata too large")
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

	info := wasm.CallerInfo{PeerKey: wirePK, Attributes: map[string]any{"role": "relay"}}
	ctx := wasm.WithCallerInfo(context.Background(), info)

	server, client := pipePair(t)
	go serveWithHeaderRead(context.Background(), server, authPK, invoker)

	output, err := invokeOverStream(ctx, client, hash, "handle", []byte("x"))
	require.NoError(t, err)
	require.Equal(t, []byte("x"), output)

	got, ok := wasm.CallerInfoFromContext(gotCtx)
	require.True(t, ok)
	require.Equal(t, authPK, got.PeerKey)
	require.Equal(t, "relay", got.Attributes["role"])
}

func TestHandleWorkloadStream_StampsCallChainForRecursionGuard(t *testing.T) {
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

func TestHandleAndInvokeRoundTrip_CallChain(t *testing.T) {
	upstream := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	hash := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	invoker := &mockInvoker{
		callFn: func(ctx context.Context, _, _ string, _ []byte) ([]byte, error) {
			require.True(t, chainContains(ctx, upstream), "forwarded stream must preserve upstream chain entries")
			require.True(t, chainContains(ctx, hash), "stream handler must stamp the current hop")
			return []byte("ok"), nil
		},
	}

	ctx := withChain(context.Background(), upstream)
	server, client := pipePair(t)
	go serveWithHeaderRead(context.Background(), server, types.PeerKey{}, invoker)

	output, err := invokeOverStream(ctx, client, hash, "handle", nil)
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), output)
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

func TestOverload_WireRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writeOverload(&buf, statusOverloaded, "node memory budget exhausted"))

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
