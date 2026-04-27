// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
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

func testGates() *gateRegistry {
	return newGateRegistry(16)
}

// noopReserver admits every call. Used by handler tests that don't
// exercise the memory admission path.
func noopReserver(string) (func(), error) { return func() {}, nil }

// serveWithHeaderRead is the production dispatch pair collapsed for
// tests: read the workload stream header via ReadHeader (what
// supervisor would do), then invoke handleWorkloadStream (what
// placement.Serve would call). Returns only when the stream closes.
func serveWithHeaderRead(ctx context.Context, stream io.ReadWriteCloser, peer types.PeerKey, invoker workloadInvoker, gates *gateRegistry) {
	info, hash, function, err := ReadHeader(stream, peer)
	if err != nil {
		stream.Close()
		return
	}
	handleWorkloadStream(ctx, stream, info, hash, function, invoker, newUtilisationTracker(), gates, noopReserver, 10*time.Second)
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
	go serveWithHeaderRead(ctx, server, types.PeerKey{}, invoker, testGates())

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
	go serveWithHeaderRead(ctx, server, types.PeerKey{}, invoker, testGates())

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
	go serveWithHeaderRead(context.Background(), server, authPK, invoker, testGates())

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
	go serveWithHeaderRead(ctx, server, types.PeerKey{}, invoker, testGates())

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
	go serveWithHeaderRead(ctx, server, types.PeerKey{}, invoker, testGates())

	output, err := invokeOverStream(ctx, client, hash, "ping", nil)
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), output)
}

// TestHandleWorkloadStream_OverloadedRoundTrip pins the wire contract
// for memory backpressure: when the target rejects admission with
// ErrOverloaded, the client must decode the response back to
// ErrOverloaded so Call's fallback loop treats it as retryable rather
// than as a terminal workload failure.
func TestHandleWorkloadStream_OverloadedRoundTrip(t *testing.T) {
	hash := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	invoker := &mockInvoker{
		callFn: func(_ context.Context, _, _ string, _ []byte) ([]byte, error) {
			t.Fatal("invoker must not run when admission rejects")
			return nil, nil
		},
	}
	rejectingReserver := func(string) (func(), error) { return nil, ErrOverloaded }

	ctx := context.Background()
	server, client := pipePair(t)
	go func() {
		info, h, fn, err := ReadHeader(server, types.PeerKey{})
		if err != nil {
			server.Close()
			return
		}
		handleWorkloadStream(ctx, server, info, h, fn, invoker, newUtilisationTracker(), testGates(), rejectingReserver, 10*time.Second)
	}()

	_, err := invokeOverStream(ctx, client, hash, "handle", nil)
	require.ErrorIs(t, err, ErrOverloaded)
	require.False(t, errors.Is(err, ErrWorkloadFailed), "must not collapse into ErrWorkloadFailed; Call's fallback loop treats that as terminal")
}

// Structured rejections carry retry-after metadata so callers (gRPC trailers,
// chained workloads) can wait the suggested duration before retrying.
func TestHandleWorkloadStream_OverloadCarriesRetryAfter(t *testing.T) {
	hash := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	invoker := &mockInvoker{
		callFn: func(_ context.Context, _, _ string, _ []byte) ([]byte, error) {
			t.Fatal("invoker must not run when admission rejects")
			return nil, nil
		},
	}
	rejectingReserver := func(string) (func(), error) { return nil, ErrOverloaded }

	ctx := context.Background()
	server, client := pipePair(t)
	go func() {
		info, h, fn, err := ReadHeader(server, types.PeerKey{})
		if err != nil {
			server.Close()
			return
		}
		handleWorkloadStream(ctx, server, info, h, fn, invoker, newUtilisationTracker(), testGates(), rejectingReserver, 10*time.Second)
	}()

	_, err := invokeOverStream(ctx, client, hash, "handle", nil)
	var ovl *OverloadError
	require.ErrorAs(t, err, &ovl)
	require.ErrorIs(t, ovl.Sentinel, ErrOverloaded)
	require.Equal(t, retryAfterDefault, ovl.RetryAfter, "retry-after must round-trip")
	require.NotEmpty(t, ovl.Reason, "reason should be populated")
}

// Even gate-at-capacity round-trips as a structured OverloadError so callers
// can distinguish concurrency pressure from memory pressure via the wrapped
// sentinel and read the suggested retry hint.
func TestHandleWorkloadStream_AtCapacityCarriesRetryAfter(t *testing.T) {
	hash := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	invoker := &mockInvoker{
		callFn: func(_ context.Context, _, _ string, _ []byte) ([]byte, error) {
			t.Fatal("invoker must not run when gate rejects")
			return nil, nil
		},
	}

	// Gate sized to one — pre-take the only slot so the inbound call is
	// rejected with ErrAtCapacity. Released at end-of-test.
	gates := newGateRegistry(1)
	gates.SetHashSize(hash, 1)
	hold, err := gates.acquire(callKey{Hash: hash, Function: "handle"})
	require.NoError(t, err)
	t.Cleanup(hold)

	ctx := context.Background()
	server, client := pipePair(t)
	go func() {
		info, h, fn, herr := ReadHeader(server, types.PeerKey{})
		if herr != nil {
			server.Close()
			return
		}
		handleWorkloadStream(ctx, server, info, h, fn, invoker, newUtilisationTracker(), gates, noopReserver, 10*time.Second)
	}()

	_, err = invokeOverStream(ctx, client, hash, "handle", nil)
	var ovl *OverloadError
	require.ErrorAs(t, err, &ovl)
	require.ErrorIs(t, ovl.Sentinel, ErrAtCapacity)
	require.Equal(t, retryAfterDefault, ovl.RetryAfter)
}
