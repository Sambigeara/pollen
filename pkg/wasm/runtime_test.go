// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package wasm_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sambigeara/pollen/pkg/wasm"
)

func loadTestModule(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return data
}

type noopRequestRouter struct{}

func (noopRequestRouter) RouteRequest(context.Context, wasm.URI, []byte) ([]byte, error) {
	return nil, fmt.Errorf("no routing in tests")
}

func newTestRuntime(t *testing.T) *wasm.Runtime {
	t.Helper()
	hostFuncs := wasm.NewHostFunctions(zap.NewNop().Sugar(), noopRequestRouter{})
	rt, err := wasm.NewRuntime(hostFuncs)
	require.NoError(t, err)
	t.Cleanup(func() { rt.Close(context.Background()) })
	return rt
}

func TestCompileAndCall(t *testing.T) {
	rt := newTestRuntime(t)
	echoBytes := loadTestModule(t, "echo.wasm")

	err := rt.Compile(t.Context(), echoBytes, "echohash", wasm.NewPluginConfig(0, 0))
	require.NoError(t, err)

	out, err := rt.Call(t.Context(), "echohash", "echo", []byte("hello"))
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), out)
}

func TestCompileCachesModule(t *testing.T) {
	rt := newTestRuntime(t)
	echoBytes := loadTestModule(t, "echo.wasm")

	err := rt.Compile(t.Context(), echoBytes, "samehash", wasm.NewPluginConfig(0, 0))
	require.NoError(t, err)

	err = rt.Compile(t.Context(), echoBytes, "samehash", wasm.NewPluginConfig(0, 0))
	require.NoError(t, err)
}

func TestDropCompiled(t *testing.T) {
	rt := newTestRuntime(t)
	echoBytes := loadTestModule(t, "echo.wasm")

	err := rt.Compile(t.Context(), echoBytes, "drophash", wasm.NewPluginConfig(0, 0))
	require.NoError(t, err)

	rt.DropCompiled(t.Context(), "drophash")

	_, err = rt.Call(t.Context(), "drophash", "echo", nil)
	require.Error(t, err)
}

func TestCallTimeout(t *testing.T) {
	rt := newTestRuntime(t)
	loopBytes := loadTestModule(t, "loop.wasm")

	err := rt.Compile(t.Context(), loopBytes, "loophash", wasm.NewPluginConfig(0, 500*time.Millisecond))
	require.NoError(t, err)

	start := time.Now()
	_, err = rt.Call(t.Context(), "loophash", "run", nil)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Greater(t, elapsed, 400*time.Millisecond)
	require.Less(t, elapsed, 5*time.Second)
}

func TestCallNonExistentFunction(t *testing.T) {
	rt := newTestRuntime(t)
	echoBytes := loadTestModule(t, "echo.wasm")

	err := rt.Compile(t.Context(), echoBytes, "fnhash", wasm.NewPluginConfig(0, 0))
	require.NoError(t, err)

	_, err = rt.Call(t.Context(), "fnhash", "nonexistent", nil)
	require.Error(t, err)
}

func TestConcurrentCalls(t *testing.T) {
	rt := newTestRuntime(t)
	echoBytes := loadTestModule(t, "echo.wasm")

	err := rt.Compile(t.Context(), echoBytes, "conchash", wasm.NewPluginConfig(0, 0))
	require.NoError(t, err)

	var eg errgroup.Group
	for i := range 10 {
		eg.Go(func() error {
			out, err := rt.Call(t.Context(), "conchash", "echo", []byte("concurrent"))
			if err != nil {
				return fmt.Errorf("goroutine %d: %w", i, err)
			}
			if string(out) != "concurrent" {
				return fmt.Errorf("goroutine %d: got %q, want %q", i, out, "concurrent")
			}
			return nil
		})
	}
	require.NoError(t, eg.Wait())
}

func TestCallUncompiledModule(t *testing.T) {
	rt := newTestRuntime(t)

	_, err := rt.Call(t.Context(), "nohash", "echo", nil)
	require.Error(t, err)
}
