package wasm_test

import (
	"context"
	"errors"
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

type noopRouter struct{}

func (noopRouter) RouteCall(context.Context, string, string, []byte) ([]byte, error) {
	return nil, fmt.Errorf("no routing in tests")
}

func newTestRuntime(t *testing.T) *wasm.Runtime {
	t.Helper()
	hostFuncs := wasm.NewHostFunctions(zap.NewNop().Sugar(), noopRouter{})
	rt, err := wasm.NewRuntime(hostFuncs, 2)
	require.NoError(t, err)
	t.Cleanup(func() { rt.Close(context.Background()) })
	return rt
}

func TestCompileAndCall(t *testing.T) {
	rt := newTestRuntime(t)
	echoBytes := loadTestModule(t, "echo.wasm")

	err := rt.Compile(t.Context(), echoBytes, "echohash", wasm.NewPluginConfig(0, 0))
	require.NoError(t, err)

	out, err := rt.Call(t.Context(), "echohash", "handle", []byte("hello"))
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

	_, err = rt.Call(t.Context(), "drophash", "handle", nil)
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
			out, err := rt.Call(t.Context(), "conchash", "handle", []byte("concurrent"))
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

func TestCallConcurrencyLimit(t *testing.T) {
	hostFuncs := wasm.NewHostFunctions(zap.NewNop().Sugar(), noopRouter{})
	rt, err := wasm.NewRuntime(hostFuncs, 1)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	echoBytes := loadTestModule(t, "echo.wasm")
	loopBytes := loadTestModule(t, "loop.wasm")

	err = rt.Compile(ctx, echoBytes, "semhash", wasm.NewPluginConfig(0, 0))
	require.NoError(t, err)
	err = rt.Compile(ctx, loopBytes, "loopsem", wasm.NewPluginConfig(0, 2*time.Second))
	require.NoError(t, err)

	// Start a long-running call that holds the single slot.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = rt.Call(ctx, "loopsem", "run", nil)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
		rt.Close(context.Background())
	})

	// Wait until the slot is occupied: a short-deadline call must fail.
	require.Eventually(t, func() bool {
		probeCtx, probeCancel := context.WithTimeout(ctx, time.Millisecond)
		defer probeCancel()
		_, err := rt.Call(probeCtx, "semhash", "handle", []byte("probe"))
		return errors.Is(err, context.DeadlineExceeded)
	}, 3*time.Second, 10*time.Millisecond)

	// Confirm a real caller also gets DeadlineExceeded.
	tightCtx, tightCancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer tightCancel()

	_, err = rt.Call(tightCtx, "semhash", "handle", []byte("blocked"))
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCallUncompiledModule(t *testing.T) {
	rt := newTestRuntime(t)

	_, err := rt.Call(t.Context(), "nohash", "handle", nil)
	require.Error(t, err)
}
