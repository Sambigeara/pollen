package wasm_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/wasm"
)

func loadTestModule(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return data
}

func newTestRuntime(t *testing.T) *wasm.Runtime {
	t.Helper()
	rt := wasm.NewRuntime(nil)
	t.Cleanup(rt.Close)
	return rt
}

func TestCompileAndCall(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()
	echoBytes := loadTestModule(t, "echo.wasm")

	err := rt.Compile(ctx, echoBytes, "echohash", wasm.PluginConfig{})
	require.NoError(t, err)

	out, err := rt.Call(ctx, "echohash", "handle", []byte("hello"))
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), out)
}

func TestCompileCachesModule(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()
	echoBytes := loadTestModule(t, "echo.wasm")

	err := rt.Compile(ctx, echoBytes, "samehash", wasm.PluginConfig{})
	require.NoError(t, err)

	err = rt.Compile(ctx, echoBytes, "samehash", wasm.PluginConfig{})
	require.NoError(t, err)

	require.True(t, rt.IsCompiled("samehash"))
}

func TestDropCompiled(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()
	echoBytes := loadTestModule(t, "echo.wasm")

	err := rt.Compile(ctx, echoBytes, "drophash", wasm.PluginConfig{})
	require.NoError(t, err)

	rt.DropCompiled("drophash")

	require.False(t, rt.IsCompiled("drophash"))

	_, err = rt.Call(ctx, "drophash", "handle", nil)
	require.Error(t, err)
}

func TestCallTimeout(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()
	loopBytes := loadTestModule(t, "loop.wasm")

	err := rt.Compile(ctx, loopBytes, "loophash", wasm.PluginConfig{
		Timeout: 500 * time.Millisecond,
	})
	require.NoError(t, err)

	start := time.Now()
	_, err = rt.Call(ctx, "loophash", "run", nil)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Greater(t, elapsed, 400*time.Millisecond)
	require.Less(t, elapsed, 5*time.Second)
}

func TestCallNonExistentFunction(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()
	echoBytes := loadTestModule(t, "echo.wasm")

	err := rt.Compile(ctx, echoBytes, "fnhash", wasm.PluginConfig{})
	require.NoError(t, err)

	_, err = rt.Call(ctx, "fnhash", "nonexistent", nil)
	require.Error(t, err)
}

func TestGreetModule(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()
	greetBytes := loadTestModule(t, "greet.wasm")

	err := rt.Compile(ctx, greetBytes, "greethash", wasm.PluginConfig{})
	require.NoError(t, err)

	out, err := rt.Call(ctx, "greethash", "greet", []byte("World"))
	require.NoError(t, err)
	require.Equal(t, "Hello, World!", string(out))
}

func TestConcurrentCalls(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()
	echoBytes := loadTestModule(t, "echo.wasm")

	err := rt.Compile(ctx, echoBytes, "conchash", wasm.PluginConfig{})
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			out, callErr := rt.Call(ctx, "conchash", "handle", []byte("concurrent"))
			if callErr != nil {
				errs[idx] = callErr
				return
			}
			if string(out) != "concurrent" {
				errs[idx] = fmt.Errorf("goroutine %d: got %q, want %q", idx, out, "concurrent")
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "goroutine %d", i)
	}
}

func TestCallUncompiledModule(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()

	_, err := rt.Call(ctx, "nohash", "handle", nil)
	require.Error(t, err)
}
