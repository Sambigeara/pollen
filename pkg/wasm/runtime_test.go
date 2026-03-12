package wasm_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tetratelabs/wazero"

	"github.com/sambigeara/pollen/pkg/wasm"
)

// minimalWASM is a valid WASM module that does nothing and exits immediately.
// Built from: (module (func (export "_start"))).
var minimalWASM = []byte{
	0x00, 0x61, 0x73, 0x6d, // magic
	0x01, 0x00, 0x00, 0x00, // version
	0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // type section: func () -> ()
	0x03, 0x02, 0x01, 0x00, // function section: 1 func of type 0
	0x07, 0x0a, 0x01, 0x06, 0x5f, 0x73, 0x74, 0x61, 0x72, 0x74, 0x00, 0x00, // export "_start" -> func 0
	0x0a, 0x04, 0x01, 0x02, 0x00, 0x0b, // code section: func body (empty)
}

// loopingWASM is a valid WASM module whose _start function loops forever.
// Built from: (module (func (export "_start") (loop (br 0)))).
var loopingWASM = []byte{
	0x00, 0x61, 0x73, 0x6d, // magic
	0x01, 0x00, 0x00, 0x00, // version
	0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // type section: func () -> ()
	0x03, 0x02, 0x01, 0x00, // function section: 1 func of type 0
	0x07, 0x0a, 0x01, 0x06, 0x5f, 0x73, 0x74, 0x61, 0x72, 0x74, 0x00, 0x00, // export "_start" -> func 0
	0x0a, 0x09, 0x01, 0x07, 0x00, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x0b, // code: loop { br 0 }
}

// memoryHungryWASM declares 10 pages (640 KiB) of initial memory.
// Built from: (module (memory 10) (func (export "_start"))).
var memoryHungryWASM = []byte{
	0x00, 0x61, 0x73, 0x6d, // magic
	0x01, 0x00, 0x00, 0x00, // version
	0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // type section: func () -> ()
	0x03, 0x02, 0x01, 0x00, // function section: 1 func of type 0
	0x05, 0x03, 0x01, 0x00, 0x0a, // memory section: 1 memory, min=10 pages
	0x07, 0x0a, 0x01, 0x06, 0x5f, 0x73, 0x74, 0x61, 0x72, 0x74, 0x00, 0x00, // export "_start" -> func 0
	0x0a, 0x04, 0x01, 0x02, 0x00, 0x0b, // code section: func body (empty)
}

func newTestRuntime(t *testing.T) *wasm.Runtime {
	t.Helper()
	ctx := context.Background()
	rt, err := wasm.NewRuntime(ctx, wasm.RuntimeConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { rt.Close(ctx) })
	return rt
}

func TestRunTrivialModule(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()

	compiled, err := rt.Compile(ctx, minimalWASM, "testhash")
	require.NoError(t, err)

	inst := rt.Instantiate(ctx, compiled, "testhash", wasm.ModuleConfig{})
	err = inst.Wait()
	if err != nil {
		t.Logf("trivial module exited with: %v", err)
	}
	require.Equal(t, "testhash", inst.Hash())
}

func TestStopCancelsBlockingModule(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()

	compiled, err := rt.Compile(ctx, loopingWASM, "stophash")
	require.NoError(t, err)

	inst := rt.Instantiate(ctx, compiled, "stophash", wasm.ModuleConfig{})

	// The looping module runs forever — verify it's still running after a beat.
	select {
	case <-inst.Done():
		t.Fatal("looping module should not have exited on its own")
	case <-time.After(100 * time.Millisecond):
	}

	inst.Stop()

	select {
	case <-inst.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("instance did not stop within timeout")
	}
}

func TestCompileInvalidModule(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()

	_, err := rt.Compile(ctx, []byte("not a wasm module"), "badhash")
	require.Error(t, err)
}

func TestCompileCachesModule(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()

	cm1, err := rt.Compile(ctx, minimalWASM, "samehash")
	require.NoError(t, err)

	cm2, err := rt.Compile(ctx, minimalWASM, "samehash")
	require.NoError(t, err)

	// Same compiled module should be returned (pointer equality).
	require.Same(t, cm1, cm2)
}

func TestDropCompiledRemovesCache(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()

	cm1, err := rt.Compile(ctx, minimalWASM, "drophash")
	require.NoError(t, err)

	rt.DropCompiled(ctx, "drophash")

	// Compiling again should succeed and return a new compiled module.
	cm2, err := rt.Compile(ctx, minimalWASM, "drophash")
	require.NoError(t, err)
	require.NotSame(t, cm1, cm2)
}

func TestTimeoutCancelsBlockingModule(t *testing.T) {
	rt := newTestRuntime(t)
	ctx := context.Background()

	compiled, err := rt.Compile(ctx, loopingWASM, "timeouthash")
	require.NoError(t, err)

	start := time.Now()
	inst := rt.Instantiate(ctx, compiled, "timeouthash", wasm.ModuleConfig{
		Timeout: 500 * time.Millisecond,
	})

	select {
	case <-inst.Done():
		elapsed := time.Since(start)
		require.Greater(t, elapsed, 400*time.Millisecond, "should have blocked until near the timeout")
		require.Less(t, elapsed, 3*time.Second, "should not have taken much longer than the timeout")
	case <-time.After(10 * time.Second):
		t.Fatal("module did not exit within outer safety timeout")
	}

	// The module should have errored from context cancellation.
	require.Error(t, inst.Err())
}

func TestFallbackToInterpreterOnMmapPanic(t *testing.T) {
	restore := wasm.SetCompilerFactory(func(context.Context, uint32) (wazero.Runtime, error) {
		panic("operation not permitted")
	})
	t.Cleanup(restore)

	ctx := context.Background()
	rt, err := wasm.NewRuntime(ctx, wasm.RuntimeConfig{})
	require.NoError(t, err, "should fall back to interpreter")
	t.Cleanup(func() { rt.Close(ctx) })

	compiled, err := rt.Compile(ctx, minimalWASM, "fallback")
	require.NoError(t, err)

	inst := rt.Instantiate(ctx, compiled, "fallback", wasm.ModuleConfig{})
	require.NoError(t, inst.Wait())
}

func TestUnrelatedCompilerPanicPropagates(t *testing.T) {
	restore := wasm.SetCompilerFactory(func(context.Context, uint32) (wazero.Runtime, error) {
		panic("some unrelated wazero bug")
	})
	t.Cleanup(restore)

	ctx := context.Background()
	require.Panics(t, func() {
		wasm.NewRuntime(ctx, wasm.RuntimeConfig{}) //nolint:errcheck
	})
}

func TestMemoryLimitRejectsLargeModule(t *testing.T) {
	ctx := context.Background()
	rt, err := wasm.NewRuntime(ctx, wasm.RuntimeConfig{
		MemoryLimitPages: 1, // 64 KiB
	})
	require.NoError(t, err)
	t.Cleanup(func() { rt.Close(ctx) })

	// memoryHungryWASM declares 10 pages of initial memory.
	// A runtime limited to 1 page should reject it.
	compiled, err := rt.Compile(ctx, memoryHungryWASM, "memlimit")
	if err != nil {
		// Rejected at compile time — that's valid enforcement.
		return
	}

	// If compile succeeded, instantiation must fail.
	inst := rt.Instantiate(ctx, compiled, "memlimit", wasm.ModuleConfig{})
	err = inst.Wait()
	require.Error(t, err, "module requesting 10 pages should fail on a 1-page runtime")
}
