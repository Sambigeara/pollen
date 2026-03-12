package workload_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/sambigeara/pollen/pkg/workload"
)

// minimalWASM is a valid WASM module with an empty _start function.
var minimalWASM = []byte{
	0x00, 0x61, 0x73, 0x6d,
	0x01, 0x00, 0x00, 0x00,
	0x01, 0x04, 0x01, 0x60, 0x00, 0x00,
	0x03, 0x02, 0x01, 0x00,
	0x07, 0x0a, 0x01, 0x06, 0x5f, 0x73, 0x74, 0x61, 0x72, 0x74, 0x00, 0x00,
	0x0a, 0x04, 0x01, 0x02, 0x00, 0x0b,
}

// loopingWASM is a valid WASM module whose _start function loops forever.
var loopingWASM = []byte{
	0x00, 0x61, 0x73, 0x6d,
	0x01, 0x00, 0x00, 0x00,
	0x01, 0x04, 0x01, 0x60, 0x00, 0x00,
	0x03, 0x02, 0x01, 0x00,
	0x07, 0x0a, 0x01, 0x06, 0x5f, 0x73, 0x74, 0x61, 0x72, 0x74, 0x00, 0x00,
	0x0a, 0x09, 0x01, 0x07, 0x00, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x0b,
}

func newTestManager(t *testing.T) *workload.Manager {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	casStore, err := cas.New(t.TempDir())
	require.NoError(t, err)

	rt, err := wasm.NewRuntime(ctx, wasm.RuntimeConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { rt.Close(context.Background()) })

	return workload.New(ctx, casStore, rt)
}

func TestSeedListUnseed(t *testing.T) {
	mgr := newTestManager(t)

	hash, err := mgr.Seed(minimalWASM)
	require.NoError(t, err)
	require.Len(t, hash, 64) // SHA-256 hex

	list := mgr.List()
	require.Len(t, list, 1)
	require.Equal(t, hash, list[0].Hash)

	err = mgr.Unseed(hash)
	require.NoError(t, err)

	list = mgr.List()
	require.Empty(t, list)
}

func TestSeedDuplicate(t *testing.T) {
	mgr := newTestManager(t)

	hash1, err := mgr.Seed(minimalWASM)
	require.NoError(t, err)

	// Wait for the first module to finish (trivial module exits immediately).
	time.Sleep(100 * time.Millisecond)

	hash2, err := mgr.Seed(minimalWASM)
	require.ErrorIs(t, err, workload.ErrAlreadyRunning)
	require.Equal(t, hash1, hash2)
}

func TestUnseedNotRunning(t *testing.T) {
	mgr := newTestManager(t)

	err := mgr.Unseed("0000000000000000000000000000000000000000000000000000000000000000")
	require.ErrorIs(t, err, workload.ErrNotRunning)
}

func TestResolvePrefix(t *testing.T) {
	mgr := newTestManager(t)

	hash, err := mgr.Seed(minimalWASM)
	require.NoError(t, err)

	resolved, err := mgr.ResolvePrefix(hash[:8])
	require.NoError(t, err)
	require.Equal(t, hash, resolved)
}

func TestResolvePrefixNotFound(t *testing.T) {
	mgr := newTestManager(t)

	_, err := mgr.ResolvePrefix("deadbeef")
	require.ErrorIs(t, err, workload.ErrNotRunning)
}

func TestClose(t *testing.T) {
	mgr := newTestManager(t)

	_, err := mgr.Seed(loopingWASM)
	require.NoError(t, err)

	mgr.Close()

	list := mgr.List()
	require.Empty(t, list)
}

func TestUnseedWaitsForShutdown(t *testing.T) {
	mgr := newTestManager(t)

	hash, err := mgr.Seed(loopingWASM)
	require.NoError(t, err)

	// The looping module runs forever. Verify it's still alive.
	time.Sleep(100 * time.Millisecond)
	list := mgr.List()
	require.Len(t, list, 1)

	// Unseed should stop the module and block until it exits.
	err = mgr.Unseed(hash)
	require.NoError(t, err)

	// After unseed, the workload should be gone from the list.
	require.Empty(t, mgr.List())
}
