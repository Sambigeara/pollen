package workload_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/sambigeara/pollen/pkg/workload"
)

var echoWASM []byte

func TestMain(m *testing.M) {
	data, err := os.ReadFile("../wasm/testdata/echo.wasm")
	if err != nil {
		panic("failed to load echo.wasm: " + err.Error())
	}
	echoWASM = data
	os.Exit(m.Run())
}

func newTestManager(t *testing.T) *workload.Manager {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	casStore, err := cas.New(t.TempDir())
	require.NoError(t, err)

	rt := wasm.NewRuntime(nil, 0)
	t.Cleanup(rt.Close)

	return workload.New(ctx, casStore, rt)
}

func TestSeedAndCall(t *testing.T) {
	mgr := newTestManager(t)

	hash, err := mgr.Seed(echoWASM, wasm.PluginConfig{})
	require.NoError(t, err)
	require.Len(t, hash, 64) // SHA-256 hex

	list := mgr.List()
	require.Len(t, list, 1)
	require.Equal(t, hash, list[0].Hash)

	out, err := mgr.Call(context.Background(), hash, "handle", []byte("hello"))
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), out)

	err = mgr.Unseed(hash)
	require.NoError(t, err)
	require.Empty(t, mgr.List())
}

func TestSeedDuplicate(t *testing.T) {
	mgr := newTestManager(t)

	hash1, err := mgr.Seed(echoWASM, wasm.PluginConfig{})
	require.NoError(t, err)

	hash2, err := mgr.Seed(echoWASM, wasm.PluginConfig{})
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

	hash, err := mgr.Seed(echoWASM, wasm.PluginConfig{})
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

	_, err := mgr.Seed(echoWASM, wasm.PluginConfig{})
	require.NoError(t, err)

	mgr.Close()
	require.Empty(t, mgr.List())
}

func TestCallUnseeded(t *testing.T) {
	mgr := newTestManager(t)

	hash, err := mgr.Seed(echoWASM, wasm.PluginConfig{})
	require.NoError(t, err)

	err = mgr.Unseed(hash)
	require.NoError(t, err)

	_, err = mgr.Call(context.Background(), hash, "handle", nil)
	require.Error(t, err)
}
