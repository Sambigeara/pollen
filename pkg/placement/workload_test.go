package placement

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/wasm"
)

type noopRouter struct{}

func (noopRouter) RouteCall(context.Context, string, string, []byte) ([]byte, error) {
	return nil, fmt.Errorf("no routing in tests")
}

var echoWASM []byte

func TestMain(m *testing.M) {
	data, err := os.ReadFile("../wasm/testdata/echo.wasm")
	if err != nil {
		panic("failed to load echo.wasm: " + err.Error())
	}
	echoWASM = data
	os.Exit(m.Run())
}

func newTestManager(t *testing.T) *manager {
	t.Helper()

	casStore, err := cas.New(t.TempDir())
	require.NoError(t, err)

	hostFuncs := wasm.NewHostFunctions(zap.NewNop().Sugar(), noopRouter{})
	rt, err := wasm.NewRuntime(hostFuncs, 2)
	require.NoError(t, err)
	t.Cleanup(func() { rt.Close(context.Background()) })

	return newManager(casStore, rt)
}

func TestSeedAndCall(t *testing.T) {
	mgr := newTestManager(t)

	hash, err := mgr.Seed(context.Background(), echoWASM, wasm.NewPluginConfig(0, 0))
	require.NoError(t, err)
	require.Len(t, hash, 64)

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

	hash1, err := mgr.Seed(context.Background(), echoWASM, wasm.NewPluginConfig(0, 0))
	require.NoError(t, err)

	hash2, err := mgr.Seed(context.Background(), echoWASM, wasm.NewPluginConfig(0, 0))
	require.ErrorIs(t, err, ErrAlreadyRunning)
	require.Equal(t, hash1, hash2)
}

func TestUnseedNotRunning(t *testing.T) {
	mgr := newTestManager(t)

	err := mgr.Unseed("0000000000000000000000000000000000000000000000000000000000000000")
	require.ErrorIs(t, err, ErrNotRunning)
}

func TestClose(t *testing.T) {
	mgr := newTestManager(t)

	_, err := mgr.Seed(context.Background(), echoWASM, wasm.NewPluginConfig(0, 0))
	require.NoError(t, err)

	mgr.Close()
	require.Empty(t, mgr.List())
}

func TestCallUnseeded(t *testing.T) {
	mgr := newTestManager(t)

	hash, err := mgr.Seed(context.Background(), echoWASM, wasm.NewPluginConfig(0, 0))
	require.NoError(t, err)

	err = mgr.Unseed(hash)
	require.NoError(t, err)

	_, err = mgr.Call(context.Background(), hash, "handle", nil)
	require.Error(t, err)
}
