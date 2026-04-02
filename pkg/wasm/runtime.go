package wasm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	extism "github.com/extism/go-sdk"
	"github.com/tetratelabs/wazero"
	"go.uber.org/zap"
)

var ErrModuleMissing = errors.New("wasm: no compiled module")

const (
	defaultMemoryLimitPages = 256 // 16 MiB
	defaultTimeout          = 30 * time.Second
	hashPreviewLen          = 12
)

// PluginConfig controls per-workload resource limits.
type PluginConfig struct {
	MemoryPages uint32
	Timeout     time.Duration
}

func NewPluginConfig(memoryPages uint32, timeout time.Duration) PluginConfig {
	if memoryPages == 0 {
		memoryPages = defaultMemoryLimitPages
	}
	if timeout == 0 {
		timeout = defaultTimeout
	}
	return PluginConfig{MemoryPages: memoryPages, Timeout: timeout}
}

type compiledEntry struct {
	plugin *extism.CompiledPlugin
	refs   sync.WaitGroup
}

// Runtime wraps Extism for compiling and calling WASM plugins.
// Compiled plugins are cached by content hash for fast re-instantiation.
type Runtime struct {
	runtimeConfig wazero.RuntimeConfig
	compiled      map[string]*compiledEntry
	sem           chan struct{}
	hostFuncs     []extism.HostFunction
	mu            sync.Mutex
}

func NewRuntime(hostFuncs []extism.HostFunction, maxConcurrency int) (*Runtime, error) {
	if maxConcurrency <= 0 {
		return nil, fmt.Errorf("wasm: maxConcurrency must be > 0, got %d", maxConcurrency)
	}
	return &Runtime{
		compiled:      make(map[string]*compiledEntry),
		hostFuncs:     hostFuncs,
		runtimeConfig: probeRuntimeConfig(),
		sem:           make(chan struct{}, maxConcurrency),
	}, nil
}

// probeRuntimeConfig returns a wazero RuntimeConfig that works on this
// system. It tries the default config (JIT compiler on amd64/arm64) first;
// if the host blocks mmap(PROT_EXEC) — e.g. systemd MemoryDenyWriteExecute —
// it falls back to the interpreter.
func probeRuntimeConfig() wazero.RuntimeConfig {
	cfg := wazero.NewRuntimeConfig()
	if tryCompilerRuntime(cfg) {
		return cfg
	}
	zap.S().Warnw("wasm compiler unavailable, falling back to interpreter")
	return wazero.NewRuntimeConfigInterpreter()
}

func tryCompilerRuntime(cfg wazero.RuntimeConfig) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
		}
	}()
	// one cheap round-trip syscall to determine the supported runtime
	r := wazero.NewRuntimeWithConfig(context.Background(), cfg)
	r.Close(context.Background())
	return true
}

// Compile compiles wasmBytes and caches the result under hash.
func (r *Runtime) Compile(ctx context.Context, wasmBytes []byte, hash string, cfg PluginConfig) error {
	r.mu.Lock()
	if _, ok := r.compiled[hash]; ok {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	manifest := extism.Manifest{
		Wasm: []extism.Wasm{
			extism.WasmData{Data: wasmBytes},
		},
		Memory: &extism.ManifestMemory{
			MaxPages: cfg.MemoryPages,
		},
		Timeout: uint64(cfg.Timeout.Milliseconds()),
	}
	compiled, err := extism.NewCompiledPlugin(ctx, manifest, extism.PluginConfig{
		EnableWasi:    true,
		RuntimeConfig: r.runtimeConfig,
	}, r.hostFuncs)
	if err != nil {
		return fmt.Errorf("wasm: compile: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check: another goroutine may have compiled concurrently.
	if _, ok := r.compiled[hash]; ok {
		compiled.Close(ctx)
		return nil
	}
	r.compiled[hash] = &compiledEntry{plugin: compiled}
	return nil
}

// Call creates a fresh plugin instance from the cached compiled module,
// invokes the named function with input, and returns the output.
// Each call is isolated — thread-safe for concurrent use.
func (r *Runtime) Call(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	r.mu.Lock()
	entry, ok := r.compiled[hash]
	if !ok {
		r.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrModuleMissing, hash)
	}
	entry.refs.Add(1)
	r.mu.Unlock()
	defer entry.refs.Done()

	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("wasm: acquire slot: %w", ctx.Err())
	}
	defer func() { <-r.sem }()

	plugin, err := entry.plugin.Instance(ctx, extism.PluginInstanceConfig{})
	if err != nil {
		return nil, fmt.Errorf("wasm: instantiate %s: %w", hash, err)
	}
	defer plugin.Close(ctx)

	_, output, err := plugin.CallWithContext(ctx, function, input)
	if err != nil {
		return nil, fmt.Errorf("wasm: call %s.%s: %w", hash[:min(hashPreviewLen, len(hash))], function, err)
	}

	return output, nil
}

func (r *Runtime) DropCompiled(ctx context.Context, hash string) {
	r.mu.Lock()
	entry, ok := r.compiled[hash]
	if ok {
		delete(r.compiled, hash)
	}
	r.mu.Unlock()

	if ok {
		entry.refs.Wait()
		entry.plugin.Close(ctx)
	}
}

func (r *Runtime) Close(ctx context.Context) {
	r.mu.Lock()
	snapshot := r.compiled
	r.compiled = make(map[string]*compiledEntry)
	r.mu.Unlock()

	for _, entry := range snapshot {
		entry.refs.Wait()
		entry.plugin.Close(ctx)
	}
}
