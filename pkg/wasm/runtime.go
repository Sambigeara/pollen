package wasm

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	extism "github.com/extism/go-sdk"
	"github.com/tetratelabs/wazero"
	"golang.org/x/sync/semaphore"
)

// ErrModuleMissing is returned when Call is invoked with a hash that has no
// compiled module in the cache.
var ErrModuleMissing = errors.New("wasm: no compiled module")

const (
	defaultMemoryLimitPages = 256 // 16 MiB
	defaultTimeout          = 30 * time.Second
	hashPreviewLen          = 12
)

// PluginConfig controls per-workload resource limits.
type PluginConfig struct {
	MemoryPages uint32        // 0 → default (256 = 16 MiB)
	Timeout     time.Duration // 0 → default (30s)
}

func (c PluginConfig) memoryPages() uint32 {
	if c.MemoryPages == 0 {
		return defaultMemoryLimitPages
	}
	return c.MemoryPages
}

func (c PluginConfig) timeout() time.Duration {
	if c.Timeout == 0 {
		return defaultTimeout
	}
	return c.Timeout
}

// Runtime wraps Extism for compiling and calling WASM plugins.
// Compiled plugins are cached by content hash for fast re-instantiation.
type Runtime struct {
	runtimeConfig wazero.RuntimeConfig
	compiled      map[string]*extism.CompiledPlugin
	configs       map[string]PluginConfig
	sem           *semaphore.Weighted
	hostFuncs     []extism.HostFunction
	mu            sync.Mutex
}

// NewRuntime creates an Extism-backed runtime with the given host functions.
// maxConcurrency limits the number of simultaneous WASM plugin instances;
// 0 defaults to GOMAXPROCS.
func NewRuntime(hostFuncs []extism.HostFunction, maxConcurrency int) *Runtime {
	if maxConcurrency <= 0 {
		maxConcurrency = max(1, runtime.GOMAXPROCS(0))
	}
	return &Runtime{
		compiled:      make(map[string]*extism.CompiledPlugin),
		configs:       make(map[string]PluginConfig),
		hostFuncs:     hostFuncs,
		runtimeConfig: probeRuntimeConfig(),
		sem:           semaphore.NewWeighted(int64(maxConcurrency)),
	}
}

// probeRuntimeConfig returns a wazero RuntimeConfig that works on this
// system. It tries the default config (JIT compiler on amd64/arm64) first;
// if the host blocks mmap(PROT_EXEC) — e.g. systemd MemoryDenyWriteExecute —
// it falls back to the interpreter.
func probeRuntimeConfig() wazero.RuntimeConfig {
	cfg := wazero.NewRuntimeConfig()
	if tryRuntime(cfg) {
		return cfg
	}
	return wazero.NewRuntimeConfigInterpreter()
}

func tryRuntime(cfg wazero.RuntimeConfig) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
		}
	}()
	r := wazero.NewRuntimeWithConfig(context.Background(), cfg)
	r.Close(context.Background())
	return true
}

// IsCompiled reports whether a module with the given hash is cached.
func (r *Runtime) IsCompiled(hash string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.compiled[hash]
	return ok
}

// Compile compiles wasmBytes and caches the result under hash.
// If the hash is already compiled with a different config, the old module is
// dropped and recompiled with the new config.
func (r *Runtime) Compile(ctx context.Context, wasmBytes []byte, hash string, cfg PluginConfig) error {
	r.mu.Lock()
	if existing, ok := r.compiled[hash]; ok {
		if r.configs[hash] == cfg {
			r.mu.Unlock()
			return nil
		}
		// Config changed — drop old module and recompile.
		delete(r.compiled, hash)
		delete(r.configs, hash)
		r.mu.Unlock()
		existing.Close(ctx)
	} else {
		r.mu.Unlock()
	}

	manifest := extism.Manifest{
		Wasm: []extism.Wasm{
			extism.WasmData{Data: wasmBytes},
		},
		Memory: &extism.ManifestMemory{
			MaxPages: cfg.memoryPages(),
		},
		Timeout: uint64(cfg.timeout().Milliseconds()),
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
	if existing, ok := r.compiled[hash]; ok {
		compiled.Close(ctx)
		_ = existing // keep the first one
		return nil
	}
	r.compiled[hash] = compiled
	r.configs[hash] = cfg
	return nil
}

// Call creates a fresh plugin instance from the cached compiled module,
// invokes the named function with input, and returns the output.
// Each call is isolated — thread-safe for concurrent use.
func (r *Runtime) Call(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	r.mu.Lock()
	compiled, ok := r.compiled[hash]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrModuleMissing, hash)
	}

	if err := r.sem.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("wasm: acquire slot: %w", err)
	}
	defer r.sem.Release(1)

	plugin, err := compiled.Instance(ctx, extism.PluginInstanceConfig{})
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

// DropCompiled closes and removes the cached compiled module for hash.
func (r *Runtime) DropCompiled(hash string) {
	r.mu.Lock()
	cp, ok := r.compiled[hash]
	if ok {
		delete(r.compiled, hash)
		delete(r.configs, hash)
	}
	r.mu.Unlock()

	if ok {
		cp.Close(context.Background())
	}
}

// Close shuts down all compiled plugins.
func (r *Runtime) Close() {
	r.mu.Lock()
	snapshot := r.compiled
	r.compiled = make(map[string]*extism.CompiledPlugin)
	r.configs = make(map[string]PluginConfig)
	r.mu.Unlock()

	ctx := context.Background()
	for _, cp := range snapshot {
		cp.Close(ctx)
	}
}
