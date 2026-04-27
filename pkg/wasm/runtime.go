// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package wasm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	extism "github.com/extism/go-sdk"
	"github.com/tetratelabs/wazero"
	"go.uber.org/zap"
)

var ErrModuleMissing = errors.New("wasm: no compiled module")

const (
	wasmPageBytes           = 64 << 10
	defaultMemoryLimitPages = 1024 // 64 MiB — fits Go-wasm modules (min memory section is typically ~32 MiB).
	defaultTimeout          = 30 * time.Second
	hashPreviewLen          = 12
	defaultIdleTTL          = 60 * time.Second
	evictInterval           = 15 * time.Second
	// idleCacheSize bounds how many warm instances each workload keeps
	// around between calls. It is not a concurrency limit — that lives
	// in the placement layer's per-workload gate. When the cache is
	// full and a call returns, the surplus instance is closed.
	idleCacheSize = 1024
)

// PluginConfig controls per-workload resource limits.
type PluginConfig struct {
	MemoryPages uint32
	Timeout     time.Duration
}

// NewPluginConfig converts a byte-denominated memory limit into the page-based
// limit that the underlying WASM runtime expects. A zero value for either
// argument selects the package default.
func NewPluginConfig(memoryBytes uint64, timeout time.Duration) PluginConfig {
	var pages uint32
	switch memoryBytes {
	case 0:
		pages = defaultMemoryLimitPages
	default:
		p := min((memoryBytes+wasmPageBytes-1)/wasmPageBytes, math.MaxUint32)
		pages = uint32(p)
	}
	if timeout == 0 {
		timeout = defaultTimeout
	}
	return PluginConfig{MemoryPages: pages, Timeout: timeout}
}

// compiledEntry bundles a compiled plugin with a cache of warm instances.
// The cache is not a concurrency bound — placement gates that — it just
// avoids paying instance-creation cost on every call. Acquire returns a
// warm instance when one is available and instantiates a new one
// otherwise; release returns the instance to the cache up to
// idleCacheSize before closing the surplus.
type compiledEntry struct {
	plugin   *extism.CompiledPlugin
	pool     chan *extism.Plugin
	refs     sync.WaitGroup
	lastUsed atomic.Int64 // unix nanos
}

// Runtime wraps Extism for compiling and calling WASM plugins.
// Compiled plugins are cached by content hash; each hash maintains its
// own warm-instance cache. Concurrency limits live in the placement
// layer; the runtime always instantiates on demand when no warm
// instance is available.
type Runtime struct {
	runtimeConfig wazero.RuntimeConfig
	compiled      map[string]*compiledEntry
	evictDone     chan struct{}
	evictCancel   context.CancelFunc
	hostFuncs     []extism.HostFunction
	idleTTL       time.Duration
	mu            sync.Mutex
}

// RuntimeOption configures a Runtime at construction.
type RuntimeOption func(*Runtime)

// WithIdleTTL configures how long a workload's pooled instances persist with
// no activity before being closed. Zero disables the evictor.
func WithIdleTTL(d time.Duration) RuntimeOption {
	return func(r *Runtime) { r.idleTTL = d }
}

func NewRuntime(hostFuncs []extism.HostFunction, opts ...RuntimeOption) (*Runtime, error) {
	r := &Runtime{
		compiled:      make(map[string]*compiledEntry),
		hostFuncs:     hostFuncs,
		runtimeConfig: probeRuntimeConfig(),
		idleTTL:       defaultIdleTTL,
	}
	for _, o := range opts {
		o(r)
	}
	if r.idleTTL > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		r.evictCancel = cancel
		r.evictDone = make(chan struct{})
		go r.runEvictor(ctx)
	}
	return r, nil
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
	if _, ok := r.compiled[hash]; ok {
		r.mu.Unlock()
		compiled.Close(ctx)
		return nil
	}
	entry := &compiledEntry{
		plugin: compiled,
		pool:   make(chan *extism.Plugin, idleCacheSize),
	}
	entry.lastUsed.Store(time.Now().UnixNano())
	r.compiled[hash] = entry
	r.mu.Unlock()

	// Pre-warm one instance so the first call avoids cold-start latency.
	if inst, err := compiled.Instance(ctx, extism.PluginInstanceConfig{}); err == nil {
		select {
		case entry.pool <- inst:
		default:
			inst.Close(ctx)
		}
	}
	return nil
}

// Call invokes the named function on a warm instance from the workload's
// cache, or instantiates a fresh one if no warm instance is available.
// On success the instance is returned to the cache (up to idleCacheSize);
// on error it is discarded so no corrupted state is reused. Concurrency
// limits live in the placement layer's per-workload gate.
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

	plugin, err := entry.acquire(ctx)
	if err != nil {
		return nil, err
	}
	entry.lastUsed.Store(time.Now().UnixNano())

	_, output, err := plugin.CallWithContext(ctx, function, input)
	if err != nil {
		entry.discard(ctx, plugin)
		return nil, fmt.Errorf("wasm: call %s.%s: %w", hash[:min(hashPreviewLen, len(hash))], function, err)
	}

	entry.release(ctx, plugin)
	return output, nil
}

// acquire returns a warm instance from the cache or creates a new one.
// No concurrency limit lives here — instantiation is unbounded subject
// only to caller context cancellation. The placement gate is the
// authoritative concurrency boundary.
func (e *compiledEntry) acquire(ctx context.Context) (*extism.Plugin, error) {
	select {
	case p := <-e.pool:
		return p, nil
	default:
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("wasm: acquire instance: %w", err)
	}
	p, err := e.plugin.Instance(ctx, extism.PluginInstanceConfig{})
	if err != nil {
		return nil, fmt.Errorf("wasm: instantiate: %w", err)
	}
	return p, nil
}

func (e *compiledEntry) release(ctx context.Context, p *extism.Plugin) {
	select {
	case e.pool <- p:
	default:
		// Cache full — close the surplus instance.
		p.Close(ctx)
	}
}

func (e *compiledEntry) discard(ctx context.Context, p *extism.Plugin) {
	p.Close(ctx)
}

// runEvictor periodically drops pooled instances for workloads that have
// been idle for longer than idleTTL. Instances currently in flight are not
// touched (they're not in the pool).
func (r *Runtime) runEvictor(ctx context.Context) {
	defer close(r.evictDone)
	ticker := time.NewTicker(evictInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.evictIdle(ctx)
		}
	}
}

func (r *Runtime) evictIdle(ctx context.Context) {
	r.mu.Lock()
	entries := make([]*compiledEntry, 0, len(r.compiled))
	for _, e := range r.compiled {
		entries = append(entries, e)
	}
	r.mu.Unlock()

	threshold := time.Now().Add(-r.idleTTL).UnixNano()
	for _, e := range entries {
		if e.lastUsed.Load() > threshold {
			continue
		}
		evictPool(ctx, e)
	}
}

// evictPool closes all pooled instances in an entry so new invocations may
// recreate them on demand.
func evictPool(ctx context.Context, e *compiledEntry) {
	for {
		select {
		case p := <-e.pool:
			p.Close(ctx)
		default:
			return
		}
	}
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
		drainPool(ctx, entry.pool)
		entry.plugin.Close(ctx)
	}
}

func (r *Runtime) Close(ctx context.Context) {
	if r.evictCancel != nil {
		r.evictCancel()
		<-r.evictDone
	}

	r.mu.Lock()
	snapshot := r.compiled
	r.compiled = make(map[string]*compiledEntry)
	r.mu.Unlock()

	for _, entry := range snapshot {
		entry.refs.Wait()
		drainPool(ctx, entry.pool)
		entry.plugin.Close(ctx)
	}
}

// drainPool closes all pooled instances. For use during entry destruction
// (DropCompiled, Close).
func drainPool(ctx context.Context, pool chan *extism.Plugin) {
	for {
		select {
		case inst := <-pool:
			inst.Close(ctx)
		default:
			return
		}
	}
}
