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
)

var ErrModuleMissing = errors.New("wasm: no compiled module")

const (
	wasmPageBytes           = 64 << 10
	defaultMemoryLimitPages = 128 // 8 MiB — fits Zig and Rust wasi guests; Go-wasm guests should declare a higher cap via the spec.
	defaultTimeout          = 30 * time.Second
	hashPreviewLen          = 12
	defaultIdleTTL          = 60 * time.Second
	evictInterval           = 15 * time.Second
)

const IdleCacheSize = 4

type PluginConfig struct {
	MemoryPages uint32
	Timeout     time.Duration
}

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

type compiledEntry struct {
	plugin   *extism.CompiledPlugin
	pool     chan *extism.Plugin
	refs     sync.WaitGroup
	lastUsed atomic.Int64 // unix nanos
}

type Runtime struct {
	runtimeConfig wazero.RuntimeConfig
	compiled      map[string]*compiledEntry
	evictDone     chan struct{}
	evictCancel   context.CancelFunc
	hostFuncs     []extism.HostFunction
	idleTTL       time.Duration
	mu            sync.Mutex
}

type RuntimeOption func(*Runtime)

func WithIdleTTL(d time.Duration) RuntimeOption {
	return func(r *Runtime) { r.idleTTL = d }
}

func NewRuntime(hostFuncs []extism.HostFunction, opts ...RuntimeOption) (*Runtime, error) {
	r := &Runtime{
		compiled:      make(map[string]*compiledEntry),
		hostFuncs:     hostFuncs,
		runtimeConfig: wazero.NewRuntimeConfig(),
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
		pool:   make(chan *extism.Plugin, IdleCacheSize),
	}
	entry.lastUsed.Store(time.Now().UnixNano())
	r.compiled[hash] = entry
	r.mu.Unlock()

	if inst, err := compiled.Instance(ctx, extism.PluginInstanceConfig{}); err == nil {
		select {
		case entry.pool <- inst:
		default:
			inst.Close(ctx)
		}
	}
	return nil
}

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
		p.Close(ctx)
	}
}

func (e *compiledEntry) discard(ctx context.Context, p *extism.Plugin) {
	p.Close(ctx)
}

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
