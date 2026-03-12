package wasm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

const defaultMemoryLimitPages = 256 // 16 MiB

// RuntimeConfig controls resource limits for the wazero runtime.
type RuntimeConfig struct {
	MemoryLimitPages uint32
}

// ModuleConfig controls per-instance execution parameters.
type ModuleConfig struct {
	Timeout time.Duration
}

// Runtime wraps a wazero runtime for compiling and running WASM modules.
// Compiled modules are cached by content hash for fast re-instantiation.
type Runtime struct {
	rt       wazero.Runtime
	compiled map[string]wazero.CompiledModule
	mu       sync.Mutex
}

// NewRuntime creates a wazero runtime with WASI support and memory limits.
func NewRuntime(ctx context.Context, cfg RuntimeConfig) (*Runtime, error) {
	memPages := cfg.MemoryLimitPages
	if memPages == 0 {
		memPages = defaultMemoryLimitPages
	}

	rtCfg := wazero.NewRuntimeConfig().
		WithMemoryLimitPages(memPages).
		WithCloseOnContextDone(true)
	rt := wazero.NewRuntimeWithConfig(ctx, rtCfg)
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		rt.Close(ctx)
		return nil, fmt.Errorf("wasm: instantiate WASI: %w", err)
	}
	return &Runtime{
		rt:       rt,
		compiled: make(map[string]wazero.CompiledModule),
	}, nil
}

// Instance represents a running WASM module.
type Instance struct {
	err  error
	done chan struct{}
	stop context.CancelFunc
	hash string
}

// Hash returns the content hash of this instance's module.
func (i *Instance) Hash() string { return i.hash }

// Done returns a channel that is closed when the instance exits.
func (i *Instance) Done() <-chan struct{} { return i.done }

// Err returns the error from the instance after Done is closed.
func (i *Instance) Err() error { return i.err }

// Wait blocks until the instance exits and returns any error.
func (i *Instance) Wait() error {
	<-i.done
	return i.err
}

// Stop cancels the instance's context, causing it to exit.
func (i *Instance) Stop() {
	i.stop()
}

// Compile compiles wasmBytes and caches the result under hash.
// Subsequent calls with the same hash return the cached module.
func (r *Runtime) Compile(ctx context.Context, wasmBytes []byte, hash string) (wazero.CompiledModule, error) {
	r.mu.Lock()
	if cm, ok := r.compiled[hash]; ok {
		r.mu.Unlock()
		return cm, nil
	}
	r.mu.Unlock()

	compiled, err := r.rt.CompileModule(ctx, wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("wasm: compile: %w", err)
	}

	r.mu.Lock()
	// Double-check: another goroutine may have compiled concurrently.
	if existing, ok := r.compiled[hash]; ok {
		r.mu.Unlock()
		compiled.Close(ctx)
		return existing, nil
	}
	r.compiled[hash] = compiled
	r.mu.Unlock()
	return compiled, nil
}

// DropCompiled closes and removes the cached compiled module for hash.
func (r *Runtime) DropCompiled(ctx context.Context, hash string) {
	r.mu.Lock()
	cm, ok := r.compiled[hash]
	if ok {
		delete(r.compiled, hash)
	}
	r.mu.Unlock()

	if ok {
		cm.Close(ctx)
	}
}

// Instantiate creates a running instance from an already-compiled module.
// The caller must Compile first.
func (r *Runtime) Instantiate(ctx context.Context, compiled wazero.CompiledModule, hash string, cfg ModuleConfig) *Instance {
	modCfg := wazero.NewModuleConfig().
		WithName("").
		WithStartFunctions("_start").
		WithStdout(discardWriter{}).
		WithStderr(discardWriter{})

	var (
		instCtx context.Context
		cancel  context.CancelFunc
	)
	if cfg.Timeout > 0 {
		instCtx, cancel = context.WithTimeout(ctx, cfg.Timeout)
	} else {
		instCtx, cancel = context.WithCancel(ctx)
	}

	inst := &Instance{
		hash: hash,
		done: make(chan struct{}),
		stop: cancel,
	}

	go func() {
		defer close(inst.done)
		mod, runErr := r.rt.InstantiateModule(instCtx, compiled, modCfg)
		if mod != nil {
			_ = mod.Close(instCtx)
		}
		inst.err = runErr
	}()

	return inst
}

// Close shuts down the wazero runtime and releases all compiled modules.
func (r *Runtime) Close(ctx context.Context) error {
	return r.rt.Close(ctx)
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
