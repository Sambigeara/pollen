package workload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/wasm"
)

var (
	ErrAlreadyRunning  = errors.New("workload already running")
	ErrNotRunning      = errors.New("workload not running")
	ErrAmbiguousPrefix = errors.New("ambiguous prefix")
	ErrStore           = errors.New("store artifact")
	ErrCompile         = errors.New("compile module")
)

// Status describes the lifecycle state of a workload.
type Status int

const (
	StatusRunning Status = iota
	StatusStopped
	StatusErrored
)

func (s Status) String() string {
	switch s {
	case StatusRunning:
		return "running"
	case StatusStopped:
		return "stopped"
	case StatusErrored:
		return "errored"
	}
	return "unknown"
}

// Summary is a snapshot of a workload for status reporting.
type Summary struct {
	CompiledAt time.Time
	Hash       string
	Status     Status
}

// Manager orchestrates workload lifecycle: seed (store + compile) and unseed (drop).
// Workloads are compiled once and invoked on demand — no long-running instances.
type Manager struct {
	ctx       context.Context
	cas       *cas.Store
	runtime   *wasm.Runtime
	workloads map[string]*entry
	mu        sync.Mutex
}

type entry struct {
	compiledAt time.Time
	config     wasm.PluginConfig
}

// New creates a workload manager backed by the given CAS store and WASM runtime.
func New(ctx context.Context, store *cas.Store, rt *wasm.Runtime) *Manager {
	return &Manager{
		cas:       store,
		runtime:   rt,
		ctx:       ctx,
		workloads: make(map[string]*entry),
	}
}

// Seed stores wasmBytes in the CAS and compiles the module for invocation.
// Returns the content hash. If the workload is already running with the same
// config, returns ErrAlreadyRunning. If the config differs, the module is
// recompiled with the new config.
func (m *Manager) Seed(wasmBytes []byte, cfg wasm.PluginConfig) (string, error) {
	hash, err := m.cas.Put(bytes.NewReader(wasmBytes))
	if err != nil {
		return "", fmt.Errorf("workload: %w: %w", ErrStore, err)
	}

	m.mu.Lock()
	if e, ok := m.workloads[hash]; ok && e.config == cfg {
		m.mu.Unlock()
		return hash, ErrAlreadyRunning
	}
	m.mu.Unlock()

	if err := m.runtime.Compile(m.ctx, wasmBytes, hash, cfg); err != nil {
		return hash, fmt.Errorf("workload: %w: %w", ErrCompile, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.workloads[hash] = &entry{
		compiledAt: time.Now(),
		config:     cfg,
	}
	return hash, nil
}

// SeedFromCAS reads WASM bytes from the CAS and compiles the module.
// If the workload is already running with the same config, returns
// ErrAlreadyRunning. If the config differs, recompiles with the new config.
func (m *Manager) SeedFromCAS(hash string, cfg wasm.PluginConfig) error {
	m.mu.Lock()
	if e, ok := m.workloads[hash]; ok && e.config == cfg {
		m.mu.Unlock()
		return ErrAlreadyRunning
	}
	m.mu.Unlock()

	rc, err := m.cas.Get(hash)
	if err != nil {
		return fmt.Errorf("workload: read CAS: %w", err)
	}
	wasmBytes, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("workload: read CAS bytes: %w", err)
	}

	if err := m.runtime.Compile(m.ctx, wasmBytes, hash, cfg); err != nil {
		return fmt.Errorf("workload: %w: %w", ErrCompile, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.workloads[hash] = &entry{
		compiledAt: time.Now(),
		config:     cfg,
	}
	return nil
}

// IsRunning reports whether a workload with the given hash is compiled and available.
func (m *Manager) IsRunning(hash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.workloads[hash]
	return ok
}

// Call invokes a function on a compiled workload.
func (m *Manager) Call(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	if !m.IsRunning(hash) {
		return nil, ErrNotRunning
	}
	out, err := m.runtime.Call(ctx, hash, function, input)
	if err != nil && errors.Is(err, wasm.ErrModuleMissing) {
		return nil, ErrNotRunning
	}
	return out, err
}

// Unseed removes a workload and drops its compiled module.
func (m *Manager) Unseed(hash string) error {
	m.mu.Lock()
	_, ok := m.workloads[hash]
	if !ok {
		m.mu.Unlock()
		return ErrNotRunning
	}
	delete(m.workloads, hash)
	m.mu.Unlock()

	m.runtime.DropCompiled(hash)
	return nil
}

// ResolvePrefix resolves a hash prefix to a full hash. Returns an error if
// the prefix is ambiguous or not found.
func (m *Manager) ResolvePrefix(prefix string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var matches []string
	for h := range m.workloads {
		if len(h) >= len(prefix) && h[:len(prefix)] == prefix {
			matches = append(matches, h)
		}
	}
	switch len(matches) {
	case 0:
		return "", ErrNotRunning
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("%w: %q matches %d workloads", ErrAmbiguousPrefix, prefix, len(matches))
	}
}

// List returns a snapshot of all workloads.
func (m *Manager) List() []Summary {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]Summary, 0, len(m.workloads))
	for hash, e := range m.workloads {
		out = append(out, Summary{
			Hash:       hash,
			Status:     StatusRunning,
			CompiledAt: e.compiledAt,
		})
	}
	return out
}

// Close drops all compiled workloads.
func (m *Manager) Close() {
	m.mu.Lock()
	snapshot := make([]string, 0, len(m.workloads))
	for hash := range m.workloads {
		snapshot = append(snapshot, hash)
	}
	clear(m.workloads)
	m.mu.Unlock()

	for _, hash := range snapshot {
		m.runtime.DropCompiled(hash)
	}
}
