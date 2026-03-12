package workload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// Summary is a snapshot of a running workload for status reporting.
type Summary struct {
	StartedAt time.Time
	Hash      string
	Status    Status
}

// Manager orchestrates workload lifecycle: seed (store + run) and unseed (stop + remove).
type Manager struct {
	ctx       context.Context
	cas       *cas.Store
	runtime   *wasm.Runtime
	workloads map[string]*entry
	mu        sync.Mutex
}

type entry struct {
	instance  *wasm.Instance
	startedAt time.Time
	stopping  bool
}

// New creates a workload manager backed by the given CAS store and WASM runtime.
// The context controls the lifetime of all spawned workloads.
func New(ctx context.Context, store *cas.Store, rt *wasm.Runtime) *Manager {
	return &Manager{
		cas:       store,
		runtime:   rt,
		ctx:       ctx,
		workloads: make(map[string]*entry),
	}
}

// Seed stores wasmBytes in the CAS, compiles the module, and starts execution.
// Returns the content hash. Reuses the cached compiled module if available.
func (m *Manager) Seed(wasmBytes []byte) (string, error) {
	hash, err := m.cas.Put(bytes.NewReader(wasmBytes))
	if err != nil {
		return "", fmt.Errorf("workload: %w: %w", ErrStore, err)
	}

	compiled, err := m.runtime.Compile(m.ctx, wasmBytes, hash)
	if err != nil {
		return hash, fmt.Errorf("workload: %w: %w", ErrCompile, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.workloads[hash]; ok {
		return hash, ErrAlreadyRunning
	}

	inst := m.runtime.Instantiate(m.ctx, compiled, hash, wasm.ModuleConfig{})
	m.workloads[hash] = &entry{
		instance:  inst,
		startedAt: time.Now(),
	}
	return hash, nil
}

// Unseed stops a running workload, waits for it to exit, and removes it.
func (m *Manager) Unseed(hash string) error {
	m.mu.Lock()
	e, ok := m.workloads[hash]
	if !ok || e.stopping {
		m.mu.Unlock()
		return ErrNotRunning
	}
	e.stopping = true
	m.mu.Unlock()

	e.instance.Stop()
	e.instance.Wait() //nolint:errcheck

	m.mu.Lock()
	delete(m.workloads, hash)
	m.mu.Unlock()

	m.runtime.DropCompiled(m.ctx, hash)
	return nil
}

// ResolvePrefix resolves a hash prefix to a full hash. Returns an error if
// the prefix is ambiguous or not found.
func (m *Manager) ResolvePrefix(prefix string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var matches []string
	for h, e := range m.workloads {
		if e.stopping {
			continue
		}
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
			Hash:      hash,
			Status:    workloadStatus(e),
			StartedAt: e.startedAt,
		})
	}
	return out
}

// Close stops all running workloads and waits for them to exit.
func (m *Manager) Close() {
	m.mu.Lock()
	snapshot := make(map[string]*entry, len(m.workloads))
	for hash, e := range m.workloads {
		e.stopping = true
		snapshot[hash] = e
	}
	m.mu.Unlock()

	for hash, e := range snapshot {
		e.instance.Stop()
		e.instance.Wait() //nolint:errcheck
		m.runtime.DropCompiled(m.ctx, hash)
	}

	m.mu.Lock()
	clear(m.workloads)
	m.mu.Unlock()
}

func workloadStatus(e *entry) Status {
	select {
	case <-e.instance.Done():
		if e.instance.Err() != nil {
			return StatusErrored
		}
		return StatusStopped
	default:
		return StatusRunning
	}
}
