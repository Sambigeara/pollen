// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/wasm"
)

type blobStore interface {
	Put(r io.Reader) (string, error)
	Get(hash string) (io.ReadCloser, error)
}

var (
	ErrAlreadyRunning = errors.New("workload already running")
	ErrNotRunning     = errors.New("workload not running")
	ErrStore          = errors.New("store blob")
	ErrCompile        = errors.New("compile module")
	ErrWorkloadFailed = errors.New("workload execution failed")
)

type WASMRuntime interface {
	Compile(ctx context.Context, wasmBytes []byte, hash string, cfg wasm.PluginConfig) error
	Call(ctx context.Context, hash, function string, input []byte) ([]byte, error)
	DropCompiled(ctx context.Context, hash string)
}

type Status int

const (
	StatusRunning Status = iota
)

func (s Status) String() string {
	if s == StatusRunning {
		return "running"
	}
	return "unknown"
}

type WorkloadSummary struct {
	CompiledAt      time.Time
	Hash            string
	Name            string
	Status          Status
	EffectiveTarget uint32
	Pressure        float64
}

type manager struct {
	blobs     blobStore
	runtime   WASMRuntime
	workloads map[string]*entry
	mu        sync.Mutex
}

type entry struct {
	compiledAt time.Time
}

func newManager(store blobStore, rt WASMRuntime) *manager {
	return &manager{
		blobs:     store,
		runtime:   rt,
		workloads: make(map[string]*entry),
	}
}

func (m *manager) Seed(ctx context.Context, wasmBytes []byte, cfg wasm.PluginConfig) (string, error) {
	hash, err := m.blobs.Put(bytes.NewReader(wasmBytes))
	if err != nil {
		return "", fmt.Errorf("workload: %w: %w", ErrStore, err)
	}
	return hash, m.compileAndRegister(ctx, wasmBytes, hash, cfg)
}

func (m *manager) SeedFromCAS(ctx context.Context, hash string, cfg wasm.PluginConfig) error {
	rc, err := m.blobs.Get(hash)
	if err != nil {
		return fmt.Errorf("workload: read blob: %w", err)
	}
	defer rc.Close()

	wasmBytes, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("workload: read blob bytes: %w", err)
	}
	return m.compileAndRegister(ctx, wasmBytes, hash, cfg)
}

func (m *manager) compileAndRegister(ctx context.Context, wasmBytes []byte, hash string, cfg wasm.PluginConfig) error {
	m.mu.Lock()
	if _, ok := m.workloads[hash]; ok {
		m.mu.Unlock()
		return ErrAlreadyRunning
	}
	m.mu.Unlock()

	if err := m.runtime.Compile(ctx, wasmBytes, hash, cfg); err != nil {
		return fmt.Errorf("workload: %w: %w", ErrCompile, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.workloads[hash] = &entry{compiledAt: time.Now()}
	return nil
}

func (m *manager) IsRunning(hash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.workloads[hash]
	return ok
}

func (m *manager) Call(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	if !m.IsRunning(hash) {
		return nil, ErrNotRunning
	}
	ctx = wasm.WithExecutingSeed(ctx, hash)
	ctx = wasm.WithExecutingFunction(ctx, function)
	out, err := m.runtime.Call(ctx, hash, function, input)
	if err != nil && errors.Is(err, wasm.ErrModuleMissing) {
		return nil, ErrNotRunning
	}
	return out, err
}

func (m *manager) Unseed(hash string) error {
	m.mu.Lock()
	if _, ok := m.workloads[hash]; !ok {
		m.mu.Unlock()
		return ErrNotRunning
	}
	delete(m.workloads, hash)
	m.mu.Unlock()

	m.runtime.DropCompiled(context.Background(), hash)
	return nil
}

func (m *manager) List() []WorkloadSummary {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]WorkloadSummary, 0, len(m.workloads))
	for hash, e := range m.workloads {
		out = append(out, WorkloadSummary{
			Hash:       hash,
			Status:     StatusRunning,
			CompiledAt: e.compiledAt,
		})
	}
	return out
}

func (m *manager) Close() {
	m.mu.Lock()
	snapshot := slices.Collect(maps.Keys(m.workloads))
	clear(m.workloads)
	m.mu.Unlock()

	for _, hash := range snapshot {
		m.runtime.DropCompiled(context.Background(), hash)
	}
}
