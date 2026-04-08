package placement

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/sysinfo"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"go.uber.org/zap"
)

const (
	workloadInvocationTimeout = 60 * time.Second
	resourceSampleInterval    = 5 * time.Second
)

type PlacementAPI interface {
	Start(ctx context.Context) error
	Stop() error

	Seed(name, hash string, binary []byte, replicas, memoryPages, timeoutMs uint32, spread float32) error
	Unseed(hash string) error
	Call(ctx context.Context, hash, function string, input []byte) ([]byte, error)
	Status() []WorkloadSummary
	PlacementInfo() map[string][2]float64

	HandleArtifactStream(stream io.ReadWriteCloser, peer types.PeerKey)
	HandleWorkloadStream(stream io.ReadWriteCloser, peer types.PeerKey)

	Signal()
}

var _ PlacementAPI = (*Service)(nil)

type WorkloadState interface {
	Snapshot() state.Snapshot
	SetWorkloadSpec(name, hash string, replicas, memoryPages, timeoutMs uint32, spread float32) []state.Event
	ClaimWorkload(hash string) []state.Event
	ReleaseWorkload(hash string) []state.Event
	SetLocalResources(cpu, mem float64, memTotalBytes uint64, numCPU, cpuBudgetPct, memBudgetPct uint32) []state.Event
	SetSeedLoad(rates map[string]float32) []state.Event
	SetSeedDemand(rates map[string]float32) []state.Event
}

type StreamOpener interface {
	OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error)
}

type Service struct {
	store            WorkloadState
	mesh             StreamOpener
	ctx              context.Context
	manager          *manager
	reconciler       *reconciler
	cas              *cas.Store
	log              *zap.SugaredLogger
	cancel           context.CancelFunc
	utilisation      *utilisationTracker
	latency          *latencyTracker
	wg               sync.WaitGroup
	localID          types.PeerKey
	cpuBudgetPercent uint32
	memBudgetPercent uint32
}

type Option func(*Service)

func WithLogger(log *zap.SugaredLogger) Option {
	return func(s *Service) { s.log = log }
}

func WithMesh(mesh StreamOpener) Option {
	return func(s *Service) { s.mesh = mesh }
}

func WithResourceBudget(cpuPercent, memPercent uint32) Option {
	return func(s *Service) {
		s.cpuBudgetPercent = cpuPercent
		s.memBudgetPercent = memPercent
	}
}

func New(self types.PeerKey, store WorkloadState, casStore *cas.Store, runtime WASMRuntime, opts ...Option) *Service {
	s := &Service{
		localID: self,
		store:   store,
		cas:     casStore,
		log:     zap.NewNop().Sugar(),
	}
	for _, o := range opts {
		o(s)
	}
	s.manager = newManager(casStore, runtime)
	s.utilisation = newUtilisationTracker()
	s.latency = newLatencyTracker()
	return s
}

func (s *Service) Start(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(ctx)
	s.ctx = ctx

	s.reconciler = newReconciler(
		s.localID,
		s.store,
		s.manager,
		s.cas,
		newArtifactFetcher(s.mesh, s.cas),
		s.utilisation,
		s.log.Named("scheduler"),
		&s.wg,
	)

	s.wg.Add(3) //nolint:mnd
	go func() {
		defer s.wg.Done()
		s.reconciler.Run(ctx)
	}()
	go func() {
		defer s.wg.Done()
		s.runResourceTicker(ctx)
	}()
	go func() {
		defer s.wg.Done()
		s.utilisation.run(ctx)
	}()

	return nil
}

func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.manager.Close()
	return nil
}

func (s *Service) runResourceTicker(ctx context.Context) {
	ticker := time.NewTicker(resourceSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cpuPct, memPct, memTotal, numCPU := sysinfo.Sample()
			s.store.SetLocalResources(float64(cpuPct), float64(memPct), memTotal, numCPU, s.cpuBudgetPercent, s.memBudgetPercent)
		}
	}
}

func (s *Service) Seed(name, hash string, binary []byte, replicas, memoryPages, timeoutMs uint32, spread float32) error {
	cfg := wasm.NewPluginConfig(memoryPages, time.Duration(timeoutMs)*time.Millisecond)
	gotHash, err := s.manager.Seed(s.ctx, binary, cfg)
	if err != nil && !errors.Is(err, ErrAlreadyRunning) {
		return err
	}
	if gotHash != hash {
		return fmt.Errorf("hash mismatch: expected %s, got %s", hash, gotHash)
	}
	if replicas == 0 {
		replicas = 1
	}

	// Supersede: if this peer already has a spec with the same name but
	// a different hash, unseed the old one first.
	if oldHash, ok := s.store.Snapshot().LocalSpecByName(name, s.localID); ok && oldHash != hash {
		if s.manager.IsRunning(oldHash) {
			_ = s.manager.Unseed(oldHash)
		}
		s.store.ReleaseWorkload(oldHash)
		s.store.SetWorkloadSpec(name, oldHash, 0, 0, 0, 0)
		s.utilisation.Clear(oldHash)
	}

	s.store.SetWorkloadSpec(name, hash, replicas, memoryPages, timeoutMs, spread)
	s.store.ClaimWorkload(hash)
	return nil
}

func (s *Service) Unseed(hash string) error {
	name, hash := s.resolveLocalFirst(hash)
	if err := s.manager.Unseed(hash); err != nil {
		return err
	}
	s.store.ReleaseWorkload(hash)
	s.store.SetWorkloadSpec(name, hash, 0, 0, 0, 0)
	s.utilisation.Clear(hash)
	return nil
}

func (s *Service) Call(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	hash = s.resolveGlobal(hash)

	locallyRunning := s.manager.IsRunning(hash)
	snap := s.store.Snapshot()
	claimants := snap.Claims[hash]

	// Record demand for every incoming call regardless of routing or claimant
	// availability. This keeps the demand signal alive during outages and when
	// serving locally, so the localDemandScore bonus persists. Only track
	// hashes with a known spec to avoid bloating the tracker.
	if _, hasSpec := snap.Specs[hash]; hasSpec {
		s.utilisation.RecordDemand(hash)
	}

	if !locallyRunning && len(claimants) == 0 {
		return nil, fmt.Errorf("no node claims workload %s: %w", shortHash(hash), ErrNotRunning)
	}

	target, isLocal := pickP2C(s.localID, locallyRunning, claimants, s.latency, hash)

	if isLocal {
		start := time.Now()
		out, err := s.manager.Call(ctx, hash, function, input)
		s.latency.Record(s.localID, hash, float64(time.Since(start).Milliseconds()))
		if !errors.Is(err, ErrNotRunning) {
			s.utilisation.RecordServed(hash)
		}
		return out, err
	}

	out, err := s.forwardCall(ctx, target, hash, function, input)
	if err == nil {
		return out, nil
	}
	if errors.Is(err, ErrWorkloadFailed) {
		return nil, err
	}

	// Fallback: try remaining claimants in shuffled order.
	for _, fallback := range shuffledClaimants(claimants, target) {
		if fallback == s.localID && locallyRunning {
			start := time.Now()
			out, err = s.manager.Call(ctx, hash, function, input)
			s.latency.Record(s.localID, hash, float64(time.Since(start).Milliseconds()))
			if !errors.Is(err, ErrNotRunning) {
				s.utilisation.RecordServed(hash)
			}
		} else {
			out, err = s.forwardCall(ctx, fallback, hash, function, input)
		}
		if err == nil {
			return out, nil
		}
		if errors.Is(err, ErrWorkloadFailed) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("all %d claimants failed for %s: %w", len(claimants), shortHash(hash), err)
}

func (s *Service) forwardCall(ctx context.Context, target types.PeerKey, hash, function string, input []byte) ([]byte, error) {
	stream, err := s.mesh.OpenStream(ctx, target, transport.StreamTypeWorkload)
	if err != nil {
		s.log.Debugw("workload stream failed", "target", target.Short(), "hash", shortHash(hash), "err", err)
		return nil, err
	}

	start := time.Now()
	out, err := invokeOverStream(ctx, stream, hash, function, input)
	if err != nil {
		s.log.Warnw("workload invocation failed", "target", target.Short(), "hash", shortHash(hash), "err", err)
		return nil, err
	}
	s.latency.Record(target, hash, float64(time.Since(start).Milliseconds()))
	return out, nil
}

func (s *Service) Status() []WorkloadSummary {
	summaries := s.manager.List()
	snap := s.store.Snapshot()
	pinfo := s.reconciler.allPlacementInfo()
	for i := range summaries {
		for _, sv := range snap.Specs {
			if sv.Spec.GetHash() == summaries[i].Hash {
				summaries[i].Name = sv.Spec.GetName()
				break
			}
		}
		if info, ok := pinfo[summaries[i].Hash]; ok {
			summaries[i].EffectiveTarget = uint32(info[0])
			summaries[i].Pressure = info[1]
		}
	}
	return summaries
}

// PlacementInfo returns effective target and pressure for all tracked hashes.
func (s *Service) PlacementInfo() map[string][2]float64 {
	return s.reconciler.allPlacementInfo()
}

func (s *Service) HandleArtifactStream(stream io.ReadWriteCloser, _ types.PeerKey) {
	handleArtifactStream(stream, s.cas)
}

func (s *Service) HandleWorkloadStream(stream io.ReadWriteCloser, peer types.PeerKey) {
	handleWorkloadStream(s.ctx, stream, peer, &trackedInvoker{inner: s.manager, ut: s.utilisation}, workloadInvocationTimeout)
}

type trackedInvoker struct {
	inner workloadInvoker
	ut    *utilisationTracker
}

func (t *trackedInvoker) Call(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	out, err := t.inner.Call(ctx, hash, function, input)
	if !errors.Is(err, ErrNotRunning) {
		t.ut.RecordServed(hash)
	}
	return out, err
}

func (s *Service) Signal() {
	s.reconciler.Signal()
}

// resolveLocalFirst resolves an identifier (name or hash prefix) for local
// operations like unseed. Prefers the local peer's spec when resolving
// by name, so operators can manage their own seeds even when another
// peer publishes the same name.
func (s *Service) resolveLocalFirst(identifier string) (string, string) {
	snap := s.store.Snapshot()

	// Name match (local peer first)
	if hash, ok := snap.LocalSpecByName(identifier, s.localID); ok {
		return identifier, hash
	}

	// Fall back to global hash resolution
	hash := s.resolveHashPrefix(identifier, snap)
	name := identifier
	for _, sv := range snap.Specs {
		if sv.Spec.GetHash() == hash {
			name = sv.Spec.GetName()
			break
		}
	}
	return name, hash
}

// resolveGlobal resolves an identifier (name or hash prefix) for global
// operations like call. Prefers the canonical name winner (lowest PeerKey)
// for routing to the best replica.
func (s *Service) resolveGlobal(identifier string) string {
	snap := s.store.Snapshot()

	// Name match (canonical winner)
	if hash, _, ok := snap.SpecByName(identifier); ok {
		return hash
	}

	return s.resolveHashPrefix(identifier, snap)
}

func (s *Service) resolveHashPrefix(prefix string, snap state.Snapshot) string {
	if s.manager.IsRunning(prefix) {
		return prefix
	}
	if _, ok := snap.Claims[prefix]; ok {
		return prefix
	}
	var match string
	for h := range snap.Specs {
		if strings.HasPrefix(h, prefix) {
			if match != "" && match != h {
				return prefix
			}
			match = h
		}
	}
	if match != "" {
		return match
	}
	return prefix
}

func shortHash(h string) string {
	const n = 12
	if len(h) <= n {
		return h
	}
	return h[:n]
}
