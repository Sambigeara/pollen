package placement

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
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

	Seed(hash string, binary []byte, replicas, memoryPages, timeoutMs uint32) error
	Unseed(hash string) error
	Call(ctx context.Context, hash, function string, input []byte) ([]byte, error)
	Status() []WorkloadSummary

	HandleArtifactStream(stream io.ReadWriteCloser, peer types.PeerKey)
	HandleWorkloadStream(stream io.ReadWriteCloser, peer types.PeerKey)

	Signal()
}

var _ PlacementAPI = (*Service)(nil)

type WorkloadState interface {
	Snapshot() state.Snapshot
	SetWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) []state.Event
	ClaimWorkload(hash string) []state.Event
	ReleaseWorkload(hash string) []state.Event
	SetLocalResources(cpu, mem float64) []state.Event
}

type StreamOpener interface {
	OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error)
}

type Service struct {
	store      WorkloadState
	mesh       StreamOpener
	ctx        context.Context
	manager    *manager
	reconciler *reconciler
	cas        *cas.Store
	log        *zap.SugaredLogger
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	localID    types.PeerKey
}

type Option func(*Service)

func WithLogger(log *zap.SugaredLogger) Option {
	return func(s *Service) { s.log = log }
}

func WithMesh(mesh StreamOpener) Option {
	return func(s *Service) { s.mesh = mesh }
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
		s.log.Named("scheduler"),
		&s.wg,
	)

	s.wg.Add(2) //nolint:mnd
	go func() {
		defer s.wg.Done()
		s.reconciler.Run(ctx)
	}()
	go func() {
		defer s.wg.Done()
		s.runResourceTicker(ctx)
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
			cpuPct, memPct, _, _ := sysinfo.Sample()
			s.store.SetLocalResources(float64(cpuPct), float64(memPct))
		}
	}
}

func (s *Service) Seed(hash string, binary []byte, replicas, memoryPages, timeoutMs uint32) error {
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
	s.store.SetWorkloadSpec(hash, replicas, memoryPages, timeoutMs)
	s.store.ClaimWorkload(hash)
	return nil
}

func (s *Service) Unseed(hash string) error {
	hash = s.resolveHash(hash)
	if err := s.manager.Unseed(hash); err != nil {
		return err
	}
	s.store.ReleaseWorkload(hash)
	s.store.SetWorkloadSpec(hash, 0, 0, 0)
	return nil
}

func (s *Service) Call(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	hash = s.resolveHash(hash)

	if s.manager.IsRunning(hash) {
		return s.manager.Call(ctx, hash, function, input)
	}

	snap := s.store.Snapshot()
	claimants := snap.Claims[hash]
	if len(claimants) == 0 {
		return nil, fmt.Errorf("no node claims workload %s: %w", shortHash(hash), ErrNotRunning)
	}

	sorted := sortedClaimants(claimants)
	var lastErr error
	for _, target := range sorted {
		stream, err := s.mesh.OpenStream(ctx, target, transport.StreamTypeWorkload)
		if err != nil {
			s.log.Debugw("workload stream failed, trying next claimant", "target", target.Short(), "hash", shortHash(hash), "err", err)
			lastErr = err
			continue
		}

		out, err := invokeOverStream(ctx, stream, hash, function, input)
		if err != nil {
			if errors.Is(err, ErrWorkloadFailed) {
				return nil, err
			}
			s.log.Warnw("workload invocation failed, trying next claimant", "target", target.Short(), "hash", shortHash(hash), "err", err)
			lastErr = err
			continue
		}
		return out, nil
	}
	return nil, fmt.Errorf("all %d claimants failed for %s: %w", len(sorted), shortHash(hash), lastErr)
}

func (s *Service) Status() []WorkloadSummary {
	return s.manager.List()
}

func (s *Service) HandleArtifactStream(stream io.ReadWriteCloser, _ types.PeerKey) {
	handleArtifactStream(stream, s.cas)
}

func (s *Service) HandleWorkloadStream(stream io.ReadWriteCloser, _ types.PeerKey) {
	handleWorkloadStream(s.ctx, stream, s.manager, workloadInvocationTimeout)
}

func (s *Service) Signal() {
	s.reconciler.Signal()
}

func (s *Service) resolveHash(prefix string) string {
	if s.manager.IsRunning(prefix) {
		return prefix
	}
	snap := s.store.Snapshot()
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

func sortedClaimants(claimants map[types.PeerKey]struct{}) []types.PeerKey {
	return slices.SortedFunc(maps.Keys(claimants), func(a, b types.PeerKey) int { return a.Compare(b) })
}
