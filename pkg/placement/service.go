package placement

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	"go.uber.org/zap"
)

const (
	// workloadInvocationTimeout is the server-side ceiling on an invocation's
	// wall time, applied per hop on the receiving node. It's a safety cap, not
	// the authoritative deadline.
	//
	// Canonical timeout stack (outermost wins):
	//   1. Caller deadline (e.g. `pln call --timeout`, upstream workload's ctx)
	//   2. gRPC server — honours caller deadline on CallWorkload
	//   3. placement.Call — inherits caller ctx; forwards it to Runtime.Call
	//      and to forwardCall, which opens a stream to the target peer
	//   4. Target peer stream handler — this ceiling (min() with caller's
	//      deadline once wire-level deadline propagation lands in Phase 4)
	//   5. wasm.Runtime.Call — inherits; Extism enforces its own per-workload
	//      timeout from the seed config as a further cap
	//
	// Never introduce a timeout above this layer that's shorter than the
	// caller would reasonably expect; silent truncation produces ghost events
	// where the WASM side-effect lands but the caller sees a timeout.
	workloadInvocationTimeout = 60 * time.Second
	resourceSampleInterval    = 5 * time.Second
)

type PlacementAPI interface {
	Start(ctx context.Context) error
	Stop() error

	Seed(binary []byte, spec state.WorkloadSpec) error
	Unseed(hash string) error
	Call(ctx context.Context, hash, function string, input []byte) ([]byte, error)
	Status() []WorkloadSummary
	PlacementInfo() map[string]PlacementInfo

	RecordDial(callerHash, targetKey string)
	RecordInvocation(seedHash string, elapsed time.Duration)
	RecordParkedTime(seedHash string, elapsed time.Duration)

	HandleArtifactStream(stream io.ReadWriteCloser, peer types.PeerKey)
	HandleWorkloadStream(stream io.ReadWriteCloser, peer types.PeerKey)

	Signal()
}

var _ PlacementAPI = (*Service)(nil)

type WorkloadState interface {
	Snapshot() state.Snapshot
	SetWorkloadSpec(spec state.WorkloadSpec) []state.Event
	DeleteWorkloadSpec(hash string) []state.Event
	ClaimWorkload(hash string) []state.Event
	ReleaseWorkload(hash string) []state.Event
	SetLocalResources(r state.NodeResources) []state.Event
	SetSeedMetrics(metrics map[string]state.SeedMetrics) []state.Event
	SetSeedDialRates(rates map[string]map[string]float32) []state.Event
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
	gates            *gateRegistry
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

func New(self types.PeerKey, store WorkloadState, casStore *cas.Store, wasmRT WASMRuntime, opts ...Option) *Service {
	s := &Service{
		localID: self,
		store:   store,
		cas:     casStore,
		log:     zap.NewNop().Sugar(),
	}
	for _, o := range opts {
		o(s)
	}
	s.manager = newManager(casStore, wasmRT)
	s.utilisation = newUtilisationTracker()
	s.latency = newLatencyTracker()
	s.gates = newGateRegistry(func(string) int { return runtime.NumCPU() * gateInitialMultiplier })
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
		s.gates,
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
			var cpuPct, memPct uint32
			var memTotal uint64
			if pcts, err := cpu.Percent(0, false); err == nil && len(pcts) > 0 {
				cpuPct = uint32(pcts[0])
			}
			if vm, err := mem.VirtualMemory(); err == nil {
				memPct = uint32(vm.UsedPercent)
				memTotal = vm.Total
			}
			numCPU := uint32(runtime.NumCPU()) //nolint:gosec
			s.store.SetLocalResources(state.NodeResources{
				CPUPercent:       cpuPct,
				MemPercent:       memPct,
				MemTotalBytes:    memTotal,
				NumCPU:           numCPU,
				CPUBudgetPercent: s.cpuBudgetPercent,
				MemBudgetPercent: s.memBudgetPercent,
			})
		}
	}
}

func (s *Service) Seed(binary []byte, spec state.WorkloadSpec) error {
	hash, name := spec.Hash, spec.Name
	snap := s.store.Snapshot()
	if oldHash, ok := snap.LocalSpecByName(name, s.localID); ok { //nolint:nestif
		if oldHash != hash {
			if s.manager.IsRunning(oldHash) {
				_ = s.manager.Unseed(oldHash)
			}
			s.store.ReleaseWorkload(oldHash)
			s.store.DeleteWorkloadSpec(oldHash)
			s.utilisation.Clear(oldHash)
			s.gates.Clear(oldHash)
		} else if sv, ok := snap.Specs[oldHash]; ok {
			old := sv.Spec
			if old.MemoryBytes != spec.MemoryBytes || old.Timeout != spec.Timeout {
				if s.manager.IsRunning(oldHash) {
					_ = s.manager.Unseed(oldHash)
				}
			}
		}
	}

	cfg := wasm.NewPluginConfig(spec.MemoryBytes, spec.Timeout)
	gotHash, err := s.manager.Seed(s.ctx, binary, cfg)
	if err != nil && !errors.Is(err, ErrAlreadyRunning) {
		return err
	}
	if gotHash != hash {
		return fmt.Errorf("hash mismatch: expected %s, got %s", hash, gotHash)
	}
	if spec.MinReplicas == 0 {
		spec.MinReplicas = 1
	}
	if spec.LatencySLO <= 0 {
		spec.LatencySLO = defaultLatencySLO
	}

	s.store.SetWorkloadSpec(spec)
	s.store.ClaimWorkload(hash)
	return nil
}

func (s *Service) Unseed(hash string) error {
	_, hash = s.resolveLocalFirst(hash)

	snap := s.store.Snapshot()
	sv, specExists := snap.Specs[hash]
	locallyRunning := s.manager.IsRunning(hash)

	if !specExists && !locallyRunning {
		return fmt.Errorf("%w: %s", ErrNotRunning, types.ShortHash(hash))
	}

	// The spec can only be deleted by its publisher — gossip ignores
	// tombstones from non-owners. If another node owns it, tell the
	// operator where to run unseed rather than silently no-op.
	if specExists && sv.Publisher != s.localID {
		return fmt.Errorf("workload %s is owned by peer %s; run unseed on that node", types.ShortHash(hash), sv.Publisher.Short())
	}

	if locallyRunning {
		if err := s.manager.Unseed(hash); err != nil {
			return err
		}
	}
	s.store.ReleaseWorkload(hash)
	s.store.DeleteWorkloadSpec(hash)
	s.utilisation.Clear(hash)
	s.gates.Clear(hash)
	return nil
}

func (s *Service) Call(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	resolved, found := s.resolveGlobal(hash)
	if !found {
		return nil, fmt.Errorf("no such workload %q: %w", hash, wasm.ErrTargetNotFound)
	}
	hash = resolved

	// Cycle detection: a hash already in the local call chain would
	// deadlock on its own instance pool. Reject fast rather than let the
	// nested Call block forever.
	if chainContains(ctx, hash) {
		return nil, fmt.Errorf("%w: %s", ErrCycle, types.ShortHash(hash))
	}
	ctx = withChain(ctx, hash)

	locallyRunning := s.manager.IsRunning(hash)
	snap := s.store.Snapshot()
	claimants := snap.Claims[hash]

	if !locallyRunning && len(claimants) == 0 {
		return nil, fmt.Errorf("no node claims workload %s: %w", types.ShortHash(hash), ErrNotRunning)
	}

	target, isLocal := pickP2C(s.localID, locallyRunning, claimants, s.latency, hash)

	if isLocal {
		return s.callLocal(ctx, hash, function, input)
	}

	out, err := s.forwardCall(ctx, target, hash, function, input)
	if err == nil {
		return out, nil
	}
	if errors.Is(err, ErrWorkloadFailed) || errors.Is(err, ErrCycle) {
		return nil, err
	}

	// Fallback: try remaining claimants in shuffled order. Transient
	// failures (overload, timeout, transport hiccups) are retryable —
	// the next peer might have slack.
	for _, fallback := range shuffledClaimants(claimants, target) {
		if fallback == s.localID && locallyRunning {
			out, err = s.callLocal(ctx, hash, function, input)
		} else {
			out, err = s.forwardCall(ctx, fallback, hash, function, input)
		}
		if err == nil {
			return out, nil
		}
		if errors.Is(err, ErrWorkloadFailed) || errors.Is(err, ErrCycle) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("all %d claimants failed for %s: %w", len(claimants), types.ShortHash(hash), err)
}

// callLocal runs the workload on this node, gated by the per-workload
// concurrency gate. The gate blocks only until ctx cancels — caller
// deadlines drive wait budgets, not a global constant.
//
// Two timers run here. callerStart spans admission wait + execution so the
// routing latency tracker reflects what callers actually experience —
// this matches forwardCall's measurement and lets P2C migrate traffic
// away from a saturated local node. workStart covers only execution so
// the per-seed compute-cost EWMA stays a clean signal for the autoscaler.
func (s *Service) callLocal(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	callerStart := time.Now()
	gateRelease, err := s.gates.acquire(ctx, hash)
	if err != nil {
		return nil, err
	}
	defer gateRelease()

	workStart := time.Now()
	out, err := s.manager.Call(ctx, hash, function, input)
	work := time.Since(workStart)
	callerElapsed := time.Since(callerStart)
	s.latency.Record(s.localID, hash, float64(callerElapsed.Milliseconds()))
	if !errors.Is(err, ErrNotRunning) {
		s.utilisation.RecordServed(hash)
		s.utilisation.RecordInvocation(hash, work)
		s.utilisation.RecordSLO(hash, callerElapsed)
	}
	return out, err
}

func (s *Service) forwardCall(ctx context.Context, target types.PeerKey, hash, function string, input []byte) ([]byte, error) {
	stream, err := s.mesh.OpenStream(ctx, target, transport.StreamTypeWorkload)
	if err != nil {
		s.log.Debugw("workload stream failed", "target", target.Short(), "hash", types.ShortHash(hash), "err", err)
		return nil, err
	}

	start := time.Now()
	out, err := invokeOverStream(ctx, stream, hash, function, input)
	elapsed := time.Since(start)
	// Record latency and SLO on every attempt except ErrNotRunning so P2C
	// learns about slow/failing remotes and the SLO autoscaler doesn't
	// undercount remote pain. ErrNotRunning means the target wasn't
	// hosting the seed at all — not informative for either signal.
	if !errors.Is(err, ErrNotRunning) {
		s.latency.Record(target, hash, float64(elapsed.Milliseconds()))
		s.utilisation.RecordSLO(hash, elapsed)
	}
	if err != nil {
		s.log.Warnw("workload invocation failed", "target", target.Short(), "hash", types.ShortHash(hash), "err", err)
		return nil, err
	}
	return out, nil
}

func (s *Service) Status() []WorkloadSummary {
	summaries := s.manager.List()
	snap := s.store.Snapshot()
	pinfo := s.reconciler.allPlacementInfo()
	for i := range summaries {
		for _, sv := range snap.Specs {
			if sv.Spec.Hash == summaries[i].Hash {
				summaries[i].Name = sv.Spec.Name
				break
			}
		}
		if info, ok := pinfo[summaries[i].Hash]; ok {
			summaries[i].EffectiveTarget = info.EffectiveTarget
			summaries[i].Pressure = info.SLOBurnRatio
		}
	}
	return summaries
}

// PlacementInfo returns per-hash autoscale state for all locally-tracked workloads.
func (s *Service) PlacementInfo() map[string]PlacementInfo {
	return s.reconciler.allPlacementInfo()
}

func (s *Service) HandleArtifactStream(stream io.ReadWriteCloser, _ types.PeerKey) {
	handleArtifactStream(stream, s.cas)
}

func (s *Service) HandleWorkloadStream(stream io.ReadWriteCloser, peer types.PeerKey) {
	handleWorkloadStream(s.ctx, stream, peer, s.manager, s.utilisation, s.gates, workloadInvocationTimeout)
}

func (s *Service) Signal() {
	s.reconciler.Signal()
}

// RecordDial observes an outbound dial from a caller workload to a target
// (another seed or a service). targetKey is prefix-disambiguated:
// "seed:<name-or-hash>" or "service:<name>". Seed targets are normalised to
// "seed:<hash>" so the dial graph aligns with the claim graph during
// placement scoring.
func (s *Service) RecordDial(callerHash, targetKey string) {
	if name, ok := strings.CutPrefix(targetKey, "seed:"); ok {
		if hash, found := s.resolveGlobal(name); found {
			targetKey = "seed:" + hash
		}
	}
	s.utilisation.RecordDial(callerHash, targetKey)
}

// RecordInvocation observes an elapsed wall-time for a single invocation
// of seedHash on this node.
func (s *Service) RecordInvocation(seedHash string, elapsed time.Duration) {
	s.utilisation.RecordInvocation(seedHash, elapsed)
}

// RecordParkedTime reports time the currently-executing invocation of
// seedHash spent blocked inside pollen_request. The tick loop aggregates
// these samples into an EWMA the reconciler uses to size the workload's
// concurrency gate adaptively.
func (s *Service) RecordParkedTime(seedHash string, elapsed time.Duration) {
	s.utilisation.RecordParkedTime(seedHash, elapsed)
}

// resolveLocalFirst resolves an identifier (name or hash prefix) for
// operator-facing operations like unseed. Prefers the local peer's spec
// when resolving by name so operators can manage their own seeds even when
// another peer publishes the same name, but falls through to any peer's
// spec so a delegated admin on a non-publisher node can still resolve the
// hash (and get a meaningful ownership error from Unseed).
func (s *Service) resolveLocalFirst(identifier string) (string, string) {
	snap := s.store.Snapshot()

	// Name match (local peer first)
	if hash, ok := snap.LocalSpecByName(identifier, s.localID); ok {
		return identifier, hash
	}

	// Name match (any publisher). Needed on admin nodes that didn't
	// publish the seed but still want the ownership error to surface
	// the real hash rather than the raw identifier.
	if hash, _, ok := snap.SpecByName(identifier); ok {
		return identifier, hash
	}

	hash, _ := s.resolveHashPrefix(identifier, snap)
	name := identifier
	for _, sv := range snap.Specs {
		if sv.Spec.Hash == hash {
			name = sv.Spec.Name
			break
		}
	}
	return name, hash
}

// resolveGlobal resolves an identifier (name or hash prefix) for global
// operations like call. Prefers the canonical name winner (lowest PeerKey)
// for routing to the best replica. The bool reports whether the identifier
// resolved to a known spec, claim, or unique prefix match.
func (s *Service) resolveGlobal(identifier string) (string, bool) {
	snap := s.store.Snapshot()

	if hash, _, ok := snap.SpecByName(identifier); ok {
		return hash, true
	}

	return s.resolveHashPrefix(identifier, snap)
}

func (s *Service) resolveHashPrefix(prefix string, snap state.Snapshot) (string, bool) {
	if s.manager.IsRunning(prefix) {
		return prefix, true
	}
	if _, ok := snap.Claims[prefix]; ok {
		return prefix, true
	}
	var match string
	for h := range snap.Specs {
		if strings.HasPrefix(h, prefix) {
			if match != "" && match != h {
				return prefix, false
			}
			match = h
		}
	}
	if match != "" {
		return match, true
	}
	return prefix, false
}
