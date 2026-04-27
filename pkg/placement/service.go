// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/process"
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
	//      deadline once wire-level deadline propagation lands)
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

	RecordParkedTime(callerHash, callerFunction string, elapsed time.Duration)

	Serve(stream io.ReadWriteCloser, peerKey types.PeerKey)

	Signal()
}

type blobsAPI interface {
	Put(r io.Reader) (string, error)
	Get(hash string) (io.ReadCloser, error)
	Has(hash string) bool
	Fetch(ctx context.Context, hash string, peers []types.PeerKey) error
	Remove(hash string) error
}

var _ PlacementAPI = (*Service)(nil)

type WorkloadState interface {
	Snapshot() state.Snapshot
	PublishWorkload(spec state.WorkloadSpec) ([]state.Event, error)
	DeleteWorkloadSpec(hash string) []state.Event
	ClaimWorkload(hash string) []state.Event
	MarkWorkloadDraining(hash string) []state.Event
	ReleaseWorkload(hash string) []state.Event
	SetLocalResources(r state.NodeResources) []state.Event
	SetSeedMetrics(metrics map[string]state.SeedMetrics) []state.Event
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
	blobs            blobsAPI
	log              *zap.SugaredLogger
	cancel           context.CancelFunc
	utilisation      *utilisationTracker
	latency          *latencyTracker
	gates            *gateRegistry
	mem              *memoryAdmission
	proc             *process.Process
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

func New(self types.PeerKey, store WorkloadState, blobs blobsAPI, wasmRT WASMRuntime, opts ...Option) *Service {
	s := &Service{
		localID: self,
		store:   store,
		blobs:   blobs,
		log:     zap.NewNop().Sugar(),
	}
	for _, o := range opts {
		o(s)
	}
	// Normalise the configured budgets to their effective defaults so the
	// gossiped values match what local admission and admissionState
	// enforce. Without this, an unset memBudget admits at 80% locally
	// (admission.go default) but gossips a raw 0 — and remote scoring
	// reads 0 as 100% (no constraint), letting a saturated peer still
	// win claims. Same idea for CPU even though every existing fallback
	// already lands on 100%; making it explicit means the wire reflects
	// the local view.
	if s.memBudgetPercent == 0 {
		s.memBudgetPercent = defaultMemBudgetPercent
	}
	if s.cpuBudgetPercent == 0 {
		s.cpuBudgetPercent = defaultCPUBudgetPercent
	}
	s.manager = newManager(blobs, wasmRT)
	s.utilisation = newUtilisationTracker()
	s.latency = newLatencyTracker()
	s.gates = newGateRegistry(runtime.NumCPU() * gateInitialMultiplier)
	s.mem = newMemoryAdmission(s.memBudgetPercent)
	if proc, err := process.NewProcess(int32(os.Getpid())); err == nil {
		s.proc = proc
	}
	return s
}

func (s *Service) Start(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(ctx)
	s.ctx = ctx

	s.reconciler = newReconciler(
		s.localID,
		s.store,
		s.manager,
		s.blobs,
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
	// Immediate sample so memory admission has a hard limit before the
	// first request lands instead of fail-open for the first interval.
	s.sampleResources()

	ticker := time.NewTicker(resourceSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sampleResources()
		}
	}
}

func (s *Service) sampleResources() {
	var cpuPct, memPct uint32
	var memTotal uint64
	if pcts, err := cpu.Percent(0, false); err == nil && len(pcts) > 0 {
		cpuPct = uint32(pcts[0])
	}
	if vm, err := mem.VirtualMemory(); err == nil {
		memPct = uint32(vm.UsedPercent)
		memTotal = vm.Total
	}
	var rss uint64
	if s.proc != nil {
		if mi, err := s.proc.MemoryInfo(); err == nil {
			rss = mi.RSS
		}
	}
	s.mem.update(rss, memTotal)
	numCPU := uint32(runtime.NumCPU()) //nolint:gosec
	s.store.SetLocalResources(state.NodeResources{
		CPUPercent:       cpuPct,
		MemPercent:       memPct,
		MemTotalBytes:    memTotal,
		NumCPU:           numCPU,
		CPUBudgetPercent: s.cpuBudgetPercent,
		MemBudgetPercent: s.memBudgetPercent,
		AdmissionState:   s.admissionState(cpuPct, memPct),
	})
}

// admissionState rolls up the local backpressure picture into a single
// gossip-friendly enum. Closed when the memory admission gate would
// reject; degraded when CPU or memory headroom is below 10% of budget;
// open otherwise. Routing peers use this to skip Closed targets
// entirely and penalise Degraded targets in pickP2C.
func (s *Service) admissionState(cpuPct, memPct uint32) state.AdmissionState {
	const degradeBand = 10
	if s.mem.closed() {
		return state.AdmissionClosed
	}
	cpuBudget := s.cpuBudgetPercent
	if cpuBudget == 0 {
		cpuBudget = 100
	}
	memBudget := s.memBudgetPercent
	if memBudget == 0 {
		memBudget = 100
	}
	if cpuBudget >= cpuPct+degradeBand && memBudget >= memPct+degradeBand {
		return state.AdmissionOpen
	}
	return state.AdmissionDegraded
}

// reserveCallMemory looks up the workload's declared memory cost and
// asks the node-global admission gate to reserve it. The release is a
// no-op when admission is disabled (no sample yet, no spec, or no
// budget) so callers can defer it unconditionally.
func (s *Service) reserveCallMemory(hash string) (func(), error) {
	return s.mem.tryReserve(s.callMemoryBytes(hash))
}

func (s *Service) callMemoryBytes(hash string) uint64 {
	if sv, ok := s.store.Snapshot().Specs[hash]; ok && sv.Spec.MemoryBytes > 0 {
		return sv.Spec.MemoryBytes
	}
	return defaultCallMemoryBytes
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
	alreadyRunning := errors.Is(err, ErrAlreadyRunning)
	if err != nil && !alreadyRunning {
		return err
	}
	if gotHash != hash {
		if !alreadyRunning {
			_ = s.manager.Unseed(gotHash)
		}
		return fmt.Errorf("hash mismatch: expected %s, got %s", hash, gotHash)
	}
	if spec.MinReplicas == 0 {
		spec.MinReplicas = 1
	}
	if spec.LatencySLO <= 0 {
		spec.LatencySLO = defaultLatencySLO
	}

	if _, err := s.store.PublishWorkload(spec); err != nil {
		// Compile happened but the publisher rejected the spec — undo
		// the Seed so a half-installed module isn't left running with
		// no spec to manage it (no autoscale, no idle release, no
		// gossip). Skip when the module was already running pre-call;
		// it predates this Seed and isn't ours to clean up.
		if !alreadyRunning {
			_ = s.manager.Unseed(hash)
		}
		return err
	}
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
	if err := s.blobs.Remove(hash); err != nil {
		s.log.Warnw("evict wasm blob failed after unseed", "hash", types.ShortHash(hash), "err", err)
	}
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

	// Record that a call for this hash originated at this node, regardless
	// of whether we host it locally. Gossiped per-peer as OriginRate —
	// placement scoring treats the cluster-wide distribution as the
	// authoritative demand signal for where the seed should live.
	s.utilisation.RecordOrigin(hash, function)

	snap := s.store.Snapshot()
	claimants := snap.Claims[hash]

	if len(claimants) == 0 {
		return nil, fmt.Errorf("no node claims workload %s: %w", types.ShortHash(hash), ErrNotRunning)
	}

	view := routingView{
		Claimants:       claimants,
		Draining:        snap.DrainingClaims[hash],
		AdmissionStates: admissionStatesFromSnap(snap),
		RejectShares:    rejectSharesFromSnap(snap, hash),
	}
	target, isLocal, ok := pickP2C(s.localID, view, s.latency, hash, function)
	if !ok {
		return nil, newOverload(ErrOverloaded, "no admissible claimants")
	}
	out, err := s.attemptCall(ctx, target, isLocal, hash, function, input)
	if err == nil {
		return out, nil
	}
	if errors.Is(err, ErrWorkloadFailed) || errors.Is(err, ErrCycle) {
		return nil, err
	}

	// One bounded fallback. The earlier shuffled-walk over every claimant
	// turned a single hot spot into N×latency cascades and ignored
	// retry-after; restricting to one P2C-picked alternate keeps the
	// blast radius of a transient failure constant. Skip the retry when
	// the remaining ctx deadline can't accommodate the target's stated
	// retry-after — better to surface the structured overload than burn
	// the deadline before the fallback can even reply.
	if !canRetry(ctx, err) {
		return nil, err
	}
	fallback, fallbackLocal, ok := pickP2CFallback(s.localID, view, target, s.latency, hash, function)
	if !ok {
		return nil, err
	}
	out2, err2 := s.attemptCall(ctx, fallback, fallbackLocal, hash, function, input)
	if err2 == nil {
		return out2, nil
	}
	if errors.Is(err2, ErrWorkloadFailed) || errors.Is(err2, ErrCycle) {
		return nil, err2
	}
	return nil, preferStructured(err, err2)
}

func (s *Service) attemptCall(ctx context.Context, target types.PeerKey, isLocal bool, hash, function string, input []byte) ([]byte, error) {
	if isLocal {
		return s.callLocal(ctx, hash, function, input)
	}
	return s.forwardCall(ctx, target, hash, function, input)
}

// canRetry reports whether the call ctx has enough remaining time to
// honour an OverloadError's RetryAfter hint before attempting the
// fallback. Non-overload errors (transport hiccups, timeouts) are
// always retried — they have no retry-after to consult.
func canRetry(ctx context.Context, err error) bool {
	var oe *OverloadError
	if !errors.As(err, &oe) || oe.RetryAfter <= 0 {
		return true
	}
	dl, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return time.Until(dl) >= oe.RetryAfter
}

// admissionStatesFromSnap projects per-peer AdmissionState out of the
// snapshot for the routing layer. Building this once per Call keeps the
// pickP2C variants pure (no snapshot dependency) and amortises the map
// allocation across primary + fallback picks.
func admissionStatesFromSnap(snap state.Snapshot) map[types.PeerKey]state.AdmissionState {
	out := make(map[types.PeerKey]state.AdmissionState, len(snap.Nodes))
	for pk, nv := range snap.Nodes {
		if nv.AdmissionState != state.AdmissionUnspecified {
			out[pk] = nv.AdmissionState
		}
	}
	return out
}

// rejectSharesFromSnap projects per-peer rejected-share for the given
// hash so pickP2C can penalise saturated claimants. Zero entries (no
// served and no rejected traffic — fresh peer or idle hash) are omitted
// so adjustedLatency leaves the EWMA untouched. RejectRate alone is the
// wrong signal: a peer absorbing 1000 req/s and shedding 10/s is fine
// (1% share); a peer absorbing 10/s and shedding 100/s is not (90%).
func rejectSharesFromSnap(snap state.Snapshot, hash string) map[types.PeerKey]float64 {
	out := make(map[types.PeerKey]float64, len(snap.Nodes))
	for pk, nv := range snap.Nodes {
		m, ok := nv.SeedMetrics[hash]
		if !ok {
			continue
		}
		total := float64(m.ServedRate) + float64(m.RejectRate)
		if total <= 0 {
			continue
		}
		out[pk] = float64(m.RejectRate) / total
	}
	return out
}

// preferStructured chooses which of two failures to surface to the
// caller. A structured OverloadError (with retry-after and reason) is
// more actionable than a bare transport error, so it wins; if both are
// structured the first attempt wins because the second hit a wider
// surface area on the way out.
func preferStructured(first, second error) error {
	var oe *OverloadError
	if errors.As(first, &oe) {
		return first
	}
	if errors.As(second, &oe) {
		return second
	}
	return first
}

// callLocal runs the workload on this node, gated by the per-workload
// concurrency gate and the node-global memory admission ceiling. Both
// gates are non-blocking: at-capacity fast-fails (ErrAtCapacity for
// concurrency, ErrOverloaded for memory) so Call can fall through to
// another claimant. Failed admission increments RejectRate (gossiped
// for the autoscaler's reject-driven scale-up) and the SLO burn signal
// (observability).
//
// Two timers run here. callerStart spans admission + execution so the
// routing latency tracker reflects what callers actually experience —
// this matches forwardCall's measurement and lets P2C migrate traffic
// away from a saturated local node. workStart covers only execution so
// the per-seed compute-cost EWMA stays a clean signal for the
// Little's-Law replica sizing in desiredReplicas.
func (s *Service) callLocal(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	callerStart := time.Now()
	gateRelease, err := s.gates.acquire(callKey{Hash: hash, Function: function})
	if err != nil {
		if errors.Is(err, ErrAtCapacity) {
			s.utilisation.RecordSLOMiss(hash, function)
			s.utilisation.RecordReject(hash, function)
			return nil, newOverload(ErrAtCapacity, "gate at capacity")
		}
		return nil, err
	}
	defer gateRelease()

	memRelease, err := s.reserveCallMemory(hash)
	if err != nil {
		if errors.Is(err, ErrOverloaded) {
			s.utilisation.RecordSLOMiss(hash, function)
			s.utilisation.RecordReject(hash, function)
			return nil, newOverload(ErrOverloaded, "node memory budget")
		}
		return nil, err
	}
	defer memRelease()

	workStart := time.Now()
	out, err := s.manager.Call(ctx, hash, function, input)
	work := time.Since(workStart)
	callerElapsed := time.Since(callerStart)
	s.latency.Record(s.localID, hash, function, float64(callerElapsed.Milliseconds()))
	if !errors.Is(err, ErrNotRunning) {
		s.utilisation.RecordServed(hash, function)
		s.utilisation.RecordInvocation(hash, function, work)
		s.utilisation.RecordSLO(hash, function, callerElapsed)
	}
	return out, err
}

func (s *Service) forwardCall(ctx context.Context, target types.PeerKey, hash, function string, input []byte) ([]byte, error) {
	stream, err := s.mesh.OpenStream(ctx, target, transport.StreamTypeWorkload)
	if err != nil {
		s.log.Debugw("workload stream failed", "target", target.Short(), "hash", types.ShortHash(hash), "err", err)
		return nil, err
	}

	// Cancel a blocked read/write if the call ctx fires. Tracked under
	// s.wg so service shutdown drains the watcher before returning —
	// otherwise the goroutine could outlive Stop() and panic on a closed
	// channel.
	if ctx.Done() != nil {
		done := make(chan struct{})
		s.wg.Go(func() {
			select {
			case <-ctx.Done():
				_ = stream.Close()
			case <-done:
			}
		})
		defer close(done)
	}

	start := time.Now()
	out, err := invokeOverStream(ctx, stream, hash, function, input)
	elapsed := time.Since(start)
	// Latency and SLO accounting for the remote attempt. Three branches:
	//   - ErrNotRunning: the target isn't hosting the seed at all —
	//     skip both signals; the bounce isn't informative.
	//   - At-capacity / overloaded: a fast reject. Don't update the
	//     latency EWMA — a 2 ms bounce would make a saturated peer
	//     LOOK fast and bias the next P2C pick toward the rejector
	//     (the original "routing rewards overload" bug). Burn ratio
	//     still reflects the pain via RecordSLOMiss; the per-peer
	//     RejectRate gossip is what feeds rejection-aware routing.
	//   - Anything else: real wall time, real SLO classification.
	switch {
	case errors.Is(err, ErrNotRunning):
		// Skip both.
	case errors.Is(err, ErrAtCapacity), errors.Is(err, ErrOverloaded):
		s.utilisation.RecordSLOMiss(hash, function)
	default:
		s.latency.Record(target, hash, function, float64(elapsed.Milliseconds()))
		s.utilisation.RecordSLO(hash, function, elapsed)
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

// Serve handles an inbound workload call stream end-to-end: reads the
// caller-info envelope, seed hash, and function name from the wire (so
// the placement protocol stays inside this package), then runs the
// invocation under the per-(hash, function) gate, the node-global
// memory admission, and the per-call timeout. peerKey is the
// transport-authenticated identity — wire-reported caller keys are
// ignored to prevent spoofed attribution.
func (s *Service) Serve(stream io.ReadWriteCloser, peerKey types.PeerKey) {
	info, hash, function, err := ReadHeader(stream, peerKey)
	if err != nil {
		_ = stream.Close()
		return
	}
	handleWorkloadStream(s.ctx, stream, info, hash, function, s.manager, s.utilisation, s.gates, s.reserveCallMemory, workloadInvocationTimeout)
}

func (s *Service) Signal() {
	s.reconciler.Signal()
}

// RecordParkedTime reports time the currently-executing invocation of
// (callerHash, callerFunction) spent blocked inside pollen_request.
func (s *Service) RecordParkedTime(callerHash, callerFunction string, elapsed time.Duration) {
	s.utilisation.RecordParkedTime(callerHash, callerFunction, elapsed)
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
	// CRDT only — never consult the local manager. Resolution that
	// trusts in-process runtime over gossip lets a stale local module
	// short-circuit a peer-published workload's identity, and would
	// give a peer with a vanishing CRDT claim the false confidence
	// that the workload is still resolvable.
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
