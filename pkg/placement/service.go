// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

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

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
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
	//      deadline once wire-level deadline propagation lands)
	//   5. wasm.Runtime.Call — inherits; Extism enforces its own per-workload
	//      timeout from the seed config as a further cap
	//
	// Never introduce a timeout above this layer that's shorter than the
	// caller would reasonably expect; silent truncation produces ghost events
	// where the WASM side-effect lands but the caller sees a timeout.
	workloadInvocationTimeout = 60 * time.Second

	backoffTTL = 1 * time.Second

	callTrackerWindow = 30 * time.Second

	placementTickInterval     = 10 * time.Second
	placementMigrateThreshold = 50.0
	placementMinDwell         = 60 * time.Second

	replicaTickInterval       = 3 * time.Second
	replicaScaleUpThreshold   = 0.3
	replicaScaleDownThreshold = 0.1
	replicaScaleDownGrace     = 2 * time.Minute

	resourceSampleInterval = 5 * time.Second
)

type PlacementAPI interface {
	Start(ctx context.Context) error
	Stop() error

	Seed(binary []byte, spec state.WorkloadSpec, policy *admissionv1.Predicate) error
	Unseed(hash string) error
	Call(ctx context.Context, hash, function string, input []byte) ([]byte, error)
	Status() []WorkloadSummary

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
	PublishWorkload(spec state.WorkloadSpec, policy *admissionv1.Predicate) ([]state.Event, error)
	DeleteWorkloadSpec(hash string) ([]state.Event, error)
	ClaimWorkload(hash string) []state.Event
	MarkWorkloadDraining(hash string) []state.Event
	ReleaseWorkload(hash string) []state.Event
	SetLocalResources(r state.NodeResources) []state.Event
	SetBackoffTTL(expiresAt time.Time) []state.Event
	SetPerSeedCallCounts(counts map[string]uint64) []state.Event
}

type StreamOpener interface {
	OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error)
}

// InvokeGate authorises a workload invocation and returns the cert-bound
// CallerInfo to plumb into the seed's execution context. It must be
// consulted before a seed runs whether the call is dispatched inbound from
// a remote peer or outbound from a local host call.
type InvokeGate interface {
	Invoke(peerKey types.PeerKey, hash, function string) (wasm.CallerInfo, error)
}

type Service struct {
	store        WorkloadState
	mesh         StreamOpener
	ctx          context.Context
	manager      *manager
	reconciler   *reconciler
	blobs        blobsAPI
	log          *zap.SugaredLogger
	cancel       context.CancelFunc
	dispatcher   *dispatcher
	backoff      *backoff
	budget       *budget
	calls        *callTracker
	placement    *placementLoop
	replicaCount *replicaCountLoop
	gate         InvokeGate
	wg           sync.WaitGroup
	localID      types.PeerKey
}

type Option func(*Service)

func WithLogger(log *zap.SugaredLogger) Option {
	return func(s *Service) { s.log = log }
}

func WithMesh(mesh StreamOpener) Option {
	return func(s *Service) { s.mesh = mesh }
}

func WithInvokeGate(g InvokeGate) Option {
	return func(s *Service) { s.gate = g }
}

func New(self types.PeerKey, store WorkloadState, blobs blobsAPI, wasmRT WASMRuntime, opts ...Option) *Service {
	s := &Service{
		ctx:     context.Background(),
		localID: self,
		store:   store,
		blobs:   blobs,
		log:     zap.NewNop().Sugar(),
	}
	for _, o := range opts {
		o(s)
	}
	s.manager = newManager(blobs, wasmRT)

	s.backoff = newBackoff(backoffConfig{ttl: backoffTTL}, func(ttl time.Duration) {
		store.SetBackoffTTL(time.Now().Add(ttl))
	})
	s.budget = newBudget(detectMemoryBudget())
	s.calls = newCallTracker(callTrackerWindow, func(counts map[string]uint64) {
		store.SetPerSeedCallCounts(counts)
	})
	s.dispatcher = newDispatcher(store, self)
	s.placement = newPlacementLoop(self, placementConfig{
		tick:             placementTickInterval,
		migrateThreshold: placementMigrateThreshold,
		minDwell:         placementMinDwell,
	}, store)
	s.replicaCount = newReplicaCountLoop(self, replicaCountConfig{
		tick:               replicaTickInterval,
		scaleUpThreshold:   replicaScaleUpThreshold,
		scaleDownThreshold: replicaScaleDownThreshold,
		scaleDownGrace:     replicaScaleDownGrace,
	}, store, s.calls, s.backoff)

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
		s.budget,
		s.backoff,
		s.log.Named("scheduler"),
		&s.wg,
	)

	s.publishResources()

	s.wg.Add(5) //nolint:mnd
	go func() {
		defer s.wg.Done()
		s.reconciler.Run(ctx)
	}()
	go func() {
		defer s.wg.Done()
		s.calls.Run(ctx)
	}()
	go func() {
		defer s.wg.Done()
		s.placement.Run(ctx)
	}()
	go func() {
		defer s.wg.Done()
		s.replicaCount.Run(ctx)
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
			s.publishResources()
		}
	}
}

func (s *Service) publishResources() {
	var cpuPct, memPct uint32
	var memTotal uint64
	if pcts, err := cpu.Percent(0, false); err == nil && len(pcts) > 0 {
		cpuPct = uint32(pcts[0]) //nolint:gosec
	}
	if vm, err := mem.VirtualMemory(); err == nil {
		memPct = uint32(vm.UsedPercent) //nolint:gosec
		memTotal = vm.Total
	}
	s.store.SetLocalResources(state.NodeResources{
		CPUPercent:    cpuPct,
		MemPercent:    memPct,
		MemTotalBytes: memTotal,
		NumCPU:        uint32(runtime.NumCPU()), //nolint:gosec
	})
}

func (s *Service) Seed(binary []byte, spec state.WorkloadSpec, policy *admissionv1.Predicate) error {
	hash, name := spec.Hash, spec.Name
	snap := s.store.Snapshot()
	if oldHash, ok := snap.LocalSpecByName(name, s.localID); ok { //nolint:nestif
		if oldHash != hash {
			if s.manager.IsRunning(oldHash) {
				_ = s.manager.Unseed(oldHash)
				s.budget.Release(oldHash)
			}
			s.store.ReleaseWorkload(oldHash)
			if _, err := s.store.DeleteWorkloadSpec(oldHash); err != nil {
				return err
			}
		} else if sv, ok := snap.Specs[oldHash]; ok {
			old := sv.Spec
			if old.MemoryBytes != spec.MemoryBytes || old.Timeout != spec.Timeout {
				if s.manager.IsRunning(oldHash) {
					_ = s.manager.Unseed(oldHash)
					s.budget.Release(oldHash)
				}
			}
		}
	}

	if !s.budget.Reserve(hash, replicaMemoryBytes(spec.MemoryBytes)) {
		s.backoff.SignalRefusal()
		return newOverload(ErrOverloaded, "memory budget exhausted")
	}

	cfg := wasm.NewPluginConfig(spec.MemoryBytes, spec.Timeout)
	gotHash, err := s.manager.Seed(s.ctx, binary, cfg)
	alreadyRunning := errors.Is(err, ErrAlreadyRunning)
	if err != nil && !alreadyRunning {
		s.budget.Release(hash)
		return err
	}
	if gotHash != hash {
		if !alreadyRunning {
			_ = s.manager.Unseed(gotHash)
		}
		s.budget.Release(hash)
		return fmt.Errorf("hash mismatch: expected %s, got %s", hash, gotHash)
	}
	if spec.MinReplicas == 0 {
		spec.MinReplicas = 1
	}

	if _, err := s.store.PublishWorkload(spec, policy); err != nil {
		// Roll back the local seed on spec rejection so we don't leave a
		// running module with no gossiped spec.
		if !alreadyRunning {
			_ = s.manager.Unseed(hash)
		}
		s.budget.Release(hash)
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

	// Tombstones from non-publishers are ignored by gossip.
	if specExists && sv.Publisher != s.localID {
		return fmt.Errorf("workload %s is owned by peer %s; run unseed on that node", types.ShortHash(hash), sv.Publisher.Short())
	}

	if locallyRunning {
		if err := s.manager.Unseed(hash); err != nil {
			return err
		}
		s.budget.Release(hash)
	}
	s.store.ReleaseWorkload(hash)
	if _, err := s.store.DeleteWorkloadSpec(hash); err != nil {
		return err
	}
	if err := s.blobs.Remove(hash); err != nil {
		s.log.Warnw("evict wasm blob failed after unseed", "hash", types.ShortHash(hash), "err", err)
	}
	return nil
}

type firstHopMode uint8

const (
	firstHopDispatch firstHopMode = iota
	firstHopLocal
)

type localCall func(context.Context, string, string, []byte) ([]byte, error)

func (s *Service) Call(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	return s.routeCall(ctx, hash, function, input, firstHopDispatch, s.callLocal)
}

func (s *Service) routeCall(ctx context.Context, hash, function string, input []byte, mode firstHopMode, local localCall) ([]byte, error) {
	tailHop := false
	for {
		var out []byte
		var err error
		if mode == firstHopLocal {
			ctx, out, err = s.callFirstLocalHop(ctx, hash, function, input, local)
			mode = firstHopDispatch
		} else {
			ctx, hash, out, err = s.callDispatchedHop(ctx, hash, function, input, tailHop, local)
		}
		if err != nil {
			if tailHop && errors.Is(err, ErrNotRunning) {
				return nil, fmt.Errorf("%w: tail call target %s not running", ErrWorkloadFailed, types.ShortHash(hash))
			}
			return nil, err
		}

		tail, ok, err := wasm.ParseTailCallMarker(out)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrWorkloadFailed, err)
		}
		if !ok {
			return out, nil
		}
		if tail.URI.Scheme != wasm.SchemeSeed {
			return nil, fmt.Errorf("%w: tail call URI scheme %q is unsupported", ErrWorkloadFailed, tail.URI.Scheme)
		}

		tailHop = true
		hash, function, input = tail.URI.Name, tail.URI.Function, tail.Input
	}
}

func (s *Service) callFirstLocalHop(ctx context.Context, hash, function string, input []byte, local localCall) (context.Context, []byte, error) {
	if chainContains(ctx, hash) {
		return ctx, nil, fmt.Errorf("%w: %s", ErrCycle, types.ShortHash(hash))
	}
	ctx = withChain(ctx, hash)
	out, err := local(ctx, hash, function, input)
	return ctx, out, err
}

func (s *Service) callDispatchedHop(ctx context.Context, hash, function string, input []byte, tailHop bool, local localCall) (context.Context, string, []byte, error) {
	resolved, found := s.resolveGlobal(hash)
	if !found {
		if tailHop {
			return ctx, hash, nil, fmt.Errorf("%w: tail call target %q not found", ErrWorkloadFailed, hash)
		}
		return ctx, hash, nil, fmt.Errorf("no such workload %q: %w", hash, wasm.ErrTargetNotFound)
	}
	hash = resolved

	if s.gate != nil {
		info, _ := wasm.CallerInfoFromContext(ctx)
		gated, err := s.gate.Invoke(info.PeerKey, hash, function)
		if err != nil {
			return ctx, hash, nil, fmt.Errorf("invoke %s: %w", types.ShortHash(hash), wasm.ErrTargetNotFound)
		}
		gated.DeadlineUnixMs = info.DeadlineUnixMs
		ctx = wasm.WithCallerInfo(ctx, gated)
	}

	if chainContains(ctx, hash) {
		return ctx, hash, nil, fmt.Errorf("%w: %s", ErrCycle, types.ShortHash(hash))
	}
	ctx = withChain(ctx, hash)
	s.calls.RecordCall(hash)

	out, err := s.callHop(ctx, hash, function, input, local)
	return ctx, hash, out, err
}

func (s *Service) callHop(ctx context.Context, hash, function string, input []byte, local localCall) ([]byte, error) {
	target, perr := s.dispatcher.Pick(hash)
	if perr != nil {
		return nil, fmt.Errorf("no node claims workload %s: %w", types.ShortHash(hash), ErrNotRunning)
	}
	isLocal := target == s.localID
	out, err := s.attemptCall(ctx, target, isLocal, hash, function, input, local)
	if err == nil {
		return out, nil
	}
	if errors.Is(err, ErrWorkloadFailed) || errors.Is(err, ErrCycle) {
		return nil, err
	}

	if !canRetry(ctx, err) {
		return nil, err
	}
	fallback, ferr := s.dispatcher.Pick(hash)
	if ferr != nil || fallback == target {
		return nil, err
	}
	fallbackLocal := fallback == s.localID
	out2, err2 := s.attemptCall(ctx, fallback, fallbackLocal, hash, function, input, local)
	if err2 == nil {
		return out2, nil
	}
	if errors.Is(err2, ErrWorkloadFailed) || errors.Is(err2, ErrCycle) {
		return nil, err2
	}
	return nil, preferStructured(err, err2)
}

func (s *Service) attemptCall(ctx context.Context, target types.PeerKey, isLocal bool, hash, function string, input []byte, local localCall) ([]byte, error) {
	if isLocal {
		return local(ctx, hash, function, input)
	}
	return s.forwardCall(ctx, target, hash, function, input)
}

func canRetry(ctx context.Context, err error) bool {
	if !errors.Is(err, ErrOverloaded) {
		return true
	}
	dl, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return time.Until(dl) >= retryAfterDefault
}

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

func (s *Service) callLocal(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	release, ok := s.budget.ReserveCall(hash)
	if !ok {
		s.backoff.SignalRefusal()
		return nil, newOverload(ErrOverloaded, "node memory budget exhausted")
	}
	defer release()
	return s.manager.Call(ctx, hash, function, input)
}

func (s *Service) callLocalServeHop(ctx context.Context, hash, function string, input []byte) ([]byte, error) {
	hopCtx, cancel := context.WithTimeout(ctx, workloadInvocationTimeout)
	defer cancel()
	return s.callLocal(hopCtx, hash, function, input)
}

func (s *Service) forwardCall(ctx context.Context, target types.PeerKey, hash, function string, input []byte) ([]byte, error) {
	stream, err := s.mesh.OpenStream(ctx, target, transport.StreamTypeWorkload)
	if err != nil {
		s.log.Debugw("workload stream failed", "target", target.Short(), "hash", types.ShortHash(hash), "err", err)
		return nil, err
	}

	// Tracked under s.wg so Stop() drains before the stream goes away.
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

	out, err := invokeOverStream(ctx, stream, hash, function, input)
	if err != nil {
		s.log.Warnw("workload invocation failed", "target", target.Short(), "hash", types.ShortHash(hash), "err", err)
		return nil, err
	}
	return out, nil
}

func (s *Service) Status() []WorkloadSummary {
	summaries := s.manager.List()
	snap := s.store.Snapshot()
	for i := range summaries {
		for _, sv := range snap.Specs {
			if sv.Spec.Hash == summaries[i].Hash {
				summaries[i].Name = sv.Spec.Name
				break
			}
		}
	}
	return summaries
}

// Serve handles an inbound workload call. peerKey must be the
// transport-authenticated identity; wire-reported caller attributes
// are replaced by the gate's cert-bound view before the seed runs.
func (s *Service) Serve(stream io.ReadWriteCloser, peerKey types.PeerKey) {
	defer stream.Close()
	info, chain, hash, function, err := ReadHeader(stream, peerKey)
	if err != nil {
		return
	}
	if s.gate != nil {
		gated, err := s.gate.Invoke(peerKey, hash, function)
		if err != nil {
			return
		}
		gated.DeadlineUnixMs = info.DeadlineUnixMs
		info = gated
	}

	ctx := withChainSnapshot(s.ctx, chain)
	ctx = wasm.WithCallerInfo(ctx, info)
	ctx, deadlineCancel := withCallerDeadline(ctx, info)
	defer deadlineCancel()

	input, err := readWorkloadInput(stream)
	if errors.Is(err, errInputTooLarge) {
		if err := writeResponse(stream, statusError, []byte("input too large")); err != nil {
			s.log.Debugw("workload response write failed", zap.Error(err))
		}
		return
	}
	if err != nil {
		return
	}
	if err := ctx.Err(); err != nil {
		if werr := writeErrorResponse(stream, err); werr != nil {
			s.log.Debugw("workload response write failed", zap.Error(werr))
		}
		return
	}

	output, err := s.routeCall(ctx, hash, function, input, firstHopLocal, s.callLocalServeHop)
	if err != nil {
		if werr := writeErrorResponse(stream, err); werr != nil {
			s.log.Debugw("workload response write failed", zap.Error(werr))
		}
		return
	}
	if err := writeResponse(stream, statusOK, output); err != nil {
		s.log.Debugw("workload response write failed", zap.Error(err))
	}
}

func (s *Service) Signal() {
	s.reconciler.Signal()
}

// resolveLocalFirst resolves for operator-facing operations: local
// matches win so operators manage their own seeds; remote falls
// through so non-publishers still get the ownership error.
func (s *Service) resolveLocalFirst(identifier string) (string, string) {
	snap := s.store.Snapshot()

	if hash, ok := snap.LocalSpecByName(identifier, s.localID); ok {
		return identifier, hash
	}

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

func (s *Service) resolveGlobal(identifier string) (string, bool) {
	snap := s.store.Snapshot()

	if hash, _, ok := snap.SpecByName(identifier); ok {
		return hash, true
	}

	return s.resolveHashPrefix(identifier, snap)
}

func (s *Service) resolveHashPrefix(prefix string, snap state.Snapshot) (string, bool) {
	// Gossip-only: trusting the local manager would let a stale
	// in-process module shadow a peer-published workload.
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
