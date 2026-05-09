// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"sync"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"go.uber.org/zap"
)

const (
	debounceInterval      = 200 * time.Millisecond
	evictionCooldown      = 30 * time.Second
	minResidencyDuration  = 10 * time.Second
	reconcilePollInterval = 2 * time.Second
)

type workloadManager interface {
	SeedFromCAS(ctx context.Context, hash string, cfg wasm.PluginConfig) error
	Unseed(hash string) error
	IsRunning(hash string) bool
}

type reconciler struct {
	store          WorkloadState
	workloads      workloadManager
	blobs          blobsAPI
	budget         *budget
	backoff        *backoff
	gate           Gate
	claimStartTime map[string]time.Time
	pendingRelease map[string]time.Time
	triggerCh      chan struct{}
	fetchSem       chan struct{}
	log            *zap.SugaredLogger
	inFlight       map[string]struct{}
	nowFunc        func() time.Time
	wg             *sync.WaitGroup
	inFlightMu     sync.Mutex
	localID        types.PeerKey
}

func newReconciler(
	localID types.PeerKey,
	store WorkloadState,
	workloads workloadManager,
	blobs blobsAPI,
	budget *budget,
	backoff *backoff,
	gate Gate,
	log *zap.SugaredLogger,
	wg *sync.WaitGroup,
) *reconciler {
	return &reconciler{
		localID:        localID,
		store:          store,
		workloads:      workloads,
		blobs:          blobs,
		budget:         budget,
		backoff:        backoff,
		gate:           gate,
		triggerCh:      make(chan struct{}, 1),
		fetchSem:       make(chan struct{}, 4), //nolint:mnd
		pendingRelease: make(map[string]time.Time),
		claimStartTime: make(map[string]time.Time),
		inFlight:       make(map[string]struct{}),
		log:            log,
		wg:             wg,
		nowFunc:        time.Now,
	}
}

func (r *reconciler) Signal() {
	select {
	case r.triggerCh <- struct{}{}:
	default:
	}
}

func (r *reconciler) Run(ctx context.Context) {
	r.cleanupStaleClaims(r.store.Snapshot().Claims)

	pollTicker := time.NewTicker(reconcilePollInterval)
	defer pollTicker.Stop()

	var debounce *time.Timer
	var debounceC <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			if debounce != nil {
				debounce.Stop()
			}
			return
		case <-pollTicker.C:
			r.reconcile(ctx)
		case <-r.triggerCh:
			if debounce != nil {
				debounce.Stop()
			}
			debounce = time.NewTimer(debounceInterval)
			debounceC = debounce.C
		case <-debounceC:
			r.reconcile(ctx)
			debounceC = nil
		}
	}
}

func (r *reconciler) reconcile(ctx context.Context) {
	snap := r.store.Snapshot()
	localCert := localCertFromSnap(snap)

	// Lowest-PeerKey publisher wins when names collide; unnamed specs
	// pass through unchanged.
	nameWinners := make(map[string]string)
	for hash, sv := range snap.Specs {
		name := sv.Spec.Name
		if name == "" {
			continue
		}
		if existing, ok := nameWinners[name]; !ok || sv.Publisher.Compare(snap.Specs[existing].Publisher) < 0 {
			nameWinners[name] = hash
		}
	}

	specs := make(map[string]spec, len(snap.Specs))
	for hash, sv := range snap.Specs {
		name := sv.Spec.Name
		if name != "" && nameWinners[name] != hash {
			continue
		}
		if !r.mayHost(localCert, sv.Auth) {
			continue
		}
		specs[hash] = spec{
			MinReplicas: sv.Spec.MinReplicas,
			MemoryBytes: sv.Spec.MemoryBytes,
			Spread:      sv.Spec.Spread,
		}
	}

	// Drop existing claims that the local cert no longer entitles us to
	// host. Spec deletion is handled in the action loop below; this
	// branch covers the case where the spec is still live but our
	// entitlement (cert attributes, policy revision, parent revocation)
	// has shifted under us.
	for hash, claimants := range snap.Claims {
		if _, mine := claimants[r.localID]; !mine {
			continue
		}
		sv, ok := snap.Specs[hash]
		if !ok {
			continue
		}
		if r.mayHost(localCert, sv.Auth) {
			continue
		}
		r.log.Infow("policy denies local hosting, releasing claim", "hash", types.ShortHash(hash), "name", sv.Spec.Name)
		r.executeRelease(hash)
		delete(r.pendingRelease, hash)
	}

	actions := evaluate(evaluateInput{
		localID:   r.localID,
		allPeers:  snap.PeerKeys,
		specs:     specs,
		claims:    snap.Claims,
		draining:  snap.DrainingClaims,
		isRunning: r.workloads.IsRunning,
	})
	wantRelease := make(map[string]struct{})
	now := r.nowFunc()

	for _, a := range actions {
		switch a.Kind {
		case actionClaim:
			if _, draining := snap.DrainingClaims[a.Hash][r.localID]; draining {
				// Drain rescinded: republish the claim without the
				// flag, no fetch/seed needed.
				delete(r.pendingRelease, a.Hash)
				r.store.ClaimWorkload(a.Hash)
				continue
			}
			r.startClaim(ctx, a.Hash, snap.Specs, snap.Claims)
		case actionRelease:
			if _, specExists := snap.Specs[a.Hash]; !specExists {
				r.executeRelease(a.Hash)
				delete(r.pendingRelease, a.Hash)
				continue
			}
			wantRelease[a.Hash] = struct{}{}
			if _, pending := r.pendingRelease[a.Hash]; !pending {
				r.pendingRelease[a.Hash] = now.Add(evictionCooldown)
				// Make-before-break: signal drain so peers can mint a
				// replacement claim during the cooldown.
				r.store.MarkWorkloadDraining(a.Hash)
			}
		}
	}

	for hash, deadline := range r.pendingRelease {
		if _, stillWanted := wantRelease[hash]; !stillWanted {
			delete(r.pendingRelease, hash)
			continue
		}
		if now.Before(deadline) {
			continue
		}

		r.inFlightMu.Lock()
		claimTime, hasClaimTime := r.claimStartTime[hash]
		r.inFlightMu.Unlock()

		if hasClaimTime && now.Sub(claimTime) < minResidencyDuration {
			if sv, specExists := snap.Specs[hash]; specExists && uint32(len(snap.Claims[hash])) <= sv.Spec.MinReplicas {
				continue
			}
		}
		r.executeRelease(hash)
		delete(r.pendingRelease, hash)
	}
}

func (r *reconciler) startClaim(ctx context.Context, hash string, specViews map[string]state.WorkloadSpecView, claims map[string]map[types.PeerKey]struct{}) {
	r.inFlightMu.Lock()
	if _, ok := r.inFlight[hash]; ok {
		r.inFlightMu.Unlock()
		return
	}
	r.inFlight[hash] = struct{}{}
	r.inFlightMu.Unlock()

	select {
	case r.fetchSem <- struct{}{}:
	default:
		r.inFlightMu.Lock()
		delete(r.inFlight, hash)
		r.inFlightMu.Unlock()
		return
	}

	var peers []types.PeerKey
	if sv, ok := specViews[hash]; ok {
		peers = append(peers, sv.Publisher)
	}
	for pk := range claims[hash] {
		peers = append(peers, pk)
	}

	r.wg.Go(func() {
		defer func() {
			<-r.fetchSem
			r.inFlightMu.Lock()
			delete(r.inFlight, hash)
			r.inFlightMu.Unlock()
			r.Signal()
		}()
		r.executeClaim(ctx, hash, peers)
	})
}

func (r *reconciler) executeClaim(ctx context.Context, hash string, peers []types.PeerKey) {
	if !r.blobs.Has(hash) {
		if err := r.blobs.Fetch(ctx, hash, peers); err != nil {
			r.log.Warnw("fetch blob failed", "hash", hash, "err", err)
			return
		}
	}

	// TODO(saml) should we thread the snapshot through for the lifecycle of the event?
	// would negate the need for the double gate (`mayHost`) below
	snap := r.store.Snapshot()
	sv, specExists := snap.Specs[hash]
	if !specExists {
		r.log.Infow("spec removed during fetch, skipping claim", "hash", hash)
		return
	}

	if !r.mayHost(localCertFromSnap(snap), sv.Auth) {
		r.log.Infow("policy denies local hosting, abandoning claim", "hash", types.ShortHash(hash), "name", sv.Spec.Name)
		return
	}

	if !r.budget.Reserve(hash, replicaMemoryBytes(sv.Spec.MemoryBytes)) {
		r.backoff.SignalRefusal()
		r.log.Infow("refused claim: memory budget exhausted", "name", sv.Spec.Name, "hash", types.ShortHash(hash))
		return
	}

	cfg := wasm.NewPluginConfig(sv.Spec.MemoryBytes, sv.Spec.Timeout)
	if err := r.workloads.SeedFromCAS(ctx, hash, cfg); err != nil {
		r.budget.Release(hash)
		r.log.Warnw("seed from CAS failed", "name", sv.Spec.Name, "hash", types.ShortHash(hash), "err", err)
		return
	}

	r.store.ClaimWorkload(hash)
	r.inFlightMu.Lock()
	r.claimStartTime[hash] = r.nowFunc()
	r.inFlightMu.Unlock()
	r.log.Infow("claimed workload", "hash", hash)
}

func (r *reconciler) executeRelease(hash string) {
	if err := r.workloads.Unseed(hash); err != nil {
		r.log.Warnw("unseed failed", "hash", hash, "err", err)
	}
	r.budget.Release(hash)
	r.store.ReleaseWorkload(hash)
	r.inFlightMu.Lock()
	delete(r.claimStartTime, hash)
	r.inFlightMu.Unlock()
	r.log.Infow("released workload", "hash", hash)
}

// mayHost returns true when the local node is entitled to host the
// workload described by sa. A nil gate (test fixtures) is treated as
// permissive; a nil cert with a configured gate is treated as denial,
// since hosting without a verifiable identity must fail closed.
func (r *reconciler) mayHost(cert *admissionv1.DelegationCert, sa *admissionv1.SpecAuth) bool {
	if r.gate == nil {
		return true
	}
	return r.gate.MayHost(cert, sa) == nil
}

func localCertFromSnap(snap state.Snapshot) *admissionv1.DelegationCert {
	nv, ok := snap.Nodes[snap.LocalID]
	if !ok {
		return nil
	}
	return nv.Cert
}

func (r *reconciler) cleanupStaleClaims(claims map[string]map[types.PeerKey]struct{}) {
	now := r.nowFunc()
	for hash, claimants := range claims {
		if _, mine := claimants[r.localID]; !mine {
			continue
		}
		if !r.workloads.IsRunning(hash) {
			r.store.ReleaseWorkload(hash)
			r.log.Infow("cleaned up stale claim", "hash", hash)
		} else {
			r.inFlightMu.Lock()
			r.claimStartTime[hash] = now
			r.inFlightMu.Unlock()
		}
	}
}
