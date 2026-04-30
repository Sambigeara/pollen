// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// Package static replicates static-site manifests and their referenced
// blobs to claiming nodes.
package static

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const (
	reconcileInterval = 5 * time.Second
	digestSize        = 32
	eventBufferSize   = 32
)

type StaticAPI interface {
	Start(ctx context.Context) error
	Stop() error
	Signal()
	Events() <-chan state.Event
	SeedStatic(name string, manifestDigest []byte) error
	UnseedStatic(name string) error
	StaticBlobs() map[string]struct{}
}

type stateStore interface {
	Snapshot() state.Snapshot
	SetStaticSpec(spec state.StaticSpec) ([]state.Event, error)
	DeleteStaticSpec(name string) []state.Event
	ClaimStatic(name string) []state.Event
	ReleaseStatic(name string) []state.Event
}

type blobStore interface {
	Has(hash string) bool
	Get(hash string) (io.ReadCloser, error)
	Fetch(ctx context.Context, hash string, peers []types.PeerKey) error
}

type Service struct {
	store         stateStore
	blobs         blobStore
	log           *zap.SugaredLogger
	manifestCache *manifestCache
	trigger       chan struct{}
	events        chan state.Event
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	localID       types.PeerKey
	canServe      bool
}

var _ StaticAPI = (*Service)(nil)

// canServe gates file-fetch and claim; the reconciler still runs on
// non-serving peers so every node pulls manifests (admin intent) and can
// enumerate the referenced file digests.
func New(localID types.PeerKey, store stateStore, blobs blobStore, canServe bool, log *zap.SugaredLogger) *Service {
	return &Service{
		store:         store,
		blobs:         blobs,
		log:           log,
		localID:       localID,
		canServe:      canServe,
		manifestCache: newManifestCache(),
		trigger:       make(chan struct{}, 1),
		events:        make(chan state.Event, eventBufferSize),
	}
}

func (s *Service) Start(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Go(func() { s.run(ctx) })
	return nil
}

func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	return nil
}

func (s *Service) Signal() {
	select {
	case s.trigger <- struct{}{}:
	default:
	}
}

func (s *Service) Events() <-chan state.Event { return s.events }

func (s *Service) forwardEvents(events []state.Event) {
	for _, ev := range events {
		select {
		case s.events <- ev:
		default:
			s.log.Warnw("static events buffer full; dropping event", "event", ev)
		}
	}
}

func (s *Service) SeedStatic(name string, manifestDigest []byte) error {
	if len(manifestDigest) != digestSize {
		return fmt.Errorf("manifest digest must be %d bytes", digestSize)
	}
	events, err := s.store.SetStaticSpec(state.StaticSpec{
		Name:           name,
		ManifestDigest: hex.EncodeToString(manifestDigest),
	})
	if err != nil {
		return err
	}
	s.forwardEvents(events)
	return nil
}

func (s *Service) UnseedStatic(name string) error {
	snap := s.store.Snapshot()
	sv, ok := snap.StaticSpecs[name]
	if !ok {
		return fmt.Errorf("static site %q not published", name)
	}
	if sv.Publisher != s.localID {
		return fmt.Errorf("static site %q is owned by peer %s; run unseed on that node", name, sv.Publisher.Short())
	}
	// TODO(saml) could these be consolidated into one API?
	s.forwardEvents(s.store.DeleteStaticSpec(name))
	s.forwardEvents(s.store.ReleaseStatic(name))
	return nil
}

func (s *Service) run(ctx context.Context) {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcile(ctx)
		case <-s.trigger:
			s.reconcile(ctx)
		}
	}
}

func (s *Service) reconcile(ctx context.Context) {
	snap := s.store.Snapshot()
	for name, spec := range snap.StaticSpecs {
		if err := s.ensureReplicated(ctx, snap, name, spec.Spec); err != nil {
			s.log.Debugw("static replication pending", "name", name, "err", err)
		}
	}
}

// StaticBlobs returns the hex digests of all blobs that back published
// static sites — manifest blobs plus the file blobs they reference. Sites
// whose manifest cannot be loaded locally yet contribute only the manifest
// digest; file digests join once the manifest is fetched.
func (s *Service) StaticBlobs() map[string]struct{} {
	snap := s.store.Snapshot()
	out := make(map[string]struct{}, len(snap.StaticSpecs))
	for _, spec := range snap.StaticSpecs {
		digest := spec.Spec.ManifestDigest
		out[digest] = struct{}{}
		manifest, err := s.loadManifest(digest)
		if err != nil {
			continue
		}
		for _, fileDigest := range manifest.paths {
			out[hex.EncodeToString(fileDigest)] = struct{}{}
		}
	}
	return out
}

func (s *Service) ensureReplicated(ctx context.Context, snap state.Snapshot, name string, spec state.StaticSpec) error {
	if err := s.ensureLocal(ctx, snap, spec.ManifestDigest); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}

	manifest, err := s.loadManifest(spec.ManifestDigest)
	if err != nil {
		return err
	}

	// Non-serving peers stop after the manifest — they don't hold the
	// referenced files or claim the site, but the manifest is enough to
	// enumerate file digests for keep-set membership.
	if !s.canServe {
		return nil
	}

	for path, digest := range manifest.paths {
		if err := s.ensureLocal(ctx, snap, hex.EncodeToString(digest)); err != nil {
			return fmt.Errorf("path %s: %w", path, err)
		}
	}

	if _, alreadyClaimed := snap.StaticClaims[name][s.localID]; alreadyClaimed {
		return nil
	}
	s.forwardEvents(s.store.ClaimStatic(name))
	s.log.Infow("claimed static site", "name", name, "paths", len(manifest.paths))
	return nil
}

func (s *Service) ensureLocal(ctx context.Context, snap state.Snapshot, hash string) error {
	if s.blobs.Has(hash) {
		return nil
	}
	peers := snap.PeersWithBlob(hash)
	if len(peers) == 0 {
		return fmt.Errorf("no peers advertise blob %s", types.ShortHash(hash))
	}
	return s.blobs.Fetch(ctx, hash, peers)
}
