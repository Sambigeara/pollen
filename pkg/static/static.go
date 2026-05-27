// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// Package static replicates static-site manifests and their referenced
// blobs to claiming nodes.
package static

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
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
	SeedStatic(name string, manifestDigest []byte, policy *admissionv1.Predicate) error
	SeedStaticPresigned(name string, manifestDigest []byte, presignedFact *factv1.Fact) error
	UnseedStatic(name string) error
	UnseedStaticPresigned(name string, presignedFact *factv1.Fact) error
	StaticBlobs() map[string]struct{}
}

type stateStore interface {
	Snapshot() state.Snapshot
	SetStaticSpec(spec state.StaticSpec, policy *admissionv1.Predicate) ([]state.Event, error)
	SetStaticSpecPresigned(spec state.StaticSpec, presignedFact *factv1.Fact) ([]state.Event, error)
	DeleteStaticSpec(name string) ([]state.Event, error)
	DeleteStaticSpecPresigned(name string, presignedFact *factv1.Fact) ([]state.Event, error)
	ClaimStatic(name string, authority types.PeerKey) []state.Event
	ReleaseStatic(name string, authority types.PeerKey) []state.Event
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
	domain        string
	wg            sync.WaitGroup
	localID       types.PeerKey
	canServe      bool
}

var _ StaticAPI = (*Service)(nil)

// canServe gates file-fetch; non-serving peers still pull manifests for digest enumeration.
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

// SetDomain sets the public DNS suffix this listener resolves Host
// against (Host = `<name>-<slug>.<domain>`). Empty preserves pre-
// Pollen-Cloud Host == spec-name behaviour. Input must be the canonical
// leading-dot, lower-case form (supervisor normalises). Call before
// Start: handlers read it from spawned goroutines.
func (s *Service) SetDomain(d string) {
	s.domain = d
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

// ErrPolicyOnStatic rejects publish-time policy on static sites: HTTP
// serving is unauthenticated by design, so a caller predicate has no
// principal to evaluate against and would be silently ignored.
var ErrPolicyOnStatic = errors.New("static sites are served via plain HTTP; caller policies have no principal to evaluate against")

func (s *Service) SeedStatic(name string, manifestDigest []byte, policy *admissionv1.Predicate) error {
	if policy != nil {
		return ErrPolicyOnStatic
	}
	if len(manifestDigest) != digestSize {
		return fmt.Errorf("manifest digest must be %d bytes", digestSize)
	}
	events, err := s.store.SetStaticSpec(state.StaticSpec{
		Name:           name,
		ManifestDigest: hex.EncodeToString(manifestDigest),
	}, policy)
	if err != nil {
		return err
	}
	s.forwardEvents(events)
	return nil
}

// SeedStaticPresigned stores a tenant-signed static spec without
// re-signing. Used by the wire-mode caller flow where the daemon acts
// as a relay: the Fact is validated against the cluster root and
// gossipped as-is.
func (s *Service) SeedStaticPresigned(name string, manifestDigest []byte, presignedFact *factv1.Fact) error {
	if presignedFact.GetPolicy() != nil {
		return ErrPolicyOnStatic
	}
	if len(manifestDigest) != digestSize {
		return fmt.Errorf("manifest digest must be %d bytes", digestSize)
	}
	events, err := s.store.SetStaticSpecPresigned(state.StaticSpec{
		Name:           name,
		ManifestDigest: hex.EncodeToString(manifestDigest),
	}, presignedFact)
	if err != nil {
		return err
	}
	s.forwardEvents(events)
	return nil
}

func (s *Service) UnseedStatic(name string) error {
	events, err := s.store.DeleteStaticSpec(name)
	if err != nil {
		return err
	}
	s.forwardEvents(events)
	s.forwardEvents(s.store.ReleaseStatic(name, s.localID))
	return nil
}

// UnseedStaticPresigned applies a tenant-signed tombstone for the
// static spec named name. The daemon re-wraps the auth against the
// live body in its slot before gossiping.
func (s *Service) UnseedStaticPresigned(name string, presignedFact *factv1.Fact) error {
	events, err := s.store.DeleteStaticSpecPresigned(name, presignedFact)
	if err != nil {
		return err
	}
	s.forwardEvents(events)
	s.forwardEvents(s.store.ReleaseStatic(name, types.PeerKeyFromBytes(presignedFact.GetAuthorityPub())))
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
	// Iterate the per-publisher view, not the deduped one. When two
	// publishers seed sites under the same name, the deduped view
	// drops one of them and the losing publisher's bytes never
	// replicate; the per-publisher view keeps every (publisher, name)
	// pair so both tenants converge.
	for _, sv := range snap.StaticSpecsAll {
		if err := s.ensureReplicated(ctx, snap, sv.Spec, sv.Publisher); err != nil {
			s.log.Debugw("static replication pending", "name", sv.Spec.Name, "publisher", sv.Publisher.Short(), "err", err)
		}
	}
}

func (s *Service) StaticBlobs() map[string]struct{} {
	snap := s.store.Snapshot()
	out := make(map[string]struct{}, len(snap.StaticSpecsAll))
	for _, spec := range snap.StaticSpecsAll {
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

func (s *Service) ensureReplicated(ctx context.Context, snap state.Snapshot, spec state.StaticSpec, authority types.PeerKey) error {
	if err := s.ensureLocal(ctx, snap, spec.ManifestDigest); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}

	manifest, err := s.loadManifest(spec.ManifestDigest)
	if err != nil {
		return err
	}

	if !s.canServe {
		return nil
	}

	for path, digest := range manifest.paths {
		if err := s.ensureLocal(ctx, snap, hex.EncodeToString(digest)); err != nil {
			return fmt.Errorf("path %s: %w", path, err)
		}
	}

	if _, alreadyClaimed := snap.StaticClaims[state.StaticClaimKey{Authority: authority, Name: spec.Name}][s.localID]; alreadyClaimed {
		return nil
	}
	s.forwardEvents(s.store.ClaimStatic(spec.Name, authority))
	s.log.Infow("claimed static site", "name", spec.Name, "publisher", authority.Short(), "paths", len(manifest.paths))
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
