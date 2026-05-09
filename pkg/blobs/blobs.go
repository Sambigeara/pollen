// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

const defaultFetchTimeout = 15 * time.Second

var (
	ErrNotLocal    = errors.New("blob not present in local store")
	ErrNotEntitled = errors.New("local cert not entitled to hold blob")
)

type BlobsAPI interface {
	Put(r io.Reader) (string, error)
	Get(hash string) (io.ReadCloser, error)
	Has(hash string) bool
	Fetch(ctx context.Context, hash string, peers []types.PeerKey) error
	Serve(stream io.ReadWriteCloser, hash string)
	Announce(hash string) error
	Publish(hash, name string, policy *admissionv1.Predicate) error
	Remove(hash string) error
	Rescan() error
	Prune(keep map[string]struct{}, minAge time.Duration) ([]string, error)
}

type streamOpener interface {
	OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error)
}

type blobState interface {
	Snapshot() state.Snapshot
	SetLocalBlobs(digests []string) []state.Event
	SetBlobSpec(spec state.BlobSpec, policy *admissionv1.Predicate) ([]state.Event, error)
	DeleteBlobSpec(digest string) ([]state.Event, error)
}

type blobStore interface {
	Put(r io.Reader) (string, error)
	Get(hash string) (io.ReadCloser, error)
	Has(hash string) bool
	Remove(hash string) error
	Entries() ([]cas.Entry, error)
}

// hostGate authorises a host (the local node) to hold bytes covered by
// specAuth's policy. Nil gate means hosting is unrestricted, used by
// tests that don't exercise entitlement.
type hostGate interface {
	MayHost(hostCert *admissionv1.DelegationCert, specAuth *admissionv1.SpecAuth) error
}

type Service struct {
	store          blobStore
	mesh           streamOpener
	state          blobState
	gate           hostGate
	local          map[string]struct{}
	parsedManifest map[string]map[string]struct{}
	timeout        time.Duration
	mu             sync.Mutex
	manifestMu     sync.Mutex
	self           types.PeerKey
}

var _ BlobsAPI = (*Service)(nil)

func New(pollenDir string, self types.PeerKey, mesh streamOpener, st blobState, gate hostGate) (*Service, error) {
	c, err := cas.New(pollenDir)
	if err != nil {
		return nil, err
	}
	return &Service{
		store:          c,
		mesh:           mesh,
		state:          st,
		gate:           gate,
		self:           self,
		timeout:        defaultFetchTimeout,
		local:          make(map[string]struct{}),
		parsedManifest: make(map[string]map[string]struct{}),
	}, nil
}

func (s *Service) Put(r io.Reader) (string, error) {
	hash, err := s.store.Put(r)
	if err != nil {
		return "", err
	}
	if err := s.Announce(hash); err != nil {
		return "", err
	}
	return hash, nil
}

func (s *Service) Get(hash string) (io.ReadCloser, error) { return s.store.Get(hash) }
func (s *Service) Has(hash string) bool                   { return s.store.Has(hash) }

func (s *Service) Announce(hash string) error {
	if !s.store.Has(hash) {
		return ErrNotLocal
	}
	s.mu.Lock()
	if _, ok := s.local[hash]; ok {
		s.mu.Unlock()
		return nil
	}
	s.local[hash] = struct{}{}
	digests := slices.Sorted(maps.Keys(s.local))
	s.mu.Unlock()
	s.publish(digests)
	return nil
}

func (s *Service) Publish(hash, name string, policy *admissionv1.Predicate) error {
	if !s.store.Has(hash) {
		return ErrNotLocal
	}
	if s.state == nil {
		return nil
	}
	_, err := s.state.SetBlobSpec(state.BlobSpec{Name: name, Digest: hash}, policy)
	return err
}

func (s *Service) Remove(hash string) error {
	if err := s.store.Remove(hash); err != nil {
		if errors.Is(err, cas.ErrNotFound) {
			return ErrNotLocal
		}
		return err
	}
	s.mu.Lock()
	delete(s.local, hash)
	digests := slices.Sorted(maps.Keys(s.local))
	s.mu.Unlock()
	s.publish(digests)
	if s.state != nil {
		if _, err := s.state.DeleteBlobSpec(hash); err != nil {
			return err
		}
	}
	return nil
}

func KeepSet(snap state.Snapshot, extras ...map[string]struct{}) map[string]struct{} {
	keep := make(map[string]struct{}, len(snap.Specs)+len(snap.BlobSpecs))
	for h := range snap.Specs {
		keep[h] = struct{}{}
	}
	for h := range snap.BlobSpecs {
		keep[h] = struct{}{}
	}
	for _, extra := range extras {
		for h := range extra {
			keep[h] = struct{}{}
		}
	}
	return keep
}

func (s *Service) Prune(keep map[string]struct{}, minAge time.Duration) ([]string, error) {
	entries, err := s.store.Entries()
	if err != nil {
		return nil, err
	}

	// A blob in the keep set can still be evicted when our local cert is
	// no longer entitled by any referencing spec. Filter the keep set
	// down to entitled blobs first so the age-based loop below treats
	// policy-denied entries the same as orphans.
	keep = s.filterByEntitlement(keep)

	cutoff := time.Now().Add(-minAge)
	var removed []string
	for _, e := range entries {
		if _, ok := keep[e.Hash]; ok {
			continue
		}
		if e.ModTime.After(cutoff) {
			continue
		}
		if err := s.Remove(e.Hash); err != nil {
			if errors.Is(err, ErrNotLocal) {
				continue
			}
			return removed, err
		}
		removed = append(removed, e.Hash)
	}
	return removed, nil
}

// MayStore decides whether the local node is entitled to hold the bytes
// addressed by hash. A blob is held iff at least one referencing spec's
// policy is satisfied by the local cert, where references are direct
// (workload hash, blob digest, static manifest digest) and nested
// (paths inside a locally-available static manifest).
//
// Returns ErrNotEntitled when no referencing spec admits the local
// cert, including the case where no referencing spec exists at all.
func (s *Service) MayStore(hash string) error {
	if s.gate == nil || s.state == nil {
		return nil
	}
	snap := s.state.Snapshot()
	cert := localCertFromSnap(snap)
	if cert == nil {
		return ErrNotEntitled
	}
	return s.mayStoreSnap(snap, cert, hash)
}

func (s *Service) mayStoreSnap(snap state.Snapshot, cert *admissionv1.DelegationCert, hash string) error {
	auths := snap.BlobEntitlements(hash)
	auths = append(auths, s.nestedManifestEntitlements(snap, hash)...)
	if len(auths) == 0 {
		return ErrNotEntitled
	}
	for _, sa := range auths {
		if s.gate.MayHost(cert, sa) == nil {
			return nil
		}
	}
	return ErrNotEntitled
}

func (s *Service) filterByEntitlement(keep map[string]struct{}) map[string]struct{} {
	if s.gate == nil || s.state == nil {
		return keep
	}
	snap := s.state.Snapshot()
	cert := localCertFromSnap(snap)
	if cert == nil {
		return keep
	}
	out := make(map[string]struct{}, len(keep))
	for h := range keep {
		if s.mayStoreSnap(snap, cert, h) == nil {
			out[h] = struct{}{}
		}
	}
	return out
}

func localCertFromSnap(snap state.Snapshot) *admissionv1.DelegationCert {
	nv, ok := snap.Nodes[snap.LocalID]
	if !ok {
		return nil
	}
	return nv.Cert
}

// nestedManifestEntitlements reports static-spec auths whose manifest is
// locally available and references hash as one of its path digests.
// Skips manifests that aren't readable yet: a not-yet-fetched manifest
// can't entitle its nested blobs, but the entitlement materialises on
// the next pass once the manifest arrives in CAS.
func (s *Service) nestedManifestEntitlements(snap state.Snapshot, hash string) []*admissionv1.SpecAuth {
	var out []*admissionv1.SpecAuth
	for _, sv := range snap.StaticSpecs {
		digest := sv.Spec.ManifestDigest
		if digest == "" || sv.Auth == nil {
			continue
		}
		paths, ok := s.manifestPaths(digest)
		if !ok {
			continue
		}
		if _, hit := paths[hash]; hit {
			out = append(out, sv.Auth)
		}
	}
	return out
}

func (s *Service) manifestPaths(digest string) (map[string]struct{}, bool) {
	if s.store == nil {
		return nil, false
	}
	s.manifestMu.Lock()
	cached, ok := s.parsedManifest[digest]
	s.manifestMu.Unlock()
	if ok {
		return cached, true
	}

	rc, err := s.store.Get(digest)
	if err != nil {
		return nil, false
	}
	data, err := io.ReadAll(rc)
	rc.Close() //nolint:errcheck
	if err != nil {
		return nil, false
	}
	m := &statev1.StaticManifest{}
	if err := m.UnmarshalVT(data); err != nil {
		return nil, false
	}
	paths := make(map[string]struct{}, len(m.GetPaths()))
	for _, p := range m.GetPaths() {
		paths[hex.EncodeToString(p.GetDigest())] = struct{}{}
	}

	s.manifestMu.Lock()
	s.parsedManifest[digest] = paths
	s.manifestMu.Unlock()
	return paths, true
}

func (s *Service) Rescan() error {
	entries, err := s.store.Entries()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.local = make(map[string]struct{}, len(entries))
	for _, e := range entries {
		s.local[e.Hash] = struct{}{}
	}
	digests := slices.Sorted(maps.Keys(s.local))
	s.mu.Unlock()
	s.publish(digests)
	return nil
}

func (s *Service) publish(digests []string) {
	if s.state == nil {
		return
	}
	s.state.SetLocalBlobs(digests)
}
