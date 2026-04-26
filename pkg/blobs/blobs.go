// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// Package blobs is a content-addressed byte store with peer-to-peer fetch
// over StreamTypeBlob.
package blobs

import (
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

const defaultFetchTimeout = 15 * time.Second

var ErrNotLocal = errors.New("blob not present in local store")

type BlobsAPI interface {
	Put(r io.Reader) (string, error)
	Get(hash string) (io.ReadCloser, error)
	Has(hash string) bool
	Fetch(ctx context.Context, hash string, peers []types.PeerKey) error
	Serve(stream io.ReadWriteCloser, hash string)
	Announce(hash string) error
	SetName(hash, name string) error
	Remove(hash string) error
	Rescan() error
	Prune(keep map[string]struct{}, minAge time.Duration) ([]string, error)
}

type streamOpener interface {
	OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error)
}

type blobState interface {
	SetLocalBlobs(digests []string) []state.Event
	SetBlobSpec(spec state.BlobSpec) ([]state.Event, error)
	DeleteBlobSpec(digest string) []state.Event
}

type blobStore interface {
	Put(r io.Reader) (string, error)
	Get(hash string) (io.ReadCloser, error)
	Has(hash string) bool
	Remove(hash string) error
	Entries() ([]cas.Entry, error)
}

type Service struct {
	store   blobStore
	mesh    streamOpener
	state   blobState
	local   map[string]struct{}
	timeout time.Duration
	mu      sync.Mutex
	self    types.PeerKey
}

var _ BlobsAPI = (*Service)(nil)

func New(pollenDir string, self types.PeerKey, mesh streamOpener, st blobState) (*Service, error) {
	c, err := cas.New(pollenDir)
	if err != nil {
		return nil, err
	}
	return &Service{
		store:   c,
		mesh:    mesh,
		state:   st,
		self:    self,
		timeout: defaultFetchTimeout,
		local:   make(map[string]struct{}),
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

// SetName requires the blob to be present locally. Re-naming the same
// digest replaces any previous name from this publisher.
func (s *Service) SetName(hash, name string) error {
	if !s.store.Has(hash) {
		return ErrNotLocal
	}
	if s.state == nil {
		return nil
	}
	_, err := s.state.SetBlobSpec(state.BlobSpec{Name: name, Digest: hash})
	return err
}

// Remove evicts the blob from the local store, un-announces availability,
// and tombstones any BlobSpec previously published by this peer. Other
// peers retain their own copies — removal is publisher-local only.
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
		s.state.DeleteBlobSpec(hash)
	}
	return nil
}

// KeepSet returns digests protected from eviction: workload spec hashes,
// named blob-spec digests, plus any extras the caller pre-collects (blobs
// must not import sibling packages, so static passes its manifest + file
// digests in here).
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

// Prune evicts blobs outside keep. minAge shields freshly-written files
// that don't yet have a spec referencing them — e.g. `pln seed` on a
// directory uploads file blobs anonymously before publishing the
// StaticSpec.
func (s *Service) Prune(keep map[string]struct{}, minAge time.Duration) ([]string, error) {
	entries, err := s.store.Entries()
	if err != nil {
		return nil, err
	}
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
