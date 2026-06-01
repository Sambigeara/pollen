// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const defaultFetchTimeout = 15 * time.Second

var (
	ErrNotLocal    = errors.New("blob not present in local store")
	ErrNotEntitled = errors.New("local grant not entitled to hold blob")
	errNoWrapping  = errors.New("local node has no DEK wrapping for blob")
)

type BlobsAPI interface {
	Put(r io.Reader) (string, error)
	Get(hash string) (io.ReadCloser, error)
	Has(hash string) bool
	Fetch(ctx context.Context, hash string, peers []types.PeerKey) error
	FetchPlaintext(ctx context.Context, hash string) (io.ReadCloser, error)
	Serve(stream io.ReadWriteCloser, hash string, requester types.PeerKey)
	ServePlaintext(stream io.ReadWriteCloser, hash string)
	Announce(hash string) error
	Publish(hash, name string, policy *admissionv1.Predicate) error
	PublishPresigned(hash, name string, presignedFact *factv1.Fact) error
	Remove(hash string) error
	RemovePresigned(hash string, presignedFact *factv1.Fact) error
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
	SetBlobSpecPresigned(spec state.BlobSpec, presignedFact *factv1.Fact) ([]state.Event, error)
	DeleteBlobSpec(digest string) ([]state.Event, error)
	DeleteBlobSpecPresigned(digest string, presignedFact *factv1.Fact) ([]state.Event, error)
	SetBlobWrapping(wrapping *factv1.BlobWrapping) []state.Event
}

type blobStore interface {
	Put(r io.Reader, dek []byte) (string, error)
	PutCiphertext(plaintextHash string, r io.Reader) error
	Get(hash string, dek []byte) (io.ReadCloser, error)
	GetCiphertext(hash string) (io.ReadCloser, error)
	Has(hash string) bool
	Remove(hash string) error
	Entries() ([]cas.Entry, error)
}

// hostGate authorises a host (the local node) to hold bytes covered by
// the Fact's policy. Nil gate means hosting is unrestricted, used by
// tests that don't exercise entitlement.
type hostGate interface {
	MayHost(hostGrant *identityv1.Grant, f *factv1.Fact) error
}

type Service struct {
	store          blobStore
	mesh           streamOpener
	state          blobState
	gate           hostGate
	signer         *fact.Signer
	dekCache       map[string][]byte
	local          map[string]struct{}
	parsedManifest map[string]map[string]struct{}
	log            *zap.SugaredLogger
	signPub        ed25519.PublicKey
	signPriv       ed25519.PrivateKey
	timeout        time.Duration
	mu             sync.Mutex
	manifestMu     sync.Mutex
	dekMu          sync.Mutex
	self           types.PeerKey
}

var _ BlobsAPI = (*Service)(nil)

func New(pollenDir string, self types.PeerKey, mesh streamOpener, st blobState, gate hostGate, signer *fact.Signer, signPriv ed25519.PrivateKey) (*Service, error) {
	c, err := cas.New(pollenDir)
	if err != nil {
		return nil, err
	}
	var signPub ed25519.PublicKey
	if signPriv != nil {
		signPub = signPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	}
	return &Service{
		store:          c,
		mesh:           mesh,
		state:          st,
		gate:           gate,
		signer:         signer,
		signPriv:       signPriv,
		signPub:        signPub,
		self:           self,
		timeout:        defaultFetchTimeout,
		local:          make(map[string]struct{}),
		parsedManifest: make(map[string]map[string]struct{}),
		dekCache:       make(map[string][]byte),
		log:            zap.S().Named("blobs"),
	}, nil
}

// Put encrypts under a fresh DEK and gossips a self-addressed wrapping
// so the publisher retains read access. The returned hash is sha256 of
// the plaintext, not the envelope.
//
// Put is idempotent: re-Putting content we already hold under a valid
// self-wrapping skips re-encryption, so wrappings already issued stay
// valid against the stored envelope (see cas.ErrAEADAuth). The refanout
// below only fires when the envelope went missing (e.g. manual cleanup)
// and we have to re-Put through the full path.
func (s *Service) Put(r io.Reader) (string, error) {
	plaintext, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("read plaintext: %w", err)
	}
	digest := sha256.Sum256(plaintext)
	hash := hex.EncodeToString(digest[:])

	if s.store.Has(hash) {
		// State-nil path (test fixtures, early bootstrap): we hold the
		// envelope, no gossip-side wrappings exist yet, the fast path
		// is safe. Without this branch we'd fall through to a
		// destructive re-encrypt that overwrites the on-disk envelope
		// under a fresh DEK.
		if s.state == nil {
			return hash, s.Announce(hash)
		}
		if _, wrapped := s.state.Snapshot().WrappingFor(hash, s.self); wrapped {
			return hash, s.Announce(hash)
		}
	}

	dek, err := cas.GenerateDEK()
	if err != nil {
		return "", err
	}
	if _, err := s.store.Put(bytes.NewReader(plaintext), dek); err != nil {
		return "", err
	}
	if err := s.publishSelfWrapping(hash, dek); err != nil {
		// Roll back the on-disk envelope so callers don't see an
		// undecryptable orphan if wrapping fails (e.g. signer not
		// configured during early startup).
		_ = s.store.Remove(hash) //nolint:errcheck
		return "", err
	}
	s.cacheDEK(hash, dek)
	// Refresh fanout wrappings under the fresh DEK; only reached when the
	// envelope was missing but stale wrappings lingered in gossip. Warn
	// rather than unwind: the local envelope is valid under the new DEK,
	// so a failed refresh only means remote recipients may need a manual
	// re-wrap.
	if err := s.refanoutWrappings(hash, dek); err != nil {
		s.log.Warnw("refanout wrappings failed; existing recipients may need manual re-wrap",
			"hash", types.ShortHash(hash), "err", err)
	}
	if err := s.Announce(hash); err != nil {
		return "", err
	}
	return hash, nil
}

// refanoutWrappings walks every wrapping we previously issued for hash
// and re-issues it under the supplied DEK. Wrappings issued by other
// peers are left alone: those gossip slots are theirs to refresh.
func (s *Service) refanoutWrappings(hash string, dek []byte) error {
	if s.state == nil || s.signPriv == nil || s.signer == nil {
		return nil
	}
	snap := s.state.Snapshot()
	existing, ok := snap.Wrappings[hash]
	if !ok {
		return nil
	}
	for recipient, w := range existing {
		if recipient == s.self {
			continue
		}
		if !bytes.Equal(w.GetAuthorityPub(), s.signPub) {
			continue
		}
		if err := s.issueWrappingForKey(hash, ed25519.PublicKey(recipient.Bytes()), func() ([]byte, error) {
			return dek, nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// Get returns ErrNotLocal when the ciphertext has not been pulled into
// local CAS, and errNoWrapping when bytes are present but no gossiped
// wrapping addresses this node yet. Has-first ordering separates a
// byte-replication race from a key-distribution race in the log.
//
// On cas.ErrAEADAuth, evict both the envelope and the wrapping so the
// next ensureLocal and reconcile pull a fresh, matching pair.
func (s *Service) Get(hash string) (io.ReadCloser, error) {
	if !s.store.Has(hash) {
		return nil, ErrNotLocal
	}
	dek, err := s.localDEK(hash)
	if err != nil {
		return nil, err
	}
	rc, err := s.store.Get(hash, dek)
	if err != nil {
		if errors.Is(err, cas.ErrAEADAuth) {
			s.evictDEK(hash)
			_ = s.store.Remove(hash) //nolint:errcheck
		}
		return nil, err
	}
	return rc, nil
}

func (s *Service) Has(hash string) bool { return s.store.Has(hash) }

func (s *Service) localDEK(hash string) ([]byte, error) {
	// Always hand callers a copy: evictDEK zeroises the cache backing
	// in place, so a Get racing a Remove must not see its DEK mutated
	// mid-decrypt.
	s.dekMu.Lock()
	if dek, ok := s.dekCache[hash]; ok {
		out := slices.Clone(dek)
		s.dekMu.Unlock()
		return out, nil
	}
	s.dekMu.Unlock()

	if s.state == nil || s.signPriv == nil {
		return nil, errNoWrapping
	}
	snap := s.state.Snapshot()
	wrapping, ok := snap.WrappingFor(hash, s.self)
	if !ok {
		return nil, errNoWrapping
	}
	dek, err := cas.UnwrapDEK(wrapping.GetWrappedDek(), s.signPub, s.signPriv)
	if err != nil {
		return nil, err
	}
	s.cacheDEK(hash, dek)
	return slices.Clone(dek), nil
}

func (s *Service) cacheDEK(hash string, dek []byte) {
	s.dekMu.Lock()
	defer s.dekMu.Unlock()
	s.dekCache[hash] = dek
}

func (s *Service) evictDEK(hash string) {
	s.dekMu.Lock()
	defer s.dekMu.Unlock()
	if dek, ok := s.dekCache[hash]; ok {
		// Zeroise the live slice; the GC may still hold a copy, which
		// is acceptable for code blobs. Larger payloads would need an
		// mlock-backed cache.
		for i := range dek {
			dek[i] = 0
		}
		delete(s.dekCache, hash)
	}
}

// publishSelfWrapping ties decryption access to the gossip layer
// rather than to a sidecar key file that would survive cert revocation.
func (s *Service) publishSelfWrapping(hash string, dek []byte) error {
	return s.issueWrappingForKey(hash, s.signPub, func() ([]byte, error) { return dek, nil })
}

// IssueWrappingsFor pre-positions wrappings for every (hash, recipient)
// pair so callers (notably static seed) can hand serving peers a
// usable DEK before they fetch, rather than relying on the best-effort
// lazy-wrap in Serve to race the first request. Self entries are
// skipped via issueWrappingFor. Stops and returns on the first failure
// so the caller sees the cause; partial progress is fine because every
// wrapping is an append-only gossip fact.
func (s *Service) IssueWrappingsFor(hashes []string, recipients []types.PeerKey) error {
	for _, hash := range hashes {
		for _, recipient := range recipients {
			if err := s.issueWrappingFor(hash, recipient); err != nil {
				return err
			}
		}
	}
	return nil
}

// issueWrappingFor gives recipient a published path back to the DEK.
// No-op when this node lacks a signer (test fixtures that bypass
// wrapping) or recipient is self (Put's self-wrap already covers it).
//
// Always re-issues rather than trusting a wrapping already in gossip,
// which may be stale (see cas.ErrAEADAuth). SetBlobWrapping dedups
// byte-equal entries, so a re-issue only gossips when it changed.
func (s *Service) issueWrappingFor(hash string, recipient types.PeerKey) error {
	if s.signPriv == nil || s.state == nil || s.signer == nil {
		return nil
	}
	if recipient == s.self {
		return nil
	}
	return s.issueWrappingForKey(hash, ed25519.PublicKey(recipient.Bytes()), func() ([]byte, error) {
		return s.localDEK(hash)
	})
}

func (s *Service) issueWrappingForKey(hash string, recipient ed25519.PublicKey, source func() ([]byte, error)) error {
	if s.state == nil {
		return nil
	}
	if s.signer == nil {
		return errors.New("blobs: cannot publish wrapping without a fact signer")
	}
	dek, err := source()
	if err != nil {
		return err
	}
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return err
	}
	wrapped, err := cas.WrapDEK(dek, recipient)
	if err != nil {
		return err
	}
	wrapping, err := s.signer.IssueBlobWrapping(hashBytes, recipient, wrapped)
	if err != nil {
		return err
	}
	s.state.SetBlobWrapping(wrapping)
	return nil
}

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

// PublishPresigned records a tenant-signed BlobSpec. The bytes must
// already be in local CAS (the daemon stored them via Put earlier);
// the spec carries the tenant's signature.
func (s *Service) PublishPresigned(hash, name string, presignedFact *factv1.Fact) error {
	if !s.store.Has(hash) {
		return ErrNotLocal
	}
	if s.state == nil {
		return nil
	}
	_, err := s.state.SetBlobSpecPresigned(state.BlobSpec{Name: name, Digest: hash}, presignedFact)
	return err
}

// Remove tombstones the named blob's spec and gossips it synchronously.
// It deliberately does not touch local bytes: a digest may be shared by
// another owner's still-live spec, so byte reclamation is the keep-set
// janitor's sole responsibility (Prune).
func (s *Service) Remove(hash string) error {
	if s.state == nil {
		return nil
	}
	_, err := s.state.DeleteBlobSpec(hash)
	return err
}

// RemovePresigned applies a tenant-signed tombstone for a named blob and
// gossips it. In wire mode the caller may dial any edge node; only one
// holds the bytes, but all can relay the tombstone now that
// DeleteBlobSpecPresigned looks up the body across every peer's log.
// Like Remove it leaves bytes to the janitor.
func (s *Service) RemovePresigned(hash string, presignedFact *factv1.Fact) error {
	if s.state == nil {
		return nil
	}
	_, err := s.state.DeleteBlobSpecPresigned(hash, presignedFact)
	return err
}

// removeLocalBytes evicts hash from the CAS and re-publishes the local
// blob list. It is the keep-set janitor's byte-evict primitive, called
// only from Prune once a digest has dropped out of the keep set (every
// referencing spec already tombstoned). Wrappings are append-only on
// the wire (admission rejects tombstones), so eviction here is what
// revokes receiver access: the receiver's evictDEK runs and the
// stranded wrapping in gossip ages out with the wrapper's grant.
func (s *Service) removeLocalBytes(hash string) error {
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
	s.evictDEK(hash)
	return nil
}

// KeepSet is the set of content hashes the local node must retain: a
// hash is pinned iff at least one live (authority, name) spec from any
// owner references it. It iterates the un-deduped per-(authority, name)
// sources, not the deduped runtime maps, so a tie-break loser's
// reference still pins the bytes a co-owner serves. The static extras
// (state.StaticBlobs) already enumerate StaticSpecsAll.
func KeepSet(snap state.Snapshot, extras ...map[string]struct{}) map[string]struct{} {
	keep := make(map[string]struct{}, len(snap.SpecsAll)+len(snap.BlobSpecsAll))
	for _, sv := range snap.SpecsAll {
		keep[sv.Spec.Hash] = struct{}{}
	}
	for _, bv := range snap.BlobSpecsAll {
		keep[bv.Spec.Digest] = struct{}{}
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
		if err := s.removeLocalBytes(e.Hash); err != nil {
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
// addressed by hash. A blob is held iff at least one referencing Fact's
// policy is satisfied by the local grant, where references are direct
// (workload hash, blob digest, static manifest digest) and nested
// (paths inside a locally-available static manifest).
//
// Returns ErrNotEntitled when no referencing Fact admits the local
// grant, including the case where no referencing Fact exists at all.
func (s *Service) MayStore(hash string) error {
	if s.gate == nil || s.state == nil {
		return nil
	}
	snap := s.state.Snapshot()
	grant := snap.LocalGrant()
	if grant == nil {
		return ErrNotEntitled
	}
	return s.mayStoreSnap(snap, grant, hash)
}

func (s *Service) mayStoreSnap(snap state.Snapshot, grant *identityv1.Grant, hash string) error {
	facts := snap.BlobEntitlements(hash, s)
	if len(facts) == 0 {
		return ErrNotEntitled
	}
	for _, f := range facts {
		if s.gate.MayHost(grant, f) == nil {
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
	grant := snap.LocalGrant()
	if grant == nil {
		return keep
	}
	out := make(map[string]struct{}, len(keep))
	for h := range keep {
		if s.mayStoreSnap(snap, grant, h) == nil {
			out[h] = struct{}{}
		}
	}
	return out
}

// ManifestPaths resolves a static-manifest digest to its referenced
// file digests, reading the manifest blob from local CAS on first
// access and caching the result. Returns (nil, false) when the
// manifest isn't readable yet; callers re-poll on the next pass.
func (s *Service) ManifestPaths(digest string) (map[string]struct{}, bool) {
	if s.store == nil {
		return nil, false
	}
	s.manifestMu.Lock()
	cached, ok := s.parsedManifest[digest]
	s.manifestMu.Unlock()
	if ok {
		return cached, true
	}

	// Manifests live encrypted on disk; route through Service.Get so
	// the decryption + DEK lookup happens once and is cached.
	rc, err := s.Get(digest)
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
