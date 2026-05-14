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
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const defaultFetchTimeout = 15 * time.Second

var (
	ErrNotLocal    = errors.New("blob not present in local store")
	ErrNotEntitled = errors.New("local cert not entitled to hold blob")
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
	PublishPresigned(hash, name string, presignedAuth *admissionv1.SpecAuth) error
	Remove(hash string) error
	RemovePresigned(hash string, presignedAuth *admissionv1.SpecAuth) error
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
	SetBlobSpecPresigned(spec state.BlobSpec, presignedAuth *admissionv1.SpecAuth) ([]state.Event, error)
	DeleteBlobSpec(digest string) ([]state.Event, error)
	DeleteBlobSpecPresigned(digest string, presignedAuth *admissionv1.SpecAuth) ([]state.Event, error)
	SetBlobWrapping(wrapping *statev1.BlobWrappingChange) []state.Event
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
	creds          credsProvider
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

// credsProvider lets blobs.Service look up the local node's current
// delegation cert without coupling to the auth package's full
// NodeCredentials surface. The cert is needed to sign self-wrappings
// at publish time.
type credsProvider interface {
	Cert() *admissionv1.DelegationCert
}

var _ BlobsAPI = (*Service)(nil)

func New(pollenDir string, self types.PeerKey, mesh streamOpener, st blobState, gate hostGate, creds credsProvider, signPriv ed25519.PrivateKey) (*Service, error) {
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
		creds:          creds,
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
// self-wrapping skips re-encryption entirely. AES-GCM uses a fresh
// nonce per Encrypt, so a naive re-Put would overwrite the envelope on
// disk with one encrypted under a new DEK while the wrappings issued
// to remote peers still encoded the old DEK — the receiver would then
// AEAD-fail at Get time. Idempotency keeps wrappings consistent across
// re-seeds of the same content. The defence-in-depth refanout below
// only fires when the envelope went missing (e.g. manual cleanup) and
// we have to re-Put through the full path.
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
	// Refresh any existing fanout wrappings under the fresh DEK. Only
	// reachable when the local envelope was missing but stale wrappings
	// were left in gossip from a previous tenure (manual cleanup,
	// half-pruned state). The common re-Put case takes the idempotent
	// fast path above. Surface the error at warn so an operator can
	// see when fanout-refresh has failed and a manual re-wrap is
	// needed; we don't unwind the local Put because the local
	// envelope IS valid under the new DEK and self-wrapping succeeded.
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
// peers are left alone — those gossip slots are theirs to refresh.
func (s *Service) refanoutWrappings(hash string, dek []byte) error {
	if s.state == nil || s.signPriv == nil || s.creds == nil {
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
		if !bytes.Equal(w.GetWrapper().GetClaims().GetSubjectPub(), s.signPub) {
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

// Get returns an error when no path back to the DEK has been gossiped
// to this node yet; the caller can retry once gossip settles.
//
// AEAD authentication failure means our envelope and the unwrapped DEK
// disagree — almost certainly stale state from a prior Put cycle where
// the wrapping in gossip got refreshed but the envelope on disk
// didn't (or vice versa). Evict both so the next ensureLocal +
// reconcile pulls a fresh copy and the publisher re-issues a wrapping
// that matches the envelope it currently holds.
func (s *Service) Get(hash string) (io.ReadCloser, error) {
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

// issueWrappingFor gives recipient a published path back to the DEK.
// No-op when this node lacks a signer (test fixtures that bypass
// wrapping) or recipient is self (Put's self-wrap already covers it).
//
// Always re-issues, even when a wrapping already exists in gossip: a
// stale wrapping from a prior Put cycle would still appear "present"
// but decrypt to a DEK that no longer matches the current envelope.
// SetBlobWrapping dedups exact byte-equality, so this is only spammy
// when the wrapping actually needs to change.
func (s *Service) issueWrappingFor(hash string, recipient types.PeerKey) error {
	if s.signPriv == nil || s.state == nil || s.creds == nil {
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
	if s.signPriv == nil || s.creds == nil {
		return errors.New("blobs: cannot publish wrapping without signer + cert")
	}
	cert := s.creds.Cert()
	if cert == nil {
		return errors.New("blobs: local cert unavailable; cannot wrap DEK")
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
	wrapping, err := auth.IssueBlobWrapping(s.signPriv, cert, hashBytes, recipient, wrapped)
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
func (s *Service) PublishPresigned(hash, name string, presignedAuth *admissionv1.SpecAuth) error {
	if !s.store.Has(hash) {
		return ErrNotLocal
	}
	if s.state == nil {
		return nil
	}
	_, err := s.state.SetBlobSpecPresigned(state.BlobSpec{Name: name, Digest: hash}, presignedAuth)
	return err
}

func (s *Service) Remove(hash string) error {
	if err := s.removeLocalBytes(hash); err != nil {
		return err
	}
	if s.state == nil {
		return nil
	}
	if _, err := s.state.DeleteBlobSpec(hash); err != nil {
		return err
	}
	return nil
}

// RemovePresigned applies a tenant-signed tombstone for a named blob.
// The daemon evicts the local bytes (and DEK) when it has them, then
// gossips the presigned tombstone regardless. In wire mode the caller
// may dial any edge node — only one of them holds the bytes, but all
// of them can relay the tombstone now that DeleteBlobSpecPresigned
// looks up the body across every peer's log (Phase 3f). See Remove
// for the wrapping-vs-spec lifecycle.
func (s *Service) RemovePresigned(hash string, presignedAuth *admissionv1.SpecAuth) error {
	if err := s.removeLocalBytes(hash); err != nil && !errors.Is(err, ErrNotLocal) {
		return err
	}
	if s.state == nil {
		return nil
	}
	_, err := s.state.DeleteBlobSpecPresigned(hash, presignedAuth)
	return err
}

// removeLocalBytes evicts hash from the CAS and re-publishes the local
// blob list. Wrappings are append-only on the wire (admission rejects
// tombstones), so the spec deletion that callers chain after this is
// what eventually revokes receiver access: BlobSpec drops out of the
// keep set at the next Prune, the receiver's evictDEK runs, and the
// stranded wrapping in gossip ages out with the wrapper's cert.
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
	cert := snap.LocalCert()
	if cert == nil {
		return ErrNotEntitled
	}
	return s.mayStoreSnap(snap, cert, hash)
}

func (s *Service) mayStoreSnap(snap state.Snapshot, cert *admissionv1.DelegationCert, hash string) error {
	auths := snap.BlobEntitlements(hash, s)
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
	cert := snap.LocalCert()
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
