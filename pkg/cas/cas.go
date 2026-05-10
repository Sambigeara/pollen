// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package cas

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sambigeara/pollen/pkg/plnfs"
)

var (
	ErrNotFound = errors.New("artifact not found")

	// ErrLegacyStore is returned when the on-disk CAS lacks the
	// version marker but is non-empty: contents predate the
	// encryption-at-rest format and the daemon refuses to start
	// rather than treating plaintext bytes as envelopes. Recovery is
	// to remove the cas directory; blobs re-fetch from peers, and
	// blobs only the local node ever held need re-uploading.
	ErrLegacyStore = errors.New("cas: pre-encryption store; remove the cas directory to reset")
)

const (
	versionFile    = ".version"
	currentVersion = "v1"
	tempPrefix     = ".put-"
	tempSuffix     = ".tmp"
)

type Store struct {
	root string
}

type Entry struct {
	ModTime time.Time
	Hash    string
}

// New opens the CAS rooted at pollenDir/cas. A fresh or empty directory
// is initialised at the current envelope format; an existing non-empty
// directory without the version marker returns ErrLegacyStore so the
// daemon refuses to read pre-encryption blobs as if they were
// envelopes.
func New(pollenDir string) (*Store, error) {
	s := &Store{root: filepath.Join(pollenDir, "cas")}
	if err := s.ensureVersion(); err != nil {
		return nil, err
	}
	return s, nil
}

// Put encrypts plaintext under dek with AES-256-GCM and writes the
// resulting envelope (nonce || ciphertext+tag) to disk. The address
// returned is sha256(plaintext), so the same plaintext always lands at
// the same address regardless of which DEK or nonce was used.
func (s *Store) Put(r io.Reader, dek []byte) (string, error) {
	// TODO(saml) this requires us to buffer the whole payload in memory which
	// could become problematic. Alternative approaches?
	plaintext, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("cas: read plaintext: %w", err)
	}
	envelope, err := Encrypt(plaintext, dek)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(plaintext)
	hash := hex.EncodeToString(digest[:])
	if err := s.writeFile(hash, envelope); err != nil {
		return "", err
	}
	return hash, nil
}

// PutCiphertext writes a peer-supplied envelope to disk under the
// declared plaintext hash. The store cannot verify the hash without a
// DEK, so corruption is caught at the next Get; the trade-off is that
// ciphertext flows through the wire path unaltered, never touching
// plaintext on intermediate hops.
func (s *Store) PutCiphertext(plaintextHash string, r io.Reader) error {
	envelope, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("cas: read envelope: %w", err)
	}
	return s.writeFile(plaintextHash, envelope)
}

// Get reads the envelope from disk and decrypts under dek, returning
// the plaintext stream. Tag mismatch surfaces as a decrypt error. The
// returned reader implements io.Seeker so http.ServeContent and other
// range-aware consumers can seek without re-decrypting.
func (s *Store) Get(hash string, dek []byte) (io.ReadCloser, error) {
	envelope, err := s.readFile(hash)
	if err != nil {
		return nil, err
	}
	plaintext, err := Decrypt(envelope, dek)
	if err != nil {
		return nil, err
	}
	return &seekableEnvelope{Reader: bytes.NewReader(plaintext)}, nil
}

// seekableEnvelope satisfies io.ReadCloser while preserving io.Seeker
// from the underlying *bytes.Reader. io.NopCloser would strip Seek and
// silently downgrade callers like http.ServeContent to a 503 fallback.
type seekableEnvelope struct {
	*bytes.Reader
}

func (*seekableEnvelope) Close() error { return nil }

// GetCiphertext returns the raw on-disk envelope for serving to peers
// without decryption. Wire transport is already TLS-protected, so
// ciphertext-on-wire keeps both confidentiality (under the DEK
// wrapping) and integrity (under the GCM tag) without needing the
// receiving node to ever observe plaintext until it has its own
// wrapping.
func (s *Store) GetCiphertext(hash string) (io.ReadCloser, error) {
	p := s.path(hash)
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("cas: open envelope: %w", err)
	}
	return f, nil
}

func (s *Store) Has(hash string) bool {
	_, err := os.Stat(s.path(hash))
	return err == nil
}

func (s *Store) Remove(hash string) error {
	p := s.path(hash)
	if err := os.Remove(p); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotFound
		}
		return fmt.Errorf("cas: remove artifact: %w", err)
	}
	// Best-effort drop of the now-empty shard dir; ENOTEMPTY is the
	// expected outcome when a sibling blob remains.
	_ = os.Remove(filepath.Dir(p)) //nolint:errcheck
	return nil
}

func (s *Store) Entries() ([]Entry, error) {
	shards, err := os.ReadDir(s.root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("cas: read root: %w", err)
	}
	var out []Entry
	for _, shard := range shards {
		if !shard.IsDir() || len(shard.Name()) != 2 {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(s.root, shard.Name()))
		if err != nil {
			return nil, fmt.Errorf("cas: read shard %s: %w", shard.Name(), err)
		}
		for _, e := range entries {
			hash := e.Name()
			if len(hash) != hex.EncodedLen(sha256.Size) {
				continue
			}
			info, err := e.Info()
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return nil, fmt.Errorf("cas: stat %s: %w", hash, err)
			}
			out = append(out, Entry{Hash: hash, ModTime: info.ModTime()})
		}
	}
	return out, nil
}

func (s *Store) ensureVersion() error {
	if err := plnfs.EnsureDir(s.root); err != nil {
		return fmt.Errorf("cas: ensure root: %w", err)
	}
	if err := s.sweepOrphanTempfiles(); err != nil {
		return err
	}

	versionPath := filepath.Join(s.root, versionFile)
	data, err := os.ReadFile(versionPath)
	if err == nil {
		if string(data) != currentVersion {
			return fmt.Errorf("cas: unsupported store version %q at %s", string(data), s.root)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cas: read version: %w", err)
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return fmt.Errorf("cas: read root: %w", err)
	}
	for _, e := range entries {
		if e.Name() == versionFile {
			continue
		}
		return fmt.Errorf("%w: %s", ErrLegacyStore, s.root)
	}
	return os.WriteFile(versionPath, []byte(currentVersion), 0o600) //nolint:mnd
}

// sweepOrphanTempfiles drops any `.put-*.tmp` left behind by a Put that
// crashed before its rename. Runs on every open so orphans don't
// accumulate after the version marker is written; bounded by the size
// of the cas root directory listing.
func (s *Store) sweepOrphanTempfiles() error {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("cas: read root for tempfile sweep: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, tempPrefix) || !strings.HasSuffix(name, tempSuffix) {
			continue
		}
		_ = os.Remove(filepath.Join(s.root, name)) //nolint:errcheck
	}
	return nil
}

func (s *Store) writeFile(hash string, envelope []byte) error {
	tmp, err := os.CreateTemp(s.root, tempPrefix+"*"+tempSuffix)
	if err != nil {
		return fmt.Errorf("cas: create temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) //nolint:errcheck

	if _, err := tmp.Write(envelope); err != nil {
		tmp.Close() //nolint:errcheck
		return fmt.Errorf("cas: write envelope: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cas: close temp: %w", err)
	}
	// Apply perms before rename so the commit step leaves the inode in
	// its final state; a post-rename chmod would briefly expose a
	// discoverable artifact that outside group members can't read.
	if err := plnfs.SetGroupReadable(tmpName); err != nil {
		return fmt.Errorf("cas: set perms: %w", err)
	}

	dir := filepath.Join(s.root, hash[:2])
	if err := plnfs.EnsureDir(dir); err != nil {
		return fmt.Errorf("cas: create shard dir: %w", err)
	}
	dest := filepath.Join(dir, hash)
	if err := os.Rename(tmpName, dest); err != nil {
		return fmt.Errorf("cas: commit artifact: %w", err)
	}
	return nil
}

func (s *Store) readFile(hash string) ([]byte, error) {
	data, err := os.ReadFile(s.path(hash))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("cas: read artifact: %w", err)
	}
	return data, nil
}

func (s *Store) path(hash string) string {
	return filepath.Join(s.root, hash[:2], hash)
}
