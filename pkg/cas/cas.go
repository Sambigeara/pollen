// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sambigeara/pollen/pkg/plnfs"
)

var ErrNotFound = errors.New("artifact not found")

// Store is a content-addressable artifact store backed by the local filesystem.
type Store struct {
	root string
}

func New(pollenDir string) (*Store, error) {
	return &Store{root: filepath.Join(pollenDir, "cas")}, nil
}

// Put streams the artifact from r into the store and returns its SHA-256 hex digest.
func (s *Store) Put(r io.Reader) (string, error) {
	if err := plnfs.EnsureDir(s.root); err != nil {
		return "", fmt.Errorf("cas: ensure root: %w", err)
	}
	tmp, err := os.CreateTemp(s.root, ".put-*.tmp")
	if err != nil {
		return "", fmt.Errorf("cas: create temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) //nolint:errcheck

	hasher := sha256.New()
	if _, err := io.Copy(tmp, io.TeeReader(r, hasher)); err != nil {
		tmp.Close() //nolint:errcheck
		return "", fmt.Errorf("cas: write artifact: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("cas: close temp: %w", err)
	}
	// Apply perms before rename so the commit step leaves the inode in
	// its final state; a post-rename chmod would briefly expose a
	// discoverable artifact that outside group members can't read.
	if err := plnfs.SetGroupReadable(tmpName); err != nil {
		return "", fmt.Errorf("cas: set perms: %w", err)
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	dir := filepath.Join(s.root, hash[:2])
	if err := plnfs.EnsureDir(dir); err != nil {
		return "", fmt.Errorf("cas: create shard dir: %w", err)
	}
	dest := filepath.Join(dir, hash+".wasm")
	if err := os.Rename(tmpName, dest); err != nil {
		return "", fmt.Errorf("cas: commit artifact: %w", err)
	}
	return hash, nil
}

func (s *Store) Get(hash string) (io.ReadCloser, error) {
	p := s.path(hash)
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("cas: open artifact: %w", err)
	}
	return f, nil
}

func (s *Store) Has(hash string) bool {
	_, err := os.Stat(s.path(hash))
	return err == nil
}

func (s *Store) path(hash string) string {
	return filepath.Join(s.root, hash[:2], hash+".wasm")
}
