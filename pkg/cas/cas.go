package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var ErrNotFound = errors.New("artifact not found")

const dirPerm = 0o750

// Store is a content-addressable artifact store backed by the local filesystem.
// Artifacts are stored under <root>/<sha256hex[0:2]>/<sha256hex>.wasm.
type Store struct {
	root string
}

// New creates a CAS store rooted at <pollenDir>/cas.
func New(pollenDir string) (*Store, error) {
	root := filepath.Join(pollenDir, "cas")
	if err := os.MkdirAll(root, dirPerm); err != nil {
		return nil, fmt.Errorf("cas: create store dir: %w", err)
	}
	return &Store{root: root}, nil
}

// Put writes the artifact from r and returns its SHA-256 hex digest.
func (s *Store) Put(r io.Reader) (string, error) {
	h := sha256.New()
	tmp, err := os.CreateTemp(s.root, "cas-tmp-*")
	if err != nil {
		return "", fmt.Errorf("cas: create temp file: %w", err)
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()

	if _, err := io.Copy(io.MultiWriter(tmp, h), r); err != nil {
		return "", fmt.Errorf("cas: write temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("cas: close temp file: %w", err)
	}

	hash := hex.EncodeToString(h.Sum(nil))
	dir := filepath.Join(s.root, hash[:2])
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return "", fmt.Errorf("cas: create shard dir: %w", err)
	}

	dest := filepath.Join(dir, hash+".wasm")
	if err := os.Rename(tmp.Name(), dest); err != nil {
		return "", fmt.Errorf("cas: rename artifact: %w", err)
	}
	return hash, nil
}

// Get opens the artifact identified by hash for reading.
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

// Has reports whether the store contains the given artifact.
func (s *Store) Has(hash string) bool {
	_, err := os.Stat(s.path(hash))
	return err == nil
}

func (s *Store) path(hash string) string {
	if len(hash) < 2 { //nolint:mnd
		return filepath.Join(s.root, hash+".wasm")
	}
	return filepath.Join(s.root, hash[:2], hash+".wasm")
}
