package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/renameio/v2"
	"github.com/sambigeara/pollen/pkg/config"
)

var ErrNotFound = errors.New("artifact not found")

// Store is a content-addressable artifact store backed by the local filesystem.
// Artifacts are stored under <root>/<sha256hex[0:2]>/<sha256hex>.wasm.
type Store struct {
	root string
}

// New creates a CAS store rooted at <pollenDir>/cas.
func New(pollenDir string) (*Store, error) {
	root := filepath.Join(pollenDir, "cas")
	if err := config.EnsureDir(root); err != nil {
		return nil, fmt.Errorf("cas: create store dir: %w", err)
	}
	return &Store{root: root}, nil
}

// Put writes the artifact from r and returns its SHA-256 hex digest.
func (s *Store) Put(r io.Reader) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("cas: read artifact: %w", err)
	}

	h := sha256.Sum256(data)
	hash := hex.EncodeToString(h[:])
	dir := filepath.Join(s.root, hash[:2])
	if err := config.EnsureDir(dir); err != nil {
		return "", fmt.Errorf("cas: create shard dir: %w", err)
	}

	dest := filepath.Join(dir, hash+".wasm")
	if err := renameio.WriteFile(dest, data, 0o644); err != nil { //nolint:mnd
		return "", fmt.Errorf("cas: write artifact: %w", err)
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
