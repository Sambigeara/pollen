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

// Put writes the artifact from r and returns its SHA-256 hex digest.
func (s *Store) Put(r io.Reader) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("cas: read artifact: %w", err)
	}

	h := sha256.Sum256(data)
	hash := hex.EncodeToString(h[:])
	dir := filepath.Join(s.root, hash[:2])
	if err := plnfs.EnsureDir(dir); err != nil {
		return "", fmt.Errorf("cas: create shard dir: %w", err)
	}

	dest := filepath.Join(dir, hash+".wasm")
	if err := plnfs.WriteGroupReadable(dest, data); err != nil {
		return "", fmt.Errorf("cas: write artifact: %w", err)
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
