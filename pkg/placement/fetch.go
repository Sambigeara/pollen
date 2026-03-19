package placement

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

const artifactFetchTimeout = 15 * time.Second

type casWriter interface {
	Put(r io.Reader) (string, error)
}

type casReader interface {
	Get(hash string) (io.ReadCloser, error)
}

type meshFetcher struct {
	mesh    StreamOpener
	cas     casWriter
	timeout time.Duration
}

func newArtifactFetcher(mesh StreamOpener, cas casWriter) artifactFetcher {
	return &meshFetcher{mesh: mesh, cas: cas, timeout: artifactFetchTimeout}
}

const (
	artifactStatusOK       byte = 0
	artifactStatusNotFound byte = 1
	hashDisplayLen              = 16
	sha256Len                   = 32
)

// Fetch tries each peer in order until the artifact is fetched successfully.
func (f *meshFetcher) Fetch(ctx context.Context, hash string, peers []types.PeerKey) error {
	var lastErr error
	for _, pk := range peers {
		if err := f.fetchFrom(ctx, hash, pk); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	if lastErr != nil {
		return fmt.Errorf("fetch artifact %s: %w", hash[:min(hashDisplayLen, len(hash))], lastErr)
	}
	return fmt.Errorf("fetch artifact %s: no peers", hash[:min(hashDisplayLen, len(hash))])
}

func (f *meshFetcher) fetchFrom(ctx context.Context, hash string, peer types.PeerKey) error {
	ctx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()

	stream, err := f.mesh.OpenStream(ctx, peer, transport.StreamTypeArtifact)
	if err != nil {
		return fmt.Errorf("open stream to %s: %w", peer.Short(), err)
	}

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			stream.Close()
		case <-done:
		}
	}()
	defer stream.Close()

	if len(hash) != hex.EncodedLen(sha256Len) {
		return fmt.Errorf("invalid hash length: %d", len(hash))
	}
	if _, err := stream.Write([]byte(hash)); err != nil {
		return fmt.Errorf("write hash: %w", err)
	}

	var status [1]byte
	if _, err := io.ReadFull(stream, status[:]); err != nil {
		return fmt.Errorf("read status: %w", err)
	}
	if status[0] != artifactStatusOK {
		return fmt.Errorf("peer %s does not have artifact", peer.Short())
	}

	gotHash, err := f.cas.Put(stream)
	if err != nil {
		return fmt.Errorf("store artifact: %w", err)
	}
	if gotHash != hash {
		return fmt.Errorf("hash mismatch: expected %s, got %s", hash, gotHash)
	}

	return nil
}

// handleArtifactStream is the server-side handler for artifact fetch requests.
// It reads a 64-byte hex hash, looks up the artifact in the CAS, and writes
// a 1-byte status followed by the WASM bytes.
func handleArtifactStream(stream io.ReadWriteCloser, cas casReader) {
	defer stream.Close()

	var hashBuf [64]byte
	if _, err := io.ReadFull(stream, hashBuf[:]); err != nil {
		return
	}
	hash := string(hashBuf[:])

	rc, err := cas.Get(hash)
	if err != nil {
		stream.Write([]byte{artifactStatusNotFound}) //nolint:errcheck
		return
	}
	defer rc.Close()

	stream.Write([]byte{artifactStatusOK}) //nolint:errcheck
	io.Copy(stream, rc)                    //nolint:errcheck
}
