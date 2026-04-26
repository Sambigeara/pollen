// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	statusOK           byte = 0
	statusNotFound     byte = 1
	statusUnauthorized byte = 2

	hashDisplayLen = 16
	sha256Len      = 32
	sha256HexLen   = 64 // 2 * sha256Len
)

func (s *Service) Fetch(ctx context.Context, hash string, peers []types.PeerKey) error {
	if s.store.Has(hash) {
		return nil
	}
	var lastErr, denyErr error
	attempted := 0
	for _, pk := range peers {
		if pk == s.self {
			continue
		}
		attempted++
		err := s.fetchFrom(ctx, hash, pk)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrUnauthorized) && denyErr == nil {
			denyErr = err
		}
		lastErr = err
	}
	if attempted == 0 {
		return fmt.Errorf("fetch blob %s: no peers", hash[:min(hashDisplayLen, len(hash))])
	}
	// A definitive deny trumps a transport-level failure on a later
	// peer. Without this, a denied holder followed by a stale or
	// unreachable peer surfaces as NotFound and masks the policy
	// signal — operators see "blob not found" instead of "denied".
	if denyErr != nil {
		return fmt.Errorf("fetch blob %s: %w", hash[:min(hashDisplayLen, len(hash))], denyErr)
	}
	return fmt.Errorf("fetch blob %s: %w", hash[:min(hashDisplayLen, len(hash))], lastErr)
}

func (s *Service) fetchFrom(ctx context.Context, hash string, peer types.PeerKey) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	stream, err := s.mesh.OpenStream(ctx, peer, transport.StreamTypeBlob)
	if err != nil {
		return fmt.Errorf("open stream to %s: %w", peer.Short(), err)
	}
	defer stream.Close()
	defer watchStream(ctx, stream)()

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
	switch status[0] {
	case statusOK:
	case statusUnauthorized:
		return fmt.Errorf("%w: peer %s", ErrUnauthorized, peer.Short())
	default:
		return fmt.Errorf("peer %s does not have blob", peer.Short())
	}

	gotHash, err := s.store.Put(stream)
	if err != nil {
		return fmt.Errorf("store blob: %w", err)
	}
	if gotHash != hash {
		return fmt.Errorf("hash mismatch: expected %s, got %s", hash, gotHash)
	}
	return s.Announce(hash)
}

// ReadHash reads the 64-byte hex hash header from a blob stream.
// Supervisor's dispatch loop calls this before the authorisation gate
// so the gate decision can reference the hash.
func ReadHash(r io.Reader) (string, error) {
	var hashBuf [sha256HexLen]byte
	if _, err := io.ReadFull(r, hashBuf[:]); err != nil {
		return "", err
	}
	return string(hashBuf[:]), nil
}

// WriteStatus writes a single status byte to a blob stream. Supervisor
// uses this to surface an authorisation denial in the same wire shape
// the serve path would use for not-found. Write errors are dropped —
// every caller closes the stream immediately afterwards.
func WriteStatus(w io.Writer, status byte) {
	w.Write([]byte{status}) //nolint:errcheck
}

// Status byte values exposed so supervisor can write a denial or
// not-found response without depending on the serve path.
const (
	StatusOK           = statusOK
	StatusNotFound     = statusNotFound
	StatusUnauthorized = statusUnauthorized
)

// Serve responds to an inbound blob fetch for the given hash. The hash
// is already consumed from the stream by supervisor's dispatch loop;
// Serve assumes the caller is authorised.
func (s *Service) Serve(stream io.ReadWriteCloser, hash string) {
	defer stream.Close()

	rc, err := s.store.Get(hash)
	if err != nil {
		stream.Write([]byte{statusNotFound}) //nolint:errcheck
		return
	}
	defer rc.Close()

	stream.Write([]byte{statusOK}) //nolint:errcheck
	io.Copy(stream, rc)            //nolint:errcheck
}

func watchStream(ctx context.Context, stream io.Closer) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			stream.Close() //nolint:errcheck
		case <-done:
		}
	}()
	return func() { close(done) }
}
