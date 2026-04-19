// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

const (
	statusOK       byte = 0
	statusNotFound byte = 1

	hashDisplayLen = 16
	sha256Len      = 32
)

func (s *Service) Fetch(ctx context.Context, hash string, peers []types.PeerKey) error {
	if s.store.Has(hash) {
		return nil
	}
	var lastErr error
	attempted := 0
	for _, pk := range peers {
		if pk == s.self {
			continue
		}
		attempted++
		if err := s.fetchFrom(ctx, hash, pk); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	if attempted == 0 {
		return fmt.Errorf("fetch blob %s: no peers", hash[:min(hashDisplayLen, len(hash))])
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
	if status[0] != statusOK {
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

// Wire: 64-byte hex hash in; status byte then bytes out.
func (s *Service) HandleStream(stream io.ReadWriteCloser, _ types.PeerKey) {
	defer stream.Close()

	var hashBuf [64]byte
	if _, err := io.ReadFull(stream, hashBuf[:]); err != nil {
		return
	}

	rc, err := s.store.Get(string(hashBuf[:]))
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
