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
	sha256HexLen   = 64
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
		err := s.fetchFrom(ctx, hash, pk)
		if err == nil {
			return nil
		}
		lastErr = err
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

	// Reject before consuming the body. Storing first then evicting
	// would briefly land unentitled bytes on disk, undermining the
	// at-rest contract.
	if err := s.MayStore(hash); err != nil {
		return fmt.Errorf("local cert not entitled to hold blob %s: %w", hash[:min(hashDisplayLen, len(hash))], err)
	}

	// PutCiphertext writes the envelope as-received: ciphertext flows
	// through wire and disk without any node decrypting in transit.
	// Hash integrity is checked at first read (Get fails on tag
	// mismatch) since we have no DEK to verify here.
	if err := s.store.PutCiphertext(hash, stream); err != nil {
		return fmt.Errorf("store envelope: %w", err)
	}
	return s.Announce(hash)
}

func ReadHash(r io.Reader) (string, error) {
	var hashBuf [sha256HexLen]byte
	if _, err := io.ReadFull(r, hashBuf[:]); err != nil {
		return "", err
	}
	return string(hashBuf[:]), nil
}

// Serve responds to an inbound blob fetch. The hash must already be
// consumed from the stream before calling. After streaming ciphertext,
// the server issues a wrapping addressed to the requesting peer so
// they can decrypt locally; without this, the receiver would land
// undecryptable ciphertext on disk and would have no path back to the
// DEK without a separate RPC. Authorisation for both the fetch and
// the wrapping has already been enforced upstream by gate.Fetch.
func (s *Service) Serve(stream io.ReadWriteCloser, hash string, requester types.PeerKey) {
	defer stream.Close()

	rc, err := s.store.GetCiphertext(hash)
	if err != nil {
		stream.Write([]byte{statusNotFound}) //nolint:errcheck
		return
	}
	defer rc.Close()

	stream.Write([]byte{statusOK}) //nolint:errcheck
	io.Copy(stream, rc)            //nolint:errcheck

	if err := s.issueWrappingFor(hash, requester); err != nil {
		// Wrapping is best-effort: the requester can retry or
		// request from another holder. Logging would be helpful
		// here once the service has a logger field.
		_ = err
	}
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
