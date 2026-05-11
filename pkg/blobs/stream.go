// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package blobs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

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

// ErrNoPublisher signals that no spec in gossip references the hash, so
// there is no origin to stream plaintext from. Distinguished from
// publisher-unreachable so handlers can surface NotFound vs Unavailable.
var ErrNoPublisher = errors.New("no publisher known for blob")

// FetchPlaintext returns plaintext bytes for hash by contacting the
// blob's publisher. When the local node is the publisher, it reads from
// the local CAS directly. The returned reader does not persist anything
// to the local CAS: this is a one-shot export path for `pln fetch`.
//
// Resolution mirrors the gate's Fetch fall-through: BlobSpecs first (a
// named blob published via `pln seed <file> <name>`), then Specs (a
// workload binary, whose hash is the spec key). Static-manifest digests
// are intentionally not exposed here — they are an internal artefact
// of the static-site spec and have no user-facing export use case.
func (s *Service) FetchPlaintext(ctx context.Context, hash string) (io.ReadCloser, error) {
	publisher, ok := s.resolvePublisher(hash)
	if !ok {
		return nil, fmt.Errorf("%w %s", ErrNoPublisher, hash[:min(hashDisplayLen, len(hash))])
	}
	if publisher == s.self {
		return s.Get(hash)
	}
	return s.fetchPlaintextFrom(ctx, hash, publisher)
}

func (s *Service) resolvePublisher(hash string) (types.PeerKey, bool) {
	snap := s.state.Snapshot()
	if view, ok := snap.BlobSpecs[hash]; ok {
		return view.Publisher, true
	}
	if sv, ok := snap.Specs[hash]; ok {
		return sv.Publisher, true
	}
	return types.PeerKey{}, false
}

// fetchPlaintextFrom opens a plaintext stream to publisher. s.timeout
// bounds only the handshake (dial + hash write + status byte); the body
// inherits the caller's ctx so large transfers are not truncated.
func (s *Service) fetchPlaintextFrom(ctx context.Context, hash string, publisher types.PeerKey) (io.ReadCloser, error) {
	handshakeCtx, cancelHandshake := context.WithTimeout(ctx, s.timeout)
	defer cancelHandshake()

	stream, err := s.mesh.OpenStream(handshakeCtx, publisher, transport.StreamTypeBlobPlaintext)
	if err != nil {
		return nil, fmt.Errorf("open stream to publisher %s: %w", publisher.Short(), err)
	}
	closeOnError := watchStream(handshakeCtx, stream)
	bodyCtx, cancelBody := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancelBody()
		}
	}()

	if _, err = stream.Write([]byte(hash)); err != nil {
		closeOnError()
		stream.Close() //nolint:errcheck
		return nil, fmt.Errorf("write hash: %w", err)
	}

	var status [1]byte
	if _, err = io.ReadFull(stream, status[:]); err != nil {
		closeOnError()
		stream.Close() //nolint:errcheck
		return nil, fmt.Errorf("read status: %w", err)
	}
	if status[0] != statusOK {
		closeOnError()
		stream.Close() //nolint:errcheck
		err = fmt.Errorf("publisher %s does not have blob", publisher.Short())
		return nil, err
	}
	closeOnError()
	return newCancellingReader(stream, bodyCtx, cancelBody), nil
}

// cancellingReader wraps a stream so Close cancels the body context
// (which closes the stream via watchStream) exactly once. Used so
// FetchPlaintext can hand the body off to a downstream consumer while
// keeping shutdown deterministic.
type cancellingReader struct {
	stream      io.ReadWriteCloser
	cancelBody  context.CancelFunc
	cancelWatch func()
	once        sync.Once
}

func newCancellingReader(stream io.ReadWriteCloser, bodyCtx context.Context, cancelBody context.CancelFunc) *cancellingReader {
	return &cancellingReader{
		stream:      stream,
		cancelBody:  cancelBody,
		cancelWatch: watchStream(bodyCtx, stream),
	}
}

func (c *cancellingReader) Read(b []byte) (int, error) { return c.stream.Read(b) }

func (c *cancellingReader) Close() error {
	var err error
	c.once.Do(func() {
		c.cancelWatch()
		err = c.stream.Close()
		c.cancelBody()
	})
	return err
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

// ServePlaintext responds to an inbound plaintext-fetch stream from
// `pln fetch`. Only the blob's publisher should reach this path: the
// supervisor verifies that via the fetch gate before dispatching. The
// hash has already been consumed from the stream; the body is the local
// CAS read after decryption with the publisher's own DEK.
//
// No integrity trailer is sent because the fetcher already knows the
// expected SHA-256 (it's the request key). The CLI re-hashes on receipt.
func (s *Service) ServePlaintext(stream io.ReadWriteCloser, hash string) {
	defer stream.Close()

	rc, err := s.Get(hash)
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
