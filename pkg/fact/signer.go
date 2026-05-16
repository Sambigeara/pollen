// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package fact

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
)

// Signer issues facts under one authority key, stamping each with a
// monotonically increasing per-authority sequence. An authority key has
// a single producer (one node's daemon, or one CLI context), so a
// durable signer persists the sequence high-water and reloads it on
// construction, keeping seq monotonic across process restarts. The
// sequence is carried on every fact and is consumed in Phase 3 to
// discriminate a republish-after-unseed from a replay of the
// tombstoned original. NewSigner is the ephemeral in-memory variant
// for tests and non-persisting callers. It satisfies the state
// package's local signer contract.
type Signer struct {
	seqPath string
	priv    ed25519.PrivateKey
	seq     uint64
	mu      sync.Mutex
}

// NewSigner returns an ephemeral signer whose sequence is in-memory and
// resets on restart. Use NewDurableSigner for any producer whose facts
// outlive the process.
func NewSigner(priv ed25519.PrivateKey) *Signer {
	return &Signer{priv: priv}
}

// NewDurableSigner returns a signer that persists its per-authority
// sequence high-water to seqPath and resumes from it on construction so
// seq never repeats or regresses across restarts. A missing file starts
// the sequence at zero; a present-but-unreadable file is an error
// rather than a silent reset, since regressing the sequence would let a
// later fact reuse an earlier seq.
func NewDurableSigner(priv ed25519.PrivateKey, seqPath string) (*Signer, error) {
	s := &Signer{priv: priv, seqPath: seqPath}
	raw, err := os.ReadFile(seqPath)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read fact seq %s: %w", seqPath, err)
	}
	seq, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse fact seq %s: %w", seqPath, err)
	}
	s.seq = seq
	return s, nil
}

// AuthorityPub is the key facts issued by this signer are bound to.
func (s *Signer) AuthorityPub() ed25519.PublicKey {
	return s.priv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
}

// nextSeqLocked advances and durably persists the sequence. On a
// persistence failure it rolls the in-memory counter back so the next
// call retries the same number rather than skipping it, and the failed
// fact is never emitted.
func (s *Signer) nextSeqLocked() (uint64, error) {
	s.seq++
	if s.seqPath == "" {
		return s.seq, nil
	}
	if err := s.persistSeqLocked(); err != nil {
		s.seq--
		return 0, err
	}
	return s.seq, nil
}

func (s *Signer) persistSeqLocked() error {
	tmp := s.seqPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:mnd
	if err != nil {
		return fmt.Errorf("open fact seq: %w", err)
	}
	if _, err := f.WriteString(strconv.FormatUint(s.seq, 10)); err != nil {
		_ = f.Close()
		return fmt.Errorf("write fact seq: %w", err)
	}
	// fsync before the rename so a power loss between the two cannot
	// resurrect a lower high-water and reuse a sequence number.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync fact seq: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close fact seq: %w", err)
	}
	if err := os.Rename(tmp, s.seqPath); err != nil {
		return fmt.Errorf("commit fact seq: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(s.seqPath)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func (s *Signer) IssueFact(
	resource *admissionv1.ResourceID,
	body Body,
	policy *admissionv1.Predicate,
	deleted bool,
) (*factv1.Fact, error) {
	s.mu.Lock()
	seq, err := s.nextSeqLocked()
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return IssueFact(s.priv, resource, body, policy, seq, deleted)
}

// IssueBlobWrapping seals wrappedDEK for recipientPub under this
// authority, stamping it with the same monotonic, durable sequence as
// facts.
func (s *Signer) IssueBlobWrapping(
	blobHash, recipientPub, wrappedDEK []byte,
) (*factv1.BlobWrapping, error) {
	s.mu.Lock()
	seq, err := s.nextSeqLocked()
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return IssueBlobWrapping(s.priv, blobHash, recipientPub, wrappedDEK, seq)
}
