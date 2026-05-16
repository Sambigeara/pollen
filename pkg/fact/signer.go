// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package fact

import (
	"crypto/ed25519"
	"sync"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
)

// Signer issues facts under one authority key, stamping each with a
// monotonically increasing per-authority sequence so a re-publish
// after an unseed is never mistaken for a replay of the tombstoned
// original. It satisfies the state package's local signer contract.
type Signer struct {
	priv ed25519.PrivateKey
	mu   sync.Mutex
	seq  uint64
}

func NewSigner(priv ed25519.PrivateKey) *Signer {
	return &Signer{priv: priv}
}

// AuthorityPub is the key facts issued by this signer are bound to.
func (s *Signer) AuthorityPub() ed25519.PublicKey {
	return s.priv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
}

func (s *Signer) IssueFact(
	resource *admissionv1.ResourceID,
	body Body,
	policy *admissionv1.Predicate,
	deleted bool,
) (*factv1.Fact, error) {
	s.mu.Lock()
	s.seq++
	seq := s.seq
	s.mu.Unlock()
	return IssueFact(s.priv, resource, body, policy, seq, deleted)
}

// IssueBlobWrapping seals wrappedDEK for recipientPub under this
// authority, stamping it with the same monotonic sequence as facts so a
// re-wrap after a re-Put is never mistaken for a replay of an earlier
// wrapping.
func (s *Signer) IssueBlobWrapping(
	blobHash, recipientPub, wrappedDEK []byte,
) (*factv1.BlobWrapping, error) {
	s.mu.Lock()
	s.seq++
	seq := s.seq
	s.mu.Unlock()
	return IssueBlobWrapping(s.priv, blobHash, recipientPub, wrappedDEK, seq)
}
