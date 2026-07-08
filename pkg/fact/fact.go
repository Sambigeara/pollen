// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package fact

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"buf.build/go/protovalidate"
	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"google.golang.org/protobuf/proto"

	"github.com/sambigeara/pollen/pkg/identity"
)

const (
	sigContextFact     = "pollen.fact.v1"
	sigContextWrapping = "pollen.fact.wrapping.v1"
)

var (
	ErrFactInvalid     = errors.New("fact invalid")
	ErrWrappingInvalid = errors.New("blob wrapping invalid")
)

// Body is the spec payload a Fact attests to (workload, static, blob,
// service spec). Hashed, never embedded, so the Fact stays small and
// the body travels on its own gossip path.
type Body interface{ proto.Message }

func HashBody(body Body) ([]byte, error) {
	if body == nil {
		return nil, errors.New("fact body is nil")
	}
	b, err := identity.SignaturePayload(body)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	return sum[:], nil
}

func factPayload(f *factv1.Fact) ([]byte, error) {
	payload := proto.Clone(f).(*factv1.Fact) //nolint:forcetypeassert
	payload.Signature = nil
	return identity.SignaturePayload(payload)
}

func wrappingPayload(w *factv1.BlobWrapping) ([]byte, error) {
	payload := proto.Clone(w).(*factv1.BlobWrapping) //nolint:forcetypeassert
	payload.Signature = nil
	return identity.SignaturePayload(payload)
}

// IssueFact signs a spec attestation under the authority's own key.
// The authority is named by pubkey only; its Grant is durable cluster
// state resolved at admission, so the Fact survives the authority going
// offline up to the Grant horizon.
func IssueFact(
	authorityPriv ed25519.PrivateKey,
	resource *admissionv1.ResourceID,
	body Body,
	policy *admissionv1.Predicate,
	seq uint64,
	deleted bool,
) (*factv1.Fact, error) {
	bodyHash, err := HashBody(body)
	if err != nil {
		return nil, err
	}
	authorityPub := authorityPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	f := &factv1.Fact{
		Resource:     resource,
		Policy:       policy,
		BodyHash:     bodyHash,
		AuthorityPub: authorityPub,
		Seq:          seq,
		Deleted:      deleted,
	}
	msg, err := factPayload(f)
	if err != nil {
		return nil, err
	}
	sig, err := identity.SignPayload(authorityPriv, msg, sigContextFact)
	if err != nil {
		return nil, err
	}
	f.Signature = sig
	if err := protovalidate.Validate(f); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFactInvalid, err)
	}
	return f, nil
}

// VerifyFact authenticates a relayed spec attestation. grant is the
// authority's Grant resolved from cluster state; it is held to the
// durable-authority rule (chain + grant_deadline + chain-aware
// denylist) and must be the grant of the fact's named authority. The
// fact signature must be by that authority. denied may be nil.
func VerifyFact(
	f *factv1.Fact,
	body Body,
	grant *identityv1.Grant,
	rootPub []byte,
	now time.Time,
	denied identity.DenyChecker,
) error {
	if err := protovalidate.Validate(f); err != nil {
		return fmt.Errorf("%w: %w", ErrFactInvalid, err)
	}
	bodyHash, err := HashBody(body)
	if err != nil {
		return err
	}
	if !bytes.Equal(bodyHash, f.GetBodyHash()) {
		return fmt.Errorf("%w: body hash mismatch", ErrFactInvalid)
	}
	chk := identity.CheckGrant(grant, rootPub, now, f.GetAuthorityPub(), denied)
	if !chk.Status.Valid() {
		return fmt.Errorf("%w: authority grant %s: %s", ErrFactInvalid, chk.Status, chk.Reason)
	}
	msg, err := factPayload(f)
	if err != nil {
		return err
	}
	if err := identity.VerifyPayload(ed25519.PublicKey(f.GetAuthorityPub()), msg, f.GetSignature(), sigContextFact); err != nil {
		return fmt.Errorf("%w: signature invalid", ErrFactInvalid)
	}
	return nil
}

// IssueBlobWrapping seals a wrapped DEK under the authority's key,
// mirroring IssueFact so wrappings ride the same durable-authority
// rule as specs.
func IssueBlobWrapping(
	authorityPriv ed25519.PrivateKey,
	blobHash, recipientPub, wrappedDEK []byte,
	seq uint64,
) (*factv1.BlobWrapping, error) {
	authorityPub := authorityPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	w := &factv1.BlobWrapping{
		BlobHash:     blobHash,
		RecipientPub: recipientPub,
		WrappedDek:   wrappedDEK,
		AuthorityPub: authorityPub,
		Seq:          seq,
	}
	msg, err := wrappingPayload(w)
	if err != nil {
		return nil, err
	}
	sig, err := identity.SignPayload(authorityPriv, msg, sigContextWrapping)
	if err != nil {
		return nil, err
	}
	w.Signature = sig
	if err := protovalidate.Validate(w); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWrappingInvalid, err)
	}
	return w, nil
}

func VerifyBlobWrapping(
	w *factv1.BlobWrapping,
	grant *identityv1.Grant,
	rootPub []byte,
	now time.Time,
	denied identity.DenyChecker,
) error {
	if err := protovalidate.Validate(w); err != nil {
		return fmt.Errorf("%w: %w", ErrWrappingInvalid, err)
	}
	chk := identity.CheckGrant(grant, rootPub, now, w.GetAuthorityPub(), denied)
	if !chk.Status.Valid() {
		return fmt.Errorf("%w: authority grant %s: %s", ErrWrappingInvalid, chk.Status, chk.Reason)
	}
	msg, err := wrappingPayload(w)
	if err != nil {
		return err
	}
	if err := identity.VerifyPayload(ed25519.PublicKey(w.GetAuthorityPub()), msg, w.GetSignature(), sigContextWrapping); err != nil {
		return fmt.Errorf("%w: signature invalid", ErrWrappingInvalid)
	}
	return nil
}
