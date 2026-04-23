// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// Package claims owns Pollen's publisher-claim signing and
// verification. Every signed resource (blobs, seeds, statics, and any
// future resource type) flows through Sign at publish time and Verify
// at gossip ingest. The package provides a single canonicalisation
// tuple so adding a new resource kind means one case in the call sites,
// not N per-resource verification paths.
package claims

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/evaluator"
)

// PublisherClaim pairs the publisher-signed property bag with its
// signature. Spec messages embed a pointer so "both fields or neither"
// is unrepresentable — a nil pointer is an unsigned spec, a non-nil
// pointer is a signed claim.
type PublisherClaim struct {
	Properties map[string]any
	Signature  []byte
}

// New returns nil when every field is empty so spec constructors don't
// need a separate empty-vs-zero branch.
func New(properties map[string]any, signature []byte) *PublisherClaim {
	if len(properties) == 0 && len(signature) == 0 {
		return nil
	}
	return &PublisherClaim{Properties: properties, Signature: signature}
}

// GetProperties returns the claim's properties or nil when the claim
// is absent. Nil-safe read helper.
func (c *PublisherClaim) GetProperties() map[string]any {
	if c == nil {
		return nil
	}
	return c.Properties
}

var (
	// ErrSignatureMissing signals a claim carrying properties without a
	// signature. Accepted on the wire only when both are absent.
	ErrSignatureMissing = errors.New("claim: signature missing")
	// ErrSignatureInvalid signals a malformed signature (wrong length
	// or failed cryptographic verification).
	ErrSignatureInvalid = errors.New("claim: signature invalid")
	// ErrPropertiesTooLarge signals properties that exceed
	// auth.MaxAttributesSize when serialised.
	ErrPropertiesTooLarge = errors.New("claim: properties exceed size limit")
)

// Sign produces a signature over the canonical tuple
// (resourceType, resourceID, properties) under the publisher-claim
// Ed25519ctx context owned by pkg/auth. Properties are size-bounded at
// auth.MaxAttributesSize to cap payload-size DoS.
func Sign(priv ed25519.PrivateKey, resourceType evaluator.ResourceType, resourceID string, properties *structpb.Struct) ([]byte, error) {
	if err := validatePropertiesSize(properties); err != nil {
		return nil, err
	}
	payload, err := signingTuple(resourceType, resourceID, properties)
	if err != nil {
		return nil, err
	}
	return auth.SignPublisherClaim(priv, payload)
}

// Verify performs the full shape-plus-cryptographic check over a
// received claim. A nil Properties with empty Signature is accepted
// (unsigned spec). Properties-without-signature returns
// ErrSignatureMissing. Oversized properties return
// ErrPropertiesTooLarge. Anything else returns ErrSignatureInvalid.
func Verify(pub ed25519.PublicKey, resourceType evaluator.ResourceType, resourceID string, properties *structpb.Struct, signature []byte) error {
	if len(signature) == 0 {
		if properties == nil {
			return nil
		}
		return ErrSignatureMissing
	}
	if len(signature) != ed25519.SignatureSize {
		return fmt.Errorf("%w: length %d", ErrSignatureInvalid, len(signature))
	}
	if err := validatePropertiesSize(properties); err != nil {
		return err
	}
	payload, err := signingTuple(resourceType, resourceID, properties)
	if err != nil {
		return err
	}
	if err := auth.VerifyPublisherClaim(pub, payload, signature); err != nil {
		return fmt.Errorf("%w: %w", ErrSignatureInvalid, err)
	}
	return nil
}

func validatePropertiesSize(props *structpb.Struct) error {
	if props == nil {
		return nil
	}
	if size := proto.Size(props); size > auth.MaxAttributesSize {
		return fmt.Errorf("%w: %d bytes (max %d)", ErrPropertiesTooLarge, size, auth.MaxAttributesSize)
	}
	return nil
}

// signingTuple canonicalises (resource_type, resource_id, properties)
// with 4-byte big-endian length prefixes per segment so no value can
// straddle a boundary. Delimiter-based framing (e.g. "\n") would let a
// resource_type containing the delimiter forge membership in an
// adjacent segment; length-prefixing makes boundaries unambiguous.
// structpb.Struct is marshalled deterministically so two verifiers
// produce identical bytes for the same logical input.
func signingTuple(resourceType evaluator.ResourceType, resourceID string, properties *structpb.Struct) ([]byte, error) {
	var propsBytes []byte
	if properties != nil {
		data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(properties)
		if err != nil {
			return nil, fmt.Errorf("claim: marshal properties: %w", err)
		}
		propsBytes = data
	}
	var buf bytes.Buffer
	for _, seg := range [][]byte{[]byte(resourceType), []byte(resourceID), propsBytes} {
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(seg)))
		buf.Write(seg)
	}
	return buf.Bytes(), nil
}
