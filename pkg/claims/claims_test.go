// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package claims_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/sambigeara/pollen/pkg/claims"
	"github.com/sambigeara/pollen/pkg/evaluator"
)

func TestClaimRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	props, err := structpb.NewStruct(map[string]any{"classification": "secret", "team": "platform"})
	require.NoError(t, err)

	sig, err := claims.Sign(priv, evaluator.ResourceBlob, "deadbeef", props)
	require.NoError(t, err)
	require.Len(t, sig, ed25519.SignatureSize)

	require.NoError(t, claims.Verify(pub, evaluator.ResourceBlob, "deadbeef", props, sig))
}

// Tampering with the resource id, properties, or signature must fail
// verification — the signature binds all three.
func TestClaimRejectsTampering(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	props, _ := structpb.NewStruct(map[string]any{"role": "reader"})
	sig, err := claims.Sign(priv, evaluator.ResourceBlob, "aaaa", props)
	require.NoError(t, err)

	// Wrong resource id.
	require.ErrorIs(t, claims.Verify(pub, evaluator.ResourceBlob, "bbbb", props, sig), claims.ErrSignatureInvalid)

	// Wrong resource type.
	require.ErrorIs(t, claims.Verify(pub, evaluator.ResourceSeed, "aaaa", props, sig), claims.ErrSignatureInvalid)

	// Mutated properties.
	mutated, _ := structpb.NewStruct(map[string]any{"role": "admin"})
	require.ErrorIs(t, claims.Verify(pub, evaluator.ResourceBlob, "aaaa", mutated, sig), claims.ErrSignatureInvalid)

	// Different signer.
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	require.ErrorIs(t, claims.Verify(otherPub, evaluator.ResourceBlob, "aaaa", props, sig), claims.ErrSignatureInvalid)
}

// Nil or empty properties must round-trip identically.
func TestClaimNilProperties(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	sig, err := claims.Sign(priv, evaluator.ResourceBlob, "id", nil)
	require.NoError(t, err)
	require.NoError(t, claims.Verify(pub, evaluator.ResourceBlob, "id", nil, sig))
}

// Length-prefixed framing must not collide when an id contains the
// byte that a delimiter-based framing would have used as a segment
// boundary.
func TestClaimFramingCollisionResistant(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	props, err := structpb.NewStruct(map[string]any{"after": "segment"})
	require.NoError(t, err)

	sig, err := claims.Sign(priv, evaluator.ResourceBlob, "alpha\nbeta", props)
	require.NoError(t, err)

	// Shortening the id while re-slicing the same bytes forward into
	// properties must fail under length-prefix framing.
	require.ErrorIs(t, claims.Verify(pub, evaluator.ResourceBlob, "alpha", props, sig), claims.ErrSignatureInvalid)

	// Round-trip with the exact same id must still verify.
	require.NoError(t, claims.Verify(pub, evaluator.ResourceBlob, "alpha\nbeta", props, sig))
}

// Empty-signature-with-nil-properties is valid (unsigned spec).
// Empty-signature-with-properties must surface ErrSignatureMissing.
func TestClaimEmptySignatureShape(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	require.NoError(t, claims.Verify(pub, evaluator.ResourceBlob, "id", nil, nil))

	props, _ := structpb.NewStruct(map[string]any{"x": "y"})
	err = claims.Verify(pub, evaluator.ResourceBlob, "id", props, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, claims.ErrSignatureMissing))
}

// A signature of wrong length must be classified as invalid, not
// fall through to a cryptographic verify (which would at best waste
// cycles and at worst expose a timing difference).
func TestClaimRejectsMalformedSignatureLength(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	err = claims.Verify(pub, evaluator.ResourceBlob, "id", nil, []byte{0x00, 0x01})
	require.True(t, errors.Is(err, claims.ErrSignatureInvalid))
}

// PublisherClaim.New collapses empty input to nil so spec constructors
// don't carry an empty-yet-not-nil claim pointer.
func TestPublisherClaimNewNilsEmpty(t *testing.T) {
	require.Nil(t, claims.New(nil, nil))
	require.Nil(t, claims.New(map[string]any{}, nil))
	require.Nil(t, claims.New(nil, []byte{}))
}

func TestPublisherClaimGetPropertiesNilSafe(t *testing.T) {
	var c *claims.PublisherClaim
	require.Nil(t, c.GetProperties())

	c = claims.New(map[string]any{"k": "v"}, []byte{0x01})
	require.Equal(t, "v", c.GetProperties()["k"])
}
