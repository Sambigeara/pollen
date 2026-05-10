// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package cas_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/sambigeara/pollen/pkg/cas"
	"github.com/stretchr/testify/require"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	dek, err := cas.GenerateDEK()
	require.NoError(t, err)

	plaintext := []byte("the quick brown fox jumps over the lazy dog")
	envelope, err := cas.Encrypt(plaintext, dek)
	require.NoError(t, err)
	require.NotEqual(t, plaintext, envelope, "envelope must not equal plaintext")

	out, err := cas.Decrypt(envelope, dek)
	require.NoError(t, err)
	require.Equal(t, plaintext, out)
}

func TestDecryptRejectsWrongKey(t *testing.T) {
	dek, _ := cas.GenerateDEK()
	envelope, err := cas.Encrypt([]byte("payload"), dek)
	require.NoError(t, err)

	other, _ := cas.GenerateDEK()
	_, err = cas.Decrypt(envelope, other)
	require.Error(t, err)
}

func TestDecryptRejectsTampered(t *testing.T) {
	dek, _ := cas.GenerateDEK()
	envelope, err := cas.Encrypt([]byte("payload"), dek)
	require.NoError(t, err)

	envelope[len(envelope)-1] ^= 0x01
	_, err = cas.Decrypt(envelope, dek)
	require.Error(t, err, "GCM tag must catch single-bit flip in ciphertext")
}

func TestDecryptRejectsTruncated(t *testing.T) {
	dek, _ := cas.GenerateDEK()
	_, err := cas.Decrypt([]byte("too short"), dek)
	require.Error(t, err)
}

func TestDecryptRejectsWrongDEKSize(t *testing.T) {
	envelope, err := cas.Encrypt([]byte("payload"), make([]byte, cas.DEKSize))
	require.NoError(t, err)
	_, err = cas.Decrypt(envelope, make([]byte, 16))
	require.Error(t, err)
}

func TestEncryptRejectsWrongDEKSize(t *testing.T) {
	_, err := cas.Encrypt([]byte("payload"), make([]byte, 16))
	require.Error(t, err)
}

func TestWrapUnwrapRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	dek, err := cas.GenerateDEK()
	require.NoError(t, err)

	wrapped, err := cas.WrapDEK(dek, pub)
	require.NoError(t, err)
	require.NotEqual(t, dek, wrapped[:cas.DEKSize], "wrapped output must not contain the raw DEK")

	got, err := cas.UnwrapDEK(wrapped, pub, priv)
	require.NoError(t, err)
	require.Equal(t, dek, got)
}

func TestUnwrapRejectsWrongPrivateKey(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	dek, _ := cas.GenerateDEK()
	wrapped, err := cas.WrapDEK(dek, pub)
	require.NoError(t, err)

	_, err = cas.UnwrapDEK(wrapped, pub, otherPriv)
	require.Error(t, err)
}

func TestUnwrapRejectsTampered(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	dek, _ := cas.GenerateDEK()
	wrapped, err := cas.WrapDEK(dek, pub)
	require.NoError(t, err)

	wrapped[len(wrapped)-1] ^= 0x01
	_, err = cas.UnwrapDEK(wrapped, pub, priv)
	require.Error(t, err)
}

func TestWrapRejectsWrongDEKSize(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, err = cas.WrapDEK(make([]byte, 16), pub)
	require.Error(t, err)
}

func TestWrapRejectsWrongPubKeySize(t *testing.T) {
	dek, _ := cas.GenerateDEK()
	_, err := cas.WrapDEK(dek, ed25519.PublicKey(make([]byte, 16)))
	require.Error(t, err)
}

// Cert renewal preserves the subject pubkey, so DEKs wrapped before
// renewal must still unwrap with the post-renewal private key. This
// pins the invariant at the crypto layer.
func TestWrapSurvivesPubKeyStability(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	dek, _ := cas.GenerateDEK()
	wrapped, err := cas.WrapDEK(dek, pub)
	require.NoError(t, err)

	// Simulate cert renewal by re-deriving a copy of the keypair from
	// the same seed; the bytes must match exactly.
	clone := ed25519.NewKeyFromSeed(priv.Seed())
	got, err := cas.UnwrapDEK(wrapped, clone.Public().(ed25519.PublicKey), clone)
	require.NoError(t, err)
	require.Equal(t, dek, got)
}
