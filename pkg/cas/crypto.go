// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package cas

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/sambigeara/pollen/pkg/identity"
	"golang.org/x/crypto/nacl/box"
)

// DEKSize is the symmetric key length for blob bodies (AES-256).
const DEKSize = 32

// GenerateDEK returns a fresh 32-byte AES-256-GCM key.
func GenerateDEK() ([]byte, error) {
	dek := make([]byte, DEKSize)
	if _, err := rand.Read(dek); err != nil {
		return nil, fmt.Errorf("cas: random dek: %w", err)
	}
	return dek, nil
}

// Encrypt produces an envelope in `nonce || ciphertext+tag` form.
func Encrypt(plaintext, dek []byte) ([]byte, error) {
	aead, err := newAEAD(dek)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("cas: random nonce: %w", err)
	}
	envelope := make([]byte, aead.NonceSize(), aead.NonceSize()+len(plaintext)+aead.Overhead())
	copy(envelope, nonce)
	return aead.Seal(envelope, nonce, plaintext, nil), nil
}

// ErrAEADAuth signals the AEAD authentication tag did not match the
// envelope under the supplied DEK. Almost always means the local
// envelope was Put under one DEK while the wrapping the caller
// unwrapped was minted under another (a pre-idempotent-Put re-publish
// cycle could leave this stale state). Callers that hold both sides
// can recover by evicting the envelope + wrapping and re-fetching.
var ErrAEADAuth = errors.New("cas: aead authentication failed")

func Decrypt(envelope, dek []byte) ([]byte, error) {
	aead, err := newAEAD(dek)
	if err != nil {
		return nil, err
	}
	if len(envelope) < aead.NonceSize()+aead.Overhead() {
		return nil, errors.New("cas: envelope too short")
	}
	nonce, ct := envelope[:aead.NonceSize()], envelope[aead.NonceSize():]
	plaintext, err := aead.Open(nil, nonce, ct, nil)
	if err != nil {
		// Wrap both so callers can match ErrAEADAuth or the underlying error.
		return nil, fmt.Errorf("%w: %w", ErrAEADAuth, err)
	}
	return plaintext, nil
}

func newAEAD(dek []byte) (cipher.AEAD, error) {
	if len(dek) != DEKSize {
		return nil, fmt.Errorf("cas: dek must be %d bytes, got %d", DEKSize, len(dek))
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("cas: aes cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("cas: gcm: %w", err)
	}
	return aead, nil
}

// WrapDEK seals dek under NaCl's anonymous sealed box, keyed to the
// recipient's X25519-mapped identity key so the same key that signs certs
// also receives DEKs.
func WrapDEK(dek []byte, recipientEdPub ed25519.PublicKey) ([]byte, error) {
	if len(dek) != DEKSize {
		return nil, fmt.Errorf("cas: dek must be %d bytes, got %d", DEKSize, len(dek))
	}
	xPub, err := identity.EdPubToX25519(recipientEdPub)
	if err != nil {
		return nil, fmt.Errorf("cas: %w", err)
	}
	wrapped, err := box.SealAnonymous(nil, dek, (*[32]byte)(xPub), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("cas: seal dek: %w", err)
	}
	return wrapped, nil
}

func UnwrapDEK(wrapped []byte, recipientEdPub ed25519.PublicKey, recipientEdPriv ed25519.PrivateKey) ([]byte, error) {
	xPub, err := identity.EdPubToX25519(recipientEdPub)
	if err != nil {
		return nil, fmt.Errorf("cas: %w", err)
	}
	xPriv, err := identity.EdPrivToX25519(recipientEdPriv)
	if err != nil {
		return nil, fmt.Errorf("cas: %w", err)
	}
	dek, ok := box.OpenAnonymous(nil, wrapped, (*[32]byte)(xPub), (*[32]byte)(xPriv))
	if !ok {
		return nil, errors.New("cas: open sealed dek")
	}
	return dek, nil
}
