// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package cas

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"errors"
	"fmt"

	"filippo.io/edwards25519"
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
		return nil, fmt.Errorf("cas: open envelope: %w", err)
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

// WrapDEK seals dek under NaCl's anonymous sealed box. The recipient's
// Ed25519 identity key is converted to X25519 via the standard
// birational map so the same key that signs certs also receives DEKs.
func WrapDEK(dek []byte, recipientEdPub ed25519.PublicKey) ([]byte, error) {
	if len(dek) != DEKSize {
		return nil, fmt.Errorf("cas: dek must be %d bytes, got %d", DEKSize, len(dek))
	}
	xPub, err := edPubToX25519Pub(recipientEdPub)
	if err != nil {
		return nil, err
	}
	wrapped, err := box.SealAnonymous(nil, dek, xPub, rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("cas: seal dek: %w", err)
	}
	return wrapped, nil
}

func UnwrapDEK(wrapped []byte, recipientEdPub ed25519.PublicKey, recipientEdPriv ed25519.PrivateKey) ([]byte, error) {
	xPub, err := edPubToX25519Pub(recipientEdPub)
	if err != nil {
		return nil, err
	}
	xPriv, err := edPrivToX25519Priv(recipientEdPriv)
	if err != nil {
		return nil, err
	}
	dek, ok := box.OpenAnonymous(nil, wrapped, xPub, xPriv)
	if !ok {
		return nil, errors.New("cas: open sealed dek")
	}
	return dek, nil
}

func edPubToX25519Pub(edPub ed25519.PublicKey) (*[32]byte, error) {
	if len(edPub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("cas: ed25519 pub must be %d bytes, got %d", ed25519.PublicKeySize, len(edPub))
	}
	pt, err := new(edwards25519.Point).SetBytes(edPub)
	if err != nil {
		return nil, fmt.Errorf("cas: parse ed25519 pub: %w", err)
	}
	var out [32]byte
	copy(out[:], pt.BytesMontgomery())
	return &out, nil
}

func edPrivToX25519Priv(edPriv ed25519.PrivateKey) (*[32]byte, error) {
	if len(edPriv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("cas: ed25519 priv must be %d bytes, got %d", ed25519.PrivateKeySize, len(edPriv))
	}
	// RFC 7748 / ed25519 spec: the curve25519 scalar is the first 32
	// bytes of SHA-512(seed) with the standard clamp applied.
	h := sha512.Sum512(edPriv.Seed())
	var out [32]byte
	copy(out[:], h[:32])
	out[0] &= 248
	out[31] &= 127
	out[31] |= 64
	return &out, nil
}
