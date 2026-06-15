// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"filippo.io/edwards25519"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/sambigeara/pollen/pkg/plnfs"
)

const (
	keysSubdir = "keys"

	signingKeyName    = "ed25519.key"
	signingPubKeyName = "ed25519.pub"

	adminPrivKeyName = "admin_ed25519.key"
	adminPubKeyName  = "admin_ed25519.pub"

	rootPubName   = "root.pub"
	grantCertName = "grant.pb"
	factSeqName   = "fact.seq"

	// legacyDelegationCertName is the pre-grant (cert-era) authority file.
	// It is never written by this build; its presence marks a directory
	// enrolled by an older release, which the startup path refuses to
	// auto-initialise over.
	legacyDelegationCertName = "delegation.cert.pb"

	pemTypePriv = "ED25519 PRIVATE KEY"
	pemTypePub  = "ED25519 PUBLIC KEY"

	pemTypeAdminPriv = "POLLEN ADMIN ED25519 PRIVATE KEY"
	pemTypeAdminPub  = "POLLEN ADMIN ED25519 PUBLIC KEY"

	sigContextGrant        = "pollen.grant.v1"
	sigContextGrantSubject = "pollen.grant.subject.v1"
	sigContextSession      = "pollen.session.v1"
	sigContextGrantToken   = "pollen.granttoken.v1"
	sigContextInviteTicket = "pollen.inviteticket.v1"

	TimeSkewAllowance = time.Minute
)

const MaxAttributesSize = 4096

func IdentityPath(pollenDir string) string {
	return filepath.Join(pollenDir, keysSubdir)
}

// FactSeqPath is where a durable fact.Signer persists its per-authority
// sequence high-water. identityDir is the keys directory returned by
// IdentityPath.
func FactSeqPath(identityDir string) string {
	return filepath.Join(identityDir, factSeqName)
}

func ValidateAttributes(attrs *structpb.Struct) error {
	if attrs == nil {
		return nil
	}
	b, err := proto.Marshal(attrs)
	if err != nil {
		return fmt.Errorf("invalid attributes: %w", err)
	}
	if len(b) > MaxAttributesSize {
		return fmt.Errorf("attributes too large: %d bytes (max %d)", len(b), MaxAttributesSize)
	}
	return nil
}

// SignaturePayload deterministically marshals a proto message for
// signing or verification. Shared by the identity and fact substrates
// so both produce byte-identical payloads.
func SignaturePayload(msg proto.Message) ([]byte, error) {
	return (proto.MarshalOptions{Deterministic: true}).Marshal(msg)
}

func SignPayload(privateKey ed25519.PrivateKey, payload []byte, context string) ([]byte, error) {
	return privateKey.Sign(nil, payload, &ed25519.Options{Context: context})
}

func VerifyPayload(publicKey ed25519.PublicKey, payload, signature []byte, context string) error {
	return ed25519.VerifyWithOptions(publicKey, payload, signature, &ed25519.Options{Context: context})
}

// EdPubToX25519 maps an Ed25519 public key to its X25519 (Montgomery)
// equivalent via the standard birational map, so the same identity key
// that signs can also participate in X25519 key agreement.
func EdPubToX25519(edPub ed25519.PublicKey) ([]byte, error) {
	if len(edPub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("ed25519 pub must be %d bytes, got %d", ed25519.PublicKeySize, len(edPub))
	}
	pt, err := new(edwards25519.Point).SetBytes(edPub)
	if err != nil {
		return nil, fmt.Errorf("parse ed25519 pub: %w", err)
	}
	return pt.BytesMontgomery(), nil
}

// EdPrivToX25519 derives the X25519 scalar from an Ed25519 private key per
// RFC 7748: the first 32 bytes of SHA-512(seed) with the standard clamp.
func EdPrivToX25519(edPriv ed25519.PrivateKey) ([]byte, error) {
	if len(edPriv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("ed25519 priv must be %d bytes, got %d", ed25519.PrivateKeySize, len(edPriv))
	}
	h := sha512.Sum512(edPriv.Seed())
	var out [32]byte
	copy(out[:], h[:32])
	out[0] &= 248
	out[31] &= 127
	out[31] |= 64
	return out[:], nil
}

// StaticSharedSecret computes the X25519 ECDH secret between a local
// Ed25519 private key and a remote Ed25519 public key. Both ends of a
// pair derive the identical secret. The raw secret must be run through a
// KDF with domain separation before use as a key.
func StaticSharedSecret(localPriv ed25519.PrivateKey, remotePub ed25519.PublicKey) ([]byte, error) {
	xPrivBytes, err := EdPrivToX25519(localPriv)
	if err != nil {
		return nil, err
	}
	xPubBytes, err := EdPubToX25519(remotePub)
	if err != nil {
		return nil, err
	}
	curve := ecdh.X25519()
	xPriv, err := curve.NewPrivateKey(xPrivBytes)
	if err != nil {
		return nil, fmt.Errorf("x25519 priv: %w", err)
	}
	xPub, err := curve.NewPublicKey(xPubBytes)
	if err != nil {
		return nil, fmt.Errorf("x25519 pub: %w", err)
	}
	secret, err := xPriv.ECDH(xPub)
	if err != nil {
		return nil, fmt.Errorf("x25519 ecdh: %w", err)
	}
	return secret, nil
}

func generateKeyPair(dir, privName, pubName, privPEMType, pubPEMType string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	if err := plnfs.EnsureDir(dir); err != nil {
		return nil, nil, err
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	privPEM := pem.EncodeToMemory(&pem.Block{Type: privPEMType, Bytes: priv.Seed()})
	if err := plnfs.WriteGroupReadable(filepath.Join(dir, privName), privPEM); err != nil {
		return nil, nil, err
	}

	pubPEM := pem.EncodeToMemory(&pem.Block{Type: pubPEMType, Bytes: pub})
	if err := plnfs.WriteGroupReadable(filepath.Join(dir, pubName), pubPEM); err != nil {
		return nil, nil, err
	}

	return priv, pub, nil
}

func loadKeyPair(privPath, pubPath, privPEMType, pubPEMType string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	privRaw, err := os.ReadFile(privPath)
	if err != nil {
		return nil, nil, err
	}
	pubRaw, err := os.ReadFile(pubPath)
	if err != nil {
		return nil, nil, err
	}

	privBlock, _ := pem.Decode(privRaw)
	if privBlock == nil || privBlock.Type != privPEMType {
		return nil, nil, errors.New("invalid private key PEM")
	}

	pubBlock, _ := pem.Decode(pubRaw)
	if pubBlock == nil || pubBlock.Type != pubPEMType {
		return nil, nil, errors.New("invalid public key PEM")
	}

	// ed25519.NewKeyFromSeed panics on a seed that is not SeedSize bytes, so
	// validate lengths first: a truncated key file must surface an error, not
	// crash the daemon at load.
	if len(privBlock.Bytes) != ed25519.SeedSize {
		return nil, nil, fmt.Errorf("invalid private key: expected %d-byte seed, got %d", ed25519.SeedSize, len(privBlock.Bytes))
	}
	if len(pubBlock.Bytes) != ed25519.PublicKeySize {
		return nil, nil, fmt.Errorf("invalid public key: expected %d bytes, got %d", ed25519.PublicKeySize, len(pubBlock.Bytes))
	}

	return ed25519.NewKeyFromSeed(privBlock.Bytes), ed25519.PublicKey(pubBlock.Bytes), nil
}

func EnsureIdentityKey(identityDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	privPath := filepath.Join(identityDir, signingKeyName)
	pubPath := filepath.Join(identityDir, signingPubKeyName)

	if priv, pub, err := loadKeyPair(privPath, pubPath, pemTypePriv, pemTypePub); err == nil {
		return priv, pub, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}

	return generateKeyPair(identityDir, signingKeyName, signingPubKeyName, pemTypePriv, pemTypePub)
}

func ReadIdentityPub(identityDir string) (ed25519.PublicKey, error) {
	raw, err := os.ReadFile(filepath.Join(identityDir, signingPubKeyName))
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil || block.Type != pemTypePub {
		return nil, errors.New("invalid public key PEM")
	}
	return ed25519.PublicKey(block.Bytes), nil
}

func EnsureAdminKey(identityDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	priv, pub, err := loadKeyPair(
		filepath.Join(identityDir, adminPrivKeyName),
		filepath.Join(identityDir, adminPubKeyName),
		pemTypeAdminPriv,
		pemTypeAdminPub,
	)
	if err == nil {
		return priv, pub, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}

	return generateKeyPair(identityDir, adminPrivKeyName, adminPubKeyName, pemTypeAdminPriv, pemTypeAdminPub)
}

func LoadAdminKey(identityDir string) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	return loadKeyPair(
		filepath.Join(identityDir, adminPrivKeyName),
		filepath.Join(identityDir, adminPubKeyName),
		pemTypeAdminPriv,
		pemTypeAdminPub,
	)
}
