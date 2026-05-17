// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

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
