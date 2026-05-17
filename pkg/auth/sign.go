// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"crypto/ed25519"
	"time"

	"google.golang.org/protobuf/proto"
)

// sigContextAccessToken domain-separates access-token signatures from
// every other ed25519 signature in the system.
const sigContextAccessToken = "pollen.accesstoken.v1"

// timeSkewAllowance is the clock-skew grace applied to token validity
// windows.
const timeSkewAllowance = time.Minute

func signaturePayload(msg proto.Message) ([]byte, error) {
	return (proto.MarshalOptions{Deterministic: true}).Marshal(msg)
}

func signPayload(privateKey ed25519.PrivateKey, payload []byte, context string) ([]byte, error) {
	return privateKey.Sign(nil, payload, &ed25519.Options{Context: context})
}

func verifyPayload(publicKey ed25519.PublicKey, payload, signature []byte, context string) error {
	return ed25519.VerifyWithOptions(publicKey, payload, signature, &ed25519.Options{Context: context})
}
