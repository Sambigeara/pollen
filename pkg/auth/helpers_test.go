// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func newKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

func invalidateSignature(sig []byte) []byte {
	bad := make([]byte, len(sig))
	copy(bad, sig)
	bad[0] ^= 0xFF
	return bad
}
