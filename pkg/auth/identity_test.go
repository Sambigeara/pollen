package auth_test

import (
	"crypto/ed25519"
	"testing"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestGenIdentityKeyAndReadIdentityPub(t *testing.T) {
	dir := t.TempDir()

	priv, pub, err := auth.GenIdentityKey(dir)
	require.NoError(t, err)
	require.Len(t, priv, ed25519.PrivateKeySize)
	require.Len(t, pub, ed25519.PublicKeySize)

	// ReadIdentityPub must return the same key (exercises decodePubKeyPEM).
	gotPub, err := auth.ReadIdentityPub(dir)
	require.NoError(t, err)
	require.Equal(t, pub, gotPub)

	// Calling GenIdentityKey again returns the same keys (idempotent).
	priv2, pub2, err := auth.GenIdentityKey(dir)
	require.NoError(t, err)
	require.Equal(t, priv, priv2)
	require.Equal(t, pub, pub2)
}

func TestReadIdentityPubMissingFile(t *testing.T) {
	_, err := auth.ReadIdentityPub(t.TempDir())
	require.Error(t, err)
}
