// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity_test

import (
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/stretchr/testify/require"
)

// TestEnsureIdentityKeyRejectsCorruptKey proves a truncated or corrupted
// key PEM surfaces an error rather than panicking the daemon at load.
func TestEnsureIdentityKeyRejectsCorruptKey(t *testing.T) {
	dir := t.TempDir()
	keysDir := identity.IdentityPath(dir)
	require.NoError(t, os.MkdirAll(keysDir, 0o700))

	// A well-formed PEM block of the right type but a short (non-32-byte)
	// body: exactly what a truncated write leaves behind.
	corrupt := pem.EncodeToMemory(&pem.Block{Type: "ED25519 PRIVATE KEY", Bytes: []byte("too-short")})
	require.NoError(t, os.WriteFile(filepath.Join(keysDir, "ed25519.key"), corrupt, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(keysDir, "ed25519.pub"),
		pem.EncodeToMemory(&pem.Block{Type: "ED25519 PUBLIC KEY", Bytes: make([]byte, 32)}), 0o600))

	require.NotPanics(t, func() {
		_, _, err := identity.EnsureIdentityKey(keysDir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid private key")
	})
}

// TestPriorEnrollmentArtifact proves the startup guard distinguishes a
// genuinely fresh identity dir (auto-init root is safe) from one already
// holding a trust anchor or a legacy cert-era file (auto-init would
// overwrite the anchor and strand the node).
func TestPriorEnrollmentArtifact(t *testing.T) {
	t.Run("empty dir is fresh", func(t *testing.T) {
		_, occupied := identity.PriorEnrollmentArtifact(t.TempDir())
		require.False(t, occupied)
	})

	t.Run("keys-only dir is fresh", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "ed25519.key"), []byte("x"), 0o600))
		_, occupied := identity.PriorEnrollmentArtifact(dir)
		require.False(t, occupied)
	})

	t.Run("trust anchor without grant is occupied", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "root.pub"), make([]byte, 32), 0o600))
		reason, occupied := identity.PriorEnrollmentArtifact(dir)
		require.True(t, occupied)
		require.Contains(t, reason, "root.pub")
	})

	t.Run("legacy delegation cert is occupied", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "delegation.cert.pb"), []byte("x"), 0o600))
		reason, occupied := identity.PriorEnrollmentArtifact(dir)
		require.True(t, occupied)
		require.Contains(t, reason, "delegation certificate")
	})
}
