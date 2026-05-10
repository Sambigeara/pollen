// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package cas_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/cas"
)

func newDEK(t *testing.T) []byte {
	t.Helper()
	dek, err := cas.GenerateDEK()
	require.NoError(t, err)
	return dek
}

func TestPutGetRoundTrip(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)
	dek := newDEK(t)

	data := []byte("hello wasm world")
	hash, err := store.Put(bytes.NewReader(data), dek)
	require.NoError(t, err)

	wantHash := sha256.Sum256(data)
	require.Equal(t, hex.EncodeToString(wantHash[:]), hash, "address must be hash of plaintext, not envelope")

	rc, err := store.Get(hash, dek)
	require.NoError(t, err)
	t.Cleanup(func() { rc.Close() })

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestPutEncryptsOnDisk(t *testing.T) {
	root := t.TempDir()
	store, err := cas.New(root)
	require.NoError(t, err)

	plaintext := []byte("sensitive payload that must not appear on disk")
	hash, err := store.Put(bytes.NewReader(plaintext), newDEK(t))
	require.NoError(t, err)

	onDisk, err := os.ReadFile(filepath.Join(root, "cas", hash[:2], hash))
	require.NoError(t, err)
	require.NotContains(t, string(onDisk), string(plaintext), "plaintext must not appear on disk")
}

func TestGetWithWrongDEK(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)

	hash, err := store.Put(bytes.NewReader([]byte("payload")), newDEK(t))
	require.NoError(t, err)

	_, err = store.Get(hash, newDEK(t))
	require.Error(t, err, "wrong dek must fail GCM tag check")
}

func TestPutIdempotentDespiteRandomNonce(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)
	dek := newDEK(t)

	data := []byte("same content")
	hash1, err := store.Put(bytes.NewReader(data), dek)
	require.NoError(t, err)
	hash2, err := store.Put(bytes.NewReader(data), dek)
	require.NoError(t, err)
	require.Equal(t, hash1, hash2, "address tracks plaintext, not nonce")
}

func TestPutCiphertextRoundTrip(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)
	dek := newDEK(t)

	plaintext := []byte("payload from a peer")
	envelope, err := cas.Encrypt(plaintext, dek)
	require.NoError(t, err)
	digest := sha256.Sum256(plaintext)
	hash := hex.EncodeToString(digest[:])

	require.NoError(t, store.PutCiphertext(hash, bytes.NewReader(envelope)))
	require.True(t, store.Has(hash))

	rc, err := store.Get(hash, dek)
	require.NoError(t, err)
	t.Cleanup(func() { rc.Close() })
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

// TestGetReturnsSeekableReader pins the contract that callers like
// pkg/static rely on: the plaintext stream from Get must implement
// io.Seeker so http.ServeContent can serve byte-range requests without
// re-decrypting. Returning a non-seekable wrapper here silently routes
// every static asset request to a 503 fallback.
func TestGetReturnsSeekableReader(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)
	dek := newDEK(t)

	plaintext := []byte("seekable payload")
	hash, err := store.Put(bytes.NewReader(plaintext), dek)
	require.NoError(t, err)

	rc, err := store.Get(hash, dek)
	require.NoError(t, err)
	t.Cleanup(func() { rc.Close() })

	seeker, ok := rc.(io.Seeker)
	require.True(t, ok, "Get must return an io.Seeker")

	off, err := seeker.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Zero(t, off)
}

func TestGetCiphertextReturnsRawEnvelope(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)
	dek := newDEK(t)
	plaintext := []byte("payload")

	hash, err := store.Put(bytes.NewReader(plaintext), dek)
	require.NoError(t, err)

	rc, err := store.GetCiphertext(hash)
	require.NoError(t, err)
	t.Cleanup(func() { rc.Close() })
	envelope, err := io.ReadAll(rc)
	require.NoError(t, err)

	got, err := cas.Decrypt(envelope, dek)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestHas(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)

	hash, err := store.Put(bytes.NewReader([]byte("test artifact")), newDEK(t))
	require.NoError(t, err)

	require.True(t, store.Has(hash))
	require.False(t, store.Has("0000000000000000000000000000000000000000000000000000000000000000"))
}

func TestGetMissing(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)

	_, err = store.Get("deadbeef00000000000000000000000000000000000000000000000000000000", newDEK(t))
	require.ErrorIs(t, err, cas.ErrNotFound)
}

func TestPutSetsGroupReadableMode(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("permission semantics are Linux-only")
	}

	root := t.TempDir()
	store, err := cas.New(root)
	require.NoError(t, err)

	hash, err := store.Put(bytes.NewReader([]byte("shared artifact")), newDEK(t))
	require.NoError(t, err)

	info, err := os.Stat(filepath.Join(root, "cas", hash[:2], hash))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode())
}

func TestRemovePrunesEmptyShard(t *testing.T) {
	root := t.TempDir()
	store, err := cas.New(root)
	require.NoError(t, err)

	hash, err := store.Put(bytes.NewReader([]byte("only blob in shard")), newDEK(t))
	require.NoError(t, err)
	shard := filepath.Join(root, "cas", hash[:2])
	require.DirExists(t, shard)

	require.NoError(t, store.Remove(hash))
	require.NoDirExists(t, shard)
}

func TestRemoveLeavesNonEmptyShard(t *testing.T) {
	root := t.TempDir()
	store, err := cas.New(root)
	require.NoError(t, err)

	hash, err := store.Put(bytes.NewReader([]byte("first")), newDEK(t))
	require.NoError(t, err)
	sibling := filepath.Join(root, "cas", hash[:2], hash[:2]+"00sibling000000000000000000000000000000000000000000000000000000")
	require.NoError(t, os.WriteFile(sibling, []byte("sibling"), 0o600))

	require.NoError(t, store.Remove(hash))
	require.DirExists(t, filepath.Join(root, "cas", hash[:2]))
	require.FileExists(t, sibling)
}

func TestNewRejectsLegacyStore(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "cas", "ab"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "cas", "ab", "abcd"), []byte("plaintext"), 0o600))

	_, err := cas.New(root)
	require.Error(t, err)
	require.True(t, errors.Is(err, cas.ErrLegacyStore), "non-empty CAS without version marker must surface ErrLegacyStore")
}

// TestNewTolerantOfOrphanPutTempfiles covers the case where a previous
// Put crashed after creating its `.put-*.tmp` file but before the
// rename. Without the tolerance, ensureVersion's legacy classifier
// trips on the leftover entry and returns ErrLegacyStore on next
// start, forcing the operator to wipe the cas directory and re-fetch
// every blob to recover from a single mid-write crash.
func TestNewTolerantOfOrphanPutTempfiles(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "cas"), 0o700))
	orphan := filepath.Join(root, "cas", ".put-1234.tmp")
	require.NoError(t, os.WriteFile(orphan, []byte("partial"), 0o600))

	_, err := cas.New(root)
	require.NoError(t, err, "orphan tempfiles must not be classified as legacy plaintext")

	_, err = os.Stat(orphan)
	require.ErrorIs(t, err, os.ErrNotExist, "orphan tempfile should be cleaned up on open")

	data, err := os.ReadFile(filepath.Join(root, "cas", ".version"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), data, "version marker must be written despite the orphan")
}

// TestNewSweepsOrphansAfterVersionMarker ensures the tempfile cleanup
// runs on every open, not just the first one that writes the version
// marker. Without it, a mid-Put crash on a populated store leaves
// orphans that accumulate across restarts.
func TestNewSweepsOrphansAfterVersionMarker(t *testing.T) {
	root := t.TempDir()
	_, err := cas.New(root)
	require.NoError(t, err, "first open must succeed and write the version marker")

	orphan := filepath.Join(root, "cas", ".put-9999.tmp")
	require.NoError(t, os.WriteFile(orphan, []byte("partial-after-version"), 0o600))

	_, err = cas.New(root)
	require.NoError(t, err)

	_, err = os.Stat(orphan)
	require.ErrorIs(t, err, os.ErrNotExist, "orphan tempfile must be swept on subsequent open")
}

func TestNewWritesVersionMarkerOnFreshDir(t *testing.T) {
	root := t.TempDir()
	_, err := cas.New(root)
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(root, "cas", ".version"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), data)
}
