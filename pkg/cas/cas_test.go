package cas_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/cas"
)

func TestPutGetRoundTrip(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)

	data := []byte("hello wasm world")
	hash, err := store.Put(bytes.NewReader(data))
	require.NoError(t, err)

	wantHash := sha256.Sum256(data)
	require.Equal(t, hex.EncodeToString(wantHash[:]), hash)

	rc, err := store.Get(hash)
	require.NoError(t, err)
	t.Cleanup(func() { rc.Close() })

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestHas(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)

	data := []byte("test artifact")
	hash, err := store.Put(bytes.NewReader(data))
	require.NoError(t, err)

	require.True(t, store.Has(hash))
	require.False(t, store.Has("0000000000000000000000000000000000000000000000000000000000000000"))
}

func TestGetMissing(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)

	_, err = store.Get("deadbeef00000000000000000000000000000000000000000000000000000000")
	require.ErrorIs(t, err, cas.ErrNotFound)
}

func TestPutIdempotent(t *testing.T) {
	store, err := cas.New(t.TempDir())
	require.NoError(t, err)

	data := []byte("same content")
	hash1, err := store.Put(bytes.NewReader(data))
	require.NoError(t, err)

	hash2, err := store.Put(bytes.NewReader(data))
	require.NoError(t, err)

	require.Equal(t, hash1, hash2)
}
