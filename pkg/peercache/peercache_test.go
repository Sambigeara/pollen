// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package peercache

import (
	"crypto/ed25519"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func keyWith(b byte) types.PeerKey {
	raw := make([]byte, ed25519.PublicKeySize)
	raw[0] = b
	return types.PeerKeyFromBytes(raw)
}

func TestOpenMissingFile(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)
	require.Empty(t, s.Snapshot())
}

func TestUpsertAndFlushRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Second)
	s.Upsert(keyWith(0xAA), []string{"relay.example.com:60611"}, now)
	s.Upsert(keyWith(0xBB), []string{"a.example.com:60611", "b.example.com:60611"}, now.Add(time.Second))
	require.NoError(t, s.Flush())

	reopened, err := Open(dir)
	require.NoError(t, err)

	snap := reopened.Snapshot()
	require.Len(t, snap, 2)
	require.Equal(t, keyWith(0xBB), snap[0].PeerKey)
	require.Equal(t, []string{"a.example.com:60611", "b.example.com:60611"}, snap[0].Addrs)
	require.Equal(t, keyWith(0xAA), snap[1].PeerKey)
}

func TestUpsertIgnoresEmptyAddrs(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)

	s.Upsert(keyWith(0xAA), nil, time.Now())
	require.Empty(t, s.Snapshot())
}

func TestUpsertNoopWhenUnchanged(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	require.NoError(t, err)

	now := time.Now()
	s.Upsert(keyWith(0xAA), []string{"relay.example.com:60611"}, now)
	require.NoError(t, s.Flush())

	info, err := os.Stat(filepath.Join(dir, fileName))
	require.NoError(t, err)
	firstMtime := info.ModTime()

	time.Sleep(10 * time.Millisecond)
	s.Upsert(keyWith(0xAA), []string{"relay.example.com:60611"}, now)
	require.NoError(t, s.Flush())

	info2, err := os.Stat(filepath.Join(dir, fileName))
	require.NoError(t, err)
	require.Equal(t, firstMtime, info2.ModTime(), "identical upsert should not rewrite the file")
}

func TestForgetRemovesEntry(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	require.NoError(t, err)

	now := time.Now()
	s.Upsert(keyWith(0xAA), []string{"a.example.com:60611"}, now)
	s.Upsert(keyWith(0xBB), []string{"b.example.com:60611"}, now)
	s.Forget(keyWith(0xAA))
	require.NoError(t, s.Flush())

	reopened, err := Open(dir)
	require.NoError(t, err)
	snap := reopened.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, keyWith(0xBB), snap[0].PeerKey)
}

func TestEvictionOnOverflow(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)

	base := time.Now()
	for i := range maxEntries + 5 {
		s.Upsert(keyWith(byte(i)), []string{"x.example.com:" + strconv.Itoa(i)}, base.Add(time.Duration(i)*time.Second))
	}

	snap := s.Snapshot()
	require.Len(t, snap, maxEntries)
	for _, e := range snap {
		require.NotEqual(t, keyWith(0x00), e.PeerKey)
		require.NotEqual(t, keyWith(0x04), e.PeerKey)
	}
}

func TestFlushSkipsUnchanged(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	require.NoError(t, err)

	require.NoError(t, s.Flush())
	_, err = os.Stat(filepath.Join(dir, fileName))
	require.True(t, os.IsNotExist(err), "flush on empty+clean store must not create file")
}

func TestLoadSkipsMalformedEntries(t *testing.T) {
	dir := t.TempDir()
	wire := []wireEntry{
		{PeerPub: "not-hex", Addrs: []string{"a.example.com:60611"}, LastSeen: time.Now()},
		{PeerPub: keyWith(0xAA).String(), Addrs: nil, LastSeen: time.Now()},
		{PeerPub: keyWith(0xBB).String(), Addrs: []string{"b.example.com:60611"}, LastSeen: time.Now()},
	}
	raw, err := json.Marshal(wire)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, fileName), raw, 0o600))

	s, err := Open(dir)
	require.NoError(t, err)
	snap := s.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, keyWith(0xBB), snap[0].PeerKey)
}
