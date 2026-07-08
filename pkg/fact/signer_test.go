// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package fact_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/stretchr/testify/require"
)

func signerKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return priv
}

func issueOne(t *testing.T, s *fact.Signer) uint64 {
	t.Helper()
	body := &statev1.WorkloadSpecChange{Hash: strings.Repeat("a", 64), Name: "echo", MinReplicas: 1}
	res := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{
		Name: body.GetName(),
		Hash: bytes.Repeat([]byte{0xaa}, 32),
	}}}
	f, err := s.IssueFact(res, body, nil, false)
	require.NoError(t, err)
	return f.GetSeq()
}

// TestDurableSignerMonotonicAcrossRestart is the load-bearing property:
// a producer's per-authority sequence never repeats or regresses, even
// when the process restarts, because the high-water is persisted and
// reloaded.
func TestDurableSignerMonotonicAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fact.seq")
	priv := signerKey(t)

	s1, err := fact.NewDurableSigner(priv, path)
	require.NoError(t, err)
	require.Equal(t, uint64(1), issueOne(t, s1))
	require.Equal(t, uint64(2), issueOne(t, s1))

	w, err := s1.IssueBlobWrapping(bytes.Repeat([]byte{0xcc}, 32), bytes.Repeat([]byte{0xbb}, 32), bytes.Repeat([]byte{0xdd}, 48))
	require.NoError(t, err)
	require.Equal(t, uint64(3), w.GetSeq(), "blob wrapping shares the fact sequence")

	// Simulated restart: a fresh signer at the same path resumes from
	// the persisted high-water rather than repeating 1.
	s2, err := fact.NewDurableSigner(priv, path)
	require.NoError(t, err)
	require.Equal(t, uint64(4), issueOne(t, s2))
	require.Equal(t, uint64(5), issueOne(t, s2))
}

func TestDurableSignerMissingFileStartsAtOne(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent", "fact.seq")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	s, err := fact.NewDurableSigner(signerKey(t), path)
	require.NoError(t, err)
	require.Equal(t, uint64(1), issueOne(t, s))
}

// TestDurableSignerCorruptFileFailsClosed proves a damaged high-water
// is an error, not a silent reset: regressing the sequence would let a
// later fact reuse an earlier seq.
func TestDurableSignerCorruptFileFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fact.seq")
	require.NoError(t, os.WriteFile(path, []byte("not-a-number"), 0o600))
	_, err := fact.NewDurableSigner(signerKey(t), path)
	require.Error(t, err)
	require.ErrorContains(t, err, "parse fact seq")
}

// TestEphemeralSignerIsInMemory documents the NewSigner contract: its
// sequence is process-local and resets, so it is for tests and
// non-persisting callers only.
func TestEphemeralSignerIsInMemory(t *testing.T) {
	priv := signerKey(t)
	s1 := fact.NewSigner(priv)
	require.Equal(t, uint64(1), issueOne(t, s1))
	require.Equal(t, uint64(2), issueOne(t, s1))

	s2 := fact.NewSigner(priv)
	require.Equal(t, uint64(1), issueOne(t, s2), "ephemeral signer resets")
}
