// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/gate"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type fakeReader struct{ snap state.Snapshot }

func (f fakeReader) Snapshot() state.Snapshot { return f.snap }

// TestPresignRoundTrip locks the lock-step contract: the CLI presign
// builders must produce a (resource, body, signature) the daemon's own
// gate.Admit accepts after re-deriving the resource from the body. A
// silent divergence in either derivation site fails this test instead
// of silently dropping a user's publish.
func TestPresignRoundTrip(t *testing.T) {
	now := time.Now()
	adminPub, adminPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	authPub, authPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	grant, err := identity.IssueGrant(adminPriv, nil, authPub,
		identity.PublisherCapabilities(), identity.UnlimitedBudget(),
		now.Add(-time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)

	signer := fact.NewSigner(authPriv)
	store := fakeReader{snap: state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
		types.PeerKeyFromBytes(authPub): {Grant: grant},
	}}}
	g := gate.New(adminPub, store)

	hashBytes := make([]byte, 32)
	for i := range hashBytes {
		hashBytes[i] = 0xab
	}
	digest := make([]byte, 32)
	for i := range digest {
		digest[i] = 0xcd
	}

	t.Run("workload", func(t *testing.T) {
		body := &statev1.WorkloadSpecChange{Hash: hex.EncodeToString(hashBytes), Name: "echo", MinReplicas: 1}
		f, err := presignedWorkload(signer, hashBytes, body, nil, false)
		require.NoError(t, err)
		require.NoError(t, g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: body}}))

		// Body the daemon sees diverges from the one signed: rejected.
		other := &statev1.WorkloadSpecChange{Hash: hex.EncodeToString(hashBytes), Name: "tampered", MinReplicas: 1}
		require.Error(t, g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Workload{Workload: other}}))
	})

	t.Run("static", func(t *testing.T) {
		f, err := presignedStatic(signer, "site", digest, false)
		require.NoError(t, err)
		body := &statev1.StaticSpecChange{Name: "site", ManifestDigest: digest}
		require.NoError(t, g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Static{Static: body}}))

		f.Signature[0] ^= 0xff
		require.Error(t, g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Static{Static: body}}))
	})

	t.Run("blob", func(t *testing.T) {
		f, err := presignedBlob(signer, "blob", digest, nil, false)
		require.NoError(t, err)
		body := &statev1.BlobSpecChange{Name: "blob", Digest: digest}
		require.NoError(t, g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Blob{Blob: body}}))

		mismatch := &statev1.BlobSpecChange{Name: "renamed", Digest: digest}
		require.Error(t, g.Admit(&statev1.SpecChange{Fact: f, Body: &statev1.SpecChange_Blob{Blob: mismatch}}))
	})
}
