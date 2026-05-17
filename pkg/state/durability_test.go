// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func keyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

// validatedStore builds a store with the admission pipeline wired as the
// mutation validator, exactly as pkg/supervisor wires it in production
// (state.New -> admission.New -> SetMutationValidator). The durability
// bug only manifests with the real pipeline on the apply/restore path,
// so the test must exercise that wiring rather than a stand-in.
func validatedStore(t *testing.T, self types.PeerKey, rootPub ed25519.PublicKey) state.StateStore {
	t.Helper()
	st := state.New(self, rootPub)
	pipe := admission.New(rootPub, st)
	st.SetMutationValidator(pipe.Admit)
	return st
}

// publisherFullState produces the EncodeFull blob of a publisher that
// holds a root-issued grant and has published one workload fact. This is
// the canonical durable blob: a single delivery carrying both the
// authority grant and the fact that references it.
func publisherFullState(t *testing.T, rootPriv ed25519.PrivateKey, rootPub ed25519.PublicKey) (types.PeerKey, ed25519.PublicKey, string, []byte) {
	t.Helper()
	now := time.Now()
	pPub, pPriv := keyPair(t)
	pKey := types.PeerKeyFromBytes(pPub)

	grantP, err := identity.IssueGrant(rootPriv, nil, pPub,
		identity.PublisherCapabilities(), &identityv1.Budget{},
		now.Add(-time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, err)
	sigP, err := identity.SignGrantSubject(grantP, pPriv)
	require.NoError(t, err)

	a := state.New(pKey, rootPub)
	a.SetLocalSigner(fact.NewSigner(pPriv))
	a.SetLocalGrant(grantP, sigP)
	hash := hex.EncodeToString(bytes.Repeat([]byte{0xaa}, 32))
	_, err = a.PublishWorkload(state.WorkloadSpec{Hash: hash, Name: "echo", MinReplicas: 1}, nil)
	require.NoError(t, err)

	return pKey, pPub, hash, a.EncodeFull()
}

// TestRestoreAdmitsFactWhoseGrantSharesTheBatch is the regression for the
// disk-restore durability hole. A fresh node with the admission pipeline
// wired restores an EncodeFull blob that carries a publisher's grant and
// that publisher's signed fact in one delivery. The publisher is offline
// and never redelivers, so the restore path has exactly one chance to
// admit the fact. The single-pass apply resolved the authority grant
// from the pre-batch snapshot, so the fact was dropped and lost forever,
// breaking the cluster-scoped durability guarantee. It must survive.
func TestRestoreAdmitsFactWhoseGrantSharesTheBatch(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	pKey, pPub, hash, data := publisherFullState(t, rootPriv, rootPub)

	b := validatedStore(t, types.PeerKeyFromBytes([]byte{0x09}), rootPub)
	require.NoError(t, b.LoadGossipState(data))

	snap := b.Snapshot()
	require.NotNil(t, snap.GrantFor(pPub), "authority grant restored")
	sv, ok := snap.Specs[hash]
	require.True(t, ok, "fact survives restore though its publisher is offline")
	require.Equal(t, pKey, sv.Publisher)
	require.NotNil(t, sv.Fact)
	require.False(t, snap.IsDenied(pKey))
}

// TestLiveDeltaAdmitsGrantAndFactOnFirstDelivery proves a single gossip
// delta carrying both a peer's grant and that peer's fact admits the
// fact on first delivery with the pipeline wired, rather than dropping
// it and depending on an anti-entropy redelivery the restore path never
// gets.
func TestLiveDeltaAdmitsGrantAndFactOnFirstDelivery(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	pKey, _, hash, data := publisherFullState(t, rootPriv, rootPub)

	b := validatedStore(t, types.PeerKeyFromBytes([]byte{0x09}), rootPub)
	_, _, err := b.ApplyDelta(pKey, data)
	require.NoError(t, err)

	_, ok := b.Snapshot().Specs[hash]
	require.True(t, ok, "fact admitted on first delivery, not deferred to anti-entropy")
}

// TestRestoreStillRejectsFactWithNoAuthorityGrant proves the two-pass
// ordering did not weaken admission: a blob whose grant events are
// stripped leaves an orphan fact whose authority cannot be resolved, and
// it must still be rejected (fail-closed preserved).
func TestRestoreStillRejectsFactWithNoAuthorityGrant(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	_, _, hash, data := publisherFullState(t, rootPriv, rootPub)

	var batch statev1.GossipEventBatch
	require.NoError(t, batch.UnmarshalVT(data))
	kept := batch.GetEvents()[:0]
	strippedGrant := false
	for _, ev := range batch.GetEvents() {
		if ev.GetGrant() != nil {
			strippedGrant = true
			continue
		}
		kept = append(kept, ev)
	}
	require.True(t, strippedGrant, "publisher grant present to strip")
	batch.Events = kept
	orphan, err := batch.MarshalVT()
	require.NoError(t, err)

	b := validatedStore(t, types.PeerKeyFromBytes([]byte{0x09}), rootPub)
	require.NoError(t, b.LoadGossipState(orphan))
	_, ok := b.Snapshot().Specs[hash]
	require.False(t, ok, "fact with an unresolvable authority is still rejected")
}
