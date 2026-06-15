// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"testing"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestGrantRecencyGateRejectsRollback pins the recency gate in
// isAcceptableGrantEvent: a strictly-older grant (by signed not_before)
// must never supersede the held one, even replayed under a higher gossip
// counter.
func TestGrantRecencyGateRejectsRollback(t *testing.T) {
	rootPub, rootPriv := keyPair(t)
	pPub, pPriv := keyPair(t)
	pKey := types.PeerKeyFromBytes(pPub)
	now := time.Now()

	mkGrant := func(notBefore time.Time, caps *identityv1.Capabilities) (*identityv1.Grant, []byte) {
		g, err := identity.IssueGrant(rootPriv, nil, pPub, caps, &identityv1.Budget{},
			notBefore, now.Add(30*24*time.Hour), false)
		require.NoError(t, err)
		sig, err := identity.SignGrantSubject(g, pPriv)
		require.NoError(t, err)
		return g, sig
	}

	// older carries the broad capability; newer is the shrink the cluster has
	// since converged on. Both are validly signed and chain to root, so only
	// the recency gate distinguishes them.
	older, olderSig := mkGrant(now.Add(-2*time.Hour), identity.FullCapabilities())
	newer, newerSig := mkGrant(now.Add(-1*time.Minute), identity.LeafCapabilities())

	grantDelta := func(g *identityv1.Grant, sig []byte, counter uint64) []byte {
		batch := &statev1.GossipEventBatch{Events: []*statev1.GossipEvent{{
			PeerId:  pKey.String(),
			Counter: counter,
			Change:  &statev1.GossipEvent_Grant{Grant: &statev1.GrantChange{Grant: g, SubjectSignature: sig}},
		}}}
		data, err := batch.MarshalVT()
		require.NoError(t, err)
		return data
	}

	t.Run("strictly older grant cannot roll back a newer one", func(t *testing.T) {
		v := validatedStore(t, types.PeerKeyFromBytes([]byte{0x09}), rootPub)
		_, _, err := v.ApplyDelta(grantDelta(newer, newerSig, 1))
		require.NoError(t, err)
		// Replay the older grant under a higher counter: counter-LWW alone
		// would accept it; the recency gate must not.
		_, _, err = v.ApplyDelta(grantDelta(older, olderSig, 99))
		require.NoError(t, err)

		held := v.Snapshot().GrantFor(pPub)
		require.NotNil(t, held)
		require.Equal(t, newer.GetClaims().GetNotBeforeUnix(), held.GetClaims().GetNotBeforeUnix(),
			"rolled-back older grant must be rejected")
		require.False(t, held.GetClaims().GetCapabilities().GetCanAdmit(),
			"the shrunk (newer) capability must survive the rollback attempt")
	})

	t.Run("a newer grant still supersedes an older one", func(t *testing.T) {
		v := validatedStore(t, types.PeerKeyFromBytes([]byte{0x09}), rootPub)
		_, _, err := v.ApplyDelta(grantDelta(older, olderSig, 1))
		require.NoError(t, err)
		_, _, err = v.ApplyDelta(grantDelta(newer, newerSig, 2))
		require.NoError(t, err)

		held := v.Snapshot().GrantFor(pPub)
		require.NotNil(t, held)
		require.Equal(t, newer.GetClaims().GetNotBeforeUnix(), held.GetClaims().GetNotBeforeUnix(),
			"legitimate renewal with a later not_before must win")
	})
}
