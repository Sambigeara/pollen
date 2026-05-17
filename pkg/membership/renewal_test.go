// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package membership

import (
	"errors"
	"testing"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func pk(b byte) types.PeerKey {
	raw := make([]byte, 32)
	raw[0] = b
	return types.PeerKeyFromBytes(raw)
}

func delegatingGrant() *identityv1.Grant {
	return &identityv1.Grant{Claims: &identityv1.GrantClaims{
		Capabilities: &identityv1.Capabilities{CanDelegate: true},
	}}
}

func plainGrant() *identityv1.Grant {
	return &identityv1.Grant{Claims: &identityv1.GrantClaims{
		Capabilities: &identityv1.Capabilities{},
	}}
}

func TestFindRenewalTarget(t *testing.T) {
	self := pk(1)

	t.Run("picks a delegating peer that advertises a control endpoint", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			pk(2): {Grant: delegatingGrant(), ControlAddr: "admin:8443"},
		}}
		require.Equal(t, "admin:8443", findRenewalTarget(snap, self))
	})

	t.Run("never targets self", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			self: {Grant: delegatingGrant(), ControlAddr: "me:8443"},
		}}
		require.Equal(t, "", findRenewalTarget(snap, self))
	})

	t.Run("skips a denied peer", func(t *testing.T) {
		snap := state.Snapshot{
			Nodes: map[types.PeerKey]state.NodeView{
				pk(2): {Grant: delegatingGrant(), ControlAddr: "denied:8443"},
			},
			DeniedKeys: []types.PeerKey{pk(2)},
		}
		require.Equal(t, "", findRenewalTarget(snap, self))
	})

	t.Run("skips a peer with no control endpoint", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			pk(2): {Grant: delegatingGrant()},
		}}
		require.Equal(t, "", findRenewalTarget(snap, self))
	})

	t.Run("skips a peer that cannot delegate", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			pk(2): {Grant: plainGrant(), ControlAddr: "tenant:8443"},
			pk(3): {ControlAddr: "nogrant:8443"},
		}}
		require.Equal(t, "", findRenewalTarget(snap, self))
	})

	t.Run("is deterministic across multiple candidates", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			pk(5): {Grant: delegatingGrant(), ControlAddr: "five:8443"},
			pk(2): {Grant: delegatingGrant(), ControlAddr: "two:8443"},
			pk(9): {Grant: delegatingGrant(), ControlAddr: "nine:8443"},
		}}
		first := findRenewalTarget(snap, self)
		require.Equal(t, "two:8443", first)
		require.Equal(t, first, findRenewalTarget(snap, self))
	})
}

func TestRenewalFailing(t *testing.T) {
	t.Run("not failing before any attempt", func(t *testing.T) {
		require.False(t, (&Service{}).RenewalFailing())
	})

	t.Run("not failing after a successful attempt", func(t *testing.T) {
		s := &Service{}
		s.attemptRecord(nil)
		require.False(t, s.RenewalFailing())
	})

	t.Run("failing after a failed attempt", func(t *testing.T) {
		s := &Service{}
		s.attemptRecord(errors.New("no delegating peer reachable"))
		require.True(t, s.RenewalFailing())
	})
}
