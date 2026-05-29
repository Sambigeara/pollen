// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package membership

import (
	"errors"
	"testing"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

func TestFindRenewalTargets(t *testing.T) {
	self := pk(1)

	t.Run("picks a delegating peer that advertises a control endpoint", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			pk(2): {Grant: delegatingGrant(), ControlAddr: "admin:8443"},
		}}
		require.Equal(t, []string{"admin:8443"}, findRenewalTargets(snap, self))
	})

	t.Run("never targets self", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			self: {Grant: delegatingGrant(), ControlAddr: "me:8443"},
		}}
		require.Empty(t, findRenewalTargets(snap, self))
	})

	t.Run("skips a denied peer", func(t *testing.T) {
		snap := state.Snapshot{
			Nodes: map[types.PeerKey]state.NodeView{
				pk(2): {Grant: delegatingGrant(), ControlAddr: "denied:8443"},
			},
			DeniedKeys: []types.PeerKey{pk(2)},
		}
		require.Empty(t, findRenewalTargets(snap, self))
	})

	t.Run("skips a peer with no control endpoint", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			pk(2): {Grant: delegatingGrant()},
		}}
		require.Empty(t, findRenewalTargets(snap, self))
	})

	t.Run("skips a peer that cannot delegate", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			pk(2): {Grant: plainGrant(), ControlAddr: "tenant:8443"},
			pk(3): {ControlAddr: "nogrant:8443"},
		}}
		require.Empty(t, findRenewalTargets(snap, self))
	})

	t.Run("returns every candidate in stable order so renewal can fall through", func(t *testing.T) {
		snap := state.Snapshot{Nodes: map[types.PeerKey]state.NodeView{
			pk(5): {Grant: delegatingGrant(), ControlAddr: "five:8443"},
			pk(2): {Grant: delegatingGrant(), ControlAddr: "two:8443"},
			pk(9): {Grant: delegatingGrant(), ControlAddr: "nine:8443"},
		}}
		got := findRenewalTargets(snap, self)
		require.Equal(t, []string{"two:8443", "five:8443", "nine:8443"}, got)
		require.Equal(t, got, findRenewalTargets(snap, self))
	})
}

// TestRescheduleExpiryTimer pins the precise-shutdown contract for
// non-renewable grants: the timer fires at the actual deadline, not at
// the next 5-minute polling tick. Renewable and horizon-less grants
// are left to the existing polling path, so the helper is a no-op for
// them; passing a fresh non-renewable grant arms (and re-arms) the
// shutdown signal.
func TestRescheduleExpiryTimer(t *testing.T) {
	mkGrant := func(deadlineIn time.Duration, nonRenewable bool) *identityv1.Grant {
		claims := &identityv1.GrantClaims{NonRenewable: nonRenewable}
		if deadlineIn != 0 {
			claims.GrantDeadlineUnix = time.Now().Add(deadlineIn).Unix()
		}
		return &identityv1.Grant{Claims: claims}
	}
	// Production adds TimeSkewAllowance to every deadline; the test
	// pins it to zero so a 50ms deadline expires in 50ms rather than
	// 60.05s. The skew exists to forgive small clock drift; the
	// scheduling logic is the same with it set to zero.
	mkService := func(g *identityv1.Grant) (*Service, chan struct{}) {
		ch := make(chan struct{}, 1)
		return &Service{
			log:        zap.NewNop().Sugar(),
			shutdownCh: ch,
			creds:      identity.NewCredentials(nil, nil, g),
		}, ch
	}

	t.Run("fires shutdown at deadline for non-renewable grant", func(t *testing.T) {
		g := mkGrant(50*time.Millisecond, true)
		s, ch := mkService(g)
		s.rescheduleExpiryTimer(g)
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatal("expiry timer did not fire shutdown within 2s of deadline")
		}
	})

	t.Run("no timer for renewable grant", func(t *testing.T) {
		g := mkGrant(50*time.Millisecond, false)
		s, ch := mkService(g)
		s.rescheduleExpiryTimer(g)
		select {
		case <-ch:
			t.Fatal("renewable grant must not trigger shutdown on deadline")
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("no timer for horizonless grant", func(t *testing.T) {
		g := mkGrant(0, true)
		s, ch := mkService(g)
		s.rescheduleExpiryTimer(g)
		select {
		case <-ch:
			t.Fatal("horizonless grant must not trigger shutdown")
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("rescheduling stops the previous timer", func(t *testing.T) {
		first := mkGrant(50*time.Millisecond, true)
		s, ch := mkService(first)
		s.rescheduleExpiryTimer(first)
		second := mkGrant(time.Hour, true)
		s.creds = identity.NewCredentials(nil, nil, second)
		s.rescheduleExpiryTimer(second)
		select {
		case <-ch:
			t.Fatal("first timer must not fire after being replaced")
		case <-time.After(200 * time.Millisecond):
		}
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
