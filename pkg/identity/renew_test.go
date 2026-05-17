// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"testing"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/stretchr/testify/require"
)

// TestGrantRenewDue pins the single renew-now policy shared by the
// daemon maintenance loop and the wire-mode CLI: a finite horizon
// inside the lead window is due, a far horizon is not, no horizon
// (admin/root) is never due, and a horizon already past is still due so
// a late caller still attempts a renewal rather than silently giving
// up. It reads only the deadline, so a bare claims grant exercises it.
func TestGrantRenewDue(t *testing.T) {
	now := time.Now()
	mk := func(deadline time.Time) *identityv1.Grant {
		g := &identityv1.Grant{Claims: &identityv1.GrantClaims{}}
		if !deadline.IsZero() {
			g.Claims.GrantDeadlineUnix = deadline.Unix()
		}
		return g
	}

	require.False(t, GrantRenewDue(mk(time.Time{}), now), "no horizon is never due")
	require.False(t, GrantRenewDue(mk(now.Add(DefaultGrantDeadlineTTL)), now), "full horizon is not yet due")
	require.True(t, GrantRenewDue(mk(now.Add(renewLeadWindow-time.Hour)), now), "inside the lead window is due")
	require.True(t, GrantRenewDue(mk(now.Add(-time.Hour)), now), "already past horizon is still due")
}

// TestAdoptRenewedGrant proves the renewal response is never trusted
// blindly: only a grant that chains to our root, is for the same
// subject, is within its horizon and is not denied is swapped in; every
// other case keeps the current grant.
func TestAdoptRenewedGrant(t *testing.T) {
	rootPub, rootPriv := kp(t)
	sPub, sPriv := kp(t)
	now := time.Now()

	issue := func(t *testing.T, subject []byte, nb, dl time.Time) *identityv1.Grant {
		t.Helper()
		g, err := IssueGrant(rootPriv, nil, subject, FullCapabilities(), UnlimitedBudget(), nb, dl)
		require.NoError(t, err)
		return g
	}
	current := issue(t, sPub, now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL))

	t.Run("no current grant", func(t *testing.T) {
		c := NewCredentials(rootPub, sPriv, nil)
		err := c.AdoptRenewedGrant(issue(t, sPub, now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL)), now, nil)
		require.ErrorContains(t, err, "no current grant")
	})

	t.Run("valid same-subject grant is adopted", func(t *testing.T) {
		c := NewCredentials(rootPub, sPriv, current)
		fresh := issue(t, sPub, now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL))
		require.NoError(t, c.AdoptRenewedGrant(fresh, now, nil))
		require.Same(t, fresh, c.Grant())
	})

	t.Run("wrong-subject grant is rejected", func(t *testing.T) {
		tPub, _ := kp(t)
		c := NewCredentials(rootPub, sPriv, current)
		require.Error(t, c.AdoptRenewedGrant(issue(t, tPub, now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL)), now, nil))
		require.Same(t, current, c.Grant())
	})

	t.Run("expired grant is rejected", func(t *testing.T) {
		c := NewCredentials(rootPub, sPriv, current)
		require.Error(t, c.AdoptRenewedGrant(issue(t, sPub, now.Add(-2*time.Hour), now.Add(-time.Hour)), now, nil))
		require.Same(t, current, c.Grant())
	})

	t.Run("denied subject is rejected", func(t *testing.T) {
		c := NewCredentials(rootPub, sPriv, current)
		denied := func(_ []byte) bool { return true }
		require.Error(t, c.AdoptRenewedGrant(issue(t, sPub, now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL)), now, denied))
		require.Same(t, current, c.Grant())
	})
}
