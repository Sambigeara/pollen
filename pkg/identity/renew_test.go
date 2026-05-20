// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"crypto/ed25519"
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

// TestAdoptGrant proves the delivered grant is never trusted blindly:
// only a grant that chains to our root, names our signing key as its
// subject, is within its horizon and is not denied is swapped in. The
// same gate covers first-time enrol (no prior grant), renewal (same
// caps, fresh horizon) and admin-initiated upgrade (different caps for
// the same subject).
func TestAdoptGrant(t *testing.T) {
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

	t.Run("first-time enrol with no prior grant is adopted", func(t *testing.T) {
		c := NewCredentials(rootPub, sPriv, nil)
		fresh := issue(t, sPub, now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL))
		require.NoError(t, c.AdoptGrant(fresh, now, nil))
		require.Same(t, fresh, c.Grant())
	})

	t.Run("valid same-subject grant is adopted", func(t *testing.T) {
		c := NewCredentials(rootPub, sPriv, current)
		fresh := issue(t, sPub, now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL))
		require.NoError(t, c.AdoptGrant(fresh, now, nil))
		require.Same(t, fresh, c.Grant())
	})

	t.Run("wrong-subject grant is rejected", func(t *testing.T) {
		tPub, _ := kp(t)
		c := NewCredentials(rootPub, sPriv, current)
		require.Error(t, c.AdoptGrant(issue(t, tPub, now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL)), now, nil))
		require.Same(t, current, c.Grant())
	})

	t.Run("expired grant is rejected", func(t *testing.T) {
		c := NewCredentials(rootPub, sPriv, current)
		require.Error(t, c.AdoptGrant(issue(t, sPub, now.Add(-2*time.Hour), now.Add(-time.Hour)), now, nil))
		require.Same(t, current, c.Grant())
	})

	t.Run("denied subject is rejected", func(t *testing.T) {
		c := NewCredentials(rootPub, sPriv, current)
		denied := func(_ []byte) bool { return true }
		require.Error(t, c.AdoptGrant(issue(t, sPub, now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL)), now, denied))
		require.Same(t, current, c.Grant())
	})

	t.Run("upgrade adopts a grant whose caps grow", func(t *testing.T) {
		leafGrant, err := IssueGrant(rootPriv, nil, sPub, LeafCapabilities(), UnlimitedBudget(),
			now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL))
		require.NoError(t, err)
		c := NewCredentials(rootPub, sPriv, leafGrant)

		fullGrant, err := IssueGrant(rootPriv, nil, sPub, FullCapabilities(), UnlimitedBudget(),
			now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL))
		require.NoError(t, err)
		require.NoError(t, c.AdoptGrant(fullGrant, now, nil))
		require.True(t, c.Grant().GetClaims().GetCapabilities().GetCanAdmit(),
			"upgraded grant must carry the new CanAdmit bit")
	})

	t.Run("downgrade adopts a grant whose caps shrink", func(t *testing.T) {
		fullGrant, err := IssueGrant(rootPriv, nil, sPub, FullCapabilities(), UnlimitedBudget(),
			now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL))
		require.NoError(t, err)
		c := NewCredentials(rootPub, sPriv, fullGrant)

		leafGrant, err := IssueGrant(rootPriv, nil, sPub, LeafCapabilities(), UnlimitedBudget(),
			now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL))
		require.NoError(t, err)
		require.NoError(t, c.AdoptGrant(leafGrant, now, nil))
		require.False(t, c.Grant().GetClaims().GetCapabilities().GetCanAdmit(),
			"downgraded grant must drop the old CanAdmit bit")
	})

	t.Run("cached session is cleared so the next mint binds the new grant", func(t *testing.T) {
		c := NewCredentials(rootPub, sPriv, current)
		ttl := time.Hour
		stale, err := c.EnsureFreshSession(now, ttl, ttl/2)
		require.NoError(t, err)
		require.NotNil(t, stale)
		fresh := issue(t, sPub, now.Add(-time.Hour), now.Add(2*DefaultGrantDeadlineTTL))
		require.NoError(t, c.AdoptGrant(fresh, now, nil))
		next, err := c.EnsureFreshSession(now, ttl, ttl/2)
		require.NoError(t, err)
		require.NotSame(t, stale, next, "AdoptGrant must clear the cached session so the next mint binds the new grant")
	})
}

// TestAdoptGrantPersists proves the credentials handle returned by
// LoadCredentials rewrites its on-disk record as part of every adopt,
// so the durable grant never lags in-memory state. The companion case
// covers in-memory-only handles built via NewCredentials: those skip
// persistence entirely, which is what test fixtures rely on.
func TestAdoptGrantPersists(t *testing.T) {
	_, rootPriv := kp(t)
	rootPub := rootPriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	now := time.Now()

	t.Run("loaded handle rewrites on-disk grant on adopt", func(t *testing.T) {
		dir := t.TempDir()
		sPriv, sPub, err := EnsureIdentityKey(dir)
		require.NoError(t, err)
		seed, err := IssueGrant(rootPriv, nil, sPub, FullCapabilities(), UnlimitedBudget(), now.Add(-time.Hour), now.Add(7*24*time.Hour))
		require.NoError(t, err)
		require.NoError(t, SaveCredentials(dir, &Credentials{rootPub: rootPub, signPriv: sPriv, grant: seed}))

		loaded, err := LoadCredentials(dir)
		require.NoError(t, err)

		fresh, err := IssueGrant(rootPriv, nil, sPub, FullCapabilities(), UnlimitedBudget(), now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL))
		require.NoError(t, err)
		require.NoError(t, loaded.AdoptGrant(fresh, now, nil))

		reloaded, err := LoadCredentials(dir)
		require.NoError(t, err)
		require.Equal(t, fresh.GetClaims().GetGrantDeadlineUnix(), reloaded.Grant().GetClaims().GetGrantDeadlineUnix(),
			"persisted grant deadline must match the adopted grant's")
	})

	t.Run("in-memory handle does not touch disk", func(t *testing.T) {
		_, sPriv := kp(t)
		sPub := sPriv.Public().(ed25519.PublicKey)
		seed, err := IssueGrant(rootPriv, nil, sPub, FullCapabilities(), UnlimitedBudget(), now.Add(-time.Hour), now.Add(7*24*time.Hour))
		require.NoError(t, err)
		fresh, err := IssueGrant(rootPriv, nil, sPub, FullCapabilities(), UnlimitedBudget(), now.Add(-time.Hour), now.Add(DefaultGrantDeadlineTTL))
		require.NoError(t, err)

		c := NewCredentials(rootPub, sPriv, seed)
		require.NoError(t, c.AdoptGrant(fresh, now, nil))
		require.Same(t, fresh, c.Grant())
	})
}
