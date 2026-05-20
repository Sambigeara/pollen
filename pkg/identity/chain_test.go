// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package identity_test

import (
	"testing"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// mkGrant builds a Grant with the given subject and chain entries
// (leaf-to-root) plus an optional workspace-admin bit on the grant
// itself. The helpers walk Claims/Chain fields only, so unsigned
// hand-built grants are fine here.
func mkGrant(subject []byte, ws bool, chain []*identityv1.Grant) *identityv1.Grant {
	return &identityv1.Grant{
		Claims: &identityv1.GrantClaims{
			SubjectPub:   subject,
			Capabilities: &identityv1.Capabilities{IsWorkspaceAdmin: ws},
		},
		Chain: chain,
	}
}

func TestAncestorIn(t *testing.T) {
	rootPub := []byte{0x01}
	midPub := []byte{0x02}
	leafPub := []byte{0x03}
	otherPub := []byte{0x99}

	leaf := mkGrant(leafPub, false, []*identityv1.Grant{
		mkGrant(midPub, false, nil),
		mkGrant(rootPub, false, nil),
	})

	t.Run("direct parent is an ancestor", func(t *testing.T) {
		require.True(t, identity.AncestorIn(types.PeerKeyFromBytes(midPub), leaf))
	})

	t.Run("grandparent is an ancestor", func(t *testing.T) {
		require.True(t, identity.AncestorIn(types.PeerKeyFromBytes(rootPub), leaf))
	})

	t.Run("unrelated key is not an ancestor", func(t *testing.T) {
		require.False(t, identity.AncestorIn(types.PeerKeyFromBytes(otherPub), leaf))
	})

	t.Run("grant is not its own ancestor", func(t *testing.T) {
		require.False(t, identity.AncestorIn(types.PeerKeyFromBytes(leafPub), leaf))
	})

	t.Run("empty chain has no ancestors", func(t *testing.T) {
		bare := mkGrant(leafPub, false, nil)
		require.False(t, identity.AncestorIn(types.PeerKeyFromBytes(rootPub), bare))
	})
}

func TestWorkspaceOf(t *testing.T) {
	rootPub := []byte{0x01}
	wsPub := []byte{0x02}
	leafPub := []byte{0x03}
	zero := types.PeerKey{}

	t.Run("no workspace-admin anywhere returns zero", func(t *testing.T) {
		g := mkGrant(leafPub, false, []*identityv1.Grant{mkGrant(rootPub, false, nil)})
		require.Equal(t, zero, identity.WorkspaceOf(g))
	})

	t.Run("grant itself is a workspace-admin", func(t *testing.T) {
		g := mkGrant(wsPub, true, []*identityv1.Grant{mkGrant(rootPub, false, nil)})
		require.Equal(t, types.PeerKeyFromBytes(wsPub), identity.WorkspaceOf(g))
	})

	t.Run("workspace-admin in chain", func(t *testing.T) {
		g := mkGrant(leafPub, false, []*identityv1.Grant{
			mkGrant(wsPub, true, nil),
			mkGrant(rootPub, false, nil),
		})
		require.Equal(t, types.PeerKeyFromBytes(wsPub), identity.WorkspaceOf(g))
	})

	t.Run("nearest workspace-admin wins over a further one", func(t *testing.T) {
		nearWS := []byte{0x10}
		farWS := []byte{0x11}
		g := mkGrant(leafPub, false, []*identityv1.Grant{
			mkGrant(nearWS, true, nil),
			mkGrant(farWS, true, nil),
			mkGrant(rootPub, false, nil),
		})
		require.Equal(t, types.PeerKeyFromBytes(nearWS), identity.WorkspaceOf(g))
	})

	t.Run("two grants under the same workspace-admin share a workspace", func(t *testing.T) {
		chain := []*identityv1.Grant{
			mkGrant(wsPub, true, nil),
			mkGrant(rootPub, false, nil),
		}
		alice := mkGrant([]byte{0xa1}, false, chain)
		bob := mkGrant([]byte{0xb0}, false, chain)
		require.Equal(t, identity.WorkspaceOf(alice), identity.WorkspaceOf(bob))
	})

	t.Run("two grants under different workspace-admins do not share", func(t *testing.T) {
		wsA, wsB := []byte{0xaa}, []byte{0xbb}
		alice := mkGrant([]byte{0xa1}, false, []*identityv1.Grant{
			mkGrant(wsA, true, nil),
			mkGrant(rootPub, false, nil),
		})
		bob := mkGrant([]byte{0xb0}, false, []*identityv1.Grant{
			mkGrant(wsB, true, nil),
			mkGrant(rootPub, false, nil),
		})
		require.NotEqual(t, identity.WorkspaceOf(alice), identity.WorkspaceOf(bob))
	})
}
