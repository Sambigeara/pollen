// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cluster

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/stretchr/testify/require"
)

type nodeIdent struct {
	priv  ed25519.PrivateKey
	grant *identityv1.Grant
}

type ClusterAuth struct {
	t         testing.TB
	rootGrant *identityv1.Grant
	rootPub   ed25519.PublicKey
	rootKey   ed25519.PrivateKey
	idents    map[string]nodeIdent
}

func NewClusterAuth(t testing.TB) *ClusterAuth { //nolint:thelper
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pub := priv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	now := time.Now()
	rootGrant, err := identity.IssueGrant(
		priv,
		nil,
		pub, // self-signed: subject == issuer == rootPub
		identity.RootCapabilities(),
		identity.UnlimitedBudget(),
		now.Add(-time.Minute),
		time.Time{}, // root carries no horizon
		false,
	)
	require.NoError(t, err)
	return &ClusterAuth{rootPub: pub, rootKey: priv, rootGrant: rootGrant, t: t, idents: map[string]nodeIdent{}}
}

func (ca *ClusterAuth) RootPub() ed25519.PublicKey  { return ca.rootPub }
func (ca *ClusterAuth) RootKey() ed25519.PrivateKey { return ca.rootKey }

// MemberCredentials issues a grant for a node beneath parent (a node name,
// or empty for the cluster root) carrying caps (FullCapabilities when nil),
// and records the node's identity so its own members can be issued beneath
// it in turn. This mirrors production, where a workspace-admin invites a
// member into its own subtree rather than every node being a root-direct
// full admin. parent "" issues under the root grant, not a nil parent,
// which would yield an empty, unrooted chain.
func (ca *ClusterAuth) MemberCredentials(name string, nodePriv ed25519.PrivateKey, parent string, caps *identityv1.Capabilities) *identity.Credentials {
	nodePub := nodePriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	if caps == nil {
		caps = identity.FullCapabilities()
	}

	signKey, parentGrant := ca.rootKey, ca.rootGrant
	if parent != "" {
		p, ok := ca.idents[parent]
		require.Truef(ca.t, ok, "parent %q must be added before its members", parent)
		signKey, parentGrant = p.priv, p.grant
	}

	now := time.Now()
	grant, err := identity.IssueGrant(
		signKey,
		parentGrant,
		nodePub,
		caps,
		identity.UnlimitedBudget(),
		now.Add(-time.Minute),
		now.Add(24*time.Hour), //nolint:mnd
		false,
	)
	require.NoError(ca.t, err)

	ca.idents[name] = nodeIdent{priv: nodePriv, grant: grant}
	return identity.NewCredentials(ca.rootPub, nodePriv, grant)
}

// RootCredentials returns the self-signed root grant and records the root
// under name so members can be issued beneath it. The root node's signing
// identity is the cluster root key, mirroring how production root nodes are
// bootstrapped via EnsureLocalRootGrant. A node "is root" when its signing
// key equals the cluster root key, so that admin-scoped operations such as
// DenyPeer are authorised: only an ancestor in a peer's chain, or the root
// itself, may revoke that peer.
func (ca *ClusterAuth) RootCredentials(name string) *identity.Credentials {
	ca.idents[name] = nodeIdent{priv: ca.rootKey, grant: ca.rootGrant}
	return identity.NewCredentials(ca.rootPub, ca.rootKey, ca.rootGrant)
}
