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

type ClusterAuth struct {
	t         testing.TB
	rootGrant *identityv1.Grant
	rootPub   ed25519.PublicKey
	rootKey   ed25519.PrivateKey
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
	)
	require.NoError(t, err)
	return &ClusterAuth{rootPub: pub, rootKey: priv, rootGrant: rootGrant, t: t}
}

func (ca *ClusterAuth) RootPub() ed25519.PublicKey  { return ca.rootPub }
func (ca *ClusterAuth) RootKey() ed25519.PrivateKey { return ca.rootKey }

// NodeCredentials issues a member grant beneath the root grant, so the
// node's chain carries root as its delegation ancestor exactly as a
// production join does. Issuing with a nil parent instead would yield an
// empty chain, leaving the root unable to authorise verbs such as
// UpgradePeer/DenyPeer against the node.
func (ca *ClusterAuth) NodeCredentials(nodePriv ed25519.PrivateKey) *identity.Credentials {
	nodePub := nodePriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	now := time.Now()
	grant, err := identity.IssueGrant(
		ca.rootKey,
		ca.rootGrant,
		nodePub,
		identity.FullCapabilities(),
		identity.UnlimitedBudget(),
		now.Add(-time.Minute),
		now.Add(24*time.Hour), //nolint:mnd
	)
	require.NoError(ca.t, err)

	return identity.NewCredentials(ca.rootPub, nodePriv, grant)
}

// RootCredentials returns the self-signed root grant. The root node's
// signing identity is the cluster root key, mirroring how production
// root nodes are bootstrapped via EnsureLocalRootGrant. A node "is
// root" when its signing key equals the cluster root key, so that
// admin-scoped operations such as DenyPeer are authorised: only an
// ancestor in a peer's chain, or the root itself, may revoke that peer.
func (ca *ClusterAuth) RootCredentials() *identity.Credentials {
	return identity.NewCredentials(ca.rootPub, ca.rootKey, ca.rootGrant)
}
