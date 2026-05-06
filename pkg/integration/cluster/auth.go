// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cluster

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/stretchr/testify/require"
)

type ClusterAuth struct {
	t       testing.TB
	rootPub ed25519.PublicKey
	rootKey ed25519.PrivateKey
}

func NewClusterAuth(t testing.TB) *ClusterAuth { //nolint:thelper
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pub := priv.Public().(ed25519.PublicKey) //nolint:forcetypeassert
	return &ClusterAuth{rootPub: pub, rootKey: priv, t: t}
}

func (ca *ClusterAuth) RootPub() ed25519.PublicKey { return ca.rootPub }
func (ca *ClusterAuth) RootKey() ed25519.PrivateKey { return ca.rootKey }

func (ca *ClusterAuth) NodeCredentials(nodePriv ed25519.PrivateKey) (tls.Certificate, *admissionv1.DelegationCert) {
	t := ca.t
	nodePub := nodePriv.Public().(ed25519.PublicKey) //nolint:forcetypeassert

	now := time.Now()
	dc, err := auth.IssueDelegationCert(
		ca.rootKey,
		nil, // root issues directly
		nodePub,
		auth.FullCapabilities(),
		now.Add(-time.Minute),
		now.Add(24*time.Hour), //nolint:mnd
		time.Time{},           // no access deadline
	)
	require.NoError(t, err)

	tlsCert, err := transport.GenerateIdentityCert(nodePriv, dc, 24*time.Hour) //nolint:mnd
	require.NoError(t, err)

	return tlsCert, dc
}

// RootNodeCredentials issues the self-signed root delegation cert and
// the matching TLS identity. Used to spin up a test node that holds
// the cluster's root key. A node "is root" when its signing key equals
// the cluster root key, mirroring how production root nodes are
// bootstrapped via EnsureLocalRootCredentials.
func (ca *ClusterAuth) RootNodeCredentials() (tls.Certificate, *admissionv1.DelegationCert) {
	t := ca.t
	now := time.Now()
	dc, err := auth.IssueDelegationCert(
		ca.rootKey,
		nil,
		ca.rootPub, // self-signed: subject == issuer == rootPub
		auth.FullCapabilities(),
		now.Add(-time.Minute),
		now.Add(24*time.Hour), //nolint:mnd
		time.Time{},
	)
	require.NoError(t, err)

	tlsCert, err := transport.GenerateIdentityCert(ca.rootKey, dc, 24*time.Hour) //nolint:mnd
	require.NoError(t, err)

	return tlsCert, dc
}
