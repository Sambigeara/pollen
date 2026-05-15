// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package fact_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/fact"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/stretchr/testify/require"
)

func newKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return pub, priv
}

// authorityGrant builds a root-signed grant for the authority subject,
// returning the cluster root pub to verify against.
func authorityGrant(t *testing.T, now, deadline time.Time) (rootPub, authorityPub ed25519.PublicKey, authorityPriv ed25519.PrivateKey, grant *identityv1.Grant) {
	t.Helper()
	adminPub, adminPriv := newKeyPair(t)
	authorityPub, authorityPriv = newKeyPair(t)
	grant, err := identity.IssueGrant(adminPriv, nil, authorityPub,
		identity.PublisherCapabilities(), &identityv1.Budget{MaxSites: 2},
		now.Add(-time.Hour), deadline)
	require.NoError(t, err)
	return adminPub, authorityPub, authorityPriv, grant
}

func seedResource() (*admissionv1.ResourceID, fact.Body) {
	body := &statev1.WorkloadSpecChange{Hash: strings.Repeat("a", 64), Name: "echo", MinReplicas: 1}
	res := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{
		Name: body.GetName(),
		Hash: bytes.Repeat([]byte{0xaa}, 32),
	}}}
	return res, body
}

func TestVerifyFact(t *testing.T) {
	now := time.Now()
	res, body := seedResource()

	t.Run("valid authority grant", func(t *testing.T) {
		rootPub, _, aPriv, grant := authorityGrant(t, now, now.Add(30*24*time.Hour))
		f, err := fact.IssueFact(aPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		require.NoError(t, fact.VerifyFact(f, body, grant, rootPub, now, nil))
	})

	t.Run("survives offline authority past working window", func(t *testing.T) {
		// The grant horizon, not a short session window, bounds a Fact:
		// a non-holder still admits it long after the publisher went away.
		rootPub, _, aPriv, grant := authorityGrant(t, now, now.Add(30*24*time.Hour))
		f, err := fact.IssueFact(aPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		require.NoError(t, fact.VerifyFact(f, body, grant, rootPub, now.Add(20*24*time.Hour), nil))
	})

	t.Run("rejected past grant deadline", func(t *testing.T) {
		rootPub, _, aPriv, grant := authorityGrant(t, now, now.Add(time.Hour))
		f, err := fact.IssueFact(aPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		err = fact.VerifyFact(f, body, grant, rootPub, now.Add(2*time.Hour), nil)
		require.ErrorIs(t, err, fact.ErrFactInvalid)
		require.ErrorContains(t, err, "expired")
	})

	t.Run("rejected when authority denied", func(t *testing.T) {
		rootPub, aPub, aPriv, grant := authorityGrant(t, now, now.Add(30*24*time.Hour))
		f, err := fact.IssueFact(aPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		denied := func(p []byte) bool { return string(p) == string(aPub) }
		err = fact.VerifyFact(f, body, grant, rootPub, now, denied)
		require.ErrorContains(t, err, "revoked")
	})

	t.Run("rejected on body hash mismatch", func(t *testing.T) {
		rootPub, _, aPriv, grant := authorityGrant(t, now, now.Add(30*24*time.Hour))
		f, err := fact.IssueFact(aPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		other := &statev1.WorkloadSpecChange{Hash: strings.Repeat("b", 64), Name: "echo", MinReplicas: 1}
		err = fact.VerifyFact(f, other, grant, rootPub, now, nil)
		require.ErrorContains(t, err, "body hash mismatch")
	})

	t.Run("rejected when grant subject is not the fact authority", func(t *testing.T) {
		// Same root, different subject: the supplied grant is valid but
		// is not the grant of the fact's named authority.
		adminPub, adminPriv := newKeyPair(t)
		aPub, aPriv := newKeyPair(t)
		bPub, _ := newKeyPair(t)
		_ = aPub
		bGrant, err := identity.IssueGrant(adminPriv, nil, bPub,
			identity.PublisherCapabilities(), &identityv1.Budget{}, now.Add(-time.Hour), now.Add(30*24*time.Hour))
		require.NoError(t, err)
		f, err := fact.IssueFact(aPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		err = fact.VerifyFact(f, body, bGrant, adminPub, now, nil)
		require.ErrorContains(t, err, "subject does not match expected")
	})

	t.Run("rejected on tampered signature", func(t *testing.T) {
		rootPub, _, aPriv, grant := authorityGrant(t, now, now.Add(30*24*time.Hour))
		f, err := fact.IssueFact(aPriv, res, body, nil, 1, false)
		require.NoError(t, err)
		f.Signature[0] ^= 0xff
		err = fact.VerifyFact(f, body, grant, rootPub, now, nil)
		require.ErrorContains(t, err, "signature invalid")
	})
}

func TestVerifyBlobWrapping(t *testing.T) {
	now := time.Now()
	blobHash := bytes.Repeat([]byte{0xcc}, 32)
	recipient, _ := newKeyPair(t)
	dek := bytes.Repeat([]byte{0xdd}, 48)

	t.Run("valid", func(t *testing.T) {
		rootPub, _, aPriv, grant := authorityGrant(t, now, now.Add(30*24*time.Hour))
		w, err := fact.IssueBlobWrapping(aPriv, blobHash, recipient, dek, 1)
		require.NoError(t, err)
		require.NoError(t, fact.VerifyBlobWrapping(w, grant, rootPub, now, nil))
	})

	t.Run("denied authority fails closed", func(t *testing.T) {
		rootPub, aPub, aPriv, grant := authorityGrant(t, now, now.Add(30*24*time.Hour))
		w, err := fact.IssueBlobWrapping(aPriv, blobHash, recipient, dek, 1)
		require.NoError(t, err)
		denied := func(p []byte) bool { return string(p) == string(aPub) }
		require.ErrorContains(t, fact.VerifyBlobWrapping(w, grant, rootPub, now, denied), "revoked")
	})

	t.Run("tampered signature fails closed", func(t *testing.T) {
		rootPub, _, aPriv, grant := authorityGrant(t, now, now.Add(30*24*time.Hour))
		w, err := fact.IssueBlobWrapping(aPriv, blobHash, recipient, dek, 1)
		require.NoError(t, err)
		w.Signature[0] ^= 0xff
		require.ErrorContains(t, fact.VerifyBlobWrapping(w, grant, rootPub, now, nil), "signature invalid")
	})
}
