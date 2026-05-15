// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/stretchr/testify/require"
)

// durableCredCase exercises the credential rule shared by VerifySpecAuth
// and VerifyBlobWrapping: a publisher/wrapper cert past not_after but
// inside access_deadline must still admit (the staging US-node
// scenario), while past-access_deadline, denied, not-yet-valid, and
// wrong-root certs fail closed.
type durableCredCase struct {
	name           string
	notBefore      time.Time
	notAfter       time.Time
	accessDeadline time.Time
	wrongRoot      bool
	denied         auth.DenyChecker
	wantOK         bool
}

func durableCredCases(now time.Time) []durableCredCase {
	const day = 24 * time.Hour
	return []durableCredCase{
		{
			name:           "within working window",
			notBefore:      now.Add(-time.Hour),
			notAfter:       now.Add(time.Hour),
			accessDeadline: now.Add(30 * day),
			wantOK:         true,
		},
		{
			name:           "past not_after within access_deadline still admits",
			notBefore:      now.Add(-2 * time.Hour),
			notAfter:       now.Add(-time.Hour),
			accessDeadline: now.Add(29 * day),
			wantOK:         true,
		},
		{
			name:           "past access_deadline fails closed",
			notBefore:      now.Add(-30 * day),
			notAfter:       now.Add(-29 * day),
			accessDeadline: now.Add(-time.Hour),
			wantOK:         false,
		},
		{
			name:           "legacy no access_deadline past not_after fails closed",
			notBefore:      now.Add(-5 * time.Hour),
			notAfter:       now.Add(-time.Hour),
			accessDeadline: time.Time{},
			wantOK:         false,
		},
		{
			name:           "legacy no access_deadline before not_after admits",
			notBefore:      now.Add(-time.Hour),
			notAfter:       now.Add(time.Hour),
			accessDeadline: time.Time{},
			wantOK:         true,
		},
		{
			name:           "not yet valid fails closed",
			notBefore:      now.Add(time.Hour),
			notAfter:       now.Add(2 * time.Hour),
			accessDeadline: now.Add(30 * day),
			wantOK:         false,
		},
		{
			name:           "wrong root fails closed",
			notBefore:      now.Add(-time.Hour),
			notAfter:       now.Add(time.Hour),
			accessDeadline: now.Add(30 * day),
			wrongRoot:      true,
			wantOK:         false,
		},
		{
			name:           "denied publisher fails closed",
			notBefore:      now.Add(-time.Hour),
			notAfter:       now.Add(time.Hour),
			accessDeadline: now.Add(30 * day),
			denied:         func([]byte) bool { return true },
			wantOK:         false,
		},
	}
}

func TestVerifySpecAuthDurableCredential(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	otherPub, _ := newKeyPair(t)
	now := time.Now()

	body := &statev1.WorkloadSpecChange{Hash: strings.Repeat("a", 64), Name: "echo", MinReplicas: 1}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{
		Name: body.GetName(),
		Hash: bytes.Repeat([]byte{0xaa}, 32),
	}}}

	for _, tc := range durableCredCases(now) {
		t.Run(tc.name, func(t *testing.T) {
			publisher, err := auth.IssueDelegationCert(
				rootPriv, nil, rootPub, auth.FullCapabilities(),
				tc.notBefore, tc.notAfter, tc.accessDeadline,
			)
			require.NoError(t, err)
			specAuth, err := auth.IssueSpecAuth(rootPriv, publisher, resource, body, nil, false)
			require.NoError(t, err)

			verifyRoot := rootPub
			if tc.wrongRoot {
				verifyRoot = otherPub
			}
			err = auth.VerifySpecAuth(specAuth, body, verifyRoot, now, tc.denied)
			if tc.wantOK {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorContains(t, err, "publisher cert invalid")
		})
	}
}

// A valid cert must not let a tampered SpecAuth signature through: the
// credential-rule change must not short-circuit the signature check.
func TestVerifySpecAuthValidCertStillChecksSignature(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	now := time.Now()
	publisher, err := auth.IssueDelegationCert(
		rootPriv, nil, rootPub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour), now.Add(30*24*time.Hour),
	)
	require.NoError(t, err)
	body := &statev1.WorkloadSpecChange{Hash: strings.Repeat("a", 64), Name: "echo", MinReplicas: 1}
	resource := &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Seed{Seed: &admissionv1.SeedID{
		Name: body.GetName(),
		Hash: bytes.Repeat([]byte{0xaa}, 32),
	}}}
	specAuth, err := auth.IssueSpecAuth(rootPriv, publisher, resource, body, nil, false)
	require.NoError(t, err)
	specAuth.Signature = invalidateSignature(specAuth.GetSignature())

	require.ErrorContains(t, auth.VerifySpecAuth(specAuth, body, rootPub, now, nil), "signature invalid")
}

// Same guarantee for blob wrappings.
func TestVerifyBlobWrappingValidCertStillChecksSignature(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	recipientPub, _ := newKeyPair(t)
	now := time.Now()
	wrapper, err := auth.IssueDelegationCert(
		rootPriv, nil, rootPub, auth.FullCapabilities(),
		now.Add(-time.Minute), now.Add(24*time.Hour), now.Add(30*24*time.Hour),
	)
	require.NoError(t, err)
	wrapping, err := auth.IssueBlobWrapping(rootPriv, wrapper, bytes.Repeat([]byte{0xcc}, 32), recipientPub, bytes.Repeat([]byte{0xdd}, 48))
	require.NoError(t, err)
	wrapping.Signature = invalidateSignature(wrapping.GetSignature())

	require.ErrorContains(t, auth.VerifyBlobWrapping(wrapping, rootPub, now, nil), "signature invalid")
}

func TestVerifyBlobWrappingDurableCredential(t *testing.T) {
	rootPub, rootPriv := newKeyPair(t)
	otherPub, _ := newKeyPair(t)
	recipientPub, _ := newKeyPair(t)
	now := time.Now()

	blobHash := bytes.Repeat([]byte{0xcc}, 32)
	wrappedDEK := bytes.Repeat([]byte{0xdd}, 48)

	for _, tc := range durableCredCases(now) {
		t.Run(tc.name, func(t *testing.T) {
			wrapper, err := auth.IssueDelegationCert(
				rootPriv, nil, rootPub, auth.FullCapabilities(),
				tc.notBefore, tc.notAfter, tc.accessDeadline,
			)
			require.NoError(t, err)
			wrapping, err := auth.IssueBlobWrapping(rootPriv, wrapper, blobHash, recipientPub, wrappedDEK)
			require.NoError(t, err)

			verifyRoot := rootPub
			if tc.wrongRoot {
				verifyRoot = otherPub
			}
			err = auth.VerifyBlobWrapping(wrapping, verifyRoot, now, tc.denied)
			if tc.wantOK {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorContains(t, err, "wrapper cert invalid")
		})
	}
}
