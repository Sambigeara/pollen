// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
)

func TestRPCCallerContextRoundTrip(t *testing.T) {
	cert := mustMintCert(t, auth.FullCapabilities())
	caller := auth.NewRPCCaller(cert)

	ctx := auth.WithRPCCaller(context.Background(), caller)
	got, ok := auth.RPCCallerFromContext(ctx)
	require.True(t, ok)
	require.Same(t, cert, got.Cert(), "round-tripped caller must wrap the same cert pointer")
}

func TestRPCCallerFromEmptyContextMissing(t *testing.T) {
	_, ok := auth.RPCCallerFromContext(context.Background())
	require.False(t, ok)
}

func TestRPCCallerCapabilityHelpers(t *testing.T) {
	cases := []struct {
		name        string
		caps        *admissionv1.Capabilities
		canAdmit    bool
		canPublish  bool
		canDelegate bool
	}{
		{name: "admin", caps: auth.FullCapabilities(), canAdmit: true, canPublish: true, canDelegate: true},
		{name: "publisher", caps: auth.PublisherCapabilities(), canPublish: true},
		{name: "leaf", caps: auth.LeafCapabilities()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cert := mustMintCert(t, tc.caps)
			caller := auth.NewRPCCaller(cert)
			require.Equal(t, tc.canAdmit, caller.CanAdmit())
			require.Equal(t, tc.canPublish, caller.CanPublish())
			require.Equal(t, tc.canDelegate, caller.CanDelegate())
			require.Equal(t, cert.GetClaims().GetSubjectPub(), caller.SubjectPub().Bytes())
		})
	}
}

func mustMintCert(t *testing.T, caps *admissionv1.Capabilities) *admissionv1.DelegationCert {
	t.Helper()
	dir := t.TempDir()
	nodePub, nodePriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Now()

	if _, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, now, 24*time.Hour); err != nil {
		t.Fatalf("ensure root creds: %v", err)
	}
	signer, err := auth.NewDelegationSigner(dir, nodePriv)
	require.NoError(t, err)

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	cert, err := signer.IssueMemberCert(subjectPub, caps, now, now.Add(time.Hour), time.Time{})
	require.NoError(t, err)
	return cert
}
