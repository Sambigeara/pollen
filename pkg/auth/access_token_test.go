// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/stretchr/testify/require"
)

func staticResource(t *testing.T) *admissionv1.ResourceID {
	t.Helper()
	return &admissionv1.ResourceID{Body: &admissionv1.ResourceID_Static{Static: &admissionv1.StaticID{
		Name:           "site",
		ManifestDigest: make([]byte, 32),
	}}}
}

func TestAccessTokenRoundTrip(t *testing.T) {
	_, issuerPriv := newKeyPair(t)
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)

	token, err := auth.SignAccessToken(issuerPriv, staticResource(t), now, time.Hour)
	require.NoError(t, err)

	encoded, err := auth.EncodeAccessToken(token)
	require.NoError(t, err)
	decoded, err := auth.DecodeAccessToken(encoded)
	require.NoError(t, err)

	require.NoError(t, auth.VerifyAccessToken(decoded, now.Add(30*time.Minute)))
}

func TestAccessTokenRejectsExpired(t *testing.T) {
	_, issuerPriv := newKeyPair(t)
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)

	token, err := auth.SignAccessToken(issuerPriv, staticResource(t), now, time.Hour)
	require.NoError(t, err)

	err = auth.VerifyAccessToken(token, now.Add(2*time.Hour))
	require.ErrorContains(t, err, "expired")
}

func TestAccessTokenRejectsTamperedSignature(t *testing.T) {
	_, issuerPriv := newKeyPair(t)
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)

	token, err := auth.SignAccessToken(issuerPriv, staticResource(t), now, time.Hour)
	require.NoError(t, err)
	token.Signature = invalidateSignature(token.GetSignature())

	err = auth.VerifyAccessToken(token, now)
	require.ErrorContains(t, err, "signature invalid")
}

func TestAccessTokenIssuerEmbedded(t *testing.T) {
	issuerPub, issuerPriv := newKeyPair(t)
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)

	token, err := auth.SignAccessToken(issuerPriv, staticResource(t), now, time.Hour)
	require.NoError(t, err)

	require.Equal(t, []byte(issuerPub), token.GetClaims().GetIssuerPub())
}

func TestAccessTokenRejectsNegativeTTL(t *testing.T) {
	_, issuerPriv := newKeyPair(t)
	_, err := auth.SignAccessToken(issuerPriv, staticResource(t), time.Now(), 0)
	require.ErrorContains(t, err, "ttl must be positive")
}
