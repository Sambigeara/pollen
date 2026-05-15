// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package membership

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRenewCertPreservesCeilingRefreshesNotAfter(t *testing.T) {
	creds, _, _, _ := newRootCredentialsWithAttrs(t, nil)
	svc := newIssuerService(t, creds, newFakeCertManager())

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ceiling := time.Now().Add(10 * 24 * time.Hour)
	// not_after already in the past (NeedsRenewal), ceiling still ahead.
	current, err := creds.DelegationKey().IssueMemberCert(
		subjectPub, auth.PublisherCapabilities(),
		time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour), ceiling,
	)
	require.NoError(t, err)

	renewed, err := svc.RenewCert(current)
	require.NoError(t, err)

	require.Equal(t,
		current.GetClaims().GetAccessDeadlineUnix(),
		renewed.GetClaims().GetAccessDeadlineUnix(),
		"access_deadline ceiling must be preserved verbatim, never extended")
	require.Greater(t,
		renewed.GetClaims().GetNotAfterUnix(),
		time.Now().Unix(),
		"not_after must be refreshed into the future")
	require.True(t, renewed.GetClaims().GetCapabilities().GetCanPublish(),
		"capabilities must be preserved")
}

func TestRenewCertClampsNotAfterToCeiling(t *testing.T) {
	creds, _, _, _ := newRootCredentialsWithAttrs(t, nil)
	svc := newIssuerService(t, creds, newFakeCertManager())

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	// Ceiling is sooner than a full membership TTL away, so the
	// refreshed not_after must be clamped down to the ceiling.
	ceiling := time.Now().Add(20 * time.Minute)
	current, err := creds.DelegationKey().IssueMemberCert(
		subjectPub, auth.LeafCapabilities(),
		time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour), ceiling,
	)
	require.NoError(t, err)

	renewed, err := svc.RenewCert(current)
	require.NoError(t, err)
	require.LessOrEqual(t,
		renewed.GetClaims().GetNotAfterUnix(),
		renewed.GetClaims().GetAccessDeadlineUnix(),
		"not_after must never exceed the access_deadline ceiling")
}

func TestRenewCertRejectsPastAccessDeadline(t *testing.T) {
	creds, _, _, _ := newRootCredentialsWithAttrs(t, nil)
	svc := newIssuerService(t, creds, newFakeCertManager())

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	current, err := creds.DelegationKey().IssueMemberCert(
		subjectPub, auth.LeafCapabilities(),
		time.Now().Add(-2*time.Hour), time.Now().Add(-90*time.Minute),
		time.Now().Add(-time.Minute),
	)
	require.NoError(t, err)

	_, err = svc.RenewCert(current)
	require.ErrorIs(t, err, ErrAccessDeadlinePassed)
}

func TestRenewCertRejectsNonAdmin(t *testing.T) {
	creds, _, _, _ := newRootCredentialsWithAttrs(t, nil)
	svc := newIssuerService(t, creds, newFakeCertManager())

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	current, err := creds.DelegationKey().IssueMemberCert(
		subjectPub, auth.LeafCapabilities(),
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour),
		time.Now().Add(30*24*time.Hour),
	)
	require.NoError(t, err)

	creds.SetDelegationKey(nil)
	_, err = svc.RenewCert(current)
	require.ErrorIs(t, err, ErrNotAdmin)
}

func TestCheckCertExpiryFailsFastPastAccessDeadline(t *testing.T) {
	creds, _, _, _ := newRootCredentialsWithAttrs(t, nil)
	svc := newIssuerService(t, creds, newFakeCertManager())

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	child, err := creds.DelegationKey().IssueMemberCert(
		subjectPub, auth.LeafCapabilities(),
		time.Now().Add(-2*time.Hour), time.Now().Add(-90*time.Minute),
		time.Now().Add(-time.Minute),
	)
	require.NoError(t, err)
	creds.SetCert(child)

	require.True(t, svc.checkCertExpiry(),
		"past access_deadline must fail fast (shutdown), not spin in degraded mode")
	require.True(t, svc.renewalFailed.Load())
}

func TestCheckCertExpiryLegacyExpiredDoesNotFailFast(t *testing.T) {
	creds, _, _, _ := newRootCredentialsWithAttrs(t, nil)
	svc := newIssuerService(t, creds, newFakeCertManager())

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	// No access_deadline (legacy): past not_after but within the
	// reconnect window must keep the existing degraded-mode path.
	child, err := creds.DelegationKey().IssueMemberCert(
		subjectPub, auth.LeafCapabilities(),
		time.Now().Add(-time.Hour), time.Now().Add(-10*time.Minute), time.Time{},
	)
	require.NoError(t, err)
	creds.SetCert(child)
	creds.SetDelegationKey(nil) // force the non-root mesh-renewal path

	require.False(t, svc.checkCertExpiry(),
		"legacy no-ceiling cert past not_after must stay in the reconnect-window path, not fail fast")
}

func TestCheckCertExpiryNeedsRenewalWithinCeilingDoesNotFailFast(t *testing.T) {
	creds, _, _, _ := newRootCredentialsWithAttrs(t, nil)
	svc := newIssuerService(t, creds, newFakeCertManager())

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	child, err := creds.DelegationKey().IssueMemberCert(
		subjectPub, auth.LeafCapabilities(),
		time.Now().Add(-time.Hour), time.Now().Add(-10*time.Minute),
		time.Now().Add(10*24*time.Hour),
	)
	require.NoError(t, err)
	creds.SetCert(child)
	creds.SetDelegationKey(nil)

	require.False(t, svc.checkCertExpiry(),
		"NeedsRenewal within the ceiling must attempt renewal, not fail fast")
}

func TestRenewCertRejectsDeniedSubject(t *testing.T) {
	creds, _, _, _ := newRootCredentialsWithAttrs(t, nil)
	svc, st := newIssuerServiceWithState(t, creds, newFakeCertManager())

	subjectPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	subject := types.PeerKeyFromBytes(subjectPub)

	current, err := creds.DelegationKey().IssueMemberCert(
		subjectPub, auth.LeafCapabilities(),
		time.Now().Add(-time.Minute), time.Now().Add(time.Hour),
		time.Now().Add(30*24*time.Hour),
	)
	require.NoError(t, err)

	st.snapshot.DeniedKeys = []types.PeerKey{subject}

	_, err = svc.RenewCert(current)
	require.True(t, errors.Is(err, ErrSubjectDenied), "denied subject must be refused: %v", err)
}
