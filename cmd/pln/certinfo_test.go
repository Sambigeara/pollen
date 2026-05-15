// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWireCertDiagnosisNoCreds(t *testing.T) {
	msg := wireCertDiagnosis(t.TempDir(), "pln://edge:7443", errors.New("tls: bad certificate"))
	require.Contains(t, msg, "cannot reach pln://edge:7443")
	require.Contains(t, msg, "pln join")
}

func TestWireCertDiagnosisHealthyBlamesEndpoint(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	writeContextCert(t, dir, now.Add(-time.Minute), now.Add(time.Hour), now.Add(30*24*time.Hour))
	msg := wireCertDiagnosis(dir, "pln://edge:7443", errors.New("connection refused"))
	require.Contains(t, msg, "context cert is valid")
	require.Contains(t, msg, "endpoint is unreachable")
}

func TestWireCertDiagnosisExpiredTellsRejoin(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	writeContextCert(t, dir, now.Add(-2*time.Hour), now.Add(-90*time.Minute), now.Add(-time.Minute))
	msg := wireCertDiagnosis(dir, "pln://edge:7443", errors.New("tls: bad certificate"))
	require.Contains(t, msg, "expired at")
	require.Contains(t, msg, "pln join")
	require.Contains(t, msg, "pln invite")
}

func TestWireCertDiagnosisNeedsRenewal(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	writeContextCert(t, dir, now.Add(-2*time.Hour), now.Add(-time.Hour), now.Add(20*24*time.Hour))
	msg := wireCertDiagnosis(dir, "pln://edge:7443", errors.New("tls: bad certificate"))
	require.Contains(t, msg, "auto-renew should have run")
}
