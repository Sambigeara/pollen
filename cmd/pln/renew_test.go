// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/auth"
)

func writeContextCert(t *testing.T, dir string, notBefore, notAfter, accessDeadline time.Time) {
	t.Helper()
	identityDir := auth.IdentityPath(dir)
	_, nodePub, err := auth.EnsureIdentityKey(identityDir)
	require.NoError(t, err)
	rootPub, rootPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	cert, err := auth.IssueDelegationCert(rootPriv, nil, nodePub, auth.LeafCapabilities(),
		notBefore, notAfter, accessDeadline)
	require.NoError(t, err)
	require.NoError(t, auth.SaveNodeCredentials(identityDir, auth.NewNodeCredentials(rootPub, cert)))
}

func TestEnsureWireCertFreshNoCredsIsNoop(t *testing.T) {
	require.NoError(t, ensureWireCertFresh(context.Background(), t.TempDir(), "pln://127.0.0.1:1", "https://127.0.0.1:1"))
}

func TestEnsureWireCertFreshHealthyCertSkipsNetwork(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	// Plenty of life left (well over half): must return without dialing,
	// so an unroutable host is safe and proves no network is attempted.
	writeContextCert(t, dir, now.Add(-time.Minute), now.Add(time.Hour), now.Add(30*24*time.Hour))
	require.NoError(t, ensureWireCertFresh(context.Background(), dir, "pln://203.0.113.1:1", "https://203.0.113.1:1"))
}

func TestEnsureWireCertFreshExpiredReturnsRejoinError(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	writeContextCert(t, dir, now.Add(-2*time.Hour), now.Add(-90*time.Minute), now.Add(-time.Minute))
	err := ensureWireCertFresh(context.Background(), dir, "pln://203.0.113.1:1", "https://203.0.113.1:1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "rejoin")
}

// withEnv must not run the renewal preflight for commands that opt out
// (pln join): a hard-expired context cert would otherwise block the only
// recovery path with the very error that tells you to run pln join.
func TestWithEnvSkipCertRenewalBypassesPreflight(t *testing.T) {
	t.Setenv("PLN_CONTEXT", "default")
	dir := t.TempDir()
	now := time.Now()
	writeContextCert(t, dir, now.Add(-2*time.Hour), now.Add(-90*time.Minute), now.Add(-time.Minute))

	sentinel := errors.New("fn reached")
	fn := func(*cobra.Command, []string, *cliEnv) error { return sentinel }

	// Drive the real cobra pipeline: persistent flags only merge and
	// mark Changed during Execute, which is what resolveTarget relies on.
	run := func(opts ...envOption) error {
		sub := &cobra.Command{
			Use:  "x",
			RunE: withEnv(fn, append([]envOption{localOnly()}, opts...)...),
		}
		root := &cobra.Command{Use: "pln", SilenceErrors: true, SilenceUsage: true}
		root.PersistentFlags().String("dir", "", "")
		root.PersistentFlags().StringP("host", "H", "", "")
		root.AddCommand(sub)
		root.SetArgs([]string{"x", "--dir", dir, "--host", "pln://203.0.113.1:1"})
		return root.Execute()
	}

	gated := run()
	require.Error(t, gated)
	require.Contains(t, gated.Error(), "expired beyond renewal",
		"without the opt-out the expired context cert must block the command")

	require.ErrorIs(t, run(skipCertRenewal()), sentinel,
		"with skipCertRenewal the command must run despite the expired context cert")
}
