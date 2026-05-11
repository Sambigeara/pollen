// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
)

func TestRunInitWritesPropsToCertAndConfig(t *testing.T) {
	dir := t.TempDir()

	cmd := &cobra.Command{}
	cmd.Flags().StringArray("prop", []string{"role=primary", "region=eu"}, "")
	cmd.SetOut(&bytes.Buffer{})

	err := runInit(cmd, nil, &cliEnv{cfg: &config.Config{}, dir: dir})
	require.NoError(t, err)

	creds, err := auth.LoadNodeCredentials(auth.IdentityPath(dir))
	require.NoError(t, err)
	attrs := creds.Cert().GetClaims().GetCapabilities().GetAttributes().AsMap()
	require.Equal(t, "primary", attrs["role"])
	require.Equal(t, "eu", attrs["region"])

	cfg, err := config.Load(dir)
	require.NoError(t, err)
	require.Equal(t, "primary", cfg.Properties["role"])
	require.Equal(t, "eu", cfg.Properties["region"])

	require.FileExists(t, filepath.Join(dir, "config.yaml"))
}

func newPropsCmd(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{}
	cmd.Flags().Bool("clear", false, "")
	return cmd
}

func initRootForTest(t *testing.T, dir string) {
	t.Helper()
	cmd := &cobra.Command{}
	cmd.Flags().StringArray("prop", nil, "")
	cmd.SetOut(&bytes.Buffer{})
	require.NoError(t, runInit(cmd, nil, &cliEnv{cfg: &config.Config{}, dir: dir}))
}

func TestRunPropsListsAndPersists(t *testing.T) {
	dir := t.TempDir()
	initRootForTest(t, dir)

	out := &bytes.Buffer{}
	cmd := newPropsCmd(t)
	cmd.SetOut(out)

	cfg, err := config.Load(dir)
	require.NoError(t, err)
	env := &cliEnv{cfg: cfg, dir: dir}

	// Set.
	require.NoError(t, runProps(cmd, []string{"role=primary", "region=eu"}, env))
	cfg, err = config.Load(dir)
	require.NoError(t, err)
	require.Equal(t, "primary", cfg.Properties["role"])
	require.Equal(t, "eu", cfg.Properties["region"])

	// List (re-init env with persisted cfg).
	out.Reset()
	require.NoError(t, runProps(cmd, nil, &cliEnv{cfg: cfg, dir: dir}))
	require.Contains(t, out.String(), "role=primary")
	require.Contains(t, out.String(), "region=eu")

	// Replace semantics: new set drops unlisted keys.
	require.NoError(t, runProps(cmd, []string{"role=secondary"}, &cliEnv{cfg: cfg, dir: dir}))
	cfg, err = config.Load(dir)
	require.NoError(t, err)
	require.Equal(t, "secondary", cfg.Properties["role"])
	_, hasRegion := cfg.Properties["region"]
	require.False(t, hasRegion, "unlisted key should be removed by replace")

	// Clear.
	require.NoError(t, cmd.Flags().Set("clear", "true"))
	require.NoError(t, runProps(cmd, nil, &cliEnv{cfg: cfg, dir: dir}))
	cfg, err = config.Load(dir)
	require.NoError(t, err)
	require.Empty(t, cfg.Properties)
}

func TestRunPropsRejectsClearWithArgs(t *testing.T) {
	dir := t.TempDir()
	initRootForTest(t, dir)
	cfg, err := config.Load(dir)
	require.NoError(t, err)

	cmd := newPropsCmd(t)
	require.NoError(t, cmd.Flags().Set("clear", "true"))
	err = runProps(cmd, []string{"role=primary"}, &cliEnv{cfg: cfg, dir: dir})
	require.ErrorContains(t, err, "--clear cannot be combined")
}

func TestCapsFromFlags(t *testing.T) {
	cases := []struct {
		name      string
		admin     bool
		publisher bool
		want      string
		wantErr   string
	}{
		{name: "default", want: "leaf"},
		{name: "admin", admin: true, want: "admin"},
		{name: "publisher", publisher: true, want: "publisher"},
		{name: "both", admin: true, publisher: true, wantErr: "mutually exclusive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().Bool("admin", tc.admin, "")
			cmd.Flags().Bool("publisher", tc.publisher, "")

			caps, err := capsFromFlags(cmd, nil)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, capsTierLabel(caps))
		})
	}
}

func TestRunPropsRejectsNonRoot(t *testing.T) {
	dir := t.TempDir()
	identityDir := auth.IdentityPath(dir)
	_, nodePub, err := auth.EnsureIdentityKey(identityDir)
	require.NoError(t, err)

	rootPub, rootPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Now()
	cert, err := auth.IssueDelegationCert(rootPriv, nil, nodePub, auth.LeafCapabilities(),
		now.Add(-time.Minute), now.Add(time.Hour), time.Time{})
	require.NoError(t, err)
	require.NoError(t, auth.SaveNodeCredentials(identityDir, auth.NewNodeCredentials(rootPub, cert)))

	cmd := newPropsCmd(t)
	cmd.SetOut(&bytes.Buffer{})
	err = runProps(cmd, []string{"role=worker"}, &cliEnv{cfg: &config.Config{}, dir: dir})
	require.ErrorContains(t, err, "only root nodes")
}
