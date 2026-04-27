// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
)

// TestRunInitWritesPropsToCertAndConfig pins the v1 ergonomic story for
// root properties: `pln init --prop k=v` bakes the attribute into the
// self-signed root cert AND persists it into config.yaml so subsequent
// daemon boots re-issue with the same attrs (rather than drifting back
// to nothing on the next prop-drift detection).
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
