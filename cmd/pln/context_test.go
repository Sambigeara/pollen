// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeContextsFile(t *testing.T, home, body string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(home, plnDir), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(home, plnDir, contextsFileName), []byte(body), 0o600))
}

func TestResolveContextName_PLNContextOverridesAll(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PLN_CONTEXT", "demo")
	t.Setenv("XPC_SERVICE_NAME", "homebrew.mxcl.pln")
	writeContextsFile(t, home, "current: prod\ncontexts:\n  prod:\n    dir: /tmp/prod\n")

	require.Equal(t, "demo", resolveContextName())
}

func TestResolveContextName_BrewLaunchdPinsDefault(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PLN_CONTEXT", "")
	t.Setenv("XPC_SERVICE_NAME", "homebrew.mxcl.pln")
	writeContextsFile(t, home, "current: demo\ncontexts:\n  demo:\n    dir: /tmp/demo\n")

	require.Equal(t, defaultContextName, resolveContextName())
}

func TestResolveContextName_InteractiveShellHonoursCurrent(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PLN_CONTEXT", "")
	t.Setenv("XPC_SERVICE_NAME", "0")
	writeContextsFile(t, home, "current: demo\ncontexts:\n  demo:\n    dir: /tmp/demo\n")

	require.Equal(t, "demo", resolveContextName())
}

func TestResolveContextName_NoFileReturnsDefault(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PLN_CONTEXT", "")
	t.Setenv("XPC_SERVICE_NAME", "")

	require.Equal(t, defaultContextName, resolveContextName())
}
