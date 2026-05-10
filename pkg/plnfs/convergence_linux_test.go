// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package plnfs_test

import (
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/plnfs"
)

const casFile = "cas/ab/ab00000000000000000000000000000000000000000000000000000000000000.wasm"

var permMap = map[string]os.FileMode{
	".":                       os.ModeDir | os.ModeSetgid | 0o770,
	"keys":                    os.ModeDir | os.ModeSetgid | 0o770,
	"keys/ed25519.key":        0o640,
	"keys/ed25519.pub":        0o640,
	"keys/admin_ed25519.key":  0o640,
	"keys/admin_ed25519.pub":  0o640,
	"keys/root.pub":           0o640,
	"keys/delegation.cert.pb": 0o640,
	"config.yaml":             0o660,
	"cas":                     os.ModeDir | os.ModeSetgid | 0o770,
	"cas/ab":                  os.ModeDir | os.ModeSetgid | 0o770,
	casFile:                   0o640,
}

var coreFiles = []string{
	".", "keys",
	"keys/ed25519.key", "keys/ed25519.pub",
	"keys/admin_ed25519.key", "keys/admin_ed25519.pub",
	"keys/root.pub", "keys/delegation.cert.pb",
}

type commandSequence struct {
	name     string
	ops      []func(t *testing.T, dir string)
	required []string
}

func allSequences() []commandSequence {
	withConfig := append(append([]string{}, coreFiles...), "config.yaml")
	withCAS := append(append([]string{}, coreFiles...), "cas", "cas/ab", casFile)

	return []commandSequence{
		{"init", []func(*testing.T, string){opInit}, coreFiles},
		{"id_then_init", []func(*testing.T, string){opID, opInit}, coreFiles},
		{"init_then_serve", []func(*testing.T, string){opInit, opServe}, withConfig},
		{"serve_then_init", []func(*testing.T, string){opServe, opInit}, withConfig},
		{"double_init", []func(*testing.T, string){opInit, opInit}, coreFiles},
		{"id_then_serve_then_init", []func(*testing.T, string){opID, opServe, opInit}, withConfig},
		{"init_then_cas", []func(*testing.T, string){opInit, opCAS}, withCAS},
	}
}

func opInit(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, plnfs.EnsureDir(dir))
	identityDir := auth.IdentityPath(dir)
	_, pub, err := auth.EnsureIdentityKey(identityDir)
	require.NoError(t, err)
	_, err = auth.EnsureLocalRootCredentials(identityDir, pub, nil, time.Now(), 30*24*time.Hour) //nolint:mnd
	require.NoError(t, err)
}

func opID(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, plnfs.EnsureDir(dir))
	_, _, err := auth.EnsureIdentityKey(auth.IdentityPath(dir))
	require.NoError(t, err)
}

func opServe(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, plnfs.EnsureDir(dir))
	cfg, err := config.Load(dir)
	if err != nil {
		cfg = &config.Config{}
	}
	cfg.AddService("test-svc", 8080, "") //nolint:mnd
	require.NoError(t, config.Save(dir, cfg))
}

func opCAS(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, plnfs.EnsureDir(filepath.Join(dir, "cas")))
	require.NoError(t, plnfs.EnsureDir(filepath.Join(dir, "cas", "ab")))
	require.NoError(t, plnfs.WriteGroupReadable(filepath.Join(dir, casFile), []byte("wasm")))
}

func runSequences(t *testing.T, sequences []commandSequence) {
	t.Helper()
	for _, seq := range sequences {
		t.Run(seq.name, func(t *testing.T) {
			pollenDir := filepath.Join(t.TempDir(), "pln")
			t.Cleanup(plnfs.EnableSystemMode())
			for _, op := range seq.ops {
				op(t, pollenDir)
			}
			assertPermissions(t, pollenDir, seq.required)
		})
	}
}

func TestPermissionConvergence(t *testing.T) {
	runSequences(t, allSequences())
}

func TestPermissionConvergenceUnderRestrictiveUmask(t *testing.T) {
	old := syscall.Umask(0o077)
	t.Cleanup(func() { syscall.Umask(old) })

	runSequences(t, allSequences())
}

func TestEnsureDirRepairsPermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pln")
	t.Cleanup(plnfs.EnableSystemMode())

	require.NoError(t, os.MkdirAll(dir, 0o755))

	require.NoError(t, plnfs.EnsureDir(dir))

	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.ModeDir|os.ModeSetgid|os.FileMode(0o770), info.Mode())
}

func TestEnsureDirUserHome(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".pln")

	require.NoError(t, plnfs.EnsureDir(dir))

	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.ModeDir|os.FileMode(0o700), info.Mode())
}

func TestSocketPermissionsConvergence(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "pln.sock")

	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	require.NoError(t, plnfs.SetGroupSocket(sock))

	info, err := os.Stat(sock)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o660)|os.ModeSocket, info.Mode())
}

func assertPermissions(t *testing.T, pollenDir string, required []string) {
	t.Helper()

	found := make(map[string]bool)

	err := filepath.WalkDir(pollenDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(pollenDir, path)
		if err != nil {
			return err
		}

		// Skip .tmp files (atomic write intermediates that shouldn't persist).
		if filepath.Ext(rel) == ".tmp" {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		expected, known := permMap[rel]
		if !known {
			t.Errorf("unexpected file %q with mode %v", rel, info.Mode())
			return nil
		}

		found[rel] = true
		if info.Mode() != expected {
			t.Errorf("file %q: got mode %v, want %v", rel, info.Mode(), expected)
		}

		return nil
	})
	require.NoError(t, err)

	for _, path := range required {
		if !found[path] {
			t.Errorf("required file %q was not created", path)
		}
	}
}
