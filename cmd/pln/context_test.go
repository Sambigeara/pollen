// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
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

func TestResolveContextBindings_DefaultReadsWireFromYAML(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	writeContextsFile(t, home, "contexts:\n  default:\n    dir: ignored\n    wire: pln://edge.pln.sh:7443\n")

	entry, err := resolveContextBindings(defaultContextName, "/forced/dir")
	require.NoError(t, err)
	require.Equal(t, "/forced/dir", entry.Dir, "default ctx Dir must come from defaultDir, never the YAML")
	require.Equal(t, "pln://edge.pln.sh:7443", entry.Wire, "default ctx Wire must round-trip from YAML")
}

func TestResolveContextBindings_DefaultSynthesisesWhenAbsent(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	entry, err := resolveContextBindings(defaultContextName, "/forced/dir")
	require.NoError(t, err)
	require.Equal(t, contextEntry{Dir: "/forced/dir"}, entry)
}

func TestResolveContextBindings_DefaultSurvivesUnreadableFile(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root bypasses unix file perms")
	}
	home := t.TempDir()
	t.Setenv("HOME", home)
	path := filepath.Join(home, plnDir, contextsFileName)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte("contexts:\n  default:\n    wire: pln://edge:7443\n"), 0o000))
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })

	entry, err := resolveContextBindings(defaultContextName, "/forced/dir")
	require.NoError(t, err, "EACCES on the file must not break the default ctx")
	require.Equal(t, contextEntry{Dir: "/forced/dir"}, entry)

	_, err = resolveContextBindings("demo", "/forced/dir")
	require.Error(t, err, "EACCES must surface for named ctxs")
}

func TestPersistCtxWire_WritesWhenEmpty(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	wrote, err := persistCtxWire("default", "edge.example:7443")
	require.NoError(t, err)
	require.True(t, wrote)

	cf, err := loadContexts()
	require.NoError(t, err)
	require.Equal(t, "pln://edge.example:7443", cf.Contexts["default"].Wire)
}

func TestPersistCtxWire_NoOpWhenSet(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	writeContextsFile(t, home, "contexts:\n  default:\n    dir: /a\n    wire: pln://original:7443\n")

	wrote, err := persistCtxWire("default", "intruder:7443")
	require.NoError(t, err)
	require.False(t, wrote, "must not overwrite an operator-set wire endpoint")

	cf, err := loadContexts()
	require.NoError(t, err)
	require.Equal(t, "pln://original:7443", cf.Contexts["default"].Wire)
}

func TestRunContextList_DefaultEntryIsNotDuplicated(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	writeContextsFile(t, home, "contexts:\n  default:\n    dir: /a\n    wire: pln://edge:7443\n  cloud:\n    dir: /b\n    wire: pln://other:7443\n")

	cmd := &cobra.Command{}
	cmd.Flags().String("dir", "/forced", "")
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	require.NoError(t, runContextList(cmd, nil))

	out := buf.String()
	defaultRows := 0
	for line := range strings.SplitSeq(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "default ") {
			defaultRows++
		}
	}
	require.Equal(t, 1, defaultRows, "default row must not duplicate; got output:\n%s", out)
	require.Contains(t, out, "pln://edge:7443", "wire fallback must surface in the default row's TARGET")
}

func TestRunContextList_DefaultDirShownWhenNoFallback(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	cmd := &cobra.Command{}
	cmd.Flags().String("dir", "/forced", "")
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	require.NoError(t, runContextList(cmd, nil))

	out := buf.String()
	require.Contains(t, out, "/forced (local)", "no YAML entry means default shows its dir + (local)")
}

func TestFirstBootstrapWireEndpoint(t *testing.T) {
	require.Empty(t, firstBootstrapWireEndpoint(nil))
	require.Empty(t, firstBootstrapWireEndpoint([]*admissionv1.BootstrapPeer{
		{Addrs: []string{"192.168.0.5:60611"}},
	}))
	require.Equal(t, "edge:7443", firstBootstrapWireEndpoint([]*admissionv1.BootstrapPeer{
		{Addrs: []string{"192.168.0.5:60611"}},
		{Addrs: []string{"203.0.113.5:60611"}, WireEndpoint: "edge:7443"},
		{Addrs: []string{"203.0.113.6:60611"}, WireEndpoint: "ignored:7443"},
	}), "first non-empty wins")
}

func TestInferTarget(t *testing.T) {
	tmp := t.TempDir()

	cases := []struct {
		name     string
		arg      string
		wantHost string
		wantWire string
		wantDir  string
		wantErr  bool
		dirIsAbs bool
	}{
		{name: "ssh", arg: "user@host", wantHost: "user@host"},
		{name: "pln wire", arg: "pln://edge.pln.sh:7443", wantWire: "pln://edge.pln.sh:7443"},
		{name: "absolute path", arg: tmp, wantDir: tmp, dirIsAbs: true},
		{name: "tilde path", arg: "~/missing-pln-dir", dirIsAbs: true},
		{name: "ambiguous bare word", arg: "myhost", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir, host, wire, err := inferTarget(tc.arg)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantHost, host)
			require.Equal(t, tc.wantWire, wire)
			if tc.dirIsAbs {
				require.True(t, filepath.IsAbs(dir), "want absolute dir, got %q", dir)
			}
			if tc.wantDir != "" {
				require.Equal(t, tc.wantDir, dir)
			}
		})
	}
}

func TestContextEntry_YAML(t *testing.T) {
	t.Run("legacy host pln:// migrates to wire", func(t *testing.T) {
		body := "current: prod\ncontexts:\n  prod:\n    dir: /tmp/prod\n    host: pln://edge.pln.sh:7443\n"
		cf := contextsFile{}
		require.NoError(t, yaml.Unmarshal([]byte(body), &cf))
		entry := cf.Contexts["prod"]
		require.Equal(t, "pln://edge.pln.sh:7443", entry.Wire)
		require.Empty(t, entry.Host)
		require.Equal(t, "/tmp/prod", entry.Dir)
	})
	t.Run("host user@x stays on host", func(t *testing.T) {
		body := "contexts:\n  prod:\n    dir: /tmp/prod\n    host: root@prod.example.com\n"
		cf := contextsFile{}
		require.NoError(t, yaml.Unmarshal([]byte(body), &cf))
		entry := cf.Contexts["prod"]
		require.Equal(t, "root@prod.example.com", entry.Host)
		require.Empty(t, entry.Wire)
	})
	t.Run("new wire field round-trips", func(t *testing.T) {
		body := "contexts:\n  prod:\n    dir: /tmp/prod\n    wire: pln://edge.pln.sh:7443\n"
		cf := contextsFile{}
		require.NoError(t, yaml.Unmarshal([]byte(body), &cf))
		entry := cf.Contexts["prod"]
		require.Equal(t, "pln://edge.pln.sh:7443", entry.Wire)
		require.Empty(t, entry.Host)

		out, err := yaml.Marshal(&cf)
		require.NoError(t, err)
		var round contextsFile
		require.NoError(t, yaml.Unmarshal(out, &round))
		require.Equal(t, entry, round.Contexts["prod"])
	})
}
