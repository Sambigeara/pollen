// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"net"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// fakeProbe returns a sockProbe stub that reports the configured paths
// as active. Tests installing one must restore the original via the
// returned cleanup.
func fakeProbe(active map[string]bool) func() {
	orig := sockProbe
	sockProbe = func(path string) bool { return active[path] }
	return func() { sockProbe = orig }
}

func TestResolveTransport_Default(t *testing.T) {
	dir := "/tmp/pln-x"
	wireAddr := "edge.pln.sh:7443"
	wireURL := plnTargetScheme + wireAddr

	cases := []struct {
		name       string
		entry      contextEntry
		sockActive bool
		wantKind   transportKind
		wantAddr   string
		wantHost   string
		wantErr    bool
	}{
		{
			name:       "sock up beats wire fallback",
			entry:      contextEntry{Dir: dir, Wire: wireURL},
			sockActive: true,
			wantKind:   transportLocal,
		},
		{
			name:     "sock down with wire falls back to wire",
			entry:    contextEntry{Dir: dir, Wire: wireURL},
			wantKind: transportWire,
			wantAddr: wireAddr,
		},
		{
			name:     "wire-only entry uses wire",
			entry:    contextEntry{Wire: wireURL},
			wantKind: transportWire,
			wantAddr: wireAddr,
		},
		{
			name:    "dir-only entry with no sock errors with actionable hint",
			entry:   contextEntry{Dir: dir},
			wantErr: true,
		},
		{
			name:     "ssh bridge wins when host set and no wire",
			entry:    contextEntry{Host: "user@host"},
			wantKind: transportSSHBridge,
			wantHost: "user@host",
		},
		{
			name:     "ssh bridge with admin keys dir still picks ssh",
			entry:    contextEntry{Dir: dir, Host: "user@host"},
			wantKind: transportSSHBridge,
			wantHost: "user@host",
		},
		{
			name:    "zero entry errors",
			entry:   contextEntry{},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cleanup := fakeProbe(map[string]bool{filepath.Join(dir, socketName): tc.sockActive})
			t.Cleanup(cleanup)

			got, err := resolveTransport(tc.entry, overrideNone)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantKind, got.kind)
			require.Equal(t, tc.wantAddr, got.WireAddr())
			require.Equal(t, tc.wantHost, got.SSHHost())
		})
	}
}

func TestResolveTransport_OverrideLocal(t *testing.T) {
	dir := "/tmp/pln-x"
	wireURL := plnTargetScheme + "edge.pln.sh:7443"

	t.Run("sock active resolves local", func(t *testing.T) {
		t.Cleanup(fakeProbe(map[string]bool{filepath.Join(dir, socketName): true}))
		got, err := resolveTransport(contextEntry{Dir: dir, Wire: wireURL}, overrideLocal)
		require.NoError(t, err)
		require.Equal(t, transportLocal, got.kind)
	})

	t.Run("sock missing errors with no-local-daemon", func(t *testing.T) {
		t.Cleanup(fakeProbe(nil))
		_, err := resolveTransport(contextEntry{Dir: dir, Wire: wireURL}, overrideLocal)
		require.ErrorContains(t, err, "no local daemon")
	})

	t.Run("wire-only entry rejects --local", func(t *testing.T) {
		t.Cleanup(fakeProbe(nil))
		_, err := resolveTransport(contextEntry{Wire: wireURL}, overrideLocal)
		require.ErrorContains(t, err, "identity dir")
	})
}

func TestResolveTransport_OverrideWire(t *testing.T) {
	dir := "/tmp/pln-x"
	wireAddr := "edge.pln.sh:7443"
	wireURL := plnTargetScheme + wireAddr

	t.Run("forces wire even when sock is up", func(t *testing.T) {
		t.Cleanup(fakeProbe(map[string]bool{filepath.Join(dir, socketName): true}))
		got, err := resolveTransport(contextEntry{Dir: dir, Wire: wireURL}, overrideWire)
		require.NoError(t, err)
		require.Equal(t, transportWire, got.kind)
		require.Equal(t, wireAddr, got.WireAddr())
	})

	t.Run("rejects ctx without wire fallback", func(t *testing.T) {
		t.Cleanup(fakeProbe(nil))
		_, err := resolveTransport(contextEntry{Dir: dir}, overrideWire)
		require.ErrorContains(t, err, "wire fallback")
	})
}

func TestResolveTransport_SSHBridgeOverrideImmune(t *testing.T) {
	entry := contextEntry{Host: "user@host"}

	for _, override := range []transportOverride{overrideNone, overrideLocal, overrideWire} {
		got, err := resolveTransport(entry, override)
		require.NoError(t, err)
		require.Equal(t, transportSSHBridge, got.kind)
		require.Equal(t, "user@host", got.SSHHost())
	}
}

func TestTransportOverrideFromFlags(t *testing.T) {
	build := func() *cobra.Command {
		c := &cobra.Command{Use: "x", Run: func(*cobra.Command, []string) {}}
		c.PersistentFlags().Bool("local", false, "")
		c.PersistentFlags().Bool("wire", false, "")
		c.MarkFlagsMutuallyExclusive("local", "wire")
		return c
	}

	t.Run("neither", func(t *testing.T) {
		c := build()
		require.NoError(t, c.ParseFlags(nil))
		require.Equal(t, overrideNone, transportOverrideFromFlags(c))
	})

	t.Run("local", func(t *testing.T) {
		c := build()
		require.NoError(t, c.ParseFlags([]string{"--local"}))
		require.Equal(t, overrideLocal, transportOverrideFromFlags(c))
	})

	t.Run("wire", func(t *testing.T) {
		c := build()
		require.NoError(t, c.ParseFlags([]string{"--wire"}))
		require.Equal(t, overrideWire, transportOverrideFromFlags(c))
	})

	t.Run("mutually exclusive enforced by cobra", func(t *testing.T) {
		c := build()
		c.SetArgs([]string{"--local", "--wire"})
		c.SilenceUsage = true
		c.SilenceErrors = true
		// Cobra's mutual-exclusion check fires during Execute, not
		// ParseFlags, so drive the full command lifecycle.
		require.Error(t, c.Execute())
	})
}

// TestSockProbeOnRealSocket exercises the real nodeSocketActive against
// a unix listener so the resolver's probe + dial path is covered end to
// end, not just via the fake.
func TestSockProbeOnRealSocket(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, socketName)
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", sock)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	require.True(t, nodeSocketActive(sock))

	got, err := resolveTransport(contextEntry{Dir: dir}, overrideNone)
	require.NoError(t, err)
	require.Equal(t, transportLocal, got.kind)

	require.NoError(t, ln.Close())
	require.False(t, nodeSocketActive(sock))
}
