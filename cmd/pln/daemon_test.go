// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/xml"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderUserPlnPlist_Structure(t *testing.T) {
	got := renderUserPlnPlist("home", "/opt/homebrew/opt/pln/bin/pln", "/Users/alice/.pln-home", 0)

	var v struct {
		XMLName xml.Name `xml:"plist"`
	}
	require.NoError(t, xml.Unmarshal([]byte(got), &v))

	require.Contains(t, got, "<string>sh.pln.home</string>")
	require.Contains(t, got, "<string>/opt/homebrew/opt/pln/bin/pln</string>")
	require.Contains(t, got, "<string>--dir</string>")
	require.Contains(t, got, "<string>/Users/alice/.pln-home</string>")
	require.Contains(t, got, "<string>up</string>")
	require.Contains(t, got, "<string>/Users/alice/.pln-home/pln.log</string>")
	require.NotContains(t, got, "<string>--port</string>", "port 0 must not emit --port flag")
}

func TestRenderUserPlnPlist_EmitsPort(t *testing.T) {
	got := renderUserPlnPlist("home", "/bin/pln", "/d", 54321)

	var v struct {
		XMLName xml.Name `xml:"plist"`
	}
	require.NoError(t, xml.Unmarshal([]byte(got), &v))
	require.Contains(t, got, "<string>--port</string>")
	require.Contains(t, got, "<string>54321</string>")
}

func TestRenderUserPlnPlist_EscapesSpecialChars(t *testing.T) {
	got := renderUserPlnPlist("weird&name", "/bin/pln", "/path with <brackets>", 0)

	var v struct {
		XMLName xml.Name `xml:"plist"`
	}
	require.NoError(t, xml.Unmarshal([]byte(got), &v), "plist must be valid XML despite special chars in inputs")
	require.NotContains(t, got, "weird&name</string>")
	require.NotContains(t, got, "<brackets>")
	require.Contains(t, got, "weird&amp;name")
}

func TestUserPlnLogPath(t *testing.T) {
	require.Equal(t, "/Users/alice/.pln-home/pln.log", userPlnLogPath("/Users/alice/.pln-home"))
}

func TestUserPlnPlistPath(t *testing.T) {
	got, err := userPlnPlistPath("home")
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(got, "/Library/LaunchAgents/sh.pln.home.plist"), "got %q", got)
}

func TestInstallScriptURLUsesCanonicalInstaller(t *testing.T) {
	require.Equal(t, "https://pln.sh/install.sh", installScriptURL)
	require.NotContains(t, installScriptURL, "raw.githubusercontent.com")
	require.NotContains(t, installScriptURL, "/main/")
}

func TestLinuxUpgradeInstallArgs_PreservesPackageInstall(t *testing.T) {
	cmd := newUpgradeCmd()
	got, err := linuxUpgradeInstallArgs(cmd, func() (string, error) { return packagePlnBinary, nil })
	require.NoError(t, err)
	require.Equal(t, []string{"-s", "--"}, got)
}

func TestLinuxUpgradeInstallArgs_PreservesTarballInstall(t *testing.T) {
	cmd := newUpgradeCmd()
	got, err := linuxUpgradeInstallArgs(cmd, func() (string, error) { return tarballPlnBinary, nil })
	require.NoError(t, err)
	require.Equal(t, []string{"-s", "--", "--method", "tarball"}, got)
}

func TestLinuxUpgradeInstallArgs_ExplicitMethodOverridesExecutable(t *testing.T) {
	cmd := newUpgradeCmd()
	require.NoError(t, cmd.Flags().Set("method", "auto"))
	got, err := linuxUpgradeInstallArgs(cmd, func() (string, error) { return "", errors.New("must not inspect executable") })
	require.NoError(t, err)
	require.Equal(t, []string{"-s", "--", "--method", "auto"}, got)
}

func TestLinuxUpgradeInstallArgs_RefusesUnknownExecutable(t *testing.T) {
	cmd := newUpgradeCmd()
	_, err := linuxUpgradeInstallArgs(cmd, func() (string, error) { return "/tmp/pln", nil })
	require.ErrorContains(t, err, "cannot infer Linux install method")
}

func TestLinuxUpgradeInstallArgs_RefusesUnknownMethod(t *testing.T) {
	cmd := newUpgradeCmd()
	require.NoError(t, cmd.Flags().Set("method", "dnf"))
	_, err := linuxUpgradeInstallArgs(cmd, func() (string, error) { return packagePlnBinary, nil })
	require.ErrorContains(t, err, "unknown install method")
}

func TestRenderSystemdUnit_UsesPackageBinary(t *testing.T) {
	got, err := renderSystemdUnit(packagePlnBinary)
	require.NoError(t, err)
	require.Contains(t, string(got), "ExecStart=/usr/bin/pln --dir /var/lib/pln up")
	require.NotContains(t, string(got), systemdBinaryPlaceholder)
}

func TestRenderSystemdUnit_UsesTarballBinary(t *testing.T) {
	got, err := renderSystemdUnit(tarballPlnBinary)
	require.NoError(t, err)
	require.Contains(t, string(got), "ExecStart=/usr/local/bin/pln --dir /var/lib/pln up")
	require.NotContains(t, string(got), systemdBinaryPlaceholder)
}

func TestSupportedSystemdPlnBinary(t *testing.T) {
	require.True(t, supportedSystemdPlnBinary(packagePlnBinary))
	require.True(t, supportedSystemdPlnBinary(tarballPlnBinary))
	require.False(t, supportedSystemdPlnBinary("/tmp/pln"))
	require.False(t, supportedSystemdPlnBinary("/home/alice/bin/pln"))
}
