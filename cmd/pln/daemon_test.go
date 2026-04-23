// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderUserPlnPlist_Structure(t *testing.T) {
	got := renderUserPlnPlist("home", "/opt/homebrew/opt/pln/bin/pln", "/Users/alice/.pln-home")

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
}

func TestRenderUserPlnPlist_EscapesSpecialChars(t *testing.T) {
	got := renderUserPlnPlist("weird&name", "/bin/pln", "/path with <brackets>")

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
