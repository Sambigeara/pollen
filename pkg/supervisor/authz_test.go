// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreloadAuthz_ZeroOptionsAllowsStartup(t *testing.T) {
	mtr, err := preloadAuthz(AuthzOptions{}, false)
	require.NoError(t, err)
	require.Nil(t, mtr)
}

func TestPreloadAuthz_RelayOnlyRejectsSeedDefault(t *testing.T) {
	_, err := preloadAuthz(AuthzOptions{Default: "seed/foo"}, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "relay-only")
}

func TestPreloadAuthz_RelayOnlyRejectsSeedGate(t *testing.T) {
	_, err := preloadAuthz(AuthzOptions{
		Gates: map[string]string{"service_connect": "seed/foo"},
	}, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "relay-only")
}

func TestPreloadAuthz_RelayOnlyAcceptsAllowAll(t *testing.T) {
	mtr, err := preloadAuthz(AuthzOptions{
		Default: "allow_all",
		Gates:   map[string]string{"service_connect": "allow_all"},
	}, true)
	require.NoError(t, err)
	require.Nil(t, mtr)
}

func TestPreloadAuthz_RejectsUnknownGate(t *testing.T) {
	_, err := preloadAuthz(AuthzOptions{
		Gates: map[string]string{"nonsense": "allow_all"},
	}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown gate")
}

func TestPreloadAuthz_BadMatcherPathSurfacesEarly(t *testing.T) {
	_, err := preloadAuthz(AuthzOptions{
		MatcherRules: "/no/such/path/rules.yaml",
	}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load matcher rules")
}

func TestPreloadAuthz_BadMatcherYAMLSurfacesEarly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	require.NoError(t, os.WriteFile(path, []byte("rules: [- not: yaml"), 0o600))
	_, err := preloadAuthz(AuthzOptions{MatcherRules: path}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "load matcher rules")
}

func TestPreloadAuthz_LoadsMatcherSuccessfully(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`rules:
  - match: {action.name: connect}
    decision: allow
`), 0o600))
	mtr, err := preloadAuthz(AuthzOptions{
		Gates:        map[string]string{"service_connect": "attribute_matcher"},
		MatcherRules: path,
	}, false)
	require.NoError(t, err)
	require.NotNil(t, mtr)
}
