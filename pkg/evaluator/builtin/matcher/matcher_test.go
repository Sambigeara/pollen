// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package matcher_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/evaluator"
	"github.com/sambigeara/pollen/pkg/evaluator/builtin/matcher"
)

func build(t *testing.T, yaml string) *matcher.Matcher {
	t.Helper()
	m, err := matcher.NewFromBytes([]byte(yaml))
	require.NoError(t, err)
	return m
}

func TestMatcher_ScalarMatch(t *testing.T) {
	m := build(t, `
rules:
  - match:
      subject.properties.role: admin
    decision: allow
  - decision: deny
    match: {}
`)
	d, err := m.Allow(context.Background(), evaluator.Request{
		Subject: evaluator.Subject{Properties: map[string]any{"role": "admin"}},
	})
	require.NoError(t, err)
	require.True(t, d.Decision)
}

func TestMatcher_ListMatchAsAnyOf(t *testing.T) {
	m := build(t, `
rules:
  - match:
      subject.properties.clearance: [secret, topsecret]
      resource.properties.classification: secret
    decision: allow
  - decision: deny
    match: {}
`)
	for _, clearance := range []string{"secret", "topsecret"} {
		d, err := m.Allow(context.Background(), evaluator.Request{
			Subject:  evaluator.Subject{Properties: map[string]any{"clearance": clearance}},
			Resource: evaluator.Resource{Properties: map[string]any{"classification": "secret"}},
		})
		require.NoError(t, err)
		require.True(t, d.Decision, "clearance %q should allow", clearance)
	}

	d, err := m.Allow(context.Background(), evaluator.Request{
		Subject:  evaluator.Subject{Properties: map[string]any{"clearance": "confidential"}},
		Resource: evaluator.Resource{Properties: map[string]any{"classification": "secret"}},
	})
	require.NoError(t, err)
	require.False(t, d.Decision)
}

func TestMatcher_FirstMatchWins(t *testing.T) {
	m := build(t, `
rules:
  - match:
      subject.properties.role: banned
    decision: deny
    reason: banned user
  - match:
      action.name: fetch
    decision: allow
`)
	d, err := m.Allow(context.Background(), evaluator.Request{
		Subject: evaluator.Subject{Properties: map[string]any{"role": "banned"}},
		Action:  evaluator.Action{Name: "fetch"},
	})
	require.NoError(t, err)
	require.False(t, d.Decision, "banned rule must win over allow-fetch")
	require.Equal(t, "banned user", d.Context["reason_user"])
}

func TestMatcher_NoMatchYieldsFallbackDeny(t *testing.T) {
	m := build(t, `
rules:
  - match:
      subject.properties.role: admin
    decision: allow
`)
	d, err := m.Allow(context.Background(), evaluator.Request{Subject: evaluator.Subject{Properties: map[string]any{"role": "other"}}})
	require.NoError(t, err)
	require.False(t, d.Decision)
}

func TestMatcher_NumericEquality(t *testing.T) {
	m := build(t, `
rules:
  - match:
      action.properties.port: 8080
    decision: allow
`)
	d, err := m.Allow(context.Background(), evaluator.Request{
		Action: evaluator.Action{Properties: map[string]any{"port": float64(8080)}},
	})
	require.NoError(t, err)
	require.True(t, d.Decision)
}

func TestMatcher_RejectsInvalidDecision(t *testing.T) {
	_, err := matcher.NewFromBytes([]byte(`rules: [{decision: maybe}]`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "decision must be")
}

func TestMatcher_UnknownPathDoesNotMatch(t *testing.T) {
	m := build(t, `
rules:
  - match:
      nonsense.nested.path: x
    decision: allow
`)
	d, err := m.Allow(context.Background(), evaluator.Request{})
	require.NoError(t, err)
	require.False(t, d.Decision, "unknown top-level path must not match silently")
}

func TestMatcher_NonComparableContextDoesNotPanic(t *testing.T) {
	m := build(t, `
rules:
  - match:
      context.tags: expected
    decision: allow
  - match: {}
    decision: deny
    reason: fallback
`)
	require.NotPanics(t, func() {
		d, err := m.Allow(context.Background(), evaluator.Request{
			Context: map[string]any{"tags": []any{"a", "b"}},
		})
		require.NoError(t, err)
		require.False(t, d.Decision, "slice context value must not equal scalar rule value")
	})
	require.NotPanics(t, func() {
		d, err := m.Allow(context.Background(), evaluator.Request{
			Context: map[string]any{"tags": map[string]any{"nested": "map"}},
		})
		require.NoError(t, err)
		require.False(t, d.Decision, "map context value must not equal scalar rule value")
	})
}

func TestMatcher_ReloadReplacesRules(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/rules.yaml"
	require.NoError(t, os.WriteFile(path, []byte(`rules:
  - match: {}
    decision: allow
    reason: first
`), 0o600))

	m, err := matcher.NewFromFile(path)
	require.NoError(t, err)

	d, err := m.Allow(context.Background(), evaluator.Request{})
	require.NoError(t, err)
	require.True(t, d.Decision)
	require.Equal(t, "first", d.Context["reason_user"])

	require.NoError(t, os.WriteFile(path, []byte(`rules:
  - match: {}
    decision: deny
    reason: second
`), 0o600))
	require.NoError(t, m.Reload(path))

	d, err = m.Allow(context.Background(), evaluator.Request{})
	require.NoError(t, err)
	require.False(t, d.Decision)
	require.Equal(t, "second", d.Context["reason_user"])
}

func TestMatcher_ReloadBadYAMLKeepsPreviousRules(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/rules.yaml"
	require.NoError(t, os.WriteFile(path, []byte(`rules:
  - match: {}
    decision: allow
`), 0o600))

	m, err := matcher.NewFromFile(path)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte("not: yaml: at: all: ::"), 0o600))
	err = m.Reload(path)
	require.Error(t, err, "broken YAML must surface as an error")

	d, err := m.Allow(context.Background(), evaluator.Request{})
	require.NoError(t, err)
	require.True(t, d.Decision, "previous rules remain active after a failed reload")
}
