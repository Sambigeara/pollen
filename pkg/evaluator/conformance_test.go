// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/evaluator"
)

// TestConformanceAllGateNamesEnumerated pins the complete list of
// GateName constants. Adding a new constant requires updating the
// literal below AND AllGateNames — a drift fails the assertion.
func TestConformanceAllGateNamesEnumerated(t *testing.T) {
	want := []evaluator.GateName{
		evaluator.GateBlobFetch,
		evaluator.GateServiceConnect,
		evaluator.GateWorkloadCall,
		evaluator.GateSpecPublish,
		evaluator.GateGrantIssue,
		evaluator.GateSeedPlacement,
	}
	require.ElementsMatch(t, want, evaluator.AllGateNames())
}

// TestConformanceRouterWiresEveryGate fails when a GateName is added to
// AllGateNames without the router wiring a bound evaluator for it.
// Missing wiring would panic inside Router.Allow; this test forces the
// panic to surface under the conformance umbrella rather than
// at a random runtime dispatch site.
func TestConformanceRouterWiresEveryGate(t *testing.T) {
	r, err := evaluator.NewRouter(evaluator.Config{})
	require.NoError(t, err)
	for _, name := range evaluator.AllGateNames() {
		require.True(t, name.Valid(), "gate %q in AllGateNames must satisfy Valid", name)
		require.NotPanics(t, func() {
			_ = r.Allow(context.Background(), name, evaluator.Request{})
		}, "router missing wiring for %q", name)
	}
}
