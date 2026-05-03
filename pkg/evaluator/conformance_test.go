// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/evaluator"
)

func TestConformanceAllGateNamesEnumerated(t *testing.T) {
	want := []evaluator.GateName{evaluator.GateServiceConnect}
	require.ElementsMatch(t, want, evaluator.AllGateNames())
}

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
