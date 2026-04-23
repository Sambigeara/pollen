// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConformanceAllResourceTypesEnumerated pins the complete list of
// ResourceType constants. When adding a new constant, add it to the
// literal below AND to allResourceTypes — the test fails if they drift.
func TestConformanceAllResourceTypesEnumerated(t *testing.T) {
	want := []ResourceType{
		ResourceBlob,
		ResourceSeed,
		ResourceStatic,
		ResourceService,
		ResourceCert,
	}
	require.ElementsMatch(t, want, allResourceTypes)
}
