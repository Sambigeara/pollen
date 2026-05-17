// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"testing"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func attrs(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	if m == nil {
		return nil
	}
	s, err := structpb.NewStruct(m)
	require.NoError(t, err)
	return s
}

func TestEnforceGrantCeiling(t *testing.T) {
	full := &identityv1.Capabilities{
		CanAdmit:    true,
		CanDelegate: true,
		MaxDepth:    5,
		Publish:     &identityv1.PublishCapability{Functions: true, Blobs: true, Sites: true, Services: true},
		Attributes:  attrs(t, map[string]any{"role": "admin", "team": "core"}),
	}

	t.Run("within ceiling is permitted", func(t *testing.T) {
		req := &identityv1.Capabilities{
			CanDelegate: true,
			MaxDepth:    3,
			Publish:     &identityv1.PublishCapability{Sites: true},
			Attributes:  attrs(t, map[string]any{"team": "core"}),
		}
		require.NoError(t, enforceGrantCeiling(req, full))
	})

	t.Run("empty request is permitted", func(t *testing.T) {
		require.NoError(t, enforceGrantCeiling(&identityv1.Capabilities{}, &identityv1.Capabilities{}))
	})

	cases := []struct {
		name string
		req  *identityv1.Capabilities
		msg  string
	}{
		{"admit escalation", &identityv1.Capabilities{CanAdmit: true}, "cannot grant admit"},
		{"delegate escalation", &identityv1.Capabilities{CanDelegate: true}, "cannot grant delegate"},
		{"publish escalation", &identityv1.Capabilities{Publish: &identityv1.PublishCapability{Functions: true}}, "cannot grant publish"},
		{"max_depth escalation", &identityv1.Capabilities{MaxDepth: 9}, "cannot grant max_depth"},
		{"attribute not held", &identityv1.Capabilities{Attributes: attrs(t, map[string]any{"role": "root"})}, "cannot grant attributes"},
	}
	// A caller that itself holds nothing: every requested escalation
	// must be refused with PermissionDenied.
	bare := &identityv1.Capabilities{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := enforceGrantCeiling(tc.req, bare)
			require.Error(t, err)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			require.ErrorContains(t, err, tc.msg)
		})
	}
}

func TestEnforceBudgetCeiling(t *testing.T) {
	t.Run("unlimited caller permits any child", func(t *testing.T) {
		require.NoError(t, enforceBudgetCeiling(
			&identityv1.Budget{MaxFunctions: 9, MaxBlobs: 9, MaxSites: 9},
			&identityv1.Budget{}))
		require.NoError(t, enforceBudgetCeiling(nil, nil))
	})

	t.Run("within ceiling is permitted", func(t *testing.T) {
		caller := &identityv1.Budget{MaxFunctions: 10, MaxBlobs: 5, MaxSites: 3}
		require.NoError(t, enforceBudgetCeiling(
			&identityv1.Budget{MaxFunctions: 10, MaxBlobs: 1, MaxSites: 3}, caller))
	})

	caller := &identityv1.Budget{MaxFunctions: 4, MaxBlobs: 4, MaxSites: 4}
	cases := []struct {
		name string
		req  *identityv1.Budget
		msg  string
	}{
		{"functions over", &identityv1.Budget{MaxFunctions: 5, MaxBlobs: 1, MaxSites: 1}, "functions budget 5"},
		{"blobs over", &identityv1.Budget{MaxFunctions: 1, MaxBlobs: 99, MaxSites: 1}, "blobs budget 99"},
		{"sites over", &identityv1.Budget{MaxFunctions: 1, MaxBlobs: 1, MaxSites: 5}, "sites budget 5"},
		{"unlimited from limited caller", &identityv1.Budget{MaxFunctions: 0, MaxBlobs: 1, MaxSites: 1}, "cannot grant unlimited functions"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := enforceBudgetCeiling(tc.req, caller)
			require.Error(t, err)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			require.ErrorContains(t, err, tc.msg)
		})
	}
}

func TestGrantCapsPublishExceeds(t *testing.T) {
	parent := &identityv1.Capabilities{Publish: &identityv1.PublishCapability{Sites: true, Blobs: true}}
	cases := []struct {
		name string
		kind *identityv1.PublishCapability
		want bool
	}{
		{"subset", &identityv1.PublishCapability{Sites: true}, false},
		{"equal", &identityv1.PublishCapability{Sites: true, Blobs: true}, false},
		{"functions exceeds", &identityv1.PublishCapability{Functions: true}, true},
		{"services exceeds", &identityv1.PublishCapability{Services: true}, true},
		{"none", &identityv1.PublishCapability{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, grantCapsPublishExceeds(&identityv1.Capabilities{Publish: tc.kind}, parent))
		})
	}
}

func TestAttributesSubsetOf(t *testing.T) {
	parent := attrs(t, map[string]any{"role": "admin", "team": "core"})

	require.NoError(t, attributesSubsetOf(nil, parent), "nil child is a subset")
	require.NoError(t, attributesSubsetOf(attrs(t, map[string]any{"team": "core"}), parent))
	require.Error(t, attributesSubsetOf(attrs(t, map[string]any{"missing": "x"}), parent))
	require.Error(t, attributesSubsetOf(attrs(t, map[string]any{"role": "root"}), parent), "value mismatch is not a subset")
}
