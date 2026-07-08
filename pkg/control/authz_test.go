// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"errors"
	"fmt"
	"testing"

	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/static"
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
		{"admit escalation", &identityv1.Capabilities{CanAdmit: true}, "CanAdmit"},
		{"workspace-admin escalation", &identityv1.Capabilities{IsWorkspaceAdmin: true}, "IsWorkspaceAdmin"},
		{"delegate escalation", &identityv1.Capabilities{CanDelegate: true}, "CanDelegate"},
		{"infrastructure escalation", &identityv1.Capabilities{IsInfrastructure: true}, "IsInfrastructure"},
		{"publish escalation", &identityv1.Capabilities{Publish: &identityv1.PublishCapability{Functions: true}}, "publish functions"},
		{"max_depth escalation", &identityv1.Capabilities{MaxDepth: 9}, "MaxDepth"},
		{"attribute not held", &identityv1.Capabilities{Attributes: attrs(t, map[string]any{"role": "root"})}, "attribute"},
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

// TestFailSurfacesNoServingCapacity proves a static seed against a
// cluster with no --static-addr surfaces verbatim as
// FailedPrecondition rather than a generic Internal, so the operator
// learns the configuration gap directly from the gRPC error.
func TestFailSurfacesNoServingCapacity(t *testing.T) {
	s := &Service{}
	wrapped := fmt.Errorf("seed: %w", static.ErrNoServingCapacity)
	st, ok := status.FromError(s.fail(wrapped, "seed static"))
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
	require.Contains(t, st.Message(), "no nodes in this cluster have static serving enabled")
}

// TestFailSurfacesAdmissionRejected proves the control fail() funnel
// maps an admission authorise/account verdict to FailedPrecondition
// with the reason verbatim, instead of logging it and returning a
// generic Internal. The asserted strings are the exact text the
// operator-facing docs quote for an exhausted budget and a missing
// publish capability.
func TestFailSurfacesAdmissionRejected(t *testing.T) {
	s := &Service{}

	budget := fmt.Errorf("%w: %w", admission.ErrRejected,
		errors.New("functions budget exhausted: authority holds 1, limit 1"))
	st, ok := status.FromError(s.fail(budget, "publish blob"))
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
	require.Equal(t, "admission: functions budget exhausted: authority holds 1, limit 1", st.Message())

	capErr := fmt.Errorf("%w: %w", admission.ErrRejected,
		errors.New("authority grant lacks publish capability for functions"))
	st, ok = status.FromError(s.fail(capErr, "failed to seed workload"))
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
	require.Equal(t, "admission: authority grant lacks publish capability for functions", st.Message())
}
