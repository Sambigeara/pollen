// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
)

func newPolicyCmd(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "test"}
	addPolicyFlags(cmd)
	return cmd
}

func TestPolicyFromFlags_NoFlags(t *testing.T) {
	cmd := newPolicyCmd(t)
	policy, err := policyFromFlags(cmd)
	require.NoError(t, err)
	require.Nil(t, policy)
}

func TestPolicyFromFlags_AllowCallerEquals(t *testing.T) {
	cmd := newPolicyCmd(t)
	require.NoError(t, cmd.Flags().Set("allow-caller", "role=admin"))
	require.NoError(t, cmd.Flags().Set("allow-caller", "team=backend"))

	policy, err := policyFromFlags(cmd)
	require.NoError(t, err)
	clauses := policy.GetInline().GetClauses()
	require.Len(t, clauses, 2)
	require.Equal(t, "role", clauses[0].GetKey())
	require.Equal(t, "admin", clauses[0].GetEquals())
	require.Equal(t, "team", clauses[1].GetKey())
	require.Equal(t, "backend", clauses[1].GetEquals())
}

func TestPolicyFromFlags_AllowCallerIn(t *testing.T) {
	cmd := newPolicyCmd(t)
	require.NoError(t, cmd.Flags().Set("allow-caller-in", "team=backend,frontend"))
	require.NoError(t, cmd.Flags().Set("allow-caller-in", "tier=gold,silver"))

	policy, err := policyFromFlags(cmd)
	require.NoError(t, err)
	clauses := policy.GetInline().GetClauses()
	require.Len(t, clauses, 2)
	require.Equal(t, "team", clauses[0].GetKey())
	require.Equal(t, []string{"backend", "frontend"}, clauses[0].GetIn().GetValues())
	require.Equal(t, "tier", clauses[1].GetKey())
	require.Equal(t, []string{"gold", "silver"}, clauses[1].GetIn().GetValues())
}

func TestPolicyFromFlags_AllowTarget(t *testing.T) {
	cmd := newPolicyCmd(t)
	require.NoError(t, cmd.Flags().Set("allow-target", "verify"))
	require.NoError(t, cmd.Flags().Set("allow-target", "issue"))

	policy, err := policyFromFlags(cmd)
	require.NoError(t, err)
	clauses := policy.GetInline().GetClauses()
	require.Len(t, clauses, 1)
	require.Equal(t, targetPolicyKey, clauses[0].GetKey())
	require.Equal(t, []string{"verify", "issue"}, clauses[0].GetIn().GetValues())
}

func TestPolicyFromFlags_RejectsMalformedAllowCaller(t *testing.T) {
	cmd := newPolicyCmd(t)
	require.NoError(t, cmd.Flags().Set("allow-caller", "no-equals-sign"))

	_, err := policyFromFlags(cmd)
	require.ErrorContains(t, err, "key=value")
}

func TestPolicyFromFlags_RejectsMalformedAllowCallerIn(t *testing.T) {
	cmd := newPolicyCmd(t)
	require.NoError(t, cmd.Flags().Set("allow-caller-in", "=missing-key"))

	_, err := policyFromFlags(cmd)
	require.ErrorContains(t, err, "key=v1,v2,v3")
}

func TestPolicyFromFlags_RejectsAllowCallerInEmptyValueList(t *testing.T) {
	cmd := newPolicyCmd(t)
	require.NoError(t, cmd.Flags().Set("allow-caller-in", "team="))

	_, err := policyFromFlags(cmd)
	require.ErrorContains(t, err, "at least one value")
}

func TestPolicyFromFlags_CombinesAllClauseKinds(t *testing.T) {
	cmd := newPolicyCmd(t)
	require.NoError(t, cmd.Flags().Set("allow-caller", "role=admin"))
	require.NoError(t, cmd.Flags().Set("allow-caller-in", "team=backend,frontend"))
	require.NoError(t, cmd.Flags().Set("allow-target", "run"))

	policy, err := policyFromFlags(cmd)
	require.NoError(t, err)
	clauses := policy.GetInline().GetClauses()
	require.Len(t, clauses, 3)
	require.Equal(t, "role", clauses[0].GetKey())
	require.IsType(t, (*admissionv1.Clause_Equals)(nil), clauses[0].GetMatch())
	require.Equal(t, "team", clauses[1].GetKey())
	require.IsType(t, (*admissionv1.Clause_In)(nil), clauses[1].GetMatch())
	require.Equal(t, targetPolicyKey, clauses[2].GetKey())
	require.IsType(t, (*admissionv1.Clause_In)(nil), clauses[2].GetMatch())
}
