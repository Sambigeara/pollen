// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
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

func TestPolicyFromFlags_AllowProps(t *testing.T) {
	cmd := newPolicyCmd(t)
	require.NoError(t, cmd.Flags().Set("allow-props", "role=admin"))
	require.NoError(t, cmd.Flags().Set("allow-props", "team=backend"))

	policy, err := policyFromFlags(cmd)
	require.NoError(t, err)
	clauses := policy.GetInline().GetClauses()
	require.Len(t, clauses, 2)
	require.Equal(t, "role", clauses[0].GetKey())
	require.Equal(t, "admin", clauses[0].GetEquals())
	require.Equal(t, "team", clauses[1].GetKey())
	require.Equal(t, "backend", clauses[1].GetEquals())
}

func TestPolicyFromFlags_RejectsMalformedAllowProps(t *testing.T) {
	cmd := newPolicyCmd(t)
	require.NoError(t, cmd.Flags().Set("allow-props", "no-equals-sign"))

	_, err := policyFromFlags(cmd)
	require.ErrorContains(t, err, "key=value")
}

func TestPolicyFromFlags_RejectsEmptyValue(t *testing.T) {
	cmd := newPolicyCmd(t)
	require.NoError(t, cmd.Flags().Set("allow-props", "role="))

	_, err := policyFromFlags(cmd)
	require.ErrorContains(t, err, "empty value")
}
