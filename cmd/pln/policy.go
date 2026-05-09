// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"strings"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/spf13/cobra"
)

const targetPolicyKey = "pln.target"

func addPolicyFlags(cmd *cobra.Command) {
	cmd.Flags().StringArray("allow-caller", nil, "Require caller cert attribute key=value (repeatable)")
	cmd.Flags().StringArray("allow-caller-in", nil, "Require caller cert attribute key=v1,v2,v3 (any-of, repeatable)")
	cmd.Flags().StringArray("allow-target", nil, "Allow only this workload function target (repeatable; workloads only)")
}

func policyFromFlags(cmd *cobra.Command) (*admissionv1.Predicate, error) {
	var clauses []*admissionv1.Clause
	attrs, _ := cmd.Flags().GetStringArray("allow-caller")
	for _, raw := range attrs {
		key, val, ok := strings.Cut(raw, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("--allow-caller must be key=value")
		}
		clauses = append(clauses, &admissionv1.Clause{Key: key, Match: &admissionv1.Clause_Equals{Equals: val}})
	}
	allowIns, _ := cmd.Flags().GetStringArray("allow-caller-in")
	for _, raw := range allowIns {
		key, list, ok := strings.Cut(raw, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("--allow-caller-in must be key=v1,v2,v3")
		}
		values := strings.Split(list, ",")
		for i, v := range values {
			values[i] = strings.TrimSpace(v)
		}
		if len(values) == 0 || values[0] == "" {
			return nil, fmt.Errorf("--allow-caller-in needs at least one value after %s=", key)
		}
		clauses = append(clauses, &admissionv1.Clause{Key: key, Match: &admissionv1.Clause_In{In: &admissionv1.StringList{Values: values}}})
	}
	targets, _ := cmd.Flags().GetStringArray("allow-target")
	if len(targets) > 0 {
		clauses = append(clauses, &admissionv1.Clause{Key: targetPolicyKey, Match: &admissionv1.Clause_In{In: &admissionv1.StringList{Values: targets}}})
	}
	if len(clauses) == 0 {
		return nil, nil
	}
	return &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: clauses}}, nil
}
