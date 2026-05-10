// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"strings"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/spf13/cobra"
)

func addPolicyFlags(cmd *cobra.Command) {
	cmd.Flags().StringArray("allow-props", nil, "Require caller cert property key=value (repeatable; AND across clauses)")
}

func policyFromFlags(cmd *cobra.Command) (*admissionv1.Predicate, error) {
	attrs, _ := cmd.Flags().GetStringArray("allow-props")
	if len(attrs) == 0 {
		return nil, nil
	}
	clauses := make([]*admissionv1.Clause, 0, len(attrs))
	for _, raw := range attrs {
		key, val, ok := strings.Cut(raw, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("--allow-props must be key=value")
		}
		// Empty values would build a clause that never matches any caller;
		// the spec would look open but deny everyone.
		if val == "" {
			return nil, fmt.Errorf("--allow-props %q has empty value; use a non-empty value or omit the flag", raw)
		}
		clauses = append(clauses, &admissionv1.Clause{Key: key, Equals: val})
	}
	return &admissionv1.Predicate{Inline: &admissionv1.InlinePredicate{Clauses: clauses}}, nil
}
