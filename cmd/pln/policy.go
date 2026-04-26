// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

func newPolicyCmds() []*cobra.Command {
	check := &cobra.Command{
		Use:   "check <gate>",
		Short: "Run a request through the daemon's current policy without side effects",
		Long: `Dry-run a (subject, action, resource) against one of the configured gates.
The daemon answers from its running router — same rules, same matcher,
same fallback — but doesn't dispatch to any primitive.

Example:

  pln policy check workload_call \
      --subject peer:a9b8... \
      --subject-prop role=admin \
      --action call \
      --resource seed:deadbeef... \
      --resource-prop tier=gold
`,
		Args: cobra.ExactArgs(1),
		RunE: withEnv(runPolicyCheck),
	}
	check.Flags().String("subject", "", "<type>:<id>, e.g. peer:<hex> or seed:<hex>")
	check.Flags().StringArray("subject-prop", nil, `Subject property: key=value or {"k":v,...} JSON (repeatable)`)
	check.Flags().String("action", "", "Action name, e.g. fetch, call, publish")
	check.Flags().StringArray("action-prop", nil, `Action property: key=value or {"k":v,...} JSON (repeatable)`)
	check.Flags().String("resource", "", "<type>:<id>, e.g. blob:<hash> or service:<name>")
	check.Flags().StringArray("resource-prop", nil, `Resource property: key=value or {"k":v,...} JSON (repeatable)`)

	policy := &cobra.Command{Use: "policy", Short: "Inspect and test the daemon's authorisation policy"}
	policy.AddCommand(check)
	return []*cobra.Command{policy}
}

func runPolicyCheck(cmd *cobra.Command, args []string, env *cliEnv) error {
	subject, err := parseTypedRef(cmd, "subject")
	if err != nil {
		return err
	}
	resource, err := parseTypedRef(cmd, "resource")
	if err != nil {
		return err
	}
	subjectProps, err := parseKeyValues(cmd, "subject-prop")
	if err != nil {
		return err
	}
	actionProps, err := parseKeyValues(cmd, "action-prop")
	if err != nil {
		return err
	}
	resourceProps, err := parseKeyValues(cmd, "resource-prop")
	if err != nil {
		return err
	}
	actionName, _ := cmd.Flags().GetString("action")
	if actionName == "" {
		return fmt.Errorf("--action is required")
	}

	req := &controlv1.CheckPolicyRequest{
		Gate: args[0],
		Subject: &controlv1.AuthzSubject{
			Type:       subject.t,
			Id:         subject.id,
			Properties: subjectProps,
		},
		Action: &controlv1.AuthzAction{
			Name:       actionName,
			Properties: actionProps,
		},
		Resource: &controlv1.AuthzResource{
			Type:       resource.t,
			Id:         resource.id,
			Properties: resourceProps,
		},
	}
	resp, err := env.client.CheckPolicy(cmd.Context(), connect.NewRequest(req))
	if err != nil {
		return err
	}
	verdict := "DENY"
	if resp.Msg.GetAllow() {
		verdict = "ALLOW"
	}
	if reason := resp.Msg.GetReason(); reason != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", verdict, reason) //nolint:gosec
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n", verdict) //nolint:gosec
	}
	return nil
}

type typedRef struct {
	t, id string
}

// parseTypedRef accepts "<type>:<id>" and returns the pair. Missing
// flag or empty value is a zero-valued ref — policies may legitimately
// want to check defaults (e.g. "what does this gate say without a
// subject?").
func parseTypedRef(cmd *cobra.Command, flag string) (typedRef, error) {
	v, _ := cmd.Flags().GetString(flag)
	if v == "" {
		return typedRef{}, nil
	}
	t, id, ok := strings.Cut(v, ":")
	if !ok {
		return typedRef{}, fmt.Errorf("--%s must be <type>:<id>, got %q", flag, v)
	}
	return typedRef{t: t, id: id}, nil
}

func parseKeyValues(cmd *cobra.Command, flag string) (*structpb.Struct, error) {
	vals, _ := cmd.Flags().GetStringArray(flag)
	if len(vals) == 0 {
		return nil, nil
	}
	merged := make(map[string]any, len(vals))
	for _, raw := range vals {
		v := strings.TrimSpace(raw)
		if strings.HasPrefix(v, "{") {
			var obj map[string]any
			if err := json.Unmarshal([]byte(v), &obj); err != nil {
				return nil, fmt.Errorf("--%s: invalid JSON: %w", flag, err)
			}
			maps.Copy(merged, obj)
			continue
		}
		k, val, ok := strings.Cut(v, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("--%s: missing '=' in %q", flag, raw)
		}
		merged[k] = val
	}
	return structpb.NewStruct(merged)
}
