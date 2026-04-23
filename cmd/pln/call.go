// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/api/genpb/pollen/control/v1/controlv1connect"
)

const (
	defaultCallRetries = 3
	initialRetryDelay  = 50 * time.Millisecond
)

func newCallCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "call <name-or-hash> <function> [input]",
		Short: "Invoke a WASM function",
		Long: `Calls a function on a deployed workload. The cluster routes to the
nearest, least-loaded replica via P2C. Input may be passed as the third
positional arg or piped on stdin. Output is written raw to stdout.

Retries on transient overload/unavailability up to --retries times within
--timeout.`,
		Example: "  pln call hello greet '{\"name\":\"world\"}'\n  cat payload.json | pln call hello greet",
		Args:    cobra.RangeArgs(2, 3), //nolint:mnd
		RunE:    withEnv(runCall),
	}
	cmd.Flags().Duration("timeout", callWorkloadTimeout, "overall deadline for the invocation")
	cmd.Flags().Int("retries", defaultCallRetries, "retry budget for transient overload/unavailability")
	return cmd
}

func runCall(cmd *cobra.Command, args []string, env *cliEnv) error {
	hash := args[0]
	function := args[1]

	var input []byte
	if len(args) == 3 { //nolint:mnd
		input = []byte(args[2])
	} else if stat, err := os.Stdin.Stat(); err == nil && (stat.Mode()&os.ModeCharDevice) == 0 {
		if input, err = io.ReadAll(cmd.InOrStdin()); err != nil {
			return fmt.Errorf("reading input from stdin: %w", err)
		}
	}

	timeout, _ := cmd.Flags().GetDuration("timeout")
	retries, _ := cmd.Flags().GetInt("retries")
	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()

	req := &controlv1.CallWorkloadRequest{Hash: hash, Function: function, Input: input}

	resp, err := callWithRetry(ctx, env.client, req, retries)
	if err != nil {
		return err
	}

	_, _ = cmd.OutOrStdout().Write(resp.Msg.GetOutput())
	fmt.Fprintln(cmd.OutOrStdout())
	return nil
}

func callWithRetry(ctx context.Context, client controlv1connect.ControlServiceClient, req *controlv1.CallWorkloadRequest, retries int) (*connect.Response[controlv1.CallWorkloadResponse], error) {
	backoff := initialRetryDelay
	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		resp, err := client.CallWorkload(ctx, connect.NewRequest(req))
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !isRetryableCallError(err) || attempt == retries {
			return nil, err
		}

		jitter := time.Duration(rand.Int64N(int64(backoff))) //nolint:gosec
		wait := backoff + jitter
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) < wait {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		backoff *= 2
	}
	return nil, lastErr
}

func isRetryableCallError(err error) bool {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return false
	}
	code := ce.Code()
	return code == connect.CodeResourceExhausted || code == connect.CodeUnavailable
}
