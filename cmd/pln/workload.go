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
	"path/filepath"
	"strings"
	"time"

	"connectrpc.com/connect"
	units "github.com/docker/go-units"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/api/genpb/pollen/control/v1/controlv1connect"
)

func newWorkloadCmds() []*cobra.Command {
	seedCmd := &cobra.Command{Use: "seed <file.wasm>", Short: "Deploy a WASM workload", Args: cobra.ExactArgs(1), RunE: withEnv(runSeed)}
	seedCmd.Flags().SetNormalizeFunc(func(_ *pflag.FlagSet, name string) pflag.NormalizedName {
		if name == "mem" {
			return "memory"
		}
		return pflag.NormalizedName(name)
	})
	seedCmd.Flags().String("name", "", "workload name (defaults to filename without .wasm suffix)")
	seedCmd.Flags().Uint32("min-replicas", 2, "minimum number of replicas") //nolint:mnd
	seedCmd.Flags().Bool("all", false, "run on all nodes in the cluster")
	seedCmd.Flags().String("memory", "", "memory limit, e.g. 64MiB, 128M, 1GiB (default 64MiB, alias --mem)")
	seedCmd.Flags().Uint32("timeout-ms", 0, "per-invocation timeout in ms")
	seedCmd.Flags().Uint32("latency-slo-ms", 0, "caller-perspective latency SLO in ms; drives autoscale via burn rate")

	unseedCmd := &cobra.Command{Use: "unseed <name-or-hash>", Short: "Stop a workload", Args: cobra.ExactArgs(1), RunE: withEnv(runUnseed)}

	callCmd := &cobra.Command{Use: "call <name-or-hash> <function> [input]", Short: "Invoke a WASM function", Args: cobra.RangeArgs(2, 3), RunE: withEnv(runCall)} //nolint:mnd
	callCmd.Flags().Duration("timeout", callWorkloadTimeout, "overall deadline for the invocation")
	callCmd.Flags().Int("retries", defaultCallRetries, "retry budget for transient overload/unavailability")

	return []*cobra.Command{seedCmd, unseedCmd, callCmd}
}

// streamChunkBytes stays below the default 4 MiB gRPC per-message cap.
const streamChunkBytes = 1 << 20

func runSeed(cmd *cobra.Command, args []string, env *cliEnv) error {
	f, err := os.Open(args[0])
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", args[0], err)
	}
	defer f.Close()

	name, _ := cmd.Flags().GetString("name")
	if name == "" {
		name = strings.TrimSuffix(filepath.Base(args[0]), ".wasm")
	}
	minReplicas, _ := cmd.Flags().GetUint32("min-replicas")
	all, _ := cmd.Flags().GetBool("all")
	memoryStr, _ := cmd.Flags().GetString("memory")
	memoryBytes, err := parseMemoryBytes(memoryStr)
	if err != nil {
		return fmt.Errorf("invalid --memory: %w", err)
	}
	timeoutMs, _ := cmd.Flags().GetUint32("timeout-ms")
	latencySloMs, _ := cmd.Flags().GetUint32("latency-slo-ms")

	var spread float32
	if all {
		spread = 1.0
	}

	stream := env.client.SeedWorkload(cmd.Context())
	if err := stream.Send(&controlv1.SeedWorkloadRequest{
		Payload: &controlv1.SeedWorkloadRequest_Header{
			Header: &controlv1.SeedWorkloadHeader{
				Name:         name,
				MinReplicas:  minReplicas,
				Spread:       spread,
				MemoryBytes:  memoryBytes,
				TimeoutMs:    timeoutMs,
				LatencySloMs: latencySloMs,
			},
		},
	}); err != nil {
		return err
	}

	buf := make([]byte, streamChunkBytes)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if err := stream.Send(&controlv1.SeedWorkloadRequest{
				Payload: &controlv1.SeedWorkloadRequest_Chunk{Chunk: buf[:n]},
			}); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("failed to read %s: %w", args[0], readErr)
		}
	}

	resp, err := stream.CloseAndReceive()
	if err != nil {
		return err
	}

	hash := resp.Msg.GetHash()
	if len(hash) > shortHexLen {
		hash = hash[:shortHexLen]
	}
	fmt.Fprintf(cmd.OutOrStdout(), "seeded %s (%s)\n", resp.Msg.GetName(), hash)
	return nil
}

func runUnseed(cmd *cobra.Command, args []string, env *cliEnv) error {
	identifier := args[0]

	if _, err := env.client.UnseedWorkload(cmd.Context(), connect.NewRequest(&controlv1.UnseedWorkloadRequest{Hash: identifier})); err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "unseeded %s\n", identifier)
	return nil
}

const (
	defaultCallRetries = 3
	initialRetryDelay  = 50 * time.Millisecond
)

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

// parseMemoryBytes parses a human-readable memory size ("64MiB", "128M",
// "1GiB"). An empty string returns 0, which tells the server to use its
// default.
func parseMemoryBytes(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	n, err := units.RAMInBytes(s)
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, errors.New("size must be positive")
	}
	return uint64(n), nil
}
