package main

import (
	"fmt"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

func newWorkloadCmds() []*cobra.Command {
	seedCmd := &cobra.Command{Use: "seed <file.wasm>", Short: "Deploy a WASM workload", Args: cobra.ExactArgs(1), RunE: withEnv(false, runSeed)}
	seedCmd.Flags().Uint32("replicas", 1, "number of replicas")
	seedCmd.Flags().Uint32("memory-pages", 0, "memory limit in 64 KiB pages")
	seedCmd.Flags().Uint32("timeout-ms", 0, "per-invocation timeout in ms")

	unseedCmd := &cobra.Command{Use: "unseed <hash-prefix>", Short: "Stop a workload", Args: cobra.ExactArgs(1), RunE: withEnv(false, runUnseed)}

	callCmd := &cobra.Command{Use: "call <hash-or-prefix> <function>", Short: "Invoke a WASM function", Args: cobra.ExactArgs(2), RunE: withEnv(false, runCall)} //nolint:mnd
	callCmd.Flags().String("input", "", "input string")
	callCmd.Flags().String("input-file", "", "path to input file")

	return []*cobra.Command{seedCmd, unseedCmd, callCmd}
}

func runSeed(cmd *cobra.Command, args []string, env *cliEnv) error {
	wasmBytes, err := os.ReadFile(args[0])
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", args[0], err)
	}

	replicas, _ := cmd.Flags().GetUint32("replicas")
	memoryPages, _ := cmd.Flags().GetUint32("memory-pages")
	timeoutMs, _ := cmd.Flags().GetUint32("timeout-ms")

	resp, err := env.client.SeedWorkload(cmd.Context(), connect.NewRequest(&controlv1.SeedWorkloadRequest{
		WasmBytes:   wasmBytes,
		Replicas:    replicas,
		MemoryPages: memoryPages,
		TimeoutMs:   timeoutMs,
	}))
	if err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "seeded %s\n", resp.Msg.GetHash())
	return nil
}

func runUnseed(cmd *cobra.Command, args []string, env *cliEnv) error {
	hashPrefix := strings.ToLower(args[0])

	if _, err := env.client.UnseedWorkload(cmd.Context(), connect.NewRequest(&controlv1.UnseedWorkloadRequest{Hash: hashPrefix})); err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "unseeded %s\n", hashPrefix)
	return nil
}

func runCall(cmd *cobra.Command, args []string, env *cliEnv) error {
	hash := strings.ToLower(args[0])
	function := args[1]

	var input []byte
	if inputFile, _ := cmd.Flags().GetString("input-file"); inputFile != "" {
		var err error
		if input, err = os.ReadFile(inputFile); err != nil {
			return fmt.Errorf("failed to read %s: %w", inputFile, err)
		}
	} else if inputStr, _ := cmd.Flags().GetString("input"); inputStr != "" {
		input = []byte(inputStr)
	}

	resp, err := env.client.CallWorkload(cmd.Context(), connect.NewRequest(&controlv1.CallWorkloadRequest{
		Hash:     hash,
		Function: function,
		Input:    input,
	}))
	if err != nil {
		return err
	}

	_, _ = cmd.OutOrStdout().Write(resp.Msg.GetOutput())
	fmt.Fprintln(cmd.OutOrStdout())
	return nil
}
