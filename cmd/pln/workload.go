package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

func newWorkloadCmds() []*cobra.Command {
	seedCmd := &cobra.Command{Use: "seed <file.wasm>", Short: "Deploy a WASM workload", Args: cobra.ExactArgs(1), RunE: withEnv(false, runSeed)}
	seedCmd.Flags().String("name", "", "workload name (defaults to filename without .wasm suffix)")
	seedCmd.Flags().Uint32("min-replicas", 2, "minimum number of replicas") //nolint:mnd
	seedCmd.Flags().Bool("all", false, "run on all nodes in the cluster")
	seedCmd.Flags().Uint32("memory-pages", 0, "memory limit in 64 KiB pages")
	seedCmd.Flags().Uint32("timeout-ms", 0, "per-invocation timeout in ms")

	unseedCmd := &cobra.Command{Use: "unseed <name-or-hash>", Short: "Stop a workload", Args: cobra.ExactArgs(1), RunE: withEnv(false, runUnseed)}

	callCmd := &cobra.Command{Use: "call <name-or-hash> <function>", Short: "Invoke a WASM function", Args: cobra.ExactArgs(2), RunE: withEnv(false, runCall)} //nolint:mnd
	callCmd.Flags().String("input", "", "input string")
	callCmd.Flags().String("input-file", "", "path to input file")

	return []*cobra.Command{seedCmd, unseedCmd, callCmd}
}

func runSeed(cmd *cobra.Command, args []string, env *cliEnv) error {
	wasmBytes, err := os.ReadFile(args[0])
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", args[0], err)
	}

	name, _ := cmd.Flags().GetString("name")
	if name == "" {
		name = strings.TrimSuffix(filepath.Base(args[0]), ".wasm")
	}
	minReplicas, _ := cmd.Flags().GetUint32("min-replicas")
	all, _ := cmd.Flags().GetBool("all")
	memoryPages, _ := cmd.Flags().GetUint32("memory-pages")
	timeoutMs, _ := cmd.Flags().GetUint32("timeout-ms")

	var spread float32
	if all {
		spread = 1.0
	}

	resp, err := env.client.SeedWorkload(cmd.Context(), connect.NewRequest(&controlv1.SeedWorkloadRequest{
		WasmBytes:   wasmBytes,
		Name:        name,
		MinReplicas: minReplicas,
		Spread:      spread,
		MemoryPages: memoryPages,
		TimeoutMs:   timeoutMs,
	}))
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

func runCall(cmd *cobra.Command, args []string, env *cliEnv) error {
	hash := args[0]
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
