package main

import (
	"fmt"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

func newSeedCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "seed <file.wasm>",
		Short: "Deploy a WASM workload to the cluster",
		Args:  cobra.ExactArgs(1),
		Run:   runSeed,
	}
	cmd.Flags().Uint32("replicas", 1, "number of replicas to run across the cluster")
	cmd.Flags().Uint32("memory-pages", 0, "memory limit in 64 KiB pages (0 = default 256 = 16 MiB)")
	cmd.Flags().Uint32("timeout-ms", 0, "per-invocation timeout in milliseconds (0 = default 30s)")
	return cmd
}

func newUnseedCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unseed <hash-prefix>",
		Short: "Stop a running WASM workload",
		Args:  cobra.ExactArgs(1),
		Run:   runUnseed,
	}
}

func newCallCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "call <hash-or-prefix> <function>",
		Short: "Invoke an exported function on a WASM workload",
		Args:  cobra.ExactArgs(2), //nolint:mnd
		Run:   runCall,
	}
	cmd.Flags().String("input", "", "input string to pass to the function")
	cmd.Flags().String("input-file", "", "path to a file whose contents are passed as input")
	return cmd
}

func runSeed(cmd *cobra.Command, args []string) {
	wasmPath := args[0]
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to read %s: %v\n", wasmPath, err)
		os.Exit(1)
	}

	replicas, _ := cmd.Flags().GetUint32("replicas")
	memoryPages, _ := cmd.Flags().GetUint32("memory-pages")
	timeoutMs, _ := cmd.Flags().GetUint32("timeout-ms")

	client := newControlClient(cmd)
	resp, err := client.SeedWorkload(cmd.Context(), connect.NewRequest(&controlv1.SeedWorkloadRequest{
		WasmBytes:   wasmBytes,
		Replicas:    replicas,
		MemoryPages: memoryPages,
		TimeoutMs:   timeoutMs,
	}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "seeded %s\n", resp.Msg.GetHash())
}

func runUnseed(cmd *cobra.Command, args []string) {
	hashPrefix := strings.ToLower(args[0])

	client := newControlClient(cmd)
	_, err := client.UnseedWorkload(cmd.Context(), connect.NewRequest(&controlv1.UnseedWorkloadRequest{
		Hash: hashPrefix,
	}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "unseeded %s\n", hashPrefix)
}

func runCall(cmd *cobra.Command, args []string) {
	hash := strings.ToLower(args[0])
	function := args[1]

	var input []byte
	if inputFile, _ := cmd.Flags().GetString("input-file"); inputFile != "" {
		var err error
		input, err = os.ReadFile(inputFile)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "failed to read %s: %v\n", inputFile, err)
			os.Exit(1)
		}
	} else if inputStr, _ := cmd.Flags().GetString("input"); inputStr != "" {
		input = []byte(inputStr)
	}

	client := newControlClient(cmd)
	resp, err := client.CallWorkload(cmd.Context(), connect.NewRequest(&controlv1.CallWorkloadRequest{
		Hash:     hash,
		Function: function,
		Input:    input,
	}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	cmd.OutOrStdout().Write(resp.Msg.GetOutput()) //nolint:errcheck
	fmt.Fprintln(cmd.OutOrStdout())
}
