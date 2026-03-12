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

func runSeed(cmd *cobra.Command, args []string) {
	wasmPath := args[0]
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to read %s: %v\n", wasmPath, err)
		os.Exit(1)
	}

	replicas, _ := cmd.Flags().GetUint32("replicas")

	client := newControlClient(cmd)
	resp, err := client.SeedWorkload(cmd.Context(), connect.NewRequest(&controlv1.SeedWorkloadRequest{
		WasmBytes: wasmBytes,
		Replicas:  replicas,
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
