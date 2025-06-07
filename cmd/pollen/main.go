package main

import (
	"context"
	"log"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{Use: "pollen"}

	seedCmd := &cobra.Command{
		Use:   "seed [wasm-file]",
		Short: "Deploy a function to the network",
		Run:   runSeed,
	}

	runCmd := &cobra.Command{
		Use:   "run [function-name]",
		Short: "Execute a function",
		Run:   runRun,
	}

	rootCmd.AddCommand(seedCmd, runCmd)
	rootCmd.Execute()
}

func runSeed(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		cmd.Println("Error: wasm file argument required")
		cmd.Usage()
		return
	}

	wasmFile := args[0]

	conn, err := grpc.NewClient("unix:///tmp/pollen.sock", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to node: %v", err)
	}
	defer conn.Close()

	client := controlv1.NewControlServiceClient(conn)

	ctx := context.Background()
	_, err = client.Seed(ctx, &controlv1.SeedRequest{
		WasmPath: wasmFile,
	})
	if err != nil {
		log.Fatalf("failed to seed function: %v", err)
	}

	cmd.Printf("Successfully seeded function from %s\n", wasmFile)
}

func runRun(cmd *cobra.Command, args []string) {}
