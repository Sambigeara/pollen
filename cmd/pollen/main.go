package main

import (
	"context"
	"log"
	"path/filepath"
	"strings"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/workspace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/spf13/cobra"
)

const socketName = "pollen.sock"

func main() {
	rootCmd := &cobra.Command{Use: "pollen"}
	rootCmd.PersistentFlags().String("sock", socketName, "UDS for GRPC server")

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

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List all functions",
		Run:   runList,
	}

	rootCmd.AddCommand(seedCmd, runCmd, listCmd)
	rootCmd.Execute()
}

func runSeed(cmd *cobra.Command, args []string) {
	sock, _ := cmd.Flags().GetString("sock")

	if len(args) == 0 {
		cmd.Println("Error: wasm file argument required")
		cmd.Usage()
		return
	}

	wasmFile := args[0]

	client, cleanup, err := connectToControlService(sock)
	if err != nil {
		log.Fatalf("failed to connect to node: %v", err)
	}
	defer cleanup()

	ctx := context.Background()
	_, err = client.Seed(ctx, &controlv1.SeedRequest{
		WasmPath: wasmFile,
	})
	if err != nil {
		log.Fatalf("failed to seed function: %v", err)
	}

	cmd.Printf("Successfully seeded function from %s\n", wasmFile)
}

func runRun(cmd *cobra.Command, args []string) {
	sock, _ := cmd.Flags().GetString("sock")

	if len(args) == 0 {
		cmd.Println("Error: function name required")
		cmd.Usage()
		return
	}

	fn := args[0]

	client, cleanup, err := connectToControlService(sock)
	if err != nil {
		log.Fatalf("failed to connect to node: %v", err)
	}
	defer cleanup()

	ctx := context.Background()
	resp, err := client.Run(ctx, &controlv1.RunRequest{
		Name: fn,
	})
	if err != nil {
		log.Fatalf("failed to run function: %v", err)
	}

	cmd.Printf("%s\n", resp.Result)
}

func runList(cmd *cobra.Command, args []string) {
	sock, _ := cmd.Flags().GetString("sock")

	client, cleanup, err := connectToControlService(sock)
	if err != nil {
		log.Fatalf("failed to connect to node: %v", err)
	}
	defer cleanup()

	ctx := context.Background()
	resp, err := client.ListFunctions(ctx, &controlv1.ListFunctionsRequest{})
	if err != nil {
		log.Fatalf("failed to list functions: %v", err)
	}

	cmd.Printf("%s\n", strings.Join(resp.Functions, "\n"))
}

func connectToControlService(sock string) (controlv1.ControlServiceClient, func() error, error) {
	pollenDir, err := workspace.EnsurePollenDir()
	if err != nil {
		return nil, nil, err
	}

	conn, err := grpc.NewClient("unix:"+filepath.Join(pollenDir, sock), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}

	client := controlv1.NewControlServiceClient(conn)
	return client, conn.Close, nil
}
