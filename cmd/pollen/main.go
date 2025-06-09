package main

import (
	"context"
	"log"
	"path/filepath"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/workspace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/spf13/cobra"
)

const socketName = "pollen.sock"

func main() {
	rootCmd := &cobra.Command{Use: "pollen"}

	seedCmd := &cobra.Command{
		Use:   "seed [wasm-file]",
		Short: "Deploy a function to the network",
		Run:   runSeed,
	}
	seedCmd.Flags().String("sock", socketName, "UDS for GRPC server")

	runCmd := &cobra.Command{
		Use:   "run [function-name]",
		Short: "Execute a function",
		Run:   runRun,
	}
	// TODO(saml) can probably dedup by attaching to rootCmd or something?
	runCmd.Flags().String("sock", socketName, "UDS for GRPC server")

	rootCmd.AddCommand(seedCmd, runCmd)
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

	pollenDir, err := workspace.EnsurePollenDir()
	if err != nil {
		log.Fatal(err)
	}

	conn, err := grpc.NewClient("unix:"+filepath.Join(pollenDir, sock), grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func runRun(cmd *cobra.Command, args []string) {
	sock, _ := cmd.Flags().GetString("sock")

	if len(args) == 0 {
		cmd.Println("Error: function name required")
		cmd.Usage()
		return
	}

	fn := args[0]

	// TODO(saml) all this is duplicated, abstract out
	pollenDir, err := workspace.EnsurePollenDir()
	if err != nil {
		log.Fatal(err)
	}

	conn, err := grpc.NewClient("unix:"+filepath.Join(pollenDir, sock), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to node: %v", err)
	}
	defer conn.Close()

	client := controlv1.NewControlServiceClient(conn)

	ctx := context.Background()
	resp, err := client.Run(ctx, &controlv1.RunRequest{
		Name: fn,
	})
	if err != nil {
		log.Fatalf("failed to run function: %v", err)
	}

	cmd.Printf("%s\n", resp.Result)
}
