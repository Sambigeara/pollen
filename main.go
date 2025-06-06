package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sambigeara/pollen/pkg/node"
	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{Use: "pollen"}

	nodeCmd := &cobra.Command{
		Use:   "node",
		Short: "Start a Pollen node",
		Run:   runNode,
	}
	nodeCmd.Flags().String("listen", ":8080", "Listen address")
	nodeCmd.Flags().String("peers", "", "Initial peers")

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

	rootCmd.AddCommand(nodeCmd, seedCmd, runCmd)
	rootCmd.Execute()
}

func runNode(cmd *cobra.Command, args []string) {
	addr, _ := cmd.Flags().GetString("listen")
	peers, _ := cmd.Flags().GetString("peers")

	server := grpc.NewServer()
	controlv1.RegisterControlServiceServer(server, node.NewNodeService(addr, peers))

	socketPath := "/tmp/pollen.sock"
	os.Remove(socketPath)

	l, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer os.Remove(socketPath)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.Serve(l)
	}()

	cmd.Print("Successfully started node\n")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		cmd.Printf("Received signal %v, shutting down...\n", sig)
		server.GracefulStop()
	case err := <-serverErr:
		if err != nil {
			log.Fatalf("server error: %v", err)
		}
	}
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
