package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"google.golang.org/grpc"

	"github.com/sambigeara/pollen/pkg/node"
	"github.com/spf13/cobra"
)

func main() {
	nodeCmd := &cobra.Command{
		Use:   "pollend",
		Short: "Start a Pollen node",
		Run:   runNode,
	}
	nodeCmd.Flags().String("listen", ":8080", "Listen address")
	nodeCmd.Flags().String("peers", "", "Initial peers")

	if err := nodeCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute command: %q", err)
	}
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
