package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/cobra"

	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/server"
	"github.com/sambigeara/pollen/pkg/workspace"
)

const (
	pollenRootDir = ".pollen"
	socketName    = "pollen.sock"
)

func main() {
	nodeCmd := &cobra.Command{
		Use:   "pollend",
		Short: "Start a Pollen node",
		Run:   runNode,
	}
	nodeCmd.Flags().String("listen", ":8080", "Listen address")
	nodeCmd.Flags().String("peers", "", "Initial peers")
	nodeCmd.Flags().String("sock", socketName, "UDS for GRPC server")

	if err := nodeCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute command: %q", err)
	}
}

func runNode(cmd *cobra.Command, args []string) {
	log := logrus.New()
	log.Formatter = new(logrus.TextFormatter)
	log.Formatter.(*logrus.TextFormatter).DisableTimestamp = true
	log.Level = logrus.TraceLevel
	log.Out = os.Stdout

	addr, _ := cmd.Flags().GetString("listen")
	peerString, _ := cmd.Flags().GetString("peers")
	sock, _ := cmd.Flags().GetString("sock")

	ctx, stopFunc := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopFunc()

	pollenDir, err := workspace.EnsurePollenDir()
	if err != nil {
		log.Fatal(err)
	}

	var peers []string
	if len(peerString) > 0 {
		peers = strings.Split(peerString, ",")
	}
	n := node.NewNode(log, addr, peers)

	nodeSrv := node.NewNodeService(n)

	cmd.Print("Successfully started node\n")

	p := pool.New().WithContext(ctx).WithCancelOnError().WithFirstError()
	p.Go(func(ctx context.Context) error {
		grpcSrv := server.NewGRPCServer()
		// TODO(saml) this needs to be self healing, in case another local node which originally owned
		// the grpc server port disappears and this one needs to claim it.
		return grpcSrv.TryStart(ctx, nodeSrv, filepath.Join(pollenDir, sock))
	})

	p.Go(func(ctx context.Context) error {
		return n.Start(ctx)
	})

	if err := p.Wait(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		log.Fatal(err)
	}
}
