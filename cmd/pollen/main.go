package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/net/http2"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/api/genpb/pollen/control/v1/controlv1connect"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/invites"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/observability/logging"
	"github.com/sambigeara/pollen/pkg/server"
	"github.com/sambigeara/pollen/pkg/workspace"
)

const (
	pollenDir      = ".pollen"
	socketName     = "pollen.sock"
	defaultUDPPort = "60611"
)

func main() {
	base, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("unable to retrieve user config dir: %v", err)
	}
	defaultRootDir := filepath.Join(base, pollenDir)

	rootCmd := &cobra.Command{Use: "pollen"}

	nodeCmd := &cobra.Command{
		Use:   "node",
		Short: "Start a Pollen node",
		Run:   runNode,
	}
	nodeCmd.Flags().String("port", defaultUDPPort, "Listening port")
	nodeCmd.Flags().String("join", "", "Invite token to join remote peer")
	nodeCmd.Flags().String("dir", defaultRootDir, "Directory where Pollen state is persisted")

	inviteCmd := &cobra.Command{
		Use:   "invite",
		Short: "Generate an invite token for a peer",
		Run:   runInvite,
	}
	inviteCmd.Flags().IP("ip", []byte{}, "IP address advertised to peers")
	inviteCmd.Flags().String("port", defaultUDPPort, "Port advertised to peers")
	inviteCmd.Flags().String("dir", defaultRootDir, "Directory where Pollen state is persisted")

	peersCmd := &cobra.Command{
		Use:   "peers",
		Short: "Manage known peers",
	}
	peersListCmd := &cobra.Command{
		Use:   "list",
		Short: "List all peer keys",
		Run:   runListPeers,
	}
	peersListCmd.Flags().String("dir", defaultRootDir, "Directory where Pollen state is persisted")

	peersConnectCmd := &cobra.Command{
		Use:   "connect [peer-id]",
		Short: "Open a TCP tunnel to a peer",
		Args:  cobra.ExactArgs(1),
		Run:   runConnect,
	}
	peersConnectCmd.Flags().String("dir", defaultRootDir, "Directory where Pollen state is persisted")

	peersCmd.AddCommand(peersListCmd, peersConnectCmd)

	rootCmd.AddCommand(nodeCmd, inviteCmd, peersCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute command: %q", err)
	}
}

func runNode(cmd *cobra.Command, args []string) {
	logging.Init()
	defer zap.L().Sync()

	zap.L().Info("starting pollen...", zap.String("version", "0.1.0")) // TODO(saml) retrieve version
	log := zap.S().Named("pollen")

	portStr, _ := cmd.Flags().GetString("port")
	joinToken, _ := cmd.Flags().GetString("join")
	dir, _ := cmd.Flags().GetString("dir")

	ctx, stopFunc := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopFunc()

	pollenDir, err := workspace.EnsurePollenDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	var tkn *peerv1.Invite
	if joinToken != "" {
		tkn, err = node.DecodeToken(joinToken)
		if err != nil {
			log.Fatal(err)
		}
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("port '%s' is invalid: %v", portStr, err)
	}

	n, err := node.New(port, pollenDir)
	if err != nil {
		log.Fatal(err)
	}

	nodeSrv := node.NewNodeService(n)

	cmd.Print("Successfully started node\n")

	p := pool.New().WithContext(ctx).WithCancelOnError().WithFirstError()
	p.Go(func(ctx context.Context) error {
		grpcSrv := server.NewGRPCServer()
		return grpcSrv.Start(ctx, nodeSrv, filepath.Join(pollenDir, socketName))
	})

	p.Go(func(ctx context.Context) error {
		return n.Start(ctx, tkn)
	})

	if err := p.Wait(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		log.Fatal(err)
	}
}

func runInvite(cmd *cobra.Command, args []string) {
	ip, _ := cmd.Flags().GetIP("ip")
	port, _ := cmd.Flags().GetString("port")
	dir, _ := cmd.Flags().GetString("dir")

	pollenDir, err := workspace.EnsurePollenDir(dir)
	if err != nil {
		log.Fatalf("failed to prepare pollen dir: %v", err)
	}

	if ip == nil {
		var err error
		ip, err = mesh.GetPublicIP()
		if err != nil {
			log.Fatalf("failed to infer public IP")
		}
	}

	token, err := node.NewInvite(ip, port)
	if err != nil {
		log.Fatalf("failed to generate invite: %v", err)
	}

	encoded, err := node.EncodeToken(token)
	if err != nil {
		log.Fatalf("failed to encode invite: %v", err)
	}

	invitesStore, err := invites.Load(pollenDir)
	if err != nil {
		log.Fatalf("failed to load peers store: %v", err)
	}
	defer invitesStore.Save()

	invitesStore.AddInvite(token)

	fmt.Fprint(cmd.OutOrStdout(), encoded)
}

func runListPeers(cmd *cobra.Command, args []string) {
	client := newControlClient(cmd)

	resp, err := client.ListPeers(context.Background(), connect.NewRequest(&controlv1.ListPeersRequest{}))
	if err != nil {
		log.Fatal(err)
	}

	for _, key := range resp.Msg.GetKeys() {
		fmt.Printf("%x\n", key)
	}
}

func runConnect(cmd *cobra.Command, args []string) {
	client := newControlClient(cmd)

	resp, err := client.Connect(context.Background(), connect.NewRequest(&controlv1.ConnectRequest{
		PeerId: args[0],
	}))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	fmt.Printf("Success: %s\n", resp.Msg.Status)
}

func newControlClient(cmd *cobra.Command) controlv1connect.ControlServiceClient {
	dir, _ := cmd.Flags().GetString("dir")
	pollenDir, err := workspace.EnsurePollenDir(dir)
	if err != nil {
		log.Fatalf("failed to prepare pollen dir: %v", err)
	}

	socket := filepath.Join(pollenDir, socketName)

	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLS: func(_, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(context.Background(), "unix", socket)
		},
	}

	httpClient := &http.Client{
		Timeout:   time.Second * 10,
		Transport: transport,
	}

	return controlv1connect.NewControlServiceClient(
		httpClient,
		"http://unix",
		connect.WithGRPC(),
	)
}
