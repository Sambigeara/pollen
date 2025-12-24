package main

import (
	"context"
	"crypto/tls"
	"encoding/hex"
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
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/net/http2"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/api/genpb/pollen/control/v1/controlv1connect"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/observability/logging"
	"github.com/sambigeara/pollen/pkg/server"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/workspace"
)

const (
	pollenDir      = ".pollen"
	socketName     = "pollen.sock"
	defaultUDPPort = 60611

	defaultTimeout = 5 * time.Second
)

func main() {
	rootCmd := &cobra.Command{Use: "pollen"}
	rootCmd.PersistentFlags().String("dir", defaultRootDir(), "Directory where Pollen state is persisted")

	rootCmd.AddCommand(
		newNodeCmd(),
		newJoinCmd(),
		newInviteCmd(),
		newStatusCmd(),
		newServeCmd(),
		newConnectCmd(),
	)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("failed to execute command: %q", err)
	}
}

func defaultRootDir() string {
	base, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("unable to retrieve user config dir: %v", err)
	}
	return filepath.Join(base, pollenDir)
}

func newNodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Start a Pollen node",
		Run:   runNode,
	}
	cmd.Flags().Int("port", defaultUDPPort, "Listening port")
	cmd.Flags().String("join", "", "Invite token to join remote peer")
	return cmd
}

func newJoinCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "join [token]",
		Short: "Join a Pollen cluster",
		Args:  cobra.ExactArgs(1),
		Run:   joinCluster,
	}
}

func newInviteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "invite",
		Short: "Generate an invite token",
		Run:   runInvite,
	}
	return cmd
}

func newStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status [nodes|services]",
		Short: "Show status",
		Args:  cobra.RangeArgs(0, 1),
		Run:   runStatus,
	}
	return cmd
}

func newServeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve [port]",
		Short: "Expose a local port to the mesh",
		Args:  cobra.ExactArgs(1),
		Run:   runServe,
	}
	return cmd
}

func newConnectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connect [peer-id]",
		Short: "Tunnel a local port to a peer",
		Args:  cobra.ExactArgs(1),
		Run:   runConnect,
	}
	cmd.Flags().String("L", "", "Local port forwarding (e.g., 9090:8080)")
	return cmd
}

func pollenPath(cmd *cobra.Command) (string, error) {
	dir, err := cmd.Flags().GetString("dir")
	if err != nil {
		return "", err
	}
	return workspace.EnsurePollenDir(dir)
}

func runNode(cmd *cobra.Command, args []string) {
	logging.Init()
	defer func() { _ = zap.S().Sync() }()

	logger := zap.S()
	logger.Infow("starting pollen...", "version", "0.1.0")

	port, _ := cmd.Flags().GetInt("port")
	joinToken, _ := cmd.Flags().GetString("join")

	ctx, stopFunc := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopFunc()

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		logger.Fatal(err)
	}

	var tkn *peerv1.Invite
	if joinToken != "" {
		tkn, err = node.DecodeToken(joinToken)
		if err != nil {
			logger.Fatal(err)
		}
	}

	conf := &node.Config{
		Port:                  port,
		GossipInterval:        defaultTimeout,
		PeerReconcileInterval: defaultTimeout,
		PollenDir:             pollenDir,
	}

	n, err := node.New(conf)
	if err != nil {
		logger.Fatal(err)
	}

	nodeSrv := node.NewNodeService(n)

	logger.Info("successfully started node")

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
		logger.Fatal(err)
	}
}

func joinCluster(cmd *cobra.Command, args []string) {
	enc := args[0]

	client := newControlClient(cmd)
	_, err := client.JoinCluster(context.Background(), connect.NewRequest(&controlv1.JoinClusterRequest{
		Token: enc,
	}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}
}

func runInvite(cmd *cobra.Command, args []string) {
	client := newControlClient(cmd)
	resp, err := client.CreateInvite(context.Background(), connect.NewRequest(&controlv1.CreateInviteRequest{}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}
	fmt.Fprint(cmd.OutOrStdout(), resp.Msg.Token)
}

func runStatus(cmd *cobra.Command, args []string) {
	mode := "all"
	if len(args) == 1 {
		mode = args[0]
	}

	client := newControlClient(cmd)
	resp, err := client.GetStatus(context.Background(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	// Sort for stable output
	// sort.Slice(resp.Msg.Nodes, func(i, j int) bool {
	// 	return string(resp.Msg.Nodes[i].Node.PeerId) < string(resp.Msg.Nodes[j].Node.PeerId)
	// })
	// sort.Slice(resp.Msg.Services, func(i, j int) bool {
	// 	if resp.Msg.Services[i].Name == resp.Msg.Services[j].Name {
	// 		return string(resp.Msg.Services[i].Provider.PeerId) < string(resp.Msg.Services[j].Provider.PeerId)
	// 	}
	// 	return resp.Msg.Services[i].Name < resp.Msg.Services[j].Name
	// })

	switch mode {
	case "all":
		printNodesTable(cmd, resp.Msg)
		fmt.Fprintln(cmd.OutOrStdout())
		printServicesTable(cmd, resp.Msg)
	case "nodes", "node":
		printNodesTable(cmd, resp.Msg)
	case "services", "service", "serve":
		printServicesTable(cmd, resp.Msg)
	default:
		fmt.Fprintf(cmd.ErrOrStderr(), "unknown status selector %q (use: nodes|services)\n", mode)
	}
}

func printNodesTable(cmd *cobra.Command, st *controlv1.GetStatusResponse) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "PEER_ID\tSTATUS\tADDR")

	for _, n := range st.Nodes {
		peerKey := types.PeerKeyFromBytes(n.GetNode().GetPeerId())
		// short := peerKey.String()[:12]
		fmt.Fprintf(w, "%s\t%s\t%s\n", peerKey.String(), n.GetStatus().String(), n.Addr)
	}
	_ = w.Flush()
}

func printServicesTable(cmd *cobra.Command, st *controlv1.GetStatusResponse) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tPROVIDER_PEER\tPORT")

	for _, s := range st.Services {
		peerKey := types.PeerKeyFromBytes(s.GetProvider().GetPeerId())
		// short := peerKey.String()[:12]
		fmt.Fprintf(w, "%s\t%s\t%d\n", s.GetName(), peerKey.String(), s.GetPort())
	}
	_ = w.Flush()
}

func runServe(cmd *cobra.Command, args []string) {
	portStr := args[0]

	port, err := strconv.Atoi(portStr)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
	}

	client := newControlClient(cmd)
	if _, err = client.RegisterService(context.Background(), connect.NewRequest(&controlv1.RegisterServiceRequest{
		Port: uint32(port),
	})); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Registered service on port: %s\n", portStr)
}

func runConnect(cmd *cobra.Command, args []string) {
	peerHex := args[0]
	localMap, err := cmd.Flags().GetString("L")
	if err != nil {
		log.Fatal(err)
	}
	localPort := localMap
	if _, _, err := net.SplitHostPort(localMap); err == nil {
		_, localPort, _ = net.SplitHostPort(localMap)
	} else if _, port, err := net.SplitHostPort(":" + localMap); err == nil {
		localPort = port
	}

	peerID, err := hex.DecodeString(peerHex)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	client := newControlClient(cmd)
	_, err = client.ConnectService(context.Background(), connect.NewRequest(&controlv1.ConnectServiceRequest{
		Node: &controlv1.NodeRef{
			PeerId: peerID,
		},
		Port: localPort,
	}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}
}

func newControlClient(cmd *cobra.Command) controlv1connect.ControlServiceClient {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		log.Fatalf("failed to prepare pollen dir: %v", err)
	}

	socket := filepath.Join(pollenDir, socketName)

	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLS: func(_, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(context.Background(), "unix", socket)
		},
	}

	httpClient := &http.Client{
		Timeout:   defaultTimeout,
		Transport: tr,
	}

	return controlv1connect.NewControlServiceClient(
		httpClient,
		"http://unix",
		connect.WithGRPC(),
	)
}
