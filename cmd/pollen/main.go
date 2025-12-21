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
	"time"

	"connectrpc.com/connect"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/net/http2"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/api/genpb/pollen/control/v1/controlv1connect"
	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/observability/logging"
	"github.com/sambigeara/pollen/pkg/server"
	"github.com/sambigeara/pollen/pkg/tcp"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/workspace"
)

const (
	pollenDir      = ".pollen"
	socketName     = "pollen.sock"
	defaultUDPPort = "60611"

	defaultTimeout = 5 * time.Second
)

func main() {
	rootCmd := &cobra.Command{Use: "pollen"}
	rootCmd.PersistentFlags().String("dir", defaultRootDir(), "Directory where Pollen state is persisted")
	rootCmd.AddCommand(
		newNodeCmd(),
		newInviteCmd(),
		newServeCmd(),
		newConnectCmd(),
		newPeersCmd(),
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
	cmd.Flags().String("port", defaultUDPPort, "Listening port")
	cmd.Flags().String("join", "", "Invite token to join remote peer")
	return cmd
}

func newInviteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "invite",
		Short: "Generate an invite token for a peer",
		Run:   runInvite,
	}
	cmd.Flags().IPSlice("ips", []net.IP{}, "IP addresses advertised to peers")
	cmd.Flags().String("port", defaultUDPPort, "Port advertised to peers")
	return cmd
}

func newServeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve [port]",
		Short: "Expose a local port to the mesh",
		Args:  cobra.ExactArgs(1),
		Run:   runServe,
	}
	cmd.Flags().String("port", defaultUDPPort, "Listening UDP port for node")
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

func newPeersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "peers",
		Short: "Manage known peers",
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List all peer keys",
		Run:   runListPeers,
	})
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
	defer func() {
		_ = zap.S().Sync()
	}()

	logger := zap.S()
	logger.Infow("starting pollen...", "version", "0.1.0")

	portStr, _ := cmd.Flags().GetString("port")
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

	port, err := strconv.Atoi(portStr)
	if err != nil {
		logger.Fatalf("port '%s' is invalid: %v", portStr, err)
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

func runInvite(cmd *cobra.Command, args []string) {
	logging.Init()
	defer zap.S().Sync() //nolint:errcheck

	logger := zap.S()

	ips, _ := cmd.Flags().GetIPSlice("ips")
	port, _ := cmd.Flags().GetString("port")

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		logger.Fatalf("failed to prepare pollen dir: %v", err)
	}

	ipStrs := make([]string, 0, len(ips))
	for _, ip := range ips {
		ipStrs = append(ipStrs, ip.String())
	}

	if len(ipStrs) == 0 {
		var err error
		ipStrs, err = transport.GetAdvertisableAddrs()
		if err != nil {
			logger.Fatalf("failed to infer public IP")
		}
	}

	token, err := node.NewInvite(ipStrs, port)
	if err != nil {
		logger.Fatalf("failed to generate invite: %v", err)
	}

	encoded, err := node.EncodeToken(token)
	if err != nil {
		logger.Fatalf("failed to encode invite: %v", err)
	}

	admission, err := admission.Load(pollenDir)
	if err != nil {
		logger.Fatalf("failed to load peers store: %v", err)
	}
	defer func() {
		if err := admission.Save(); err != nil {
			logger.Errorf("failed to save admission: %w", err)
		}
	}()

	admission.AddInvite(token)

	fmt.Fprint(cmd.OutOrStdout(), encoded)
}

func runListPeers(cmd *cobra.Command, args []string) {
	client := newControlClient(cmd)

	resp, err := client.ListPeers(context.Background(), connect.NewRequest(&controlv1.ListPeersRequest{}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	for _, key := range resp.Msg.GetKeys() {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%x\n", key)
	}
}

func runServe(cmd *cobra.Command, args []string) {
	logging.Init()
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		log.Fatal(err)
	}
	portStr, err := cmd.Flags().GetString("port")
	if err != nil {
		log.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("invalid port %q: %v", portStr, err)
	}

	conf := &node.Config{
		Port:                  port,
		GossipInterval:        defaultTimeout,
		PeerReconcileInterval: defaultTimeout,
		PollenDir:             pollenDir,
	}

	n, err := node.New(conf)
	if err != nil {
		log.Fatal(err)
	}

	targetPort := args[0]
	n.Tunnel.SetIncomingHandler(func(tunnelConn net.Conn) {
		localConn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", "localhost"+targetPort)
		if err != nil {
			log.Printf("Failed to dial local service: %v", err)
			tunnelConn.Close()
			return
		}
		log.Printf("Proxying tunnel to %s", targetPort)
		tcp.Bridge(tunnelConn, localConn)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := n.Start(ctx, nil); err != nil {
		log.Print("Failed to start node")
	}
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

	logging.Init()
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		log.Fatal(err)
	}

	conf := &node.Config{
		GossipInterval:        defaultTimeout,
		PeerReconcileInterval: defaultTimeout,
		PollenDir:             pollenDir,
	}

	n, err := node.New(conf)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancelFn()

	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":"+localPort)
	if err != nil {
		log.Printf("Failed to listen on %s: %v", localPort, err)
		return
	}

	peerKey, err := hex.DecodeString(peerHex)
	if err != nil {
		log.Printf("Failed to decode peer hex: %v", err)
		return
	}

	if _, ok := n.Directory.IdentityPub(peerKey); !ok {
		log.Print("Node not recognised, offline, or missing identity keys")
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := n.Start(ctx, nil); err != nil {
			log.Printf("Node stopped: %v", err)
			os.Exit(1)
		}
	}()

	log.Printf("Listening on :%s -> Tunnelling to %s...", localPort, peerHex)

	go func() {
		for {
			clientConn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				meshConn, err := n.Tunnel.Dial(ctx, peerKey)
				if err != nil {
					log.Printf("Tunnel failed: %v", err)
					clientConn.Close()
					return
				}
				tcp.Bridge(clientConn, meshConn)
			}()
		}
	}()

	<-ctx.Done()
}

func newControlClient(cmd *cobra.Command) controlv1connect.ControlServiceClient {
	pollenDir, err := pollenPath(cmd)
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
		Timeout:   defaultTimeout,
		Transport: transport,
	}

	return controlv1connect.NewControlServiceClient(
		httpClient,
		"http://unix",
		connect.WithGRPC(),
	)
}
