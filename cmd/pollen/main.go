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

	defaultPeerReconcileInterval = time.Second * 5
	defaultGossipInterval        = time.Second * 5
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
	inviteCmd.Flags().IPSlice("ips", []net.IP{}, "IP addresses advertised to peers")
	inviteCmd.Flags().String("port", defaultUDPPort, "Port advertised to peers")
	inviteCmd.Flags().String("dir", defaultRootDir, "Directory where Pollen state is persisted")

	serveCmd := &cobra.Command{
		Use:   "serve [port]",
		Short: "Expose a local port to the mesh",
		Args:  cobra.ExactArgs(1),
		Run:   runServe,
	}
	serveCmd.Flags().String("port", defaultUDPPort, "Listening UDP port for node")
	serveCmd.Flags().String("dir", defaultRootDir, "State directory")

	connectCmd := &cobra.Command{
		Use:   "connect [peer-id]",
		Short: "Tunnel a local port to a peer",
		Args:  cobra.ExactArgs(1),
		Run:   runConnect,
	}
	connectCmd.Flags().String("L", "", "Local port forwarding (e.g., 9090:8080)")
	connectCmd.Flags().String("dir", defaultRootDir, "State directory")

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

	peersCmd.AddCommand(peersListCmd)

	rootCmd.AddCommand(nodeCmd, inviteCmd, serveCmd, connectCmd, peersCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("failed to execute command: %q", err)
	}
}

func runNode(cmd *cobra.Command, args []string) {
	logging.Init()
	defer zap.S().Sync() //nolint:errcheck

	logger := zap.S()
	logger.Infow("starting pollen...", "version", "0.1.0")

	portStr, _ := cmd.Flags().GetString("port")
	joinToken, _ := cmd.Flags().GetString("join")
	dir, _ := cmd.Flags().GetString("dir")

	ctx, stopFunc := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopFunc()

	pollenDir, err := workspace.EnsurePollenDir(dir)
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
		GossipInterval:        defaultGossipInterval,
		PeerReconcileInterval: defaultPeerReconcileInterval,
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

	ips, _ := cmd.Flags().GetStringSlice("ips")
	port, _ := cmd.Flags().GetString("port")
	dir, _ := cmd.Flags().GetString("dir")

	pollenDir, err := workspace.EnsurePollenDir(dir)
	if err != nil {
		logger.Fatalf("failed to prepare pollen dir: %v", err)
	}

	if len(ips) == 0 {
		var err error
		ips, err = transport.GetAdvertisableAddrs()
		if err != nil {
			logger.Fatalf("failed to infer public IP")
		}
	}

	token, err := node.NewInvite(ips, port)
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
		log.Fatal(err)
	}

	for _, key := range resp.Msg.GetKeys() {
		fmt.Printf("%x\n", key)
	}
}

func runServe(cmd *cobra.Command, args []string) {
	// 1. Setup Node (reuse logic from runNode)
	logging.Init()
	portStr, _ := cmd.Flags().GetString("port")
	dir, _ := cmd.Flags().GetString("dir")
	pollenDir, _ := workspace.EnsurePollenDir(dir)
	port, _ := strconv.Atoi(portStr)

	conf := &node.Config{
		Port:                  port,
		GossipInterval:        defaultGossipInterval,
		PeerReconcileInterval: defaultPeerReconcileInterval,
		PollenDir:             pollenDir,
	}

	n, err := node.New(conf)
	if err != nil {
		log.Fatal(err)
	}

	// 2. Wiring: Handle incoming tunnels by dialing localhost
	targetPort := args[0] // e.g., ":8080"
	n.Tunnel.SetIncomingHandler(func(tunnelConn net.Conn) {
		localConn, err := net.Dial("tcp", "localhost"+targetPort)
		if err != nil {
			log.Printf("Failed to dial local service: %v", err)
			tunnelConn.Close()
			return
		}
		log.Printf("Proxying tunnel to %s", targetPort)
		tcp.Bridge(tunnelConn, localConn)
	})

	// 3. Start Node
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := n.Start(ctx, nil); err != nil {
		log.Fatal(err)
	}
}

func runConnect(cmd *cobra.Command, args []string) {
	peerHex := args[0]
	localMap, _ := cmd.Flags().GetString("L") // Expected format "9090:8080" but we only need local port for now
	// Simple parse for "9090:..." -> "9090"
	localPort := localMap
	if _, _, err := net.SplitHostPort(localMap); err == nil {
		_, localPort, _ = net.SplitHostPort(localMap)
	} else if _, port, err := net.SplitHostPort(":" + localMap); err == nil {
		localPort = port
	}

	// 1. Setup Node
	logging.Init()
	dir, _ := cmd.Flags().GetString("dir")
	pollenDir, _ := workspace.EnsurePollenDir(dir)

	conf := &node.Config{
		GossipInterval:        defaultGossipInterval,
		PeerReconcileInterval: defaultPeerReconcileInterval,
		PollenDir:             pollenDir,
	}

	// Use 0 or specific port for initiator node
	n, err := node.New(conf)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelFn()

	// 2. Start Local Listener
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":"+localPort)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", localPort, err)
	}

	peerKey, err := hex.DecodeString(peerHex)
	if err != nil {
		log.Fatalf("Failed to decode peer hex: %v", err)
	}

	if _, ok := n.Directory.IdentityPub(peerKey); !ok {
		log.Fatal("Node not recognised, offline, or missing identity keys")
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
				// 3. Wiring: Dial Peer via Tunnel Manager
				meshConn, err := n.Tunnel.Dial(ctx, peerKey)
				if err != nil {
					log.Printf("Tunnel failed: %v", err)
					clientConn.Close()
					return
				}
				// 4. Bridge
				tcp.Bridge(clientConn, meshConn)
			}()
		}
	}()

	<-ctx.Done()
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
