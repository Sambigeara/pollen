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
	"strings"
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

	serviceExposeArgsMin = 1
	serviceExposeArgsMax = 2
	connectArgsMin       = 1
	connectArgsMax       = 3
	connectArgsProvider  = 2
	connectArgsLocalPort = 3
	argIndexProvider     = 1
	argIndexLocalPort    = 2
)

func main() {
	rootCmd := &cobra.Command{Use: "pollen"}
	rootCmd.PersistentFlags().String("dir", defaultRootDir(), "Directory where Pollen state is persisted")

	rootCmd.AddCommand(
		newUpCmd(),
		newJoinCmd(),
		newInviteCmd(),
		newStatusCmd(),
		newServiceCmd(),
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

func newUpCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "up",
		Short: "Start a Pollen node",
		Run:   runNode,
	}
	cmd.Flags().Int("port", defaultUDPPort, "Listening port")
	cmd.Flags().IPSlice("ips", []net.IP{}, "Advertisable IPs")
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
	cmd.Flags().Bool("wide", false, "Show full peer IDs and extra details")
	cmd.Flags().Bool("all", false, "Include offline nodes and services")
	return cmd
}

func newServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "service",
		Short: "Manage services",
	}

	listCmd := &cobra.Command{
		Use:     "ls",
		Short:   "List services",
		Aliases: []string{"list"},
		Run:     runServiceList,
	}
	listCmd.Flags().Bool("wide", false, "Show full peer IDs and extra details")
	listCmd.Flags().Bool("all", false, "Include offline services")

	removeCmd := &cobra.Command{
		Use:     "rm <port|name>",
		Short:   "Stop exposing a service",
		Aliases: []string{"remove"},
		Args:    cobra.ExactArgs(1),
		Run:     runServiceRemove,
	}

	exposeCmd := &cobra.Command{
		Use:   "expose [port] [name]",
		Short: "Expose a local port to the mesh",
		Args:  cobra.RangeArgs(serviceExposeArgsMin, serviceExposeArgsMax),
		Run:   runServe,
	}
	exposeCmd.Flags().String("name", "", "Service name")

	cmd.AddCommand(listCmd, exposeCmd, removeCmd)
	return cmd
}

func newServeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve [port]",
		Short: "Expose a local port to the mesh",
		Args:  cobra.ExactArgs(1),
		Run:   runServe,
	}
	cmd.Flags().String("name", "", "Service name")
	return cmd
}

func newConnectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connect <service> [provider] [local-port]",
		Short: "Tunnel a local port to a service",
		Args:  cobra.RangeArgs(connectArgsMin, connectArgsMax),
		Run:   runConnect,
	}
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
	ips, _ := cmd.Flags().GetIPSlice("ips")

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

	var addrs []string
	if len(ips) > 0 {
		addrs = make([]string, len(ips))
		for i, ip := range ips {
			addrs[i] = ip.String()
		}
	}

	conf := &node.Config{
		Port:                port,
		GossipInterval:      defaultTimeout,
		PeerTickInterval:    time.Second,
		PollenDir:           pollenDir,
		AdvertisedIPs:       addrs,
		PunchAttemptTimeout: 3 * time.Second,
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
	wide, _ := cmd.Flags().GetBool("wide")
	includeAll, _ := cmd.Flags().GetBool("all")
	opts := statusViewOpts{wide: wide, includeAll: includeAll}

	client := newControlClient(cmd)
	resp, err := client.GetStatus(context.Background(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	switch mode {
	case "all":
		printSelfLine(cmd, resp.Msg, opts)
		printNodesTable(cmd, resp.Msg, opts)
		hasServices := len(resp.Msg.GetServices()) > 0
		hasConnections := len(resp.Msg.GetConnections()) > 0
		if hasServices {
			fmt.Fprintln(cmd.OutOrStdout())
			printServicesTable(cmd, resp.Msg, opts)
		}
		if hasConnections {
			fmt.Fprintln(cmd.OutOrStdout())
			printConnectionsTable(cmd, resp.Msg, opts)
		}
	case "nodes", "node":
		printSelfLine(cmd, resp.Msg, opts)
		printNodesTable(cmd, resp.Msg, opts)
	case "services", "service", "serve":
		printServicesTable(cmd, resp.Msg, opts)
	default:
		fmt.Fprintf(cmd.ErrOrStderr(), "unknown status selector %q (use: nodes|services)\n", mode)
	}
}

type statusViewOpts struct {
	wide       bool
	includeAll bool
}

func printSelfLine(cmd *cobra.Command, st *controlv1.GetStatusResponse, opts statusViewOpts) {
	if st.GetSelf() == nil || st.GetSelf().GetNode() == nil {
		return
	}
	peer := formatPeerID(st.GetSelf().GetNode().GetPeerId(), opts.wide)
	addr := st.GetSelf().GetAddr()
	if addr == "" {
		addr = "-"
	}
	fmt.Fprintf(cmd.OutOrStdout(), "self  %s  online  %s\n\n", peer, addr)
}

//nolint:mnd
func printNodesTable(cmd *cobra.Command, st *controlv1.GetStatusResponse, opts statusViewOpts) {
	if len(st.Nodes) == 0 {
		return
	}
	filtered := 0
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tSTATUS\tADDR")

	for _, n := range st.Nodes {
		if !opts.includeAll && n.GetStatus() != controlv1.NodeStatus_NODE_STATUS_ONLINE {
			filtered++
			continue
		}
		peer := formatPeerID(n.GetNode().GetPeerId(), opts.wide)
		status := formatStatus(n.GetStatus())
		addr := n.GetAddr()
		if addr == "" {
			addr = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", peer, status, addr)
	}
	_ = w.Flush()

	if filtered > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "offline nodes: %d (use --all)\n", filtered)
	}
}

//nolint:mnd
func printServicesTable(cmd *cobra.Command, st *controlv1.GetStatusResponse, opts statusViewOpts) {
	if len(st.Services) == 0 {
		return
	}
	onlineProviders := map[string]bool{}
	if st.GetSelf() != nil && st.GetSelf().GetNode() != nil {
		selfID := peerKeyString(st.GetSelf().GetNode().GetPeerId())
		onlineProviders[selfID] = true
	}
	for _, n := range st.Nodes {
		if n.GetStatus() == controlv1.NodeStatus_NODE_STATUS_ONLINE {
			id := peerKeyString(n.GetNode().GetPeerId())
			onlineProviders[id] = true
		}
	}

	filtered := 0
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "SERVICE\tPROVIDER\tPORT")

	for _, s := range st.Services {
		providerID := peerKeyString(s.GetProvider().GetPeerId())
		if !opts.includeAll && !onlineProviders[providerID] {
			filtered++
			continue
		}
		provider := formatPeerID(s.GetProvider().GetPeerId(), opts.wide)
		fmt.Fprintf(w, "%s\t%s\t%d\n", s.GetName(), provider, s.GetPort())
	}
	_ = w.Flush()

	if filtered > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "offline services: %d (use --all)\n", filtered)
	}
}

//nolint:mnd
func printConnectionsTable(cmd *cobra.Command, st *controlv1.GetStatusResponse, opts statusViewOpts) {
	if len(st.Connections) == 0 {
		return
	}
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "LOCAL\tSERVICE\tPROVIDER\tREMOTE")

	for _, c := range st.Connections {
		service := c.GetServiceName()
		if service == "" {
			service = strconv.FormatUint(uint64(c.GetRemotePort()), 10)
		}
		provider := formatPeerID(c.GetPeer().GetPeerId(), opts.wide)
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\n", c.GetLocalPort(), service, provider, c.GetRemotePort())
	}
	_ = w.Flush()
}

func runServe(cmd *cobra.Command, args []string) {
	portStr := args[0]
	name, _ := cmd.Flags().GetString("name")
	if name == "" && len(args) > 1 {
		name = args[1]
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
	}

	client := newControlClient(cmd)
	var namePtr *string
	if name != "" {
		namePtr = &name
	}
	if _, err = client.RegisterService(context.Background(), connect.NewRequest(&controlv1.RegisterServiceRequest{
		Port: uint32(port),
		Name: namePtr,
	})); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	if name != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Registered service %s on port: %s\n", name, portStr)
		return
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Registered service on port: %s\n", portStr)
}

func runConnect(cmd *cobra.Command, args []string) {
	serviceArg := args[0]
	providerArg := ""
	localPortArg := ""
	if len(args) >= connectArgsProvider {
		if len(args) == connectArgsProvider && isPortArg(args[argIndexProvider]) {
			localPortArg = args[argIndexProvider]
		} else {
			providerArg = args[argIndexProvider]
		}
	}
	if len(args) == connectArgsLocalPort {
		localPortArg = args[argIndexLocalPort]
	}

	localPort := uint32(0)
	if localPortArg != "" {
		p, err := strconv.Atoi(localPortArg)
		if err != nil || p <= 0 || p > 65535 {
			fmt.Fprintln(cmd.ErrOrStderr(), "invalid local port")
			return
		}
		localPort = uint32(p)
	}

	client := newControlClient(cmd)
	statusResp, err := client.GetStatus(context.Background(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	svc, err := resolveService(statusResp.Msg, serviceArg, providerArg)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	connectResp, err := client.ConnectService(context.Background(), connect.NewRequest(&controlv1.ConnectServiceRequest{
		Node:       &controlv1.NodeRef{PeerId: svc.GetProvider().GetPeerId()},
		RemotePort: svc.GetPort(),
		LocalPort:  localPort,
	}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	local := connectResp.Msg.GetLocalPort()
	provider := formatPeerID(svc.GetProvider().GetPeerId(), false)
	fmt.Fprintf(cmd.OutOrStdout(), "forwarding localhost:%d -> %s (%s:%d)\n", local, svc.GetName(), provider, svc.GetPort())
}

func runServiceList(cmd *cobra.Command, _ []string) {
	wide, _ := cmd.Flags().GetBool("wide")
	includeAll, _ := cmd.Flags().GetBool("all")
	opts := statusViewOpts{wide: wide, includeAll: includeAll}

	client := newControlClient(cmd)
	resp, err := client.GetStatus(context.Background(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}
	printServicesTable(cmd, resp.Msg, opts)
}

func runServiceRemove(cmd *cobra.Command, args []string) {
	arg := args[0]
	port := uint32(0)
	name := ""
	if isPortArg(arg) {
		p, _ := strconv.Atoi(arg)
		port = uint32(p)
	} else {
		name = arg
	}

	var namePtr *string
	if name != "" {
		namePtr = &name
	}

	client := newControlClient(cmd)
	if _, err := client.UnregisterService(context.Background(), connect.NewRequest(&controlv1.UnregisterServiceRequest{
		Port: port,
		Name: namePtr,
	})); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	if name != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Unregistered service %s\n", name)
		return
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Unregistered service on port: %d\n", port)
}

func resolveService(st *controlv1.GetStatusResponse, serviceArg, providerArg string) (*controlv1.ServiceSummary, error) {
	if st == nil {
		return nil, errors.New("no status available")
	}

	onlineProviders := map[string]bool{}
	if st.GetSelf() != nil && st.GetSelf().GetNode() != nil {
		selfID := peerKeyString(st.GetSelf().GetNode().GetPeerId())
		onlineProviders[selfID] = true
	}
	for _, n := range st.Nodes {
		if n.GetStatus() == controlv1.NodeStatus_NODE_STATUS_ONLINE {
			onlineProviders[peerKeyString(n.GetNode().GetPeerId())] = true
		}
	}

	portFilter := uint32(0)
	if p, err := strconv.Atoi(serviceArg); err == nil && p > 0 && p <= 65535 {
		portFilter = uint32(p)
	}

	matches := make([]*controlv1.ServiceSummary, 0, len(st.Services))
	for _, svc := range st.Services {
		if portFilter > 0 {
			if svc.GetPort() != portFilter && svc.GetName() != serviceArg {
				continue
			}
		} else if svc.GetName() != serviceArg {
			continue
		}

		providerID := peerKeyString(svc.GetProvider().GetPeerId())
		if !onlineProviders[providerID] {
			continue
		}
		if providerArg != "" && !peerIDHasPrefix(svc.GetProvider().GetPeerId(), providerArg) {
			continue
		}
		matches = append(matches, svc)
	}

	if len(matches) == 0 {
		if providerArg != "" {
			return nil, fmt.Errorf("no online provider for %q on %q", serviceArg, providerArg)
		}
		return nil, fmt.Errorf("no online service match for %q", serviceArg)
	}
	if len(matches) > 1 {
		var b strings.Builder
		fmt.Fprintf(&b, "service %q has multiple providers; use: pollen connect %s <provider>\n", serviceArg, serviceArg)
		for _, svc := range matches {
			provider := formatPeerID(svc.GetProvider().GetPeerId(), false)
			fmt.Fprintf(&b, "- %s (%s:%d)\n", svc.GetName(), provider, svc.GetPort())
		}
		return nil, errors.New(strings.TrimSpace(b.String()))
	}

	return matches[0], nil
}

func peerKeyString(peerID []byte) string {
	if len(peerID) == 0 {
		return ""
	}
	key := types.PeerKeyFromBytes(peerID)
	return (&key).String()
}

func formatPeerID(peerID []byte, wide bool) string {
	full := peerKeyString(peerID)
	if full == "" {
		return "-"
	}
	if wide || len(full) <= 8 {
		return full
	}
	return full[:8]
}

func formatStatus(s controlv1.NodeStatus) string {
	switch s {
	case controlv1.NodeStatus_NODE_STATUS_ONLINE:
		return "online"
	case controlv1.NodeStatus_NODE_STATUS_OFFLINE:
		return "offline"
	default:
		return "offline"
	}
}

func peerIDHasPrefix(peerID []byte, prefix string) bool {
	if prefix == "" {
		return true
	}
	full := peerKeyString(peerID)
	return strings.HasPrefix(full, strings.ToLower(prefix))
}

func isPortArg(s string) bool {
	p, err := strconv.Atoi(s)
	return err == nil && p > 0 && p <= 65535
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
