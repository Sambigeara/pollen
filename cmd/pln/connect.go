package main

import (
	"fmt"
	"os"
	"strconv"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/config"
)

func newConnectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "connect <service> [provider] [local-port]",
		Short: "Tunnel a local port to a service",
		Long: `Tunnel a local port to a service.

If multiple providers serve the same name, use the suffixed form shown by
"pln status" (e.g. "pln connect http-a").`,
		Args: cobra.RangeArgs(1, 3), //nolint:mnd
		Run:  runConnect,
	}
}

func newDisconnectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disconnect <service|local-port> [provider]",
		Short: "Close a tunnel to a service",
		Long: `Close a tunnel to a service.

When the argument is a number it matches by local port. Otherwise it matches
by service name, and an optional provider argument filters by provider.

If multiple providers serve the same name, use the suffixed form shown by
"pln status" (e.g. "pln disconnect http-a").`,
		Args: cobra.RangeArgs(1, 2), //nolint:mnd
		Run:  runDisconnect,
	}
}

func runConnect(cmd *cobra.Command, args []string) {
	serviceArg := args[0]
	var providerArg, localPortArg string
	switch len(args) {
	case 3: //nolint:mnd
		providerArg, localPortArg = args[1], args[2]
	case 2: //nolint:mnd
		if isPortArg(args[1]) {
			localPortArg = args[1]
		} else {
			providerArg = args[1]
		}
	}

	var localPort uint32
	if localPortArg != "" {
		p, err := strconv.Atoi(localPortArg)
		if err != nil || p < minPort || p > maxPort {
			fmt.Fprintln(cmd.ErrOrStderr(), "invalid local port")
			os.Exit(1)
		}
		localPort = uint32(p)
	}

	client := newControlClient(cmd)
	statusResp, err := client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	svc, err := resolveService(statusResp.Msg, serviceArg, providerArg)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	connectResp, err := client.ConnectService(cmd.Context(), connect.NewRequest(&controlv1.ConnectServiceRequest{
		Node:       &controlv1.NodeRef{PeerId: svc.GetProvider().GetPeerId()},
		RemotePort: svc.GetPort(),
		LocalPort:  localPort,
	}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	actualLocalPort := connectResp.Msg.GetLocalPort()
	provider := formatPeerID(svc.GetProvider().GetPeerId(), false)
	peerHex := peerKeyString(svc.GetProvider().GetPeerId())

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to prepare pln dir: %v\n", err)
		os.Exit(1)
	}
	cfg := loadConfigOrDefault(pollenDir)
	cfg.AddConnection(svc.GetName(), peerHex, svc.GetPort(), actualLocalPort)
	if saveErr := config.Save(pollenDir, cfg); saveErr != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to persist connection to config: %v\n", saveErr)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "forwarding localhost:%d -> %s (%s:%d)\n", actualLocalPort, svc.GetName(), provider, svc.GetPort())
}

func runDisconnect(cmd *cobra.Command, args []string) {
	arg := args[0]
	var providerArg string
	if len(args) > 1 {
		providerArg = args[1]
	}

	client := newControlClient(cmd)
	statusResp, err := client.GetStatus(cmd.Context(), connect.NewRequest(&controlv1.GetStatusRequest{}))
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	conn, err := resolveConnection(statusResp.Msg, arg, providerArg)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	if _, err := client.DisconnectService(cmd.Context(), connect.NewRequest(&controlv1.DisconnectServiceRequest{
		LocalPort: conn.GetLocalPort(),
	})); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	peerHex := peerKeyString(conn.GetPeer().GetPeerId())
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to prepare pln dir: %v\n", err)
		os.Exit(1)
	}
	cfg := loadConfigOrDefault(pollenDir)
	cfg.RemoveConnection(conn.GetServiceName(), peerHex, conn.GetLocalPort())
	if saveErr := config.Save(pollenDir, cfg); saveErr != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to persist disconnection to config: %v\n", saveErr)
	}

	provider := formatPeerID(conn.GetPeer().GetPeerId(), false)
	name := conn.GetServiceName()
	if name == "" {
		name = strconv.FormatUint(uint64(conn.GetRemotePort()), 10)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "disconnected localhost:%d from %s (%s:%d)\n",
		conn.GetLocalPort(), name, provider, conn.GetRemotePort())
}
