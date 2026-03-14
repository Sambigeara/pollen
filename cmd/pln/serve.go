package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/config"
)

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve <port> [name]",
		Short: "Expose a local port to the mesh",
		Args:  cobra.RangeArgs(1, 2), //nolint:mnd
		Run:   runServe,
	}
}

func newUnserveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unserve <port|name>",
		Short: "Stop exposing a local service",
		Args:  cobra.ExactArgs(1),
		Run:   runUnserve,
	}
}

func runServe(cmd *cobra.Command, args []string) {
	portStr := args[0]
	name := ""
	if len(args) > 1 {
		name = args[1]
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	pollenDir := mustPollenPath(cmd)

	cfg := loadConfigOrDefault(pollenDir)
	cfg.AddService(name, uint32(port))

	sockPath := filepath.Join(pollenDir, socketName)
	if running, _ := nodeSocketActive(sockPath); running {
		client := newControlClient(cmd)
		req := &controlv1.RegisterServiceRequest{Port: uint32(port)}
		if name != "" {
			req.Name = &name
		}
		if _, err = client.RegisterService(cmd.Context(), connect.NewRequest(req)); err != nil {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
			os.Exit(1)
		}
	}

	if err := config.Save(pollenDir, cfg); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	if name != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Registered service %s on port: %s\n", name, portStr)
		return
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Registered service on port: %s\n", portStr)
}

func runUnserve(cmd *cobra.Command, args []string) {
	arg := args[0]
	var port uint32
	var name string
	if isPortArg(arg) {
		p, _ := strconv.Atoi(arg)
		port = uint32(p)
	} else {
		name = arg
	}

	pollenDir := mustPollenPath(cmd)

	cfg := loadConfigOrDefault(pollenDir)
	if name != "" {
		cfg.RemoveService(name)
	} else {
		cfg.RemoveServiceByPort(port)
	}

	sockPath := filepath.Join(pollenDir, socketName)
	if running, _ := nodeSocketActive(sockPath); running {
		client := newControlClient(cmd)
		req := &controlv1.UnregisterServiceRequest{Port: port}
		if name != "" {
			req.Name = &name
		}
		if _, err := client.UnregisterService(cmd.Context(), connect.NewRequest(req)); err != nil {
			fmt.Fprintln(cmd.ErrOrStderr(), err)
			os.Exit(1)
		}
	}

	if err := config.Save(pollenDir, cfg); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		os.Exit(1)
	}

	if name != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Unregistered service %s\n", name)
		return
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Unregistered service on port: %d\n", port)
}
