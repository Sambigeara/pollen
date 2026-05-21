// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"golang.org/x/net/http2"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/api/genpb/pollen/control/v1/controlv1connect"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/plnfs"
	"github.com/sambigeara/pollen/pkg/wire"
)

const (
	plnDir              = ".pln"
	socketName          = "pln.sock"
	callWorkloadTimeout = 60 * time.Second
	minPort             = 1
	maxPort             = 65535
	osLinux             = "linux"
	osDarwin            = "darwin"
	installScriptURL    = "https://pln.sh/install.sh"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

type cliEnv struct {
	client    controlv1connect.ControlServiceClient
	cfg       *config.Config
	dir       string
	ctxName   string
	transport transportSelection
}

type envConfig struct {
	wantsRoot     bool
	localOnly     bool
	systemService bool
}

type envOption func(*envConfig)

func wantsRoot() envOption     { return func(c *envConfig) { c.wantsRoot = true } }
func localOnly() envOption     { return func(c *envConfig) { c.localOnly = true } }
func systemService() envOption { return func(c *envConfig) { c.systemService = true } }

func withEnv(fn func(*cobra.Command, []string, *cliEnv) error, opts ...envOption) func(*cobra.Command, []string) error {
	cfg := envConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return func(cmd *cobra.Command, args []string) error {
		ctxName := resolveContextName()
		if f := cmd.Flag("ctx"); f != nil && cmd.Flags().Changed("ctx") {
			ctxName = f.Value.String()
		}
		defaultDir, _ := cmd.Flags().GetString("dir")
		entry, err := resolveTarget(cmd, ctxName, defaultDir)
		if err != nil {
			return err
		}
		override := transportOverrideFromFlags(cmd)

		var transport transportSelection
		if cfg.localOnly {
			if override == overrideWire {
				return errors.New("--wire is not applicable to commands that run locally")
			}
			// SSH-bridge would ship the command to a remote node.
			if entry.isSSHBridge() {
				return errRemoteUnsupported
			}
			// Force local even when a wire fallback is configured:
			// http2's DialTLS path has no timeout, so an unreachable
			// wire endpoint would hang the command for the OS-level TCP
			// timeout before `pln up` could launch the daemon.
			transport = transportSelection{kind: transportLocal}
		} else {
			transport, err = resolveTransport(entry, override)
			if err != nil {
				return err
			}
		}
		if cfg.systemService {
			if err := ensureSystemServiceContext(ctxName); err != nil {
				return err
			}
		}

		dir := entry.Dir
		if transport.IsLocal() {
			plnfs.SetSystemMode(dir == plnfs.SystemDir || strings.HasPrefix(dir, plnfs.SystemDir+"/"))
			if cfg.wantsRoot {
				escalateToRoot()
			}
			if err := plnfs.EnsureDir(dir); err != nil {
				return fmt.Errorf("ensure pln dir: %w", err)
			}
		}

		cliCfg, _ := config.Load(dir)
		if cliCfg == nil {
			cliCfg = &config.Config{}
		}

		baseURL := "http://unix"
		if transport.IsWire() {
			baseURL = "https://" + transport.WireAddr()
			// Wire callers have no local renewal loop, so renew
			// opportunistically when the grant is within its lead window.
			// Best-effort: the command proceeds on the current still-valid
			// grant and retries next time.
			if err := wire.MaybeRenewGrant(cmd.Context(), dir, transport.WireAddr()); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "pln: grant renewal failed: %v\n", err)
			}
		}

		env := &cliEnv{
			dir:       dir,
			ctxName:   ctxName,
			cfg:       cliCfg,
			transport: transport,
			// No http.Client.Timeout: per-command deadlines own the budget via
			// context.WithTimeout on cmd.Context(). A global wall-clock would
			// otherwise mask real errors and truncate long-lived calls before
			// the server's own timeout fires.
			client: controlv1connect.NewControlServiceClient(
				&http.Client{
					Transport: &http2.Transport{
						AllowHTTP: true,
						DialTLS:   dialTLSFunc(transport, dir),
					},
				},
				baseURL,
				connect.WithGRPC(),
			),
		}

		if err := negotiateProtocol(cmd.Context(), env.client); err != nil {
			return err
		}
		return fn(cmd, args, env)
	}
}

// negotiateProtocol runs the version handshake before the functional
// RPC. A version mismatch or a daemon too old to implement Handshake
// becomes an explicit, typed error. Availability, certificate and
// transport failures are left for the command to surface with its
// richer diagnostics rather than masked behind a generic handshake
// error.
func negotiateProtocol(ctx context.Context, c controlv1connect.ControlServiceClient) error {
	resp, err := c.Handshake(ctx, connect.NewRequest(&controlv1.HandshakeRequest{
		ClientMin: wire.ProtocolMin,
		ClientMax: wire.ProtocolMax,
	}))
	if err != nil {
		if wire.DaemonLacksHandshake(err) {
			return wire.ErrDaemonNoHandshake
		}
		return nil
	}
	return wire.CheckRange(resp.Msg.GetServerMin(), resp.Msg.GetServerMax())
}

func dialTLSFunc(t transportSelection, dir string) func(string, string, *tls.Config) (net.Conn, error) {
	switch t.kind {
	case transportWire:
		return plnNativeDialer(dir, t.WireAddr())
	case transportSSHBridge:
		host := t.SSHHost()
		return func(_, _ string, _ *tls.Config) (net.Conn, error) {
			return sshBridgeDial(host)
		}
	case transportLocal:
		return func(_, _ string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(context.Background(), "unix", filepath.Join(dir, socketName))
		}
	}
	panic("dialTLSFunc: unreachable")
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "pln",
		Short: "Peer-to-peer mesh networking",
		Long: `Pollen runs a zero-trust, leaderless mesh and a WASM workload
runtime out of a single static binary. Nodes gossip their state, route
traffic over QUIC, and decide locally whether to claim replicas — there
is no scheduler.

Two commands to a cluster:

  pln init                                # creates a new cluster rooted here
  pln bootstrap ssh user@host [--admin]   # adds nodes via SSH`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	rootCmd.PersistentFlags().String("dir", defaultRootDir(), "Directory where Pollen state is persisted (env: PLN_DIR)")
	rootCmd.PersistentFlags().StringP("host", "H", "", "Target daemon over SSH, e.g. user@host (env: PLN_HOST)")
	rootCmd.PersistentFlags().Bool("local", false, "Force the local daemon transport; error if its socket is not reachable")
	rootCmd.PersistentFlags().Bool("wire", false, "Force the wire fallback transport; error if no wire endpoint is configured")
	rootCmd.MarkFlagsMutuallyExclusive("local", "wire")

	rootCmd.AddCommand(newVersionCmd(), newIDCmd(), newBridgeCmd(), newContextCmds(), newCallCmd(), newInspectCmd(), newShareCmd())
	rootCmd.AddCommand(newDaemonCmds()...)
	rootCmd.AddCommand(newClusterCmds()...)
	rootCmd.AddCommand(newNetworkCmds()...)
	rootCmd.AddCommand(newSeedCmds()...)
	rootCmd.AddCommand(newSetCmds()...)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, errorLine(err))
		os.Exit(exitCodeOf(err))
	}
}

func newVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "version",
		Short:   "Show Pollen version information",
		Long:    "Prints the binary version, commit hash, and build date. Use --short for just the version, suitable for scripting.",
		Example: "  pln version --short",
		Args:    cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			if short, _ := cmd.Flags().GetBool("short"); short {
				fmt.Fprintln(cmd.OutOrStdout(), version)
				return
			}
			fmt.Fprintf(cmd.OutOrStdout(), "version: %s\ncommit: %s\ndate: %s\n", version, commit, date)
		},
	}
	cmd.Flags().Bool("short", false, "Print version only")
	return cmd
}

func defaultRootDir() string {
	if d := os.Getenv("PLN_DIR"); d != "" {
		return d
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = os.Getenv("HOME")
	}
	if os.Getuid() == 0 {
		if name := os.Getenv("SUDO_USER"); name != "" {
			if u, err := user.Lookup(name); err == nil {
				homeDir = u.HomeDir
			}
		}
	}
	home := filepath.Join(homeDir, plnDir)
	if runtime.GOOS != osLinux {
		return home
	}

	sysState := hasState(plnfs.SystemDir)
	homeState := hasState(home)

	switch {
	case sysState && homeState:
		fmt.Fprintf(os.Stderr, "warning: pollen state found in both %s and %s; using %s\n", plnfs.SystemDir, home, plnfs.SystemDir)
		return plnfs.SystemDir
	case sysState:
		return plnfs.SystemDir
	case homeState:
		return home
	default:
		if fi, err := os.Stat(plnfs.SystemDir); err == nil && fi.IsDir() {
			return plnfs.SystemDir
		}
		return home
	}
}

func hasState(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "keys", "ed25519.pub"))
	return err == nil
}

func escalateToRoot() {
	if runtime.GOOS != osLinux || os.Getuid() == 0 {
		return
	}
	if !plnfs.SystemMode() {
		return
	}
	plnUser, _ := user.Lookup("pln")
	if plnUser != nil && fmt.Sprint(os.Getuid()) == plnUser.Uid {
		return
	}

	binary, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot resolve binary path: %v\n", err)
		os.Exit(1)
	}
	sudoPath, err := exec.LookPath("sudo")
	if err != nil {
		fmt.Fprintf(os.Stderr, "sudo is required for system installs: %v\n", err)
		os.Exit(1)
	}

	if err := syscall.Exec(sudoPath, append([]string{"sudo", binary}, os.Args[1:]...), os.Environ()); err != nil {
		fmt.Fprintf(os.Stderr, "failed to escalate to root: %v\n", err)
		os.Exit(1)
	}
}
