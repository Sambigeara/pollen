package main

import (
	"context"
	"crypto/tls"
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

	"github.com/sambigeara/pollen/api/genpb/pollen/control/v1/controlv1connect"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/plnfs"
)

const (
	plnDir               = ".pln"
	socketName           = "pln.sock"
	controlClientTimeout = 10 * time.Second
	callWorkloadTimeout  = 60 * time.Second
	minPort              = 1
	maxPort              = 65535
	osLinux              = "linux"
	osDarwin             = "darwin"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

// cliEnv centralizes common command dependencies to eliminate boilerplate.
type cliEnv struct {
	client controlv1connect.ControlServiceClient
	cfg    *config.Config
	dir    string
}

// withEnv wraps a command function, handling directory setup, config loading, and gRPC client init.
func withEnv(needsRoot bool, fn func(*cobra.Command, []string, *cliEnv) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		dir, _ := cmd.Flags().GetString("dir")
		plnfs.SetSystemMode(dir == plnfs.SystemDir || strings.HasPrefix(dir, plnfs.SystemDir+"/"))

		if needsRoot {
			escalateToRoot()
		}

		if err := plnfs.EnsureDir(dir); err != nil {
			return fmt.Errorf("ensure pln dir: %w", err)
		}

		cfg, _ := config.Load(dir)
		if cfg == nil {
			cfg = &config.Config{}
		}

		env := &cliEnv{
			dir: dir,
			cfg: cfg,
			client: controlv1connect.NewControlServiceClient(
				&http.Client{
					Timeout: controlClientTimeout,
					Transport: &http2.Transport{
						AllowHTTP: true,
						DialTLS: func(_, _ string, _ *tls.Config) (net.Conn, error) {
							return (&net.Dialer{}).DialContext(context.Background(), "unix", filepath.Join(dir, socketName))
						},
					},
				},
				"http://unix",
				connect.WithGRPC(),
			),
		}

		return fn(cmd, args, env)
	}
}

func main() {
	rootCmd := &cobra.Command{
		Use:           "pln",
		Short:         "Peer-to-peer mesh networking",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	rootCmd.PersistentFlags().String("dir", defaultRootDir(), "Directory where Pollen state is persisted (env: PLN_DIR)")

	rootCmd.AddCommand(newVersionCmd(), newIDCmd())
	rootCmd.AddCommand(newDaemonCmds()...)
	rootCmd.AddCommand(newClusterCmds()...)
	rootCmd.AddCommand(newNetworkCmds()...)
	rootCmd.AddCommand(newWorkloadCmds()...)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Show Pollen version information",
		Args:  cobra.NoArgs,
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
