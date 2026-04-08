package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/observability/logging"
	"github.com/sambigeara/pollen/pkg/plnfs"
	"github.com/sambigeara/pollen/pkg/supervisor"
	"github.com/sambigeara/pollen/pkg/types"
)

func newDaemonCmds() []*cobra.Command {
	upCmd := &cobra.Command{
		Use:   "up",
		Short: "Start a Pollen node (foreground by default, -d for background service)",
		RunE:  withEnv(false, runUp),
	}
	upCmd.Flags().Int("port", config.DefaultBootstrapPort, "Listening port")
	upCmd.Flags().IPSlice("ips", []net.IP{}, "Advertised IPs")
	upCmd.Flags().Bool("public", false, "Mark this node as publicly accessible (relay)")
	upCmd.Flags().BoolP("detach", "d", false, "Run as a background service")
	upCmd.Flags().Bool("metrics", false, "Log metrics and trace output at debug level")
	upCmd.Flags().String("name", "", "Human-readable node name")

	upgradeCmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade pln to the latest version",
		Args:  cobra.NoArgs,
		RunE:  runUpgrade,
	}
	upgradeCmd.Flags().Bool("restart", false, "Restart the background service after upgrading")

	logsCmd := &cobra.Command{
		Use:   "logs",
		Short: "Show daemon logs",
		Args:  cobra.NoArgs,
		RunE:  runLogs,
	}
	logsCmd.Flags().BoolP("follow", "f", false, "Stream logs in real time")
	logsCmd.Flags().IntP("lines", "n", 50, "Number of lines to show") //nolint:mnd

	return []*cobra.Command{
		upCmd,
		upgradeCmd,
		logsCmd,
		{Use: "down", Short: "Stop the background service", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return servicectl("stop", cmd) }},
		{Use: "restart", Short: "Restart the background service", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return servicectl("restart", cmd) }},
		{Use: "provision", Short: "Create the pln system user, group, and state directories", RunE: runProvision},
	}
}

func runUp(cmd *cobra.Command, _ []string, env *cliEnv) error {
	detach, _ := cmd.Flags().GetBool("detach")
	public, _ := cmd.Flags().GetBool("public")

	if public {
		if _, _, err := auth.LoadAdminKey(env.dir); err != nil {
			return errors.New("--public requires admin keys; run `pln init` to create a root cluster")
		}
		env.cfg.Public = true
		if err := config.Save(env.dir, env.cfg); err != nil {
			return err
		}
	}

	if cmd.Flags().Changed("name") {
		name, _ := cmd.Flags().GetString("name")
		env.cfg.Name = name
		if err := config.Save(env.dir, env.cfg); err != nil {
			return err
		}
	}

	if detach {
		return servicectl("start", cmd)
	}

	sockPath := filepath.Join(env.dir, socketName)
	if nodeSocketActive(sockPath) {
		return errors.New("a node is already running; use `pln down` to stop the background service")
	}

	return runNode(cmd, env)
}

func runNode(cmd *cobra.Command, env *cliEnv) error {
	zapLogger, err := logging.New()
	if err != nil {
		return fmt.Errorf("can't initialize zap logger: %w", err)
	}
	zap.ReplaceGlobals(zapLogger.Named("pln"))
	defer func() { _ = zap.S().Sync() }()

	logger := zap.S()
	ctx, stopFunc := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopFunc()

	privKey, pubKey, err := auth.EnsureIdentityKey(env.dir)
	if err != nil {
		return fmt.Errorf("failed to load signing keys: %w", err)
	}

	creds, err := auth.LoadNodeCredentials(env.dir)
	if err != nil {
		if !errors.Is(err, auth.ErrCredentialsNotFound) {
			return err
		}
		logger.Info("node is not initialized; auto-initializing root cluster")
		creds, err = auth.EnsureLocalRootCredentials(env.dir, pubKey, time.Now(), config.DefaultMembershipTTL, config.DefaultDelegationTTL)
		if err != nil {
			return fmt.Errorf("auto-init failed: %w", err)
		}
	} else if auth.IsCertExpired(creds.Cert(), time.Now()) {
		logger.Warnw("delegation certificate has expired — starting in degraded mode, will attempt renewal", "expired_at", auth.CertExpiresAt(creds.Cert()))
	}

	signer, err := auth.NewDelegationSigner(env.dir, privKey, config.DefaultDelegationTTL)
	if err == nil {
		creds.SetDelegationKey(signer)
	}

	var runtimeState *statev1.RuntimeState
	if raw, readErr := os.ReadFile(filepath.Join(env.dir, "state.pb")); readErr == nil && len(raw) > 0 {
		runtimeState = loadRuntimeState(raw)
	}

	var consumedEntries []*statev1.ConsumedInvite
	if runtimeState != nil {
		consumedEntries = runtimeState.GetConsumedInvites()
	}
	inviteConsumer := auth.NewInviteConsumer(consumedEntries)

	bootstrapPeers := make([]supervisor.BootstrapTarget, 0, len(env.cfg.BootstrapPeers))
	for peerHex, addrs := range env.cfg.BootstrapPeers {
		pk, _ := types.PeerKeyFromString(peerHex)
		bootstrapPeers = append(bootstrapPeers, supervisor.BootstrapTarget{PeerKey: pk, Addrs: addrs})
	}

	metricsEnabled, _ := cmd.Flags().GetBool("metrics")
	port, _ := cmd.Flags().GetInt("port")

	var addrs []string
	if cmd.Flags().Changed("ips") {
		ips, _ := cmd.Flags().GetIPSlice("ips")
		for _, ip := range ips {
			addrs = append(addrs, ip.String())
		}
	}

	var initialConns []supervisor.ConnectionEntry
	for _, conn := range env.cfg.Connections {
		peerKey, parseErr := types.PeerKeyFromString(conn.Peer)
		if parseErr != nil {
			continue
		}
		initialConns = append(initialConns, supervisor.ConnectionEntry{PeerKey: peerKey, RemotePort: conn.RemotePort, LocalPort: conn.LocalPort, Protocol: configProtocolToProto(conn.Protocol)})
	}

	initialServices := make([]supervisor.ServiceEntry, 0, len(env.cfg.Services))
	for _, svc := range env.cfg.Services {
		initialServices = append(initialServices, supervisor.ServiceEntry{Name: svc.Name, Port: svc.Port, Protocol: configProtocolToProto(svc.Protocol)})
	}

	n, err := supervisor.New(supervisor.Options{
		SigningKey:         privKey,
		PollenDir:          env.dir,
		SocketPath:         filepath.Join(env.dir, socketName),
		RuntimeState:       runtimeState,
		ShutdownFunc:       stopFunc,
		ListenPort:         port,
		GossipInterval:     10 * time.Second, //nolint:mnd
		PeerTickInterval:   time.Second,
		AdvertisedIPs:      addrs,
		BootstrapPeers:     bootstrapPeers,
		InitialConnections: initialConns,
		InitialServices:    initialServices,
		MaxConnectionAge:   24 * time.Hour, //nolint:mnd
		BootstrapPublic:    env.cfg.Public,
		NodeName:           env.cfg.Name,
		MetricsEnabled:     metricsEnabled,
		CPUBudgetPercent:   env.cfg.Resources.CPUPercent,
		MemBudgetPercent:   env.cfg.Resources.MemPercent,
	}, creds, inviteConsumer)
	if err != nil {
		return err
	}

	logger.Info("successfully started node")
	if err := n.Run(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func runProvision(cmd *cobra.Command, _ []string) error {
	if runtime.GOOS != osLinux {
		fmt.Fprintln(cmd.OutOrStdout(), "provisioning is not required on this platform")
		return nil
	}
	if os.Getuid() != 0 {
		return errors.New("pln provision must be run as root")
	}

	dir := plnfs.SystemDir
	if cmd.Flags().Changed("dir") {
		dir, _ = cmd.Flags().GetString("dir")
	}
	plnfs.SetSystemMode(true)

	if err := plnfs.Provision(dir); err != nil {
		return err
	}

	if sudoUser := os.Getenv("SUDO_USER"); sudoUser != "" {
		if err := plnfs.AddUserToPlnGroup(sudoUser); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "warning: %v\n", err)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "added %s to the pln group -- log out and back in for CLI access without sudo\n", sudoUser)
		}
	}
	fmt.Fprintln(cmd.OutOrStdout(), "provisioned "+dir)
	return nil
}

func runLogs(cmd *cobra.Command, _ []string) error {
	follow, _ := cmd.Flags().GetBool("follow")
	lines, _ := cmd.Flags().GetInt("lines")

	var args []string
	var bin string

	if runtime.GOOS == osDarwin { //nolint:nestif
		prefix := "/usr/local"
		if out, err := exec.CommandContext(cmd.Context(), "brew", "--prefix").Output(); err == nil {
			prefix = strings.TrimSpace(string(out))
		} else if _, err := os.Stat("/opt/homebrew"); err == nil {
			prefix = "/opt/homebrew"
		}
		bin = "tail"
		args = []string{"-n", strconv.Itoa(lines)}
		if follow {
			args = append(args, "-f")
		}
		args = append(args, filepath.Join(prefix, "var", "log", "pln.log"))
	} else {
		bin = "journalctl"
		args = []string{"-u", "pln", "-n", strconv.Itoa(lines), "--no-pager"}
		if follow {
			args = append(args, "-f")
		}
	}

	c := exec.CommandContext(cmd.Context(), bin, args...)
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()
	return c.Run()
}

func runUpgrade(cmd *cobra.Command, _ []string) error {
	var err error
	switch runtime.GOOS {
	case osDarwin:
		c := exec.CommandContext(cmd.Context(), "brew", "upgrade", "sambigeara/homebrew-pln/pln")
		c.Stdout = cmd.OutOrStdout()
		c.Stderr = cmd.ErrOrStderr()
		err = c.Run()
	case osLinux:
		resp, reqErr := (&http.Client{Timeout: 30 * time.Second}).Get(fmt.Sprintf("https://raw.githubusercontent.com/%s/main/scripts/install.sh", "sambigeara/pollen")) //nolint:mnd,noctx
		if reqErr != nil || resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to fetch install script")
		}
		defer resp.Body.Close()
		script, _ := io.ReadAll(resp.Body)

		c := exec.CommandContext(cmd.Context(), "bash", "-s", "--")
		c.Stdin = bytes.NewReader(script)
		c.Stdout = cmd.OutOrStdout()
		c.Stderr = cmd.ErrOrStderr()
		err = c.Run()
	default:
		return errors.New("upgrade is only supported on macOS and Linux")
	}

	if err != nil {
		return err
	}

	if restart, _ := cmd.Flags().GetBool("restart"); restart {
		return servicectl("restart", cmd)
	}
	return nil
}

func servicectl(action string, cmd *cobra.Command) error {
	ctx := cmd.Context()
	var c *exec.Cmd
	switch runtime.GOOS {
	case osDarwin:
		c = exec.CommandContext(ctx, "brew", "services", action, "pln")
	case osLinux:
		if os.Getuid() == 0 {
			c = exec.CommandContext(ctx, "systemctl", action, "pln")
		} else {
			c = exec.CommandContext(ctx, "sudo", "systemctl", action, "pln")
		}
	default:
		return errors.New("daemon management is only supported on macOS and Linux")
	}
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()
	return c.Run()
}

func nodeSocketActive(sockPath string) bool {
	if _, err := os.Stat(sockPath); err != nil {
		return false
	}
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(context.Background(), "unix", sockPath)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func loadRuntimeState(raw []byte) *statev1.RuntimeState {
	var rs statev1.RuntimeState
	if err := rs.UnmarshalVT(raw); err == nil && (len(rs.GetGossipState()) > 0 || len(rs.GetPeers()) > 0 || len(rs.GetConsumedInvites()) > 0) {
		return &rs
	}
	return &statev1.RuntimeState{GossipState: raw}
}
