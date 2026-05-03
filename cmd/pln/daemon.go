// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/xml"
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
	"google.golang.org/protobuf/types/known/structpb"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/observability/logging"
	"github.com/sambigeara/pollen/pkg/peercache"
	"github.com/sambigeara/pollen/pkg/supervisor"
	"github.com/sambigeara/pollen/pkg/types"
)

func watchAuthzReload(ctx context.Context, n *supervisor.Supervisor, log *zap.SugaredLogger) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	go func() {
		defer signal.Stop(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				err := n.ReloadAuthzMatcher()
				switch {
				case err == nil:
					log.Infow("authz rules reloaded")
				case errors.Is(err, supervisor.ErrNoMatcher):
				default:
					log.Warnw("authz rule reload failed; keeping previous rules", zap.Error(err))
				}
			}
		}
	}()
}

func newDaemonCmds() []*cobra.Command {
	upCmd := &cobra.Command{
		Use:   "up",
		Short: "Start a Pollen node (foreground by default, -d for background service)",
		Long: `Brings the local Pollen node online. By default runs in the foreground
and exits on Ctrl-C. With -d, hands off to the system service manager
(launchd on macOS, systemd on Linux) for unattended operation.

If no cluster credentials exist, auto-initialises a single-node root
cluster — equivalent to running ` + "`pln init`" + ` first.`,
		Example: "  pln up                        # foreground\n  pln up -d                     # detached\n  pln up --public --name relay  # advertise as a relay",
		RunE:    withEnv(runUp, localOnly()),
	}
	upCmd.Flags().Int("port", config.DefaultBootstrapPort, "Listening port")
	upCmd.Flags().IPSlice("ips", []net.IP{}, "Advertised IPs")
	upCmd.Flags().Bool("public", false, "Hint that this node is publicly reachable; the mesh may use it as a relay (verified at runtime)")
	upCmd.Flags().BoolP("detach", "d", false, "Run as a background service")
	upCmd.Flags().Bool("metrics", false, "Log metrics and trace output at debug level")
	upCmd.Flags().String("name", "", "Human-readable node name")

	downCmd := &cobra.Command{
		Use:   "down",
		Short: "Stop the background service",
		Long:  "Stops the launchd (macOS) or systemd (Linux) unit. Local state and credentials are preserved — a subsequent `pln up -d` restarts the same node.",
		Args:  cobra.NoArgs,
		RunE:  withEnv(runDown, localOnly(), systemService()),
	}
	restartCmd := &cobra.Command{
		Use:   "restart",
		Short: "Restart the background service",
		Long:  "Restarts the background service so config changes (`pln set`, `pln serve`, etc.) take effect on listeners and bind addresses.",
		Args:  cobra.NoArgs,
		RunE:  withEnv(runRestart, localOnly(), systemService()),
	}

	logsCmd := &cobra.Command{
		Use:   "logs",
		Short: "Show daemon logs",
		Long: `Tails the background service log: ` + "`tail`" + ` against the Homebrew log file
on macOS, ` + "`journalctl -u pln`" + ` on Linux. Use -f to stream and -n to
control how many lines to show.`,
		Example: "  pln logs -f -n 200",
		Args:    cobra.NoArgs,
		RunE:    withEnv(runLogs, localOnly(), systemService()),
	}
	logsCmd.Flags().BoolP("follow", "f", false, "Stream logs in real time")
	logsCmd.Flags().IntP("lines", "n", 50, "Number of lines to show") //nolint:mnd

	return []*cobra.Command{upCmd, downCmd, restartCmd, logsCmd, newUpgradeCmd(), newDaemonGroupCmd()}
}

func newDaemonGroupCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "daemon",
		Short: "Manage the pln background service (install/uninstall/upgrade)",
	}
	root.AddCommand(newDaemonInstallCmd(), newDaemonUninstallCmd(), newUpgradeCmd())
	return root
}

func newUpgradeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade pln to the latest version",
		Long: `Upgrades the local pln binary via the platform package manager
(Homebrew on macOS, the install script on Linux). Linux upgrades preserve the
current install method by default: /usr/local/bin/pln stays on tarball installs,
while /usr/bin/pln uses distro package-manager detection. Pass --restart to
also bounce the background service and pick up the new binary.`,
		Example: "  pln upgrade --restart\n  pln upgrade --method tarball",
		Args:    cobra.NoArgs,
		RunE:    withEnv(runUpgrade, localOnly()),
	}
	cmd.Flags().Bool("restart", false, "Restart the background service after upgrading")
	cmd.Flags().String("method", "", "Linux install method override: auto or tarball (default: preserve current method)")
	return cmd
}

func runUp(cmd *cobra.Command, _ []string, env *cliEnv) error {
	detach, _ := cmd.Flags().GetBool("detach")
	if detach {
		if err := ensureSystemServiceContext(); err != nil {
			return err
		}
		if cmd.Flags().Changed("port") {
			return fmt.Errorf("--port cannot be set with -d; the service uses baked args. Edit the service unit or run foreground")
		}
	}
	public, _ := cmd.Flags().GetBool("public")

	cfgDirty := false
	if public {
		if _, _, err := auth.LoadAdminKey(auth.IdentityPath(env.dir)); err != nil {
			return errors.New("--public requires admin keys; run `pln init` to create a root cluster")
		}
		env.cfg.Public = true
		cfgDirty = true
	}
	if cmd.Flags().Changed("name") {
		env.cfg.Name, _ = cmd.Flags().GetString("name")
		cfgDirty = true
	}
	if cfgDirty {
		if err := config.Save(env.dir, env.cfg); err != nil {
			return err
		}
	}

	if detach {
		return servicectl("start", cmd, env)
	}

	sockPath := filepath.Join(env.dir, socketName)
	if nodeSocketActive(sockPath) {
		return errors.New("a node is already running; use `pln down` to stop the background service")
	}

	return runNode(cmd, env)
}

func runNode(cmd *cobra.Command, env *cliEnv) error {
	zapLogger, err := logging.New(env.cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("can't initialize zap logger: %w", err)
	}
	zap.ReplaceGlobals(zapLogger.Named("pln"))
	defer func() { _ = zap.S().Sync() }()

	logger := zap.S()
	ctx, stopFunc := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopFunc()

	identityDir := auth.IdentityPath(env.dir)
	privKey, pubKey, err := auth.EnsureIdentityKey(identityDir)
	if err != nil {
		return fmt.Errorf("failed to load signing keys: %w", err)
	}

	var nodeProps *structpb.Struct
	if len(env.cfg.Properties) > 0 {
		nodeProps, err = structpb.NewStruct(env.cfg.Properties)
		if err != nil {
			return fmt.Errorf("invalid node properties: %w", err)
		}
	}

	creds, err := auth.LoadNodeCredentials(identityDir)
	if err != nil && !errors.Is(err, auth.ErrCredentialsNotFound) {
		return err
	}

	_, isRoot, err := auth.LocalRootAuthority(identityDir, creds)
	if err != nil {
		return err
	}

	switch {
	case creds == nil:
		logger.Info("node is not initialized; auto-initializing root cluster")
		creds, err = auth.EnsureLocalRootCredentials(identityDir, pubKey, nodeProps, time.Now(), auth.DefaultDelegationTTL)
		if err != nil {
			return fmt.Errorf("auto-init failed: %w", err)
		}
	case isRoot:
		// Re-issue so property changes apply and the cert refreshes ahead
		// of expiry without peer-routed renewal.
		creds, err = auth.EnsureLocalRootCredentials(identityDir, pubKey, nodeProps, time.Now(), auth.DefaultDelegationTTL)
		if err != nil {
			return fmt.Errorf("root cert refresh: %w", err)
		}
	case auth.IsCertExpired(creds.Cert(), time.Now()):
		logger.Warnw("delegation certificate has expired — starting in degraded mode, will attempt renewal", "expired_at", auth.CertExpiresAt(creds.Cert()))
	}

	signer, err := auth.NewDelegationSigner(identityDir, privKey)
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

	peerCache, err := peercache.Open(env.dir)
	if err != nil {
		return fmt.Errorf("load peer cache: %w", err)
	}

	metricsEnabled, _ := cmd.Flags().GetBool("metrics")
	httpAddr := env.cfg.HTTP
	if httpAddr != "" {
		metricsEnabled = true
	}
	staticAddr := env.cfg.StaticHTTP
	port, _ := cmd.Flags().GetInt("port")

	controlAddr := env.cfg.ControlAddr
	var controlToken string
	if controlAddr != "" {
		t, err := ensureControlToken(env.dir)
		if err != nil {
			return fmt.Errorf("load control token: %w", err)
		}
		controlToken = t
	}

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
		var props *structpb.Struct
		if len(svc.Properties) > 0 {
			s, err := structpb.NewStruct(svc.Properties)
			if err != nil {
				return fmt.Errorf("invalid service %q properties: %w", svc.Name, err)
			}
			props = s
		}
		initialServices = append(initialServices, supervisor.ServiceEntry{Name: svc.Name, Port: svc.Port, Protocol: configProtocolToProto(svc.Protocol), Properties: props})
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
		PeerCache:          peerCache,
		InitialConnections: initialConns,
		InitialServices:    initialServices,
		MaxConnectionAge:   24 * time.Hour, //nolint:mnd
		BootstrapPublic:    env.cfg.Public,
		NodeName:           env.cfg.Name,
		MetricsEnabled:     metricsEnabled,
		HTTPAddr:           httpAddr,
		StaticAddr:         staticAddr,
		ControlAddr:        controlAddr,
		ControlToken:       controlToken,
		IdleInstanceTTL:    env.cfg.Placement.IdleInstanceTTL,
		Authz: supervisor.AuthzOptions{
			Default:      env.cfg.Evaluator.Default,
			Gates:        env.cfg.Evaluator.Gates,
			MatcherRules: env.cfg.Evaluator.MatcherRules,
		},
		RelayOnly: env.cfg.RelayOnly,
	}, creds, inviteConsumer)
	if err != nil {
		return err
	}
	watchAuthzReload(ctx, n, zap.S().Named("authz"))

	logger.Info("successfully started node")
	if err := n.Run(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func runDown(cmd *cobra.Command, _ []string, env *cliEnv) error {
	return servicectl("stop", cmd, env)
}

func runRestart(cmd *cobra.Command, _ []string, env *cliEnv) error {
	return servicectl("restart", cmd, env)
}

func runLogs(cmd *cobra.Command, _ []string, env *cliEnv) error {
	follow, _ := cmd.Flags().GetBool("follow")
	lines, _ := cmd.Flags().GetInt("lines")

	var args []string
	var bin string

	switch {
	case runtime.GOOS == osDarwin && resolveContextName() != defaultContextName:
		bin = "tail"
		args = []string{"-n", strconv.Itoa(lines)}
		if follow {
			args = append(args, "-f")
		}
		args = append(args, userPlnLogPath(env.dir))
	case runtime.GOOS == osDarwin:
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
	default:
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

func runUpgrade(cmd *cobra.Command, _ []string, env *cliEnv) error {
	var err error
	switch runtime.GOOS {
	case osDarwin:
		if cmd.Flags().Changed("method") {
			return errors.New("--method is only supported on Linux")
		}
		c := exec.CommandContext(cmd.Context(), "brew", "upgrade", "sambigeara/homebrew-pln/pln")
		c.Stdout = cmd.OutOrStdout()
		c.Stderr = cmd.ErrOrStderr()
		err = c.Run()
	case osLinux:
		installArgs, argsErr := linuxUpgradeInstallArgs(cmd, os.Executable)
		if argsErr != nil {
			return argsErr
		}
		resp, reqErr := (&http.Client{Timeout: 30 * time.Second}).Get(installScriptURL) //nolint:mnd,noctx
		if reqErr != nil || resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to fetch install script")
		}
		defer resp.Body.Close()
		script, _ := io.ReadAll(resp.Body)

		c := exec.CommandContext(cmd.Context(), "bash", installArgs...)
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
		return servicectl("restart", cmd, env)
	}
	return nil
}

func linuxUpgradeInstallArgs(cmd *cobra.Command, executable func() (string, error)) ([]string, error) {
	method, explicit, err := linuxUpgradeInstallMethod(cmd, executable)
	if err != nil {
		return nil, err
	}
	args := []string{"-s", "--"}
	if explicit || method == installMethodTarball {
		args = append(args, "--method", method)
	}
	return args, nil
}

func linuxUpgradeInstallMethod(cmd *cobra.Command, executable func() (string, error)) (method string, explicit bool, err error) {
	method, _ = cmd.Flags().GetString("method")
	if cmd.Flags().Changed("method") {
		method, err = validateLinuxInstallMethod(method)
		return method, true, err
	}
	exe, err := executable()
	if err != nil {
		return "", false, fmt.Errorf("resolve pln binary path: %w", err)
	}
	method, err = linuxUpgradeInstallMethodForExecutable(exe)
	return method, false, err
}

const (
	installMethodAuto    = "auto"
	installMethodTarball = "tarball"
)

func validateLinuxInstallMethod(method string) (string, error) {
	switch method {
	case installMethodAuto, installMethodTarball:
		return method, nil
	default:
		return "", fmt.Errorf("unknown install method: %s (expected auto|tarball)", method)
	}
}

func linuxUpgradeInstallMethodForExecutable(executable string) (string, error) {
	switch filepath.Clean(executable) {
	case packagePlnBinary:
		return installMethodAuto, nil
	case tarballPlnBinary:
		return installMethodTarball, nil
	default:
		return "", fmt.Errorf("cannot infer Linux install method from %s; rerun with --method auto or --method tarball", executable)
	}
}

func servicectl(action string, cmd *cobra.Command, env *cliEnv) error {
	ctx := cmd.Context()
	name := resolveContextName()

	if runtime.GOOS == osDarwin && name != defaultContextName {
		cf, err := loadContexts()
		if err != nil {
			return err
		}
		port := cf.Contexts[name].Port
		return userLaunchctl(ctx, cmd, action, name, env.dir, port)
	}

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

func userLaunchctl(ctx context.Context, cmd *cobra.Command, action, ctxName, ctxDir string, port int) error {
	plistPath, err := ensureUserPlnPlist(ctxName, ctxDir, port)
	if err != nil {
		return err
	}
	switch action {
	case "start":
		return runLaunchctl(ctx, cmd, "load", "-w", plistPath)
	case "stop":
		return runLaunchctl(ctx, cmd, "unload", "-w", plistPath)
	case "restart":
		_ = runLaunchctl(ctx, cmd, "unload", "-w", plistPath)
		return runLaunchctl(ctx, cmd, "load", "-w", plistPath)
	default:
		return fmt.Errorf("unsupported service action %q", action)
	}
}

func runLaunchctl(ctx context.Context, cmd *cobra.Command, args ...string) error {
	c := exec.CommandContext(ctx, "launchctl", args...)
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()
	return c.Run()
}

func userPlnPlistPath(ctxName string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "LaunchAgents", "sh.pln."+ctxName+".plist"), nil
}

func userPlnLogPath(ctxDir string) string {
	return filepath.Join(ctxDir, "pln.log")
}

// Existing plists are not overwritten so users can customise them.
func ensureUserPlnPlist(ctxName, ctxDir string, port int) (string, error) {
	plistPath, err := userPlnPlistPath(ctxName)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(plistPath); err == nil {
		return plistPath, nil
	}
	binary, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locate pln binary: %w", err)
	}
	if resolved, err := filepath.EvalSymlinks(binary); err == nil {
		binary = resolved
	}
	plist := renderUserPlnPlist(ctxName, binary, ctxDir, port)
	if err := os.MkdirAll(filepath.Dir(plistPath), 0o755); err != nil { //nolint:mnd
		return "", fmt.Errorf("create LaunchAgents dir: %w", err)
	}
	if err := os.WriteFile(plistPath, []byte(plist), 0o644); err != nil { //nolint:mnd,gosec
		return "", fmt.Errorf("write plist: %w", err)
	}
	return plistPath, nil
}

func renderUserPlnPlist(ctxName, binary, ctxDir string, port int) string {
	label := xmlEscape("sh.pln." + ctxName)
	portArgs := ""
	if port > 0 {
		portArgs = `
        <string>--port</string>
        <string>` + strconv.Itoa(port) + `</string>`
	}
	return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>KeepAlive</key>
    <true/>
    <key>Label</key>
    <string>` + label + `</string>
    <key>ProgramArguments</key>
    <array>
        <string>` + xmlEscape(binary) + `</string>
        <string>--dir</string>
        <string>` + xmlEscape(ctxDir) + `</string>
        <string>up</string>` + portArgs + `
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardErrorPath</key>
    <string>` + xmlEscape(userPlnLogPath(ctxDir)) + `</string>
    <key>StandardOutPath</key>
    <string>` + xmlEscape(userPlnLogPath(ctxDir)) + `</string>
</dict>
</plist>
`
}

func xmlEscape(s string) string {
	var buf bytes.Buffer
	_ = xml.EscapeText(&buf, []byte(s))
	return buf.String()
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
