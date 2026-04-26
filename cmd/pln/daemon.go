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
	"github.com/sambigeara/pollen/pkg/evaluator"
	"github.com/sambigeara/pollen/pkg/evaluator/builtin/matcher"
	"github.com/sambigeara/pollen/pkg/observability/logging"
	"github.com/sambigeara/pollen/pkg/peercache"
	"github.com/sambigeara/pollen/pkg/supervisor"
	"github.com/sambigeara/pollen/pkg/types"
)

// watchSIGHUPReload re-reads authz rules on every SIGHUP. A failed reload
// leaves the previous rules in place — a broken YAML must never brick authz.
func watchSIGHUPReload(ctx context.Context, reload func() error, log *zap.SugaredLogger) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	go func() {
		defer signal.Stop(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				if err := reload(); err != nil {
					log.Warnw("authz rule reload failed; keeping previous rules", zap.Error(err))
					continue
				}
				log.Infow("authz rules reloaded")
			}
		}
	}()
}

// buildAuthzRouter assembles the authorisation router from config.
// Returns nil router (and no error) when no evaluator section is
// configured — supervisor then wires an allow-all default. The
// reload closure is non-nil only when matcher rules are in use, so
// the SIGHUP handler can re-read the file in place without restart.
func buildAuthzRouter(cfg config.Evaluator) (*evaluator.Router, func() error, error) {
	if cfg.Default == "" && len(cfg.Gates) == 0 && cfg.MatcherRules == "" {
		return nil, nil, nil
	}
	var matcherEval evaluator.Evaluator
	var reload func() error
	if cfg.MatcherRules != "" {
		m, err := matcher.NewFromFile(cfg.MatcherRules)
		if err != nil {
			return nil, nil, fmt.Errorf("load matcher rules: %w", err)
		}
		matcherEval = m
		reload = func() error { return m.Reload(cfg.MatcherRules) }
	}
	gates := make(map[evaluator.GateName]string, len(cfg.Gates))
	for k, v := range cfg.Gates {
		gates[evaluator.GateName(k)] = v
	}
	router, err := evaluator.NewRouterFromConfig(evaluator.ConfigSpec{
		Default:          cfg.Default,
		Gates:            gates,
		AttributeMatcher: matcherEval,
		OnDeny:           logDeny(zap.S().Named("authz")),
	})
	if err != nil {
		return nil, nil, err
	}
	return router, reload, nil
}

// logDeny builds the router's DenyObserver sink that structured-logs
// every gate denial. Info level: denies are normal policy operation,
// not errors. service_connect has no other operator-visible signal, so
// this log is its only feedback surface short of the status API.
// Fallback denials (evaluator errored, gate applied its configured
// fallback) are logged at warn so a broken PDP surfaces distinctly
// from a legitimate deny.
func logDeny(log *zap.SugaredLogger) evaluator.DenyObserver {
	return func(e evaluator.DenyEvent) {
		fields := []any{
			"gate", string(e.Gate),
			"subject_type", e.Request.Subject.Type,
			"subject_id", e.Request.Subject.ID,
			"action", e.Request.Action.Name,
			"resource_type", string(e.Request.Resource.Type),
			"resource_id", e.Request.Resource.ID,
			"reason", e.Reason,
			"cached", e.Cached,
		}
		if e.Fallback {
			log.Warnw("authz fallback deny (evaluator unreachable)", fields...)
			return
		}
		log.Infow("authz deny", fields...)
	}
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

	return []*cobra.Command{upCmd, downCmd, restartCmd, logsCmd, newDaemonGroupCmd()}
}

// newDaemonGroupCmd builds the `pln daemon` subtree for admin
// operations on the daemon binary and its background unit (install,
// uninstall, upgrade). Operational lifecycle (`pln up/down/restart/logs`)
// stays flat at the top level.
func newDaemonGroupCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "daemon",
		Short: "Manage the pln background service (install/uninstall/upgrade)",
	}

	upgradeCmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade pln to the latest version",
		Long: `Upgrades the local pln binary via the platform package manager
(Homebrew on macOS, the install script on Linux). Pass --restart to also
bounce the background service and pick up the new binary.`,
		Example: "  pln daemon upgrade --restart",
		Args:    cobra.NoArgs,
		RunE:    withEnv(runUpgrade, localOnly()),
	}
	upgradeCmd.Flags().Bool("restart", false, "Restart the background service after upgrading")

	root.AddCommand(newDaemonInstallCmd(), newDaemonUninstallCmd(), upgradeCmd)
	return root
}

func runUp(cmd *cobra.Command, _ []string, env *cliEnv) error {
	detach, _ := cmd.Flags().GetBool("detach")
	if detach {
		// Detach hands off to launchd/systemd, which is single-instance per unit.
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

	creds, err := auth.LoadNodeCredentials(identityDir)
	if err != nil {
		if !errors.Is(err, auth.ErrCredentialsNotFound) {
			return err
		}
		logger.Info("node is not initialized; auto-initializing root cluster")
		creds, err = auth.EnsureLocalRootCredentials(identityDir, pubKey, time.Now(), config.DefaultMembershipTTL, config.DefaultDelegationTTL)
		if err != nil {
			return fmt.Errorf("auto-init failed: %w", err)
		}
	} else if auth.IsCertExpired(creds.Cert(), time.Now()) {
		logger.Warnw("delegation certificate has expired — starting in degraded mode, will attempt renewal", "expired_at", auth.CertExpiresAt(creds.Cert()))
	}

	signer, err := auth.NewDelegationSigner(identityDir, privKey, config.DefaultDelegationTTL)
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

	authzRouter, reloadMatcher, err := buildAuthzRouter(env.cfg.Evaluator)
	if err != nil {
		return err
	}
	if reloadMatcher != nil {
		watchSIGHUPReload(ctx, reloadMatcher, zap.S().Named("authz"))
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
		CPUBudgetPercent:   env.cfg.Resources.CPUPercent,
		MemBudgetPercent:   env.cfg.Resources.MemPercent,
		IdleInstanceTTL:    env.cfg.Placement.IdleInstanceTTL,
		AuthzRouter:        authzRouter,
		RelayOnly:          env.cfg.RelayOnly,
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
		return servicectl("restart", cmd, env)
	}
	return nil
}

// servicectl drives the platform's service manager for the currently active
// context. Default context → brew/systemctl on the canonical `pln` unit.
// Named local context on macOS → launchctl on `sh.pln.<name>`, auto-generating
// the per-context plist on first start. Named local contexts on Linux and
// remote contexts are rejected upstream by ensureSystemServiceContext.
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

// userLaunchctl drives a per-context launchd unit on macOS. Plist is
// generated on first start if missing. Restart is an unload+load pair so
// the command is idempotent and works whether the unit is loaded or not.
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

// ensureUserPlnPlist writes a per-context launchd plist if one is not already
// present. Existing plists are not overwritten so users can customise them.
// Returns the plist path regardless.
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
