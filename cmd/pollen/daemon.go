package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"
	"time"

	"github.com/spf13/cobra"
)

// effectiveUser returns the real (non-root) username, home directory, and UID.
// Under sudo, os.UserHomeDir() returns /root; this resolves SUDO_USER instead.
// homeDir never returns empty — it falls back to $HOME.
func effectiveUser() (username, homeDir, uid string) {
	if sudoUser := os.Getenv("SUDO_USER"); sudoUser != "" {
		if u, err := user.Lookup(sudoUser); err == nil {
			return u.Username, u.HomeDir, u.Uid
		}
	}
	if u, err := user.Current(); err == nil {
		return u.Username, u.HomeDir, u.Uid
	}
	return "", os.Getenv("HOME"), fmt.Sprintf("%d", os.Getuid())
}

const (
	osDarwin = "darwin"
	osLinux  = "linux"
)

const (
	launchdLabel    = "io.pollen.node"
	systemdUnitName = "pollen.service"
)

func requireRoot(cmd *cobra.Command) bool {
	if os.Getuid() != 0 {
		fmt.Fprintf(cmd.ErrOrStderr(), "this command requires root; run: sudo %s\n", cmd.CommandPath())
		return false
	}
	return true
}

func launchdPlistPath() string {
	return filepath.Join("/Library", "LaunchDaemons", launchdLabel+".plist")
}

func systemdUnitPath() string {
	return filepath.Join("/etc", "systemd", "system", systemdUnitName)
}

// Legacy user-level service paths for migration.
func legacyLaunchdPlistPath() string {
	_, homeDir, _ := effectiveUser()
	return filepath.Join(homeDir, "Library", "LaunchAgents", launchdLabel+".plist")
}

func legacySystemdUnitPath() string {
	_, homeDir, _ := effectiveUser()
	return filepath.Join(homeDir, ".config", "systemd", "user", systemdUnitName)
}

type serviceTemplateData struct {
	Label     string // launchd only
	Username  string
	Binary    string
	PollenDir string
	LogPath   string // launchd only
}

var launchdPlistTmpl = template.Must(template.New("plist").Parse(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{{ .Label }}</string>
  <key>UserName</key>
  <string>{{ .Username }}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{{ .Binary }}</string>
    <string>--dir</string>
    <string>{{ .PollenDir }}</string>
    <string>up</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>StandardOutPath</key>
  <string>{{ .LogPath }}</string>
  <key>StandardErrorPath</key>
  <string>{{ .LogPath }}</string>
</dict>
</plist>
`))

var systemdUnitTmpl = template.Must(template.New("unit").Parse(`[Unit]
Description=Pollen node
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={{ .Username }}
ExecStart="{{ .Binary }}" --dir "{{ .PollenDir }}" up
ExecStop="{{ .Binary }}" --dir "{{ .PollenDir }}" down
SyslogIdentifier=pollen
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
`))

func newDaemonCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Manage the Pollen background service",
	}

	installCmd := &cobra.Command{
		Use:   "install",
		Short: "Install and enable the Pollen background service",
		Args:  cobra.NoArgs,
		Run:   runDaemonInstall,
	}
	installCmd.Flags().Bool("start", false, "Start the service immediately after installing")
	installCmd.Flags().Bool("force", false, "Overwrite existing service file")

	uninstallCmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Stop and remove the Pollen background service",
		Args:  cobra.NoArgs,
		Run:   runDaemonUninstall,
	}

	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show whether the Pollen background service is installed and running",
		Args:  cobra.NoArgs,
		Run:   runDaemonStatus,
	}

	cmd.AddCommand(installCmd, uninstallCmd, statusCmd)
	return cmd
}

func resolveExecutable() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		return "", fmt.Errorf("resolve symlinks: %w", err)
	}
	return exe, nil
}

func runDaemonInstall(cmd *cobra.Command, _ []string) {
	if !requireRoot(cmd) {
		return
	}
	switch runtime.GOOS {
	case osDarwin:
		daemonInstallLaunchd(cmd)
	case osLinux:
		daemonInstallSystemd(cmd)
	default:
		fmt.Fprintf(cmd.ErrOrStderr(), "unsupported platform: %s (supported: darwin, linux)\n", runtime.GOOS)
	}
}

func runDaemonUninstall(cmd *cobra.Command, _ []string) {
	if !requireRoot(cmd) {
		return
	}
	switch runtime.GOOS {
	case osDarwin:
		daemonUninstallLaunchd(cmd)
	case osLinux:
		daemonUninstallSystemd(cmd)
	default:
		fmt.Fprintf(cmd.ErrOrStderr(), "unsupported platform: %s (supported: darwin, linux)\n", runtime.GOOS)
	}
}

func runDaemonStatus(cmd *cobra.Command, _ []string) {
	switch runtime.GOOS {
	case osDarwin:
		daemonStatusLaunchd(cmd)
	case osLinux:
		daemonStatusSystemd(cmd)
	default:
		fmt.Fprintf(cmd.ErrOrStderr(), "unsupported platform: %s (supported: darwin, linux)\n", runtime.GOOS)
	}
}

// --- launchd (macOS) ---

// codesignValid returns true if codesign is available and the binary has a
// valid code signature.
func codesignValid(ctx context.Context, binaryPath string) bool {
	if _, err := exec.LookPath("codesign"); err != nil {
		return false
	}
	return exec.CommandContext(ctx, "codesign", "--verify", "--strict", binaryPath).Run() == nil
}

// ensureLaunchdSafe verifies the binary has a valid code signature. macOS tags
// downloaded binaries with com.apple.provenance (SIP-protected); in the system
// launchd domain there's no user session for Gatekeeper prompts, so xpcproxy
// rejects unsigned/invalidly-signed binaries with EX_CONFIG (78). An ad-hoc
// re-sign produces a fresh local signature that satisfies launch constraints.
// Defense-in-depth: install.sh also signs at install time.
func ensureLaunchdSafe(ctx context.Context, binaryPath string, w io.Writer) {
	if codesignValid(ctx, binaryPath) {
		return
	}

	if _, err := exec.LookPath("codesign"); err != nil {
		fmt.Fprintln(w, "warning: codesign not found; cannot verify binary signature")
		return
	}

	fmt.Fprintln(w, "binary signature invalid; re-signing with ad-hoc signature")
	if err := exec.CommandContext(ctx, "codesign", "-s", "-", "--force", binaryPath).Run(); err != nil {
		fmt.Fprintf(w, "warning: codesign failed: %v\nlaunchd may reject the binary with exit code 78\n", err)
	}
}

// ensureBinarySigned resolves the current executable and ensures it has a valid
// code signature. Returns false (after printing the error) if resolution fails.
func ensureBinarySigned(cmd *cobra.Command) bool {
	binary, err := resolveExecutable()
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return false
	}
	ensureLaunchdSafe(cmd.Context(), binary, cmd.ErrOrStderr())
	return true
}

const launchdSystemTarget = "system/" + launchdLabel

// launchdStart loads and starts the service in the system domain.
func launchdStart(cmd *cobra.Command, plistPath string) error {
	ctx := cmd.Context()

	// Try kickstart first (works for loaded-but-stopped jobs).
	if err := exec.CommandContext(ctx, "launchctl", "kickstart", launchdSystemTarget).Run(); err == nil {
		return nil
	}

	// Fallback: enable + bootstrap (for unloaded jobs).
	_ = exec.CommandContext(ctx, "launchctl", "enable", launchdSystemTarget).Run() //nolint:errcheck
	return exec.CommandContext(ctx, "launchctl", "bootstrap", "system", plistPath).Run()
}

// writeLaunchdPlist writes (or overwrites) the launchd plist file.
func writeLaunchdPlist(cmd *cobra.Command) error {
	binary, err := resolveExecutable()
	if err != nil {
		return err
	}

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		return err
	}

	username, _, _ := effectiveUser()
	if username == "" {
		return fmt.Errorf("cannot determine username for service file")
	}

	plistPath := launchdPlistPath()
	logPath := filepath.Join("/Library", "Logs", "pollen.log")

	if err := os.MkdirAll(filepath.Dir(plistPath), 0o755); err != nil { //nolint:mnd
		return err
	}

	f, err := os.Create(plistPath)
	if err != nil {
		return err
	}

	data := serviceTemplateData{
		Label:     launchdLabel,
		Username:  username,
		Binary:    binary,
		PollenDir: pollenDir,
		LogPath:   logPath,
	}

	if err := launchdPlistTmpl.Execute(f, data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func daemonInstallLaunchd(cmd *cobra.Command) {
	force, _ := cmd.Flags().GetBool("force")
	start, _ := cmd.Flags().GetBool("start")

	migrateLegacyLaunchd(cmd)

	plistPath := launchdPlistPath()

	if !force {
		if _, err := os.Stat(plistPath); err == nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "service file already exists: %s\nuse --force to overwrite\n", plistPath)
			return
		}
	}

	if err := writeLaunchdPlist(cmd); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "service installed: %s\n", plistPath)

	if start {
		if !ensureBinarySigned(cmd) {
			return
		}

		ctx := cmd.Context()

		// Remove any existing instance and clear disabled state.
		_ = exec.CommandContext(ctx, "launchctl", "bootout", launchdSystemTarget).Run() //nolint:errcheck

		if err := launchdStart(cmd, plistPath); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "failed to start service: %v\nrun manually: sudo launchctl bootstrap system %s\n", err, plistPath)
			return
		}
		fmt.Fprintln(cmd.OutOrStdout(), "service started")
	}
}

func daemonUninstallLaunchd(cmd *cobra.Command) {
	plistPath := launchdPlistPath()

	// Stop service if running.
	ctx := cmd.Context()
	_ = exec.CommandContext(ctx, "launchctl", "bootout", launchdSystemTarget).Run() //nolint:errcheck

	if err := os.Remove(plistPath); err != nil && !os.IsNotExist(err) {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprintln(cmd.OutOrStdout(), "service uninstalled")
}

func daemonStatusLaunchd(cmd *cobra.Command) {
	plistPath := launchdPlistPath()

	if _, err := os.Stat(plistPath); err != nil {
		fmt.Fprintln(cmd.OutOrStdout(), "installed: no")
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "installed: yes (%s)\n", plistPath)

	out, err := exec.CommandContext(cmd.Context(), "launchctl", "print", launchdSystemTarget).CombinedOutput()
	if err != nil {
		fmt.Fprintln(cmd.OutOrStdout(), "running: no")
	} else if strings.Contains(string(out), "state = running") {
		fmt.Fprintln(cmd.OutOrStdout(), "running: yes")
	} else {
		fmt.Fprintln(cmd.OutOrStdout(), "running: no")
	}

	if binary, err := resolveExecutable(); err == nil {
		if codesignValid(cmd.Context(), binary) {
			fmt.Fprintln(cmd.OutOrStdout(), "signature: valid")
		} else if _, err := exec.LookPath("codesign"); err == nil {
			fmt.Fprintf(cmd.OutOrStdout(), "signature: invalid (run: sudo codesign -s - --force %s)\n", binary)
		}
	}
}

// --- systemd (Linux) ---

func systemdReloadAndEnable(ctx context.Context, w io.Writer) {
	if err := exec.CommandContext(ctx, "systemctl", "daemon-reload").Run(); err != nil {
		fmt.Fprintf(w, "warning: systemctl daemon-reload failed: %v\n", err)
	}
	if err := exec.CommandContext(ctx, "systemctl", "enable", systemdUnitName).Run(); err != nil {
		fmt.Fprintf(w, "warning: systemctl enable failed: %v\n", err)
	}
}

// writeSystemdUnit writes (or overwrites) the systemd unit file.
func writeSystemdUnit(cmd *cobra.Command) error {
	binary, err := resolveExecutable()
	if err != nil {
		return err
	}

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		return err
	}

	username, _, _ := effectiveUser()
	if username == "" {
		return fmt.Errorf("cannot determine username for service file")
	}

	unitPath := systemdUnitPath()

	if err := os.MkdirAll(filepath.Dir(unitPath), 0o755); err != nil { //nolint:mnd
		return err
	}

	f, err := os.Create(unitPath)
	if err != nil {
		return err
	}

	data := serviceTemplateData{
		Username:  username,
		Binary:    binary,
		PollenDir: pollenDir,
	}

	if err := systemdUnitTmpl.Execute(f, data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func daemonInstallSystemd(cmd *cobra.Command) {
	force, _ := cmd.Flags().GetBool("force")
	start, _ := cmd.Flags().GetBool("start")

	migrateLegacySystemd(cmd)

	unitPath := systemdUnitPath()

	if !force {
		if _, err := os.Stat(unitPath); err == nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "service file already exists: %s\nuse --force to overwrite\n", unitPath)
			return
		}
	}

	if err := writeSystemdUnit(cmd); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "service installed: %s\n", unitPath)

	ctx := cmd.Context()
	systemdReloadAndEnable(ctx, cmd.ErrOrStderr())

	if start {
		if err := exec.CommandContext(ctx, "systemctl", "restart", systemdUnitName).Run(); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "failed to start service: %v\n", err)
			return
		}
		fmt.Fprintln(cmd.OutOrStdout(), "service started")
	}
}

func daemonUninstallSystemd(cmd *cobra.Command) {
	unitPath := systemdUnitPath()

	// Stop and disable.
	ctx := cmd.Context()
	_ = exec.CommandContext(ctx, "systemctl", "stop", systemdUnitName).Run()    //nolint:errcheck
	_ = exec.CommandContext(ctx, "systemctl", "disable", systemdUnitName).Run() //nolint:errcheck

	if err := os.Remove(unitPath); err != nil && !os.IsNotExist(err) {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	_ = exec.CommandContext(ctx, "systemctl", "daemon-reload").Run() //nolint:errcheck

	fmt.Fprintln(cmd.OutOrStdout(), "service uninstalled")
}

func daemonStatusSystemd(cmd *cobra.Command) {
	unitPath := systemdUnitPath()

	if _, err := os.Stat(unitPath); err != nil {
		fmt.Fprintln(cmd.OutOrStdout(), "installed: no")
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "installed: yes (%s)\n", unitPath)

	out, err := exec.CommandContext(cmd.Context(), "systemctl", "is-active", systemdUnitName).CombinedOutput()
	if err == nil && strings.TrimSpace(string(out)) == "active" {
		fmt.Fprintln(cmd.OutOrStdout(), "running: yes")
	} else {
		fmt.Fprintln(cmd.OutOrStdout(), "running: no")
	}
}

// --- legacy migration ---

func migrateLegacyLaunchd(cmd *cobra.Command) {
	legacyPath := legacyLaunchdPlistPath()
	if _, err := os.Stat(legacyPath); err != nil {
		return
	}
	fmt.Fprintln(cmd.OutOrStdout(), "migrating from user-level to system-level service")
	_, _, uid := effectiveUser()
	ctx := cmd.Context()
	_ = exec.CommandContext(ctx, "launchctl", "bootout", fmt.Sprintf("gui/%s/%s", uid, launchdLabel)).Run()  //nolint:errcheck
	_ = exec.CommandContext(ctx, "launchctl", "bootout", fmt.Sprintf("user/%s/%s", uid, launchdLabel)).Run() //nolint:errcheck
	_ = os.Remove(legacyPath)                                                                                //nolint:errcheck
}

func migrateLegacySystemd(cmd *cobra.Command) {
	legacyPath := legacySystemdUnitPath()
	if _, err := os.Stat(legacyPath); err != nil {
		return
	}
	fmt.Fprintln(cmd.OutOrStdout(), "migrating from user-level to system-level service")
	username, _, uid := effectiveUser()
	ctx := cmd.Context()
	env := append(os.Environ(), "XDG_RUNTIME_DIR=/run/user/"+uid)

	stopCmd := exec.CommandContext(ctx, "sudo", "-u", username, "systemctl", "--user", "stop", systemdUnitName)
	stopCmd.Env = env
	_ = stopCmd.Run() //nolint:errcheck

	disableCmd := exec.CommandContext(ctx, "sudo", "-u", username, "systemctl", "--user", "disable", systemdUnitName)
	disableCmd.Env = env
	_ = disableCmd.Run() //nolint:errcheck

	_ = os.Remove(legacyPath) //nolint:errcheck

	reloadCmd := exec.CommandContext(ctx, "sudo", "-u", username, "systemctl", "--user", "daemon-reload")
	reloadCmd.Env = env
	_ = reloadCmd.Run() //nolint:errcheck
}

// --- pollen up -d ---

const daemonStartTimeout = 5 * time.Second

// daemonServiceInstalled returns true if the platform service file exists.
func daemonServiceInstalled() bool {
	switch runtime.GOOS {
	case osDarwin:
		_, err := os.Stat(launchdPlistPath())
		return err == nil
	case osLinux:
		_, err := os.Stat(systemdUnitPath())
		return err == nil
	default:
		return false
	}
}

// waitForSocket polls nodeSocketActive until the socket responds or timeout elapses.
func waitForSocket(sockPath string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if active, _ := nodeSocketActive(sockPath); active {
			return true
		}
		time.Sleep(250 * time.Millisecond) //nolint:mnd
	}
	return false
}

// runUpDaemon dispatches pollen up -d to the platform-specific daemon start flow.
func runUpDaemon(cmd *cobra.Command) {
	if os.Getuid() != 0 {
		fmt.Fprintf(cmd.ErrOrStderr(), "this command requires root; run: sudo %s -d\n", cmd.CommandPath())
		return
	}
	switch runtime.GOOS {
	case osDarwin:
		upDaemonLaunchd(cmd)
	case osLinux:
		upDaemonSystemd(cmd)
	default:
		fmt.Fprintf(cmd.ErrOrStderr(), "unsupported platform: %s (supported: darwin, linux)\n", runtime.GOOS)
	}
}

func upDaemonLaunchd(cmd *cobra.Command) {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	sockPath := filepath.Join(pollenDir, socketName)

	// Already running?
	if active, _ := nodeSocketActive(sockPath); active {
		fmt.Fprintln(cmd.OutOrStdout(), "node is already running")
		return
	}

	plistPath := launchdPlistPath()

	// Auto-install if plist doesn't exist.
	if _, err := os.Stat(plistPath); err != nil {
		fmt.Fprintln(cmd.OutOrStdout(), "service not installed; installing now")
		migrateLegacyLaunchd(cmd)
		if writeErr := writeLaunchdPlist(cmd); writeErr != nil {
			fmt.Fprintln(cmd.ErrOrStderr(), writeErr)
			return
		}
		fmt.Fprintf(cmd.OutOrStdout(), "service installed: %s\n", plistPath)
	}

	if !ensureBinarySigned(cmd) {
		return
	}

	if err := launchdStart(cmd, plistPath); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to start service: %v\n", err)
		return
	}

	if waitForSocket(sockPath, daemonStartTimeout) {
		fmt.Fprintln(cmd.OutOrStdout(), "node started (background)")
	} else {
		fmt.Fprintln(cmd.ErrOrStderr(), "service was started but node did not become ready; check logs with: pollen logs")
		fmt.Fprintln(cmd.ErrOrStderr(), "or check launchd status with: sudo launchctl print system/io.pollen.node")
	}
}

func upDaemonSystemd(cmd *cobra.Command) {
	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	sockPath := filepath.Join(pollenDir, socketName)

	// Already running?
	if active, _ := nodeSocketActive(sockPath); active {
		fmt.Fprintln(cmd.OutOrStdout(), "node is already running")
		return
	}

	unitPath := systemdUnitPath()

	// Auto-install if unit doesn't exist.
	if _, err := os.Stat(unitPath); err != nil {
		fmt.Fprintln(cmd.OutOrStdout(), "service not installed; installing now")
		migrateLegacySystemd(cmd)
		if writeErr := writeSystemdUnit(cmd); writeErr != nil {
			fmt.Fprintln(cmd.ErrOrStderr(), writeErr)
			return
		}
		fmt.Fprintf(cmd.OutOrStdout(), "service installed: %s\n", unitPath)
		systemdReloadAndEnable(cmd.Context(), cmd.ErrOrStderr())
	}

	ctx := cmd.Context()
	if err := exec.CommandContext(ctx, "systemctl", "start", systemdUnitName).Run(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to start service: %v\n", err)
		return
	}

	if waitForSocket(sockPath, daemonStartTimeout) {
		fmt.Fprintln(cmd.OutOrStdout(), "node started (background)")
	} else {
		fmt.Fprintln(cmd.ErrOrStderr(), "service was started but node did not become ready; check logs with: pollen logs")
	}
}
