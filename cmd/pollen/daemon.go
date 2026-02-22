package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"
	"time"

	"github.com/spf13/cobra"
)

const (
	osDarwin = "darwin"
	osLinux  = "linux"
)

const (
	launchdLabel    = "io.pollen.node"
	systemdUnitName = "pollen.service"
)

func launchdPlistPath() string {
	return filepath.Join(os.Getenv("HOME"), "Library", "LaunchAgents", launchdLabel+".plist")
}

func systemdUnitPath() string {
	return filepath.Join(os.Getenv("HOME"), ".config", "systemd", "user", systemdUnitName)
}

var launchdPlistTmpl = template.Must(template.New("plist").Parse(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{{ .Label }}</string>
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
ExecStart="{{ .Binary }}" --dir "{{ .PollenDir }}" up
ExecStop="{{ .Binary }}" --dir "{{ .PollenDir }}" down
Restart=on-failure
RestartSec=3

[Install]
WantedBy=default.target
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

// launchdTargets returns the gui and user domain/target strings for launchctl.
func launchdTargets(uid int) (guiDomain, guiTarget, userDomain, userTarget string) {
	guiDomain = fmt.Sprintf("gui/%d", uid)
	guiTarget = fmt.Sprintf("gui/%d/%s", uid, launchdLabel)
	userDomain = fmt.Sprintf("user/%d", uid)
	userTarget = fmt.Sprintf("user/%d/%s", uid, launchdLabel)
	return
}

// launchdStart attempts to start the service via kickstart, falling back to
// enable + bootstrap. It tries the gui domain first, then the user domain.
func launchdStart(cmd *cobra.Command, plistPath string) error {
	ctx := cmd.Context()
	uid := os.Getuid()
	guiDomain, guiTarget, userDomain, userTarget := launchdTargets(uid)

	// Try kickstart first (works for loaded-but-stopped jobs).
	if err := exec.CommandContext(ctx, "launchctl", "kickstart", guiTarget).Run(); err == nil { //nolint:gosec
		return nil
	}
	if err := exec.CommandContext(ctx, "launchctl", "kickstart", userTarget).Run(); err == nil { //nolint:gosec
		return nil
	}

	// Fallback: enable + bootstrap (for unloaded jobs).
	_ = exec.CommandContext(ctx, "launchctl", "enable", guiTarget).Run()  //nolint:errcheck,gosec
	_ = exec.CommandContext(ctx, "launchctl", "enable", userTarget).Run() //nolint:errcheck,gosec

	if err := exec.CommandContext(ctx, "launchctl", "bootstrap", guiDomain, plistPath).Run(); err == nil { //nolint:gosec
		return nil
	}
	return exec.CommandContext(ctx, "launchctl", "bootstrap", userDomain, plistPath).Run() //nolint:gosec
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

	plistPath := launchdPlistPath()
	logPath := filepath.Join(os.Getenv("HOME"), "Library", "Logs", "pollen.log")

	if err := os.MkdirAll(filepath.Dir(plistPath), 0o755); err != nil { //nolint:mnd
		return err
	}

	f, err := os.Create(plistPath)
	if err != nil {
		return err
	}
	defer f.Close()

	data := struct {
		Label     string
		Binary    string
		PollenDir string
		LogPath   string
	}{
		Label:     launchdLabel,
		Binary:    binary,
		PollenDir: pollenDir,
		LogPath:   logPath,
	}

	if err := launchdPlistTmpl.Execute(f, data); err != nil {
		return err
	}
	// Close the file before launchctl reads it.
	return f.Close()
}

func daemonInstallLaunchd(cmd *cobra.Command) {
	force, _ := cmd.Flags().GetBool("force")
	start, _ := cmd.Flags().GetBool("start")

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
		ctx := cmd.Context()
		uid := os.Getuid()
		_, guiTarget, _, userTarget := launchdTargets(uid)

		// Remove any existing instance and clear disabled state.
		_ = exec.CommandContext(ctx, "launchctl", "bootout", guiTarget).Run()  //nolint:errcheck,gosec
		_ = exec.CommandContext(ctx, "launchctl", "bootout", userTarget).Run() //nolint:errcheck,gosec

		if err := launchdStart(cmd, plistPath); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "failed to start service: %v\nrun manually: launchctl bootstrap gui/%d %s\n", err, uid, plistPath)
			return
		}
		fmt.Fprintln(cmd.OutOrStdout(), "service started")
	}
}

func daemonUninstallLaunchd(cmd *cobra.Command) {
	plistPath := launchdPlistPath()

	// Stop service if running.
	ctx := cmd.Context()
	uid := os.Getuid()
	_ = exec.CommandContext(ctx, "launchctl", "bootout", fmt.Sprintf("gui/%d/%s", uid, launchdLabel)).Run()  //nolint:errcheck,gosec
	_ = exec.CommandContext(ctx, "launchctl", "bootout", fmt.Sprintf("user/%d/%s", uid, launchdLabel)).Run() //nolint:errcheck,gosec

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

	ctx := cmd.Context()
	uid := os.Getuid()
	out, err := exec.CommandContext(ctx, "launchctl", "print", fmt.Sprintf("gui/%d/%s", uid, launchdLabel)).CombinedOutput() //nolint:gosec
	if err != nil {
		out, err = exec.CommandContext(ctx, "launchctl", "print", fmt.Sprintf("user/%d/%s", uid, launchdLabel)).CombinedOutput() //nolint:gosec
	}

	if err != nil {
		fmt.Fprintln(cmd.OutOrStdout(), "running: no")
		return
	}

	if strings.Contains(string(out), "state = running") {
		fmt.Fprintln(cmd.OutOrStdout(), "running: yes")
	} else {
		fmt.Fprintln(cmd.OutOrStdout(), "running: no")
	}
}

// --- systemd (Linux) ---

// writeSystemdUnit writes (or overwrites) the systemd user unit file.
func writeSystemdUnit(cmd *cobra.Command) error {
	binary, err := resolveExecutable()
	if err != nil {
		return err
	}

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		return err
	}

	unitPath := systemdUnitPath()

	if err := os.MkdirAll(filepath.Dir(unitPath), 0o755); err != nil { //nolint:mnd
		return err
	}

	f, err := os.Create(unitPath)
	if err != nil {
		return err
	}
	defer f.Close()

	data := struct {
		Binary    string
		PollenDir string
	}{
		Binary:    binary,
		PollenDir: pollenDir,
	}

	if err := systemdUnitTmpl.Execute(f, data); err != nil {
		return err
	}
	return f.Close()
}

func daemonInstallSystemd(cmd *cobra.Command) {
	force, _ := cmd.Flags().GetBool("force")
	start, _ := cmd.Flags().GetBool("start")

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

	// Reload and enable.
	ctx := cmd.Context()
	if err := exec.CommandContext(ctx, "systemctl", "--user", "daemon-reload").Run(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: systemctl daemon-reload failed: %v\n", err)
	}
	if err := exec.CommandContext(ctx, "systemctl", "--user", "enable", systemdUnitName).Run(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: systemctl enable failed: %v\n", err)
	}

	if start {
		if err := exec.CommandContext(ctx, "systemctl", "--user", "restart", systemdUnitName).Run(); err != nil {
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
	_ = exec.CommandContext(ctx, "systemctl", "--user", "stop", systemdUnitName).Run()    //nolint:errcheck
	_ = exec.CommandContext(ctx, "systemctl", "--user", "disable", systemdUnitName).Run() //nolint:errcheck

	if err := os.Remove(unitPath); err != nil && !os.IsNotExist(err) {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	_ = exec.CommandContext(ctx, "systemctl", "--user", "daemon-reload").Run() //nolint:errcheck

	fmt.Fprintln(cmd.OutOrStdout(), "service uninstalled")
}

func daemonStatusSystemd(cmd *cobra.Command) {
	unitPath := systemdUnitPath()

	if _, err := os.Stat(unitPath); err != nil {
		fmt.Fprintln(cmd.OutOrStdout(), "installed: no")
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "installed: yes (%s)\n", unitPath)

	out, err := exec.CommandContext(cmd.Context(), "systemctl", "--user", "is-active", systemdUnitName).CombinedOutput()
	if err == nil && strings.TrimSpace(string(out)) == "active" {
		fmt.Fprintln(cmd.OutOrStdout(), "running: yes")
	} else {
		fmt.Fprintln(cmd.OutOrStdout(), "running: no")
	}
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
		if writeErr := writeLaunchdPlist(cmd); writeErr != nil {
			fmt.Fprintln(cmd.ErrOrStderr(), writeErr)
			return
		}
		fmt.Fprintf(cmd.OutOrStdout(), "service installed: %s\n", plistPath)
	}

	if err := launchdStart(cmd, plistPath); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to start service: %v\n", err)
		return
	}

	if waitForSocket(sockPath, daemonStartTimeout) {
		fmt.Fprintln(cmd.OutOrStdout(), "node started (background)")
	} else {
		fmt.Fprintln(cmd.ErrOrStderr(), "service was started but node did not become ready; check logs with: cat ~/Library/Logs/pollen.log")
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
		if writeErr := writeSystemdUnit(cmd); writeErr != nil {
			fmt.Fprintln(cmd.ErrOrStderr(), writeErr)
			return
		}
		fmt.Fprintf(cmd.OutOrStdout(), "service installed: %s\n", unitPath)

		ctx := cmd.Context()
		if reloadErr := exec.CommandContext(ctx, "systemctl", "--user", "daemon-reload").Run(); reloadErr != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "warning: systemctl daemon-reload failed: %v\n", reloadErr)
		}
		if enableErr := exec.CommandContext(ctx, "systemctl", "--user", "enable", systemdUnitName).Run(); enableErr != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "warning: systemctl enable failed: %v\n", enableErr)
		}
	}

	ctx := cmd.Context()
	if err := exec.CommandContext(ctx, "systemctl", "--user", "start", systemdUnitName).Run(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to start service: %v\n", err)
		return
	}

	if waitForSocket(sockPath, daemonStartTimeout) {
		fmt.Fprintln(cmd.OutOrStdout(), "node started (background)")
	} else {
		fmt.Fprintln(cmd.ErrOrStderr(), "service was started but node did not become ready; check logs with: journalctl --user -u pollen.service")
	}
}
