package main

import (
	"fmt"
	htmltemplate "html/template"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"

	"github.com/spf13/cobra"
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

var launchdPlistTmpl = htmltemplate.Must(htmltemplate.New("plist").Parse(`<?xml version="1.0" encoding="UTF-8"?>
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
	case "darwin":
		daemonInstallLaunchd(cmd)
	case "linux":
		daemonInstallSystemd(cmd)
	default:
		fmt.Fprintf(cmd.ErrOrStderr(), "unsupported platform: %s (supported: darwin, linux)\n", runtime.GOOS)
	}
}

func runDaemonUninstall(cmd *cobra.Command, _ []string) {
	switch runtime.GOOS {
	case "darwin":
		daemonUninstallLaunchd(cmd)
	case "linux":
		daemonUninstallSystemd(cmd)
	default:
		fmt.Fprintf(cmd.ErrOrStderr(), "unsupported platform: %s (supported: darwin, linux)\n", runtime.GOOS)
	}
}

func runDaemonStatus(cmd *cobra.Command, _ []string) {
	switch runtime.GOOS {
	case "darwin":
		daemonStatusLaunchd(cmd)
	case "linux":
		daemonStatusSystemd(cmd)
	default:
		fmt.Fprintf(cmd.ErrOrStderr(), "unsupported platform: %s (supported: darwin, linux)\n", runtime.GOOS)
	}
}

// --- launchd (macOS) ---

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

	binary, err := resolveExecutable()
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	logPath := filepath.Join(os.Getenv("HOME"), "Library", "Logs", "pollen.log")

	if err := os.MkdirAll(filepath.Dir(plistPath), 0o755); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	f, err := os.Create(plistPath)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
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
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "service installed: %s\n", plistPath)

	if start {
		uid := os.Getuid()
		// Try to bootout any existing instance first.
		_ = exec.Command("launchctl", "bootout", fmt.Sprintf("gui/%d/%s", uid, launchdLabel)).Run() //nolint:errcheck
		// Try gui domain first, fall back to user domain.
		if err := exec.Command("launchctl", "bootstrap", fmt.Sprintf("gui/%d", uid), plistPath).Run(); err != nil {
			if err2 := exec.Command("launchctl", "bootstrap", fmt.Sprintf("user/%d", uid), plistPath).Run(); err2 != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "failed to start service: %v\nrun manually: launchctl bootstrap gui/%d %s\n", err, uid, plistPath)
				return
			}
		}
		fmt.Fprintln(cmd.OutOrStdout(), "service started")
	}
}

func daemonUninstallLaunchd(cmd *cobra.Command) {
	plistPath := launchdPlistPath()

	// Stop service if running.
	uid := os.Getuid()
	_ = exec.Command("launchctl", "bootout", fmt.Sprintf("gui/%d/%s", uid, launchdLabel)).Run()  //nolint:errcheck
	_ = exec.Command("launchctl", "bootout", fmt.Sprintf("user/%d/%s", uid, launchdLabel)).Run() //nolint:errcheck

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

	uid := os.Getuid()
	out, err := exec.Command("launchctl", "print", fmt.Sprintf("gui/%d/%s", uid, launchdLabel)).CombinedOutput()
	if err != nil {
		out, err = exec.Command("launchctl", "print", fmt.Sprintf("user/%d/%s", uid, launchdLabel)).CombinedOutput()
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

	binary, err := resolveExecutable()
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	pollenDir, err := pollenPath(cmd)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	if err := os.MkdirAll(filepath.Dir(unitPath), 0o755); err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	f, err := os.Create(unitPath)
	if err != nil {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
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
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "service installed: %s\n", unitPath)

	// Reload and enable.
	if err := exec.Command("systemctl", "--user", "daemon-reload").Run(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: systemctl daemon-reload failed: %v\n", err)
	}
	if err := exec.Command("systemctl", "--user", "enable", systemdUnitName).Run(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: systemctl enable failed: %v\n", err)
	}

	if start {
		if err := exec.Command("systemctl", "--user", "restart", systemdUnitName).Run(); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "failed to start service: %v\n", err)
			return
		}
		fmt.Fprintln(cmd.OutOrStdout(), "service started")
	}
}

func daemonUninstallSystemd(cmd *cobra.Command) {
	unitPath := systemdUnitPath()

	// Stop and disable.
	_ = exec.Command("systemctl", "--user", "stop", systemdUnitName).Run()    //nolint:errcheck
	_ = exec.Command("systemctl", "--user", "disable", systemdUnitName).Run() //nolint:errcheck

	if err := os.Remove(unitPath); err != nil && !os.IsNotExist(err) {
		fmt.Fprintln(cmd.ErrOrStderr(), err)
		return
	}

	_ = exec.Command("systemctl", "--user", "daemon-reload").Run() //nolint:errcheck

	fmt.Fprintln(cmd.OutOrStdout(), "service uninstalled")
}

func daemonStatusSystemd(cmd *cobra.Command) {
	unitPath := systemdUnitPath()

	if _, err := os.Stat(unitPath); err != nil {
		fmt.Fprintln(cmd.OutOrStdout(), "installed: no")
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "installed: yes (%s)\n", unitPath)

	out, err := exec.Command("systemctl", "--user", "is-active", systemdUnitName).CombinedOutput()
	if err == nil && strings.TrimSpace(string(out)) == "active" {
		fmt.Fprintln(cmd.OutOrStdout(), "running: yes")
	} else {
		fmt.Fprintln(cmd.OutOrStdout(), "running: no")
	}
}
