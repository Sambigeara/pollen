// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	_ "embed"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/sambigeara/pollen/pkg/plnfs"
)

const (
	plnUnitDir               = "/lib/systemd/system"
	plnUnitName              = "pln.service"
	systemdBinaryPlaceholder = "{{PLN_BINARY}}"
	packagePlnBinary         = "/usr/bin/pln"
	tarballPlnBinary         = "/usr/local/bin/pln"
)

//go:embed pln.service
var systemdUnit []byte

func newDaemonInstallCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "install",
		Short: "Install pln as a system service running as a dedicated pln user",
		Long: `Creates the pln system user and state directories, drops the
embedded systemd unit, and enables it. If invoked via sudo, the invoking
user is added to the pln group for CLI access. Idempotent.`,
		Args: cobra.NoArgs,
		RunE: runDaemonInstall,
	}
}

func newDaemonUninstallCmd() *cobra.Command {
	uninstall := &cobra.Command{
		Use:   "uninstall",
		Short: "Stop and remove the pln system service",
		Long: `Stops the pln systemd unit, disables auto-start, and removes the unit
file. State under /var/lib/pln is preserved unless --purge is given —
which permanently deletes cluster credentials and CAS contents.`,
		Example: "  sudo pln daemon uninstall\n  sudo pln daemon uninstall --purge   # also wipe state",
		Args:    cobra.NoArgs,
		RunE:    runDaemonUninstall,
	}
	uninstall.Flags().Bool("purge", false, "Also delete /var/lib/pln state (irreversible)")
	return uninstall
}

func runDaemonInstall(cmd *cobra.Command, _ []string) error {
	if runtime.GOOS != osLinux {
		fmt.Fprintln(cmd.OutOrStdout(), "daemon install is Linux-only; on macOS install via Homebrew (`brew install sambigeara/homebrew-pln/pln`)")
		return nil
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		fmt.Fprintln(cmd.OutOrStdout(), "systemd not detected; skipping daemon install. Run `pln up` directly if you don't need a system service.")
		return nil //nolint:nilerr
	}
	if os.Getuid() != 0 {
		return errors.New("must be run as root (try: sudo pln daemon install)")
	}

	plnfs.SetSystemMode(true)
	if err := plnfs.Provision(plnfs.SystemDir); err != nil {
		return fmt.Errorf("provision: %w", err)
	}
	plnBinary, err := systemdPlnBinaryPath()
	if err != nil {
		return err
	}
	unit, err := renderSystemdUnit(plnBinary)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(plnUnitDir, 0o755); err != nil { //nolint:mnd
		return fmt.Errorf("create %s: %w", plnUnitDir, err)
	}
	if err := os.WriteFile(filepath.Join(plnUnitDir, plnUnitName), unit, 0o644); err != nil { //nolint:mnd,gosec
		return fmt.Errorf("write unit: %w", err)
	}

	if err := exec.CommandContext(cmd.Context(), "systemctl", "daemon-reload").Run(); err != nil {
		return fmt.Errorf("daemon-reload: %w", err)
	}
	if err := exec.CommandContext(cmd.Context(), "systemctl", "enable", "pln").Run(); err != nil {
		return fmt.Errorf("enable pln: %w", err)
	}

	if sudoUser := os.Getenv("SUDO_USER"); sudoUser != "" && sudoUser != "root" {
		if err := plnfs.AddUserToPlnGroup(sudoUser); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "warning: could not add %s to pln group: %v\n", sudoUser, err)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "added %s to the pln group -- log out and back in for CLI access without sudo\n", sudoUser)
		}
	}
	fmt.Fprintf(cmd.OutOrStdout(), "pln daemon installed at %s/%s using %s\n", plnUnitDir, plnUnitName, plnBinary)
	return nil
}

func renderSystemdUnit(plnBinary string) ([]byte, error) {
	unit := string(systemdUnit)
	if !strings.Contains(unit, systemdBinaryPlaceholder) {
		return nil, errors.New("systemd unit template is missing pln binary placeholder")
	}
	return []byte(strings.ReplaceAll(unit, systemdBinaryPlaceholder, plnBinary)), nil
}

func systemdPlnBinaryPath() (string, error) {
	plnBinary, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve pln binary path: %w", err)
	}
	plnBinary = filepath.Clean(plnBinary)
	if !supportedSystemdPlnBinary(plnBinary) {
		return "", fmt.Errorf("refusing to install systemd service for %s; install pln to %s or %s first", plnBinary, packagePlnBinary, tarballPlnBinary)
	}
	if err := validateRootOwnedSystemdPath(plnBinary); err != nil {
		return "", err
	}
	return plnBinary, nil
}

func supportedSystemdPlnBinary(plnBinary string) bool {
	switch filepath.Clean(plnBinary) {
	case packagePlnBinary, tarballPlnBinary:
		return true
	default:
		return false
	}
}

func validateRootOwnedSystemdPath(path string) error {
	for p := path; ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil {
			return fmt.Errorf("inspect %s: %w", p, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing to install systemd service with symlinked path component %s", p)
		}
		if p == path {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("refusing to install systemd service for non-regular binary %s", path)
			}
		} else if !info.IsDir() {
			return fmt.Errorf("refusing to install systemd service with non-directory path component %s", p)
		}
		if info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("refusing to install systemd service with group/world-writable path component %s", p)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 {
			return fmt.Errorf("refusing to install systemd service with non-root-owned path component %s", p)
		}
		if p == string(filepath.Separator) {
			break
		}
	}
	return nil
}

func runDaemonUninstall(cmd *cobra.Command, _ []string) error {
	if runtime.GOOS != osLinux {
		return nil
	}
	if os.Getuid() != 0 {
		return errors.New("must be run as root (try: sudo pln daemon uninstall)")
	}

	_ = exec.CommandContext(cmd.Context(), "systemctl", "stop", "pln").Run()
	_ = exec.CommandContext(cmd.Context(), "systemctl", "disable", "pln").Run()
	_ = os.Remove(filepath.Join(plnUnitDir, plnUnitName))
	_ = exec.CommandContext(cmd.Context(), "systemctl", "daemon-reload").Run()

	if purge, _ := cmd.Flags().GetBool("purge"); purge {
		if err := os.RemoveAll(plnfs.SystemDir); err != nil {
			return fmt.Errorf("remove state %s: %w", plnfs.SystemDir, err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "purged %s\n", plnfs.SystemDir)
	}

	fmt.Fprintln(cmd.OutOrStdout(), "pln daemon uninstalled")
	return nil
}
