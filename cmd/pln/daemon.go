package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"

	"github.com/spf13/cobra"
)

const (
	osDarwin = "darwin"
	osLinux  = "linux"
)

func newDownCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "down",
		Short: "Stop the background service",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			if err := servicectl("stop", cmd); err != nil {
				os.Exit(1)
			}
		},
	}
}

func newUpgradeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade pln to the latest version",
		Args:  cobra.NoArgs,
		Run:   runUpgrade,
	}
	cmd.Flags().Bool("restart", false, "Restart the background service after upgrading")
	return cmd
}

func runUpgrade(cmd *cobra.Command, _ []string) {
	var err error
	switch runtime.GOOS {
	case osDarwin:
		c := exec.CommandContext(cmd.Context(), "brew", "upgrade", "sambigeara/homebrew-pln/pln")
		c.Stdout = cmd.OutOrStdout()
		c.Stderr = cmd.ErrOrStderr()
		err = c.Run()
	case osLinux:
		err = upgradeLinux(cmd)
	default:
		fmt.Fprintln(cmd.ErrOrStderr(), "upgrade is only supported on macOS and Linux")
		os.Exit(1)
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		os.Exit(exitErr.ExitCode())
	}
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "upgrade failed: %v\n", err)
		os.Exit(1)
	}

	if restart, _ := cmd.Flags().GetBool("restart"); restart {
		if err := servicectl("restart", cmd); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "upgraded but failed to restart daemon: %v\n", err)
			os.Exit(1)
		}
	}
}

func upgradeLinux(cmd *cobra.Command) error {
	script, err := fetchInstallScript()
	if err != nil {
		return err
	}

	c := exec.CommandContext(cmd.Context(), "bash", "-s", "--")
	c.Stdin = bytes.NewReader(script)
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()
	return c.Run()
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
