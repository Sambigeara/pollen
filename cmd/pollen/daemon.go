package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"runtime"

	"github.com/spf13/cobra"
)

// effectiveHomeDir returns the home directory of the real (non-root) user.
// Under sudo, os.UserHomeDir() returns /root; this resolves SUDO_USER instead.
func effectiveHomeDir() string {
	if name := os.Getenv("SUDO_USER"); name != "" {
		if u, err := user.Lookup(name); err == nil {
			return u.HomeDir
		}
	}
	if u, err := user.Current(); err == nil {
		return u.HomeDir
	}
	return os.Getenv("HOME")
}

const (
	osDarwin = "darwin"
	osLinux  = "linux"
)

func newDaemonCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Manage the Pollen background service",
	}
	for _, sub := range []struct{ use, short string }{
		{"start", "Start the service"},
		{"stop", "Stop the service"},
		{"restart", "Restart the service"},
		{"status", "Check service status"},
	} {
		action := sub.use
		cmd.AddCommand(&cobra.Command{
			Use: sub.use, Short: sub.short, Args: cobra.NoArgs,
			Run: func(cmd *cobra.Command, _ []string) {
				if err := servicectl(action, cmd); err != nil {
					os.Exit(1)
				}
			},
		})
	}
	return cmd
}

func servicectl(action string, cmd *cobra.Command) error {
	ctx := cmd.Context()
	var c *exec.Cmd
	switch runtime.GOOS {
	case osDarwin:
		brewAction := action
		if brewAction == "status" {
			brewAction = "info"
		}
		c = exec.CommandContext(ctx, "brew", "services", brewAction, "pollen")
	case osLinux:
		switch {
		case action == "status":
			c = exec.CommandContext(ctx, "systemctl", "status", "pollen", "--no-pager")
		case os.Getuid() == 0:
			c = exec.CommandContext(ctx, "systemctl", action, "pollen")
		default:
			c = exec.CommandContext(ctx, "sudo", "systemctl", action, "pollen")
		}
	default:
		return fmt.Errorf("daemon management is only supported on macOS and Linux")
	}
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()
	return c.Run()
}
