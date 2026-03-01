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

func newServiceCmd(action, short string) *cobra.Command {
	return &cobra.Command{
		Use:   action,
		Short: short,
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			if err := servicectl(action, cmd); err != nil {
				os.Exit(1)
			}
		},
	}
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
		return fmt.Errorf("daemon management is only supported on macOS and Linux")
	}
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()
	return c.Run()
}
