package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

func newLogsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Show daemon logs",
		Args:  cobra.NoArgs,
		Run:   runLogs,
	}
	cmd.Flags().BoolP("follow", "f", false, "Stream logs in real time")
	cmd.Flags().IntP("lines", "n", 50, "Number of lines to show") //nolint:mnd
	return cmd
}

func runLogs(cmd *cobra.Command, _ []string) {
	follow, _ := cmd.Flags().GetBool("follow")
	lines, _ := cmd.Flags().GetInt("lines")

	switch runtime.GOOS {
	case osDarwin:
		logsDarwin(cmd, follow, lines)
	case osLinux:
		logsLinux(cmd, follow, lines)
	default:
		fmt.Fprintf(cmd.ErrOrStderr(), "unsupported platform: %s (supported: darwin, linux)\n", runtime.GOOS)
	}
}

func logsDarwin(cmd *cobra.Command, follow bool, lines int) {
	prefix := "/usr/local"
	if out, err := exec.CommandContext(cmd.Context(), "brew", "--prefix").Output(); err == nil {
		prefix = strings.TrimSpace(string(out))
	} else if _, err := os.Stat("/opt/homebrew"); err == nil {
		prefix = "/opt/homebrew"
	}
	logPath := filepath.Join(prefix, "var", "log", "pollen.log")

	args := []string{"-n", strconv.Itoa(lines)}
	if follow {
		args = append(args, "-f")
	}
	args = append(args, logPath)

	c := exec.CommandContext(cmd.Context(), "tail", args...)
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()
	_ = c.Run()
}

func logsLinux(cmd *cobra.Command, follow bool, lines int) {
	args := []string{"-u", "pollen", "-n", strconv.Itoa(lines), "--no-pager"}
	if follow {
		args = append(args, "-f")
	}

	c := exec.CommandContext(cmd.Context(), "journalctl", args...)
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()
	_ = c.Run()
}
