package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"

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
	logPath := filepath.Join(os.Getenv("HOME"), "Library", "Logs", "pollen.log")

	if _, err := os.Stat(logPath); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "log file not found: %s\nis the daemon installed? try: pollen up -d\n", logPath)
		return
	}

	args := []string{"-n", strconv.Itoa(lines)}
	if follow {
		args = append(args, "-f")
	}
	args = append(args, logPath)

	c := exec.CommandContext(cmd.Context(), "tail", args...)
	c.Stdin = cmd.InOrStdin()
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()

	if err := c.Run(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to read logs: %v\n", err)
	}
}

func logsLinux(cmd *cobra.Command, follow bool, lines int) {
	unitPath := systemdUnitPath()
	if _, err := os.Stat(unitPath); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "service not installed: %s\nis the daemon installed? try: pollen up -d\n", unitPath)
		return
	}

	args := []string{"--user", "-u", systemdUnitName, "-n", strconv.Itoa(lines), "--no-pager"}
	if follow {
		args = append(args, "-f")
	}

	c := exec.CommandContext(cmd.Context(), "journalctl", args...)
	c.Stdin = cmd.InOrStdin()
	c.Stdout = cmd.OutOrStdout()
	c.Stderr = cmd.ErrOrStderr()

	if err := c.Run(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "failed to read logs: %v\n", err)
	}
}
