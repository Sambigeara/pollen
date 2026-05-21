// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/spf13/cobra"
)

var errRemoteUnsupported = errors.New("this command must run on the local node; the active ctx targets an SSH host")

func ensureSystemServiceContext(name string) error {
	if name == defaultContextName {
		return nil
	}
	cf, err := loadContexts()
	if err != nil {
		return err
	}
	entry, ok := cf.Contexts[name]
	if !ok {
		return notFoundErr("context %q not found", name)
	}
	if entry.isSSHBridge() {
		return fmt.Errorf("ctx %q is an SSH bridge; run `pln up` on %s directly", name, entry.Host)
	}
	if runtime.GOOS != osDarwin {
		return fmt.Errorf("per-ctx background services are only supported on macOS; run `pln up --ctx %s` in the foreground", name)
	}
	return nil
}

func resolveTarget(cmd *cobra.Command, name, defaultDir string) (contextEntry, error) {
	entry, err := resolveContextBindings(name, defaultDir)
	if err != nil {
		return contextEntry{}, err
	}
	if cmd.Flags().Changed("dir") {
		entry.Dir, _ = cmd.Flags().GetString("dir")
	}
	override := ""
	if f := cmd.Flag("host"); f != nil {
		override = f.Value.String()
	}
	if !cmd.Flags().Changed("host") {
		if v := os.Getenv("PLN_HOST"); v != "" {
			override = v
		}
	}
	if override != "" {
		entry.Host, entry.Wire = "", ""
		if strings.HasPrefix(override, plnTargetScheme) {
			entry.Wire = override
		} else {
			entry.Host = override
		}
	}
	return entry, nil
}

// net.Pipe so SetReadDeadline works; raw exec.Cmd pipes don't support deadlines.
func sshBridgeDial(target string) (net.Conn, error) {
	cmdCtx, cancel := context.WithCancel(context.Background())

	args := slices.Concat(sshBaseArgs, []string{target, "sudo", "-n", "-u", "pln", "pln", "bridge"})
	cmd := exec.CommandContext(cmdCtx, "ssh", args...)
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("ssh bridge stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("ssh bridge stdout: %w", err)
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("ssh bridge start: %w", err)
	}

	clientSide, serverSide := net.Pipe()

	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = io.Copy(stdin, serverSide)
		_ = stdin.Close()
	})
	wg.Go(func() {
		_, _ = io.Copy(serverSide, stdout)
		_ = serverSide.Close()
	})

	return &sshConn{
		Conn:   clientSide,
		cancel: cancel,
		cmd:    cmd,
		wg:     &wg,
	}, nil
}

type sshConn struct {
	net.Conn
	cancel    context.CancelFunc
	cmd       *exec.Cmd
	wg        *sync.WaitGroup
	closeOnce sync.Once
}

func (c *sshConn) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		_ = c.Conn.Close()
		c.wg.Wait()
		_ = c.cmd.Wait()
	})
	return nil
}
