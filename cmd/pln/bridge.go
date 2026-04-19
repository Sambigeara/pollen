// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/spf13/cobra"
)

func newBridgeCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "bridge",
		Short:  "Relay stdin/stdout to the local daemon socket (used by --host)",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE:   withEnv(runBridge),
	}
}

// Anything written to stdout other than relayed bytes corrupts the HTTP/2
// stream, so diagnostics go to stderr via returned errors.
func runBridge(cmd *cobra.Command, _ []string, env *cliEnv) error {
	socketPath := filepath.Join(env.dir, socketName)
	conn, err := (&net.Dialer{}).DialContext(cmd.Context(), "unix", socketPath)
	if err != nil {
		return fmt.Errorf("dial %s: %w", socketPath, err)
	}
	defer conn.Close()
	uc := conn.(*net.UnixConn) //nolint:forcetypeassert

	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = io.Copy(conn, os.Stdin)
		_ = uc.CloseWrite()
	})

	_, copyErr := io.Copy(os.Stdout, conn)
	_ = conn.Close()
	wg.Wait()
	if copyErr != nil && !errors.Is(copyErr, io.EOF) {
		return copyErr
	}
	return nil
}
