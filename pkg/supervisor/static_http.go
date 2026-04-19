// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

const staticReadHeaderTimeout = 10 * time.Second

func (n *Supervisor) startStaticHTTP(ctx context.Context, addr string) error {
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("static http listen: %w", err)
	}
	n.log.Infow("static http listener", "addr", l.Addr().String())
	srv := &http.Server{Handler: n.staticSvc, ReadHeaderTimeout: staticReadHeaderTimeout}
	go func() {
		<-ctx.Done()
		srv.Close() //nolint:errcheck
	}()
	if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
