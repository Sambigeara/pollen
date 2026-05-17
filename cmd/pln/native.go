// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/tls"
	"net"
	"strings"

	"github.com/sambigeara/pollen/pkg/wire"
)

const plnTargetScheme = "pln://"

// parsePlnTarget returns (host:port, true) when the target uses the
// pln:// scheme; (_, false) otherwise.
func parsePlnTarget(target string) (string, bool) {
	if !strings.HasPrefix(target, plnTargetScheme) {
		return "", false
	}
	return strings.TrimPrefix(target, plnTargetScheme), true
}

func plnNativeDialer(dir, addr string) func(string, string, *tls.Config) (net.Conn, error) {
	return func(_, _ string, _ *tls.Config) (net.Conn, error) {
		cfg, err := wire.ClientTLSConfig(dir)
		if err != nil {
			return nil, err
		}
		dialer := &tls.Dialer{Config: cfg}
		return dialer.Dial("tcp", addr)
	}
}
