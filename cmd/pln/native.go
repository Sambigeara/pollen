// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/tls"
	"net"

	"github.com/sambigeara/pollen/pkg/wire"
)

const plnTargetScheme = "pln://"

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
