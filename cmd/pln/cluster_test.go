// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitListenAddr(t *testing.T) {
	cases := []struct {
		in       string
		wantHost string
		wantPort string
		wantErr  bool
	}{
		{in: "7443", wantPort: "7443"},
		{in: ":7443", wantPort: "7443"},
		{in: "0.0.0.0:7443", wantHost: "0.0.0.0", wantPort: "7443"},
		{in: "[::]:7443", wantHost: "::", wantPort: "7443"},
		{in: "", wantErr: true},
		{in: "garbage", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			host, port, err := splitListenAddr(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantHost, host)
			require.Equal(t, tc.wantPort, port)
		})
	}
}

func TestPickFounderWireEndpoint(t *testing.T) {
	peers := []bootstrapResult{
		{target: "lan", addrs: []string{"192.168.0.10:60611"}, public: false},
		{target: "edge", addrs: []string{"91.99.170.199:60611"}, public: true},
		{target: "edge2", addrs: []string{"203.0.113.5:60611"}, public: true},
	}
	require.Equal(t, "91.99.170.199:7443", pickFounderWireEndpoint(peers, ":7443"))
	require.Equal(t, "91.99.170.199:7443", pickFounderWireEndpoint(peers, "7443"))
	require.Empty(t, pickFounderWireEndpoint(peers, ""), "no port means no endpoint")

	lanOnly := []bootstrapResult{
		{target: "lan", addrs: []string{"192.168.0.10:60611"}, public: false},
	}
	require.Empty(t, pickFounderWireEndpoint(lanOnly, ":7443"), "no public peer means no fallback")
}
