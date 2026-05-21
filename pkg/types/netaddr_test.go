// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsRoutableIP(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"91.99.170.199", true},
		{"192.168.0.5", true},
		{"10.0.0.42", true},
		{"2001:db8::1", true},
		{"fd07:b51a:cc66::1", true},
		{"127.0.0.1", false},
		{"::1", false},
		{"169.254.1.1", false},
		{"fe80::1", false},
		{"224.0.0.1", false},
		{"0.0.0.0", false},
		{"::", false},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			require.Equal(t, tc.want, IsRoutableIP(netip.MustParseAddr(tc.in)))
		})
	}
	require.False(t, IsRoutableIP(netip.Addr{}), "zero-value addr is never routable")
}

func TestIsPublicIP(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"91.99.170.199", true},
		{"2001:db8::1", true},
		{"192.168.0.5", false},
		{"10.0.0.42", false},
		{"172.16.5.1", false},
		{"fd07:b51a:cc66::1", false},
		{"127.0.0.1", false},
		{"169.254.1.1", false},
		{"0.0.0.0", false},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			require.Equal(t, tc.want, IsPublicIP(netip.MustParseAddr(tc.in)))
		})
	}
}
