// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func peerKeyN(n byte) types.PeerKey {
	var b [32]byte
	b[0] = n
	return types.PeerKeyFromBytes(b[:])
}

func TestClassifyNetwork(t *testing.T) {
	cases := []struct {
		input  string
		bucket string
	}{
		{"192.168.0.5:60611", "lan"},
		{"[::ffff:192.168.0.5]:60611", "lan"},
		{"10.0.0.42:60611", "lan"},
		{"172.16.5.1:60611", "lan"},
		{"172.20.5.1:60611", "lan"},
		{"[fd07:b51a:cc66::1]:60611", "lan"},
		{"91.99.170.199:60611", "public-v4"},
		{"[2001:db8::1]:60611", "public-v6"},
		{"127.0.0.1:60611", ""},
		{"169.254.1.1:60611", ""},
		{"0.0.0.0:60611", ""},
		{"[fe80::1%eth0]:60611", ""}, // invalid as host:port
		{"not-an-addr", ""},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			require.Equal(t, tc.bucket, classifyNetwork(tc.input))
		})
	}
}

func TestPickBootstrapPeers_OnePerNetwork(t *testing.T) {
	now := time.Now()
	local := peerKeyN(1)
	remote := peerKeyN(2)
	snap := state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local:  {IPs: []string{"192.168.0.10"}, LocalPort: 60611, LastEventAt: now},
			remote: {IPs: []string{"91.99.170.199"}, LocalPort: 60611, LastEventAt: now},
		},
	}

	peers := pickBootstrapPeers(snap)
	require.Len(t, peers, 2, "LAN + public should produce two entries")

	for _, p := range peers {
		require.Len(t, p.Addrs, 1)
	}
}

func TestPickBootstrapPeers_CollapsesSameLAN(t *testing.T) {
	now := time.Now()
	local := peerKeyN(1)
	other := peerKeyN(2)
	third := peerKeyN(3)
	snap := state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local: {IPs: []string{"192.168.0.10"}, LocalPort: 60611, LastEventAt: now},
			other: {IPs: []string{"192.168.0.11"}, LocalPort: 60611, LastEventAt: now.Add(-time.Hour)},
			third: {IPs: []string{"192.168.0.12"}, LocalPort: 60611, LastEventAt: now.Add(-2 * time.Hour)},
		},
	}

	peers := pickBootstrapPeers(snap)
	require.Len(t, peers, 1, "three peers on the same LAN must collapse to one entry")
	require.Equal(t, local.Bytes(), peers[0].Peer.PeerPub, "self preferred over other LAN peers")
}

func TestPickBootstrapPeers_PrefersSelfPerNetwork(t *testing.T) {
	now := time.Now()
	local := peerKeyN(1)
	other := peerKeyN(2)
	snap := state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local: {IPs: []string{"192.168.0.10"}, LocalPort: 60611, LastEventAt: now.Add(-time.Hour)},
			other: {IPs: []string{"192.168.0.11"}, LocalPort: 60611, LastEventAt: now},
		},
	}

	peers := pickBootstrapPeers(snap)
	require.Len(t, peers, 1)
	require.Equal(t, local.Bytes(), peers[0].Peer.PeerPub,
		"self wins even when another LAN peer is more recently active")
}

func TestPickBootstrapPeers_PreferentialAddressInBucket(t *testing.T) {
	now := time.Now()
	local := peerKeyN(1)
	remote := peerKeyN(2)
	snap := state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local: {
				IPs:                []string{"192.168.0.10"},
				LocalPort:          60611,
				LastEventAt:        now,
				ObservedExternalIP: "203.0.113.5",
				ExternalPort:       60611,
			},
			remote: {IPs: []string{"203.0.113.10"}, LocalPort: 60611, LastEventAt: now},
		},
	}

	peers := pickBootstrapPeers(snap)
	require.Len(t, peers, 2, "self covers LAN and observed-external; remote covers public")
	gotAddrs := map[string][]string{}
	for _, p := range peers {
		gotAddrs[string(p.Peer.PeerPub)] = p.Addrs
	}
	require.Contains(t, gotAddrs[string(local.Bytes())], "192.168.0.10:60611")
	require.NotContains(t, gotAddrs[string(local.Bytes())], "203.0.113.5:60611",
		"self's public address must not appear when a stable public peer is available")
}

func TestPickBootstrapPeers_CollapsesIPv4AndULAOnSamePeer(t *testing.T) {
	now := time.Now()
	local := peerKeyN(1)
	snap := state.Snapshot{
		LocalID: local,
		Nodes: map[types.PeerKey]state.NodeView{
			local: {
				IPs:         []string{"192.168.0.10", "fd07:b51a:cc66::1"},
				LocalPort:   60611,
				LastEventAt: now,
			},
		},
	}

	peers := pickBootstrapPeers(snap)
	require.Len(t, peers, 1, "v4 LAN and v6 ULA must share one bucket")
	require.Len(t, peers[0].Addrs, 1, "merged LAN bucket emits one address per peer")
}
