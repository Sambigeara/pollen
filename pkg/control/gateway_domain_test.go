// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestClusterGatewayDomain(t *testing.T) {
	mk := func(domains ...string) state.Snapshot {
		nodes := make(map[types.PeerKey]state.NodeView, len(domains))
		for i, d := range domains {
			pk := types.PeerKey{byte(i + 1)}
			nodes[pk] = state.NodeView{GatewayDomain: d}
		}
		return state.Snapshot{Nodes: nodes}
	}

	cases := []struct {
		name    string
		domains []string
		want    string
	}{
		{"empty cluster", nil, ""},
		{"all blank", []string{"", ""}, ""},
		{"single configured", []string{"", ".staging.pln.sh"}, ".staging.pln.sh"},
		{"lex-min tie-break", []string{".b.example", ".a.example"}, ".a.example"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, clusterGatewayDomain(mk(tc.domains...)))
		})
	}
}

func TestURLBuilders(t *testing.T) {
	publisher := types.PeerKey{0xaa}
	t.Run("host-based requires domain and name", func(t *testing.T) {
		require.Empty(t, hostBasedURL("", "docs", publisher))
		require.Empty(t, hostBasedURL(".pln.sh", "", publisher))
		require.Equal(t, "https://docs-"+publisher.Slug()+".pln.sh", hostBasedURL(".pln.sh", "docs", publisher))
	})
	t.Run("path-based gated on public flag", func(t *testing.T) {
		require.Empty(t, pathBasedURL(".pln.sh", "fn", "echo", publisher, false))
		require.Empty(t, pathBasedURL("", "fn", "echo", publisher, true))
		require.Equal(t, "https://fn.pln.sh/"+publisher.Slug()+"/echo", pathBasedURL(".pln.sh", "fn", "echo", publisher, true))
	})
}
