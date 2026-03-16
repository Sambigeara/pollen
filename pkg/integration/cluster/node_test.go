//go:build integration

package cluster

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTestNode_StartsAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sw := NewVirtualSwitch(SwitchConfig{})
	auth := NewClusterAuth(t)

	addr := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 60611}
	tn := NewTestNode(t, TestNodeConfig{
		Context: ctx,
		Switch:  sw,
		Auth:    auth,
		Addr:    addr,
		Name:    "node-1",
		Role:    Public,
	})

	require.NotEqual(t, [32]byte{}, tn.PeerKey())
	require.NotNil(t, tn.Node())
	require.NotNil(t, tn.Store())
	require.Equal(t, Public, tn.Role())
	require.Equal(t, "node-1", tn.Name())
}
