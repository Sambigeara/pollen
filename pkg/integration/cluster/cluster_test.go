//go:build integration

package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCluster_Builder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(t).
		AddNode("a", Public).
		AddNode("b", Public).
		Introduce("a", "b")

	c := b.Start(ctx)
	require.NotNil(t, c)
	require.Len(t, c.Nodes(), 2)
	require.NotNil(t, c.Node("a"))
	require.NotNil(t, c.Node("b"))
}

func TestCluster_PublicMeshPreset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := PublicMesh(t, 3, ctx)
	require.Len(t, c.Nodes(), 3)
}

func TestCluster_RelayRegionsPreset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := RelayRegions(t, 2, 1, ctx)
	require.Len(t, c.Nodes(), 4) // 2 relays + 2 private
	require.Len(t, c.NodesByRole(Public), 2)
	require.Len(t, c.NodesByRole(Private), 2)
}
