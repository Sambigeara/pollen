//go:build consolidation_target

package store

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestGetAndAllNodesRemoved verifies that the old leak-prone APIs are gone.
// This file will only compile once Get() and AllNodes() are deleted and
// the new purpose-built query methods exist.

func TestLocalIDMethod(t *testing.T) {
	// LocalID is now a method, not a public field.
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	id := s.LocalID()
	require.Equal(t, types.PeerKeyFromBytes(pub), id)
}

func TestAllRouteInfoExists(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	info := s.AllRouteInfo()
	require.NotNil(t, info)

	// The local node should appear in the route info.
	localID := s.LocalID()
	_, ok := info[localID]
	require.True(t, ok, "local node should appear in AllRouteInfo")
}

func TestRouteNodeInfoFields(t *testing.T) {
	// Verify the RouteNodeInfo struct has the expected fields.
	rni := RouteNodeInfo{
		IPs:                []string{"10.0.0.1"},
		Reachable:          map[types.PeerKey]struct{}{},
		PubliclyAccessible: true,
	}
	require.Equal(t, []string{"10.0.0.1"}, rni.IPs)
	require.NotNil(t, rni.Reachable)
	require.True(t, rni.PubliclyAccessible)
}

func TestExternalPortExists(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	// ExternalPort for an unknown peer should return false.
	unknownPK := types.PeerKeyFromBytes(make([]byte, 32))
	_, ok := s.ExternalPort(unknownPK)
	require.False(t, ok)
}

func TestLocalRecordExists(t *testing.T) {
	pub := make([]byte, 32)
	pub[0] = 1
	s := newTestStore(pub)

	view := s.LocalRecord()
	// LocalNodeView should be a non-zero value for a store with local state.
	_ = view
}

func TestConnectionUsedAsMapKey(t *testing.T) {
	// Connection should be usable as a map key (comparable).
	c1 := Connection{PeerID: types.PeerKeyFromBytes([]byte{1}), RemotePort: 9000, LocalPort: 8000}
	c2 := Connection{PeerID: types.PeerKeyFromBytes([]byte{2}), RemotePort: 9000, LocalPort: 8000}

	m := map[Connection]bool{
		c1: true,
		c2: false,
	}
	require.True(t, m[c1])
	require.False(t, m[c2])
}
