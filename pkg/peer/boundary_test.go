//go:build consolidation_target

package peer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStutterRemoval verifies that the target API uses non-stuttering names.
// In the target state:
//   - PeerState       -> State
//   - PeerStateDiscovered  -> Discovered
//   - PeerStateConnecting  -> Connecting
//   - PeerStateConnected   -> Connected
//   - PeerStateUnreachable -> Unreachable
//   - PeerStateCounts      -> StateCounts
//   - ConnectStageUnspecified is deleted

func TestStateTypeExists(t *testing.T) {
	// State is the renamed PeerState type.
	var s State
	s = Discovered
	require.Equal(t, Discovered, s)

	s = Connecting
	require.Equal(t, Connecting, s)

	s = Connected
	require.Equal(t, Connected, s)

	s = Unreachable
	require.Equal(t, Unreachable, s)
}

func TestStateCountsTypeExists(t *testing.T) {
	// StateCounts is the renamed PeerStateCounts type.
	var sc StateCounts
	_ = sc
}

func TestConnectStageUnspecifiedDeleted(t *testing.T) {
	// ConnectStageDirect should be the iota zero value after
	// ConnectStageUnspecified is deleted.
	require.Equal(t, ConnectStage(0), ConnectStageDirect)
}
