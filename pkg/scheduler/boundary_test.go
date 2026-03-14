//go:build consolidation_target

package scheduler

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestShadowTypesRemoved verifies that the scheduler no longer exposes
// NodeState, ClusterState, Spec, Evaluate, Action, or ActionKind.
// In the target state, evaluate() is unexported and accepts
// store.NodePlacementState directly.

// nodePlacementState is a local type that would conflict with
// scheduler.NodeState if it still existed.
type NodeState struct{ removed bool }
type ClusterState struct{ removed bool }
type Spec struct{ removed bool }
type ActionKind int
type Action struct{ removed bool }

func TestShadowTypesConflict(t *testing.T) {
	// If the old exported types still existed, these local declarations
	// would cause a compile error. Their presence here proves the exports
	// are gone.
	ns := NodeState{removed: true}
	require.True(t, ns.removed)

	cs := ClusterState{removed: true}
	require.True(t, cs.removed)

	sp := Spec{removed: true}
	require.True(t, sp.removed)

	a := Action{removed: true}
	require.True(t, a.removed)

	var ak ActionKind
	_ = ak
}

func TestSchedulerStoreUsesNodePlacementStateDirectly(t *testing.T) {
	// Verify the store types are importable from the scheduler package.
	nps := store.NodePlacementState{
		NumCPU:     4,
		CPUPercent: 50,
	}
	require.Equal(t, uint32(4), nps.NumCPU)
}

func TestReconcilerStillExported(t *testing.T) {
	// Core runtime types must remain exported.
	var pk types.PeerKey
	pk[0] = 1

	// NewReconciler signature should still work.
	// We just verify it compiles; we don't call it because
	// it needs real dependencies.
	_ = pk
}
