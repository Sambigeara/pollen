//go:build consolidation_target

package traffic

import (
	"testing"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestNoopRecorderExists verifies that the target API exposes a Noop Recorder.
func TestNoopRecorderExists(t *testing.T) {
	// Noop must be assignable to the Recorder interface.
	var r Recorder = Noop
	require.NotNil(t, r)

	// Calling Record on the noop must not panic.
	var pk types.PeerKey
	pk[0] = 1
	r.Record(pk, 100, 200)
}

// TestNoopRecorderSatisfiesInterface is a compile-time assertion.
func TestNoopRecorderSatisfiesInterface(t *testing.T) {
	// This assignment will fail to compile if Noop does not implement Recorder.
	var _ Recorder = Noop

	// The real Tracker must still satisfy Recorder.
	var _ Recorder = New()
}
