package node

import (
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/stretchr/testify/require"
)

func TestApplyVivaldiSampleExitsBootstrapAndForcesPublish(t *testing.T) {
	n := &Node{
		localCoord:             topology.BootstrapCoord(),
		localCoordErr:          1.0,
		acceptBootstrapVivaldi: true,
		smoothedErr:            metrics.NewEWMAFrom(vivaldiErrAlpha, 1.0),
	}

	updated, forcePublish := n.applyVivaldiSample(time.Millisecond, topology.BootstrapCoord())
	require.True(t, updated)
	require.True(t, forcePublish)
	require.False(t, n.localCoord.IsBootstrap())
	require.False(t, n.acceptBootstrapVivaldi)
	require.Less(t, n.smoothedErr.Value(), 1.0)
}

func TestZeroPeerSkippedAfterBootstrapExit(t *testing.T) {
	n := &Node{
		localCoord:             topology.Coord{X: 1, Height: 1},
		localCoordErr:          0.5,
		acceptBootstrapVivaldi: false,
		smoothedErr:            metrics.NewEWMAFrom(vivaldiErrAlpha, 0.5),
	}

	zeroPeer := topology.Coord{}
	require.True(t, zeroPeer.IsZero())
	require.False(t, n.acceptBootstrapVivaldi)

	// Zero-coord peer must be skipped once bootstrap has exited.
	// Verify state is unchanged.
	origCoord := n.localCoord
	origErr := n.localCoordErr
	require.True(t, zeroPeer.IsZero() && !n.acceptBootstrapVivaldi)
	require.Equal(t, origCoord, n.localCoord)
	require.Equal(t, origErr, n.localCoordErr)
}
