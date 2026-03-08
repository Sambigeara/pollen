package topology

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDistanceSymmetric(t *testing.T) {
	a := Coord{X: 1, Y: 2, Height: 0.5}
	b := Coord{X: 4, Y: 6, Height: 0.3}
	require.InDelta(t, Distance(a, b), Distance(b, a), 1e-12)
}

func TestDistanceIncludesHeight(t *testing.T) {
	a := Coord{X: 0, Y: 0, Height: 1.0}
	b := Coord{X: 0, Y: 0, Height: 2.0}
	require.InDelta(t, 3.0, Distance(a, b), 1e-12)
}

func TestCoordIsZero(t *testing.T) {
	require.True(t, Coord{}.IsZero())
	require.True(t, Coord{X: 0, Y: 0, Height: 0}.IsZero())
	require.False(t, Coord{X: 1, Y: 0, Height: 0}.IsZero())
	require.False(t, Coord{X: 0, Y: 0, Height: MinHeight}.IsZero())
}

func TestDistanceZeroCoords(t *testing.T) {
	a := Coord{}
	b := Coord{}
	require.InDelta(t, 0.0, Distance(a, b), 1e-12)
}

func TestMovementDistanceZeroForEqualCoords(t *testing.T) {
	c := Coord{X: 1.2, Y: -3.4, Height: 5.6}
	require.InDelta(t, 0.0, MovementDistance(c, c), 1e-12)
}

func TestMovementDistanceIncludesHeightDelta(t *testing.T) {
	a := Coord{X: 10, Y: -2, Height: 1}
	b := Coord{X: 10, Y: -2, Height: 4}
	require.InDelta(t, 3.0, MovementDistance(a, b), 1e-12)
}

func TestUpdateZeroRTTIsNoop(t *testing.T) {
	local := Coord{X: 1, Y: 2, Height: 0.5}
	localErr := 0.5
	s := Sample{RTT: 0, PeerCoord: Coord{X: 5, Y: 5, Height: 0.1}}
	newCoord, newErr := Update(local, localErr, s)
	require.Equal(t, local, newCoord)
	require.Equal(t, localErr, newErr)
}

func TestUpdateNegativeRTTIsNoop(t *testing.T) {
	local := Coord{X: 1, Y: 2, Height: 0.5}
	localErr := 0.5
	s := Sample{RTT: -5 * time.Millisecond, PeerCoord: Coord{X: 5, Y: 5, Height: 0.1}}
	newCoord, newErr := Update(local, localErr, s)
	require.Equal(t, local, newCoord)
	require.Equal(t, localErr, newErr)
}

func TestConvergence(t *testing.T) {
	// Two nodes with a 50ms RTT between them. After many updates, the
	// distance between their coordinates should converge toward 50.
	a := Coord{}
	b := Coord{X: 100, Y: 0, Height: MinHeight}
	aErr, bErr := 1.0, 1.0
	rtt := 50 * time.Millisecond

	for range 200 {
		a, aErr = Update(a, aErr, Sample{RTT: rtt, PeerCoord: b})
		b, bErr = Update(b, bErr, Sample{RTT: rtt, PeerCoord: a})
	}

	dist := Distance(a, b)
	require.InDelta(t, 50.0, dist, 5.0, "distance should converge near 50ms RTT, got %f", dist)
}

func TestConvergenceThreeNodes(t *testing.T) {
	// Triangle: A-B=10ms, B-C=20ms, A-C=25ms.
	a, b, c := Coord{}, Coord{X: 50, Y: 0}, Coord{X: 0, Y: 50}
	aErr, bErr, cErr := 1.0, 1.0, 1.0

	for range 300 {
		a, aErr = Update(a, aErr, Sample{RTT: 10 * time.Millisecond, PeerCoord: b})
		a, aErr = Update(a, aErr, Sample{RTT: 25 * time.Millisecond, PeerCoord: c})
		b, bErr = Update(b, bErr, Sample{RTT: 10 * time.Millisecond, PeerCoord: a})
		b, bErr = Update(b, bErr, Sample{RTT: 20 * time.Millisecond, PeerCoord: c})
		c, cErr = Update(c, cErr, Sample{RTT: 25 * time.Millisecond, PeerCoord: a})
		c, cErr = Update(c, cErr, Sample{RTT: 20 * time.Millisecond, PeerCoord: b})
	}

	require.InDelta(t, 10.0, Distance(a, b), 5.0)
	require.InDelta(t, 20.0, Distance(b, c), 5.0)
	require.InDelta(t, 25.0, Distance(a, c), 5.0)
	_ = cErr
}

func TestClampingPreventsRunaway(t *testing.T) {
	local := Coord{X: MaxCoord - 1, Y: MaxCoord - 1, Height: 1}
	localErr := 1.0
	// Huge RTT should push far, but clamping limits coordinates.
	s := Sample{RTT: 999 * time.Second, PeerCoord: Coord{X: -MaxCoord, Y: -MaxCoord, Height: 1}}
	newCoord, _ := Update(local, localErr, s)
	require.LessOrEqual(t, math.Abs(newCoord.X), float64(MaxCoord))
	require.LessOrEqual(t, math.Abs(newCoord.Y), float64(MaxCoord))
	require.LessOrEqual(t, newCoord.Height, float64(MaxCoord))
}

func TestHeightFloor(t *testing.T) {
	// When coords are very close and RTT is very small, height shouldn't go below MinHeight.
	local := Coord{X: 0, Y: 0, Height: MinHeight}
	localErr := 1.0
	s := Sample{RTT: time.Microsecond, PeerCoord: Coord{X: 0.001, Y: 0, Height: MinHeight}}
	newCoord, _ := Update(local, localErr, s)
	require.GreaterOrEqual(t, newCoord.Height, MinHeight)
}

func TestLowLatencyConvergence(t *testing.T) {
	// Simulate a 5-node LAN cluster with 2-3ms RTTs and ~0.5ms jitter.
	// With the RTT floor, error should settle below the 0.75 degradation
	// threshold despite the high relative jitter on fast links.
	local := Coord{}
	localErr := 1.0
	peers := []Coord{
		{X: 2, Y: 1, Height: MinHeight},
		{X: -1, Y: 3, Height: MinHeight},
		{X: 3, Y: -2, Height: MinHeight},
		{X: -2, Y: -1, Height: MinHeight},
	}
	peerErrs := []float64{0.5, 0.5, 0.5, 0.5}

	baseRTTs := []time.Duration{
		2 * time.Millisecond,
		3 * time.Millisecond,
		1500 * time.Microsecond,
		2500 * time.Microsecond,
	}

	for round := range 300 {
		for i, p := range peers {
			// Add ±0.5ms jitter.
			jitter := time.Duration((round%5)-2) * 100 * time.Microsecond
			rtt := baseRTTs[i] + jitter
			if rtt <= 0 {
				rtt = 100 * time.Microsecond
			}
			local, localErr = Update(local, localErr, Sample{RTT: rtt, PeerCoord: p})
			peers[i], peerErrs[i] = Update(p, peerErrs[i], Sample{RTT: rtt, PeerCoord: local})
		}
	}

	require.Less(t, localErr, 0.75, "error should converge below 0.75 for LAN latencies with RTT floor")
}

func TestErrorEstimateDecreases(t *testing.T) {
	local := Coord{}
	localErr := 1.0
	peer := Coord{X: 50, Y: 0, Height: MinHeight}

	for range 100 {
		local, localErr = Update(local, localErr, Sample{
			RTT:       50 * time.Millisecond,
			PeerCoord: peer,
		})
	}

	require.Less(t, localErr, 0.5, "error estimate should decrease with consistent samples")
}
