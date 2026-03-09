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

func TestRandomCoord(t *testing.T) {
	c1 := RandomCoord()
	require.Equal(t, MinHeight, c1.Height)
	require.LessOrEqual(t, c1.X*c1.X+c1.Y*c1.Y, initRadius*initRadius+1e-9)

	c2 := RandomCoord()
	require.False(t, c1.X == c2.X && c1.Y == c2.Y, "two random coords should differ")
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
	// With the RTT floor, error should settle below the 0.6 degradation
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

	require.Less(t, localErr, 0.6, "error should converge below 0.6 for LAN latencies with RTT floor")
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

func TestRandomInitLANConvergence(t *testing.T) {
	// Regression: random-init coords spread over 10ms radius produce predicted
	// distances far exceeding LAN RTTs (2-3ms). Without capping per-sample
	// relative error at 1.0, the error estimate stays pinned at 1.0 for
	// hundreds of ticks because every uncapped sample (err >> 1) is an EMA
	// input that can only push the average toward the ceiling.
	a := Coord{X: -7, Y: 5, Height: MinHeight} // ~8.6ms from origin
	b := Coord{X: 6, Y: -4, Height: MinHeight} // ~7.2ms from origin
	aErr, bErr := 1.0, 1.0
	rtt := 2 * time.Millisecond // LAN link

	for range 60 {
		a, aErr = Update(a, aErr, Sample{RTT: rtt, PeerCoord: b})
		b, bErr = Update(b, bErr, Sample{RTT: rtt, PeerCoord: a})
	}

	require.Less(t, aErr, 0.9, "error should drop below degraded threshold within 60 ticks")
	require.Less(t, bErr, 0.9, "error should drop below degraded threshold within 60 ticks")
}
