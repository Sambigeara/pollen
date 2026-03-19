package coords

import (
	"math"
	"math/rand/v2"
	"time"
)

// Coord represents a Vivaldi network coordinate: 2D Euclidean position plus a
// non-negative height that models last-mile latency asymmetries.
type Coord struct {
	X      float64
	Y      float64
	Height float64
}

// Sample is a single RTT measurement to a peer whose coordinate is known.
type Sample struct {
	RTT       time.Duration
	PeerCoord Coord
}

const (
	CcDefault = 0.25   // adaptive filter tuning constant
	CeDefault = 0.25   // error weight tuning constant
	MinHeight = 10e-6  // floor to keep height positive
	MaxCoord  = 10_000 // clamp bound for coordinate components

	// MinRTTFloor dampens the relative error calculation for low-latency links.
	// Without it, sub-millisecond jitter on a 1-3ms LAN link produces 16-50%
	// relative error, preventing the error estimate from settling below the
	// health-check threshold. The floor dampens the relative error for low-RTT
	// links, reducing both the error estimate and the weight given to coordinate
	// adjustments.
	MinRTTFloor = 2.0 // milliseconds

	// PublishEpsilon is the minimum coordinate movement (in distance units)
	// before a node should re-publish its coordinates via gossip.
	PublishEpsilon = 0.5
)

const initRadius = 10.0 // ms-scale spread for initial coordinate diversity

// RandomCoord returns a coordinate uniformly distributed within a circle of
// radius initRadius. The non-trivial inter-node distances let Vivaldi error
// begin dropping from tick 1, avoiding the stuck-at-1.0 cold-start problem.
func RandomCoord() Coord {
	angle := rand.Float64() * 2 * math.Pi       //nolint:gosec,mnd
	r := math.Sqrt(rand.Float64()) * initRadius //nolint:gosec
	return Coord{
		X:      r * math.Cos(angle),
		Y:      r * math.Sin(angle),
		Height: MinHeight,
	}
}

// Distance returns the Vivaldi distance between two coordinates:
// ||a.pos − b.pos|| + a.Height + b.Height.
func Distance(a, b Coord) float64 {
	dx := a.X - b.X
	dy := a.Y - b.Y
	return math.Sqrt(dx*dx+dy*dy) + a.Height + b.Height
}

// MovementDistance returns how far a coordinate moved in Euclidean 3D
// coordinate space (x, y, height).
func MovementDistance(a, b Coord) float64 {
	dx := a.X - b.X
	dy := a.Y - b.Y
	dh := a.Height - b.Height
	return math.Sqrt(dx*dx + dy*dy + dh*dh)
}

// Update applies a single RTT sample to the local coordinate using the Vivaldi
// algorithm. It returns the updated coordinate and error estimate.
//
// localErr represents the node's confidence in its current position
// (higher = less confidence, 0 = perfect). It should stay in [0, 1]
// because the per-sample relative error is bounded to [0, 1) by the
// max(rtt, dist, MinRTTFloor) denominator.
func Update(local Coord, localErr float64, s Sample) (Coord, float64) {
	rtt := s.RTT.Seconds() * 1000 //nolint:mnd
	if rtt <= 0 {
		return local, localErr
	}

	dist := Distance(local, s.PeerCoord)
	if dist < MinHeight {
		dist = MinHeight
	}

	// Normalize by max(rtt, dist) so the per-sample relative error stays in
	// [0, 1). Using just rtt as the denominator causes LAN peers with large
	// predicted distances to produce errors of 100+, which drives the error
	// estimate to extreme values and prevents convergence.
	err := math.Abs(rtt-dist) / max(rtt, dist, MinRTTFloor)
	relWeight := localErr / (localErr + CeDefault)

	newErr := localErr + CcDefault*relWeight*(err-localErr)
	newErr = clamp(newErr, 0, 1)

	// Compute force: positive = push apart, negative = pull together.
	delta := CcDefault * relWeight
	force := delta * (rtt - dist)

	// Unit vector from peer to local.
	dx := local.X - s.PeerCoord.X
	dy := local.Y - s.PeerCoord.Y
	mag := math.Sqrt(dx*dx + dy*dy)
	if mag < MinHeight {
		angle := rand.Float64() * 2 * math.Pi //nolint:gosec,mnd
		dx = math.Cos(angle) * MinHeight
		dy = math.Sin(angle) * MinHeight
		mag = MinHeight
	}
	ux, uy := dx/mag, dy/mag

	// Apply force to the Euclidean component.
	newX := local.X + ux*(force)
	newY := local.Y + uy*(force)

	// Apply force to height.
	newHeight := local.Height + force
	if newHeight < MinHeight {
		newHeight = MinHeight
	}

	newX = clamp(newX, -MaxCoord, MaxCoord)
	newY = clamp(newY, -MaxCoord, MaxCoord)
	newHeight = clamp(newHeight, MinHeight, MaxCoord)

	return Coord{X: newX, Y: newY, Height: newHeight}, newErr
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
