// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package coords

import (
	"math"
	"math/rand/v2"
	"time"
)

type Coord struct {
	X      float64
	Y      float64
	Height float64
}

type Sample struct {
	RTT       time.Duration
	PeerCoord Coord
	PeerErr   float64
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

	PublishEpsilon = 0.5
)

const initRadius = 10.0 // ms-scale spread for initial coordinate diversity

// RandomCoord spreads initial coords so Vivaldi error drops from tick 1
// instead of stalling at 1.0 when all nodes start at the origin.
func RandomCoord() Coord {
	angle := rand.Float64() * 2 * math.Pi       //nolint:gosec,mnd
	r := math.Sqrt(rand.Float64()) * initRadius //nolint:gosec
	return Coord{
		X:      r * math.Cos(angle),
		Y:      r * math.Sin(angle),
		Height: MinHeight,
	}
}

func Distance(a, b Coord) float64 {
	dx := a.X - b.X
	dy := a.Y - b.Y
	return math.Sqrt(dx*dx+dy*dy) + a.Height + b.Height
}

func MovementDistance(a, b Coord) float64 {
	dx := a.X - b.X
	dy := a.Y - b.Y
	dh := a.Height - b.Height
	return math.Sqrt(dx*dx + dy*dy + dh*dh)
}

func Update(local Coord, localErr float64, s Sample) (Coord, float64) {
	rtt := s.RTT.Seconds() * 1000 //nolint:mnd
	if rtt <= 0 {
		return local, localErr
	}

	dist := Distance(local, s.PeerCoord)
	if dist < MinHeight {
		dist = MinHeight
	}

	// max(rtt, dist, MinRTTFloor) keeps per-sample relative error in [0, 1).
	// Using just rtt as denominator causes LAN peers with large predicted
	// distances to produce errors of 100+, preventing convergence.
	err := math.Abs(rtt-dist) / max(rtt, dist, MinRTTFloor)

	// Zero peer error means the peer hasn't gossiped its error yet.
	peerErr := s.PeerErr
	if peerErr == 0 {
		peerErr = CeDefault
	}
	relWeight := localErr / (localErr + peerErr)

	newErr := localErr + CeDefault*relWeight*(err-localErr)
	newErr = clamp(newErr, 0, 1)

	delta := CcDefault * relWeight
	force := delta * (rtt - dist)

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

	newX := local.X + ux*(force)
	newY := local.Y + uy*(force)

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
