package metrics

import (
	"math"
	"sync/atomic"
)

// EWMA is a lock-free exponentially-weighted moving average.
type EWMA struct {
	bits  atomic.Uint64
	alpha float64
}

func NewEWMA(alpha, initial float64) *EWMA {
	e := &EWMA{alpha: alpha}
	e.Reset(initial)
	return e
}

// Update adds a new sample to the moving average.
func (e *EWMA) Update(sample float64) {
	for {
		oldBits := e.bits.Load()
		oldVal := math.Float64frombits(oldBits)
		newVal := e.alpha*sample + (1-e.alpha)*oldVal

		if e.bits.CompareAndSwap(oldBits, math.Float64bits(newVal)) {
			return
		}
	}
}

// Reset sets the EWMA to an exact value, discarding history.
func (e *EWMA) Reset(value float64) {
	e.bits.Store(math.Float64bits(value))
}

// Value returns the current moving average.
func (e *EWMA) Value() float64 {
	return math.Float64frombits(e.bits.Load())
}
