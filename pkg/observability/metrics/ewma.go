package metrics

import (
	"math"
	"sync/atomic"
)

// EWMA is a lock-free exponentially-weighted moving average. A nil *EWMA is
// safe to use — Update is a no-op and Value returns 0.
type EWMA struct {
	bits  atomic.Uint64
	alpha float64
}

// NewEWMA creates an EWMA with the given smoothing factor, starting at 0.
func NewEWMA(alpha float64) *EWMA {
	return &EWMA{alpha: alpha}
}

// NewEWMAFrom creates an EWMA with the given smoothing factor and initial value.
func NewEWMAFrom(alpha, initial float64) *EWMA {
	e := &EWMA{alpha: alpha}
	e.bits.Store(math.Float64bits(initial))
	return e
}

func (e *EWMA) Update(sample float64) {
	if e == nil {
		return
	}
	for {
		old := e.bits.Load()
		oldVal := math.Float64frombits(old)
		newVal := e.alpha*sample + (1-e.alpha)*oldVal
		if e.bits.CompareAndSwap(old, math.Float64bits(newVal)) {
			return
		}
	}
}

// Reset sets the EWMA to an exact value, discarding history.
func (e *EWMA) Reset(value float64) {
	if e == nil {
		return
	}
	e.bits.Store(math.Float64bits(value))
}

func (e *EWMA) Value() float64 {
	if e == nil {
		return 0
	}
	return math.Float64frombits(e.bits.Load())
}
