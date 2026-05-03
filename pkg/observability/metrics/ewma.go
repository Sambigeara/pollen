// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"math"
	"sync/atomic"
)

type EWMA struct {
	bits  atomic.Uint64
	alpha float64
}

func NewEWMA(alpha, initial float64) *EWMA {
	e := &EWMA{alpha: alpha}
	e.Reset(initial)
	return e
}

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

func (e *EWMA) Reset(value float64) {
	e.bits.Store(math.Float64bits(value))
}

func (e *EWMA) Value() float64 {
	return math.Float64frombits(e.bits.Load())
}
