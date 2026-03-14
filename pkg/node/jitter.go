package node

import (
	"context"
	"math/rand/v2"
	"time"
)

const jitterScale = 2

type jitterTicker struct {
	C    <-chan time.Time
	stop context.CancelFunc
}

func newJitterTicker(ctx context.Context, base time.Duration, percent float64) *jitterTicker {
	tickCh := make(chan time.Time)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(tickCh)
		timer := time.NewTimer(jitterDuration(base, percent))
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-timer.C:
				select {
				case <-ctx.Done():
					return
				case tickCh <- t:
				}
				timer.Reset(jitterDuration(base, percent))
			}
		}
	}()
	return &jitterTicker{C: tickCh, stop: cancel}
}

func (t *jitterTicker) Stop() {
	t.stop()
}

func jitterDuration(d time.Duration, percent float64) time.Duration {
	if percent <= 0 {
		return d
	}
	delta := time.Duration(float64(d) * percent)
	if delta <= 0 {
		return d
	}
	n := int64(delta)*jitterScale + 1
	offset := time.Duration(rand.N(n)) - delta //nolint:gosec
	return d + offset
}
