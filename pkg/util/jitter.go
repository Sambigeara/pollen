package util

import (
	"context"
	"math/rand/v2"
	"time"
)

const jitterScale = 2

type JitterTicker struct {
	C    <-chan time.Time
	bump chan struct{}
	stop context.CancelFunc
}

func NewJitterTicker(ctx context.Context, base time.Duration, percent float64) *JitterTicker {
	tickCh := make(chan time.Time)
	bump := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(tickCh)
		timer := time.NewTimer(jitter(base, percent))
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-bump:
				timer.Reset(jitter(base, percent))
			case t := <-timer.C:
				select {
				case <-ctx.Done():
					return
				case tickCh <- t:
				}
				timer.Reset(jitter(base, percent))
			}
		}
	}()
	return &JitterTicker{C: tickCh, bump: bump, stop: cancel}
}

func (t *JitterTicker) Bump() {
	select {
	case t.bump <- struct{}{}:
	default:
	}
}

func (t *JitterTicker) Stop() {
	t.stop()
}

func jitter(d time.Duration, percent float64) time.Duration {
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
