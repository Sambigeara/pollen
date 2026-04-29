// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockClock struct {
	now time.Time
}

func (c *mockClock) Now() time.Time          { return c.now }
func (c *mockClock) advance(d time.Duration) { c.now = c.now.Add(d) }

type emitRecorder struct {
	ttls []time.Duration
}

func (r *emitRecorder) emit(ttl time.Duration) {
	r.ttls = append(r.ttls, ttl)
}

func defaultBackoffCfg() backoffConfig {
	return backoffConfig{ttl: time.Second}
}

func newBackoffHarness(t *testing.T, cfg backoffConfig) (*backoff, *mockClock, *emitRecorder) {
	t.Helper()
	clock := &mockClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	r := &emitRecorder{}
	b := newBackoff(cfg, r.emit)
	b.now = clock.Now
	return b, clock, r
}

func TestBackoff_SignalRefusalActivatesImmediately(t *testing.T) {
	b, _, r := newBackoffHarness(t, defaultBackoffCfg())

	b.SignalRefusal()
	require.True(t, b.IsLocallyOverloaded(), "first refusal must arm the gate without any prior signal")
	require.Equal(t, []time.Duration{time.Second}, r.ttls)
}

func TestBackoff_ExitsOnceTTLExpires(t *testing.T) {
	b, clock, _ := newBackoffHarness(t, defaultBackoffCfg())

	b.SignalRefusal()
	require.True(t, b.IsLocallyOverloaded())

	clock.advance(2 * time.Second)
	require.False(t, b.IsLocallyOverloaded(), "active flag must lapse once now exceeds lastEmit+ttl")
}

func TestBackoff_RepeatRefusalsKeepWindowFreshWithoutGrowing(t *testing.T) {
	b, clock, r := newBackoffHarness(t, defaultBackoffCfg())

	b.SignalRefusal()
	clock.advance(500 * time.Millisecond)
	b.SignalRefusal()
	clock.advance(500 * time.Millisecond)
	b.SignalRefusal()

	require.Equal(t, []time.Duration{time.Second, time.Second, time.Second}, r.ttls,
		"sustained refusals must re-emit the configured TTL — never grow it")

	clock.advance(500 * time.Millisecond)
	require.True(t, b.IsLocallyOverloaded(), "lastEmit was refreshed by the third signal, so window holds")

	clock.advance(time.Second)
	require.False(t, b.IsLocallyOverloaded(), "no further refusals — window expires within one TTL")
}

func TestBackoff_RecoversWithinOneTTLAfterRefusalsStop(t *testing.T) {
	b, clock, _ := newBackoffHarness(t, defaultBackoffCfg())

	for range 5 {
		b.SignalRefusal()
		clock.advance(200 * time.Millisecond)
	}
	require.True(t, b.IsLocallyOverloaded())

	clock.advance(2 * time.Second)
	require.False(t, b.IsLocallyOverloaded(), "recovery is bounded by the configured TTL, not by refusal history")
}
