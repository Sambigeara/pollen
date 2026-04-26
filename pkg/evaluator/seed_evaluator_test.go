// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeSeedCaller struct {
	handler func(seed, function string, input []byte) ([]byte, error)
	lastIn  []byte
	seedArg string
	funcArg string
}

func (f *fakeSeedCaller) Call(_ context.Context, s, fn string, input []byte) ([]byte, error) {
	f.lastIn = input
	f.seedArg = s
	f.funcArg = fn
	return f.handler(s, fn, input)
}

func TestSeedEvaluator_RoundTripsRequest(t *testing.T) {
	want := Decision{Decision: true, Context: map[string]any{"reason_user": "ok"}}
	fake := &fakeSeedCaller{
		handler: func(_, _ string, input []byte) ([]byte, error) {
			var req Request
			require.NoError(t, json.Unmarshal(input, &req))
			require.Equal(t, "peer", req.Subject.Type)
			require.Equal(t, "fetch", req.Action.Name)
			return json.Marshal(want)
		},
	}
	eval := newSeedEvaluator("policy-pdp", func() Caller { return fake })

	got, err := eval.Allow(context.Background(), Request{
		Subject:  Subject{Type: "peer", ID: "abc"},
		Action:   Action{Name: "fetch"},
		Resource: Resource{Type: "blob", ID: "hash"},
	})
	require.NoError(t, err)
	require.Equal(t, want.Decision, got.Decision)
	require.Equal(t, "ok", got.Context["reason_user"])
	require.Equal(t, "policy-pdp", fake.seedArg)
	require.Equal(t, "check", fake.funcArg)
}

func TestSeedEvaluator_CallerNotAvailable(t *testing.T) {
	eval := newSeedEvaluator("policy-pdp", func() Caller { return nil })
	_, err := eval.Allow(context.Background(), Request{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "caller not available")
}

func TestSeedEvaluator_CallError(t *testing.T) {
	fake := &fakeSeedCaller{
		handler: func(_, _ string, _ []byte) ([]byte, error) {
			return nil, errors.New("seed not placed")
		},
	}
	eval := newSeedEvaluator("policy-pdp", func() Caller { return fake })
	_, err := eval.Allow(context.Background(), Request{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "seed not placed")
}

func TestSeedEvaluator_MalformedResponse(t *testing.T) {
	fake := &fakeSeedCaller{
		handler: func(_, _ string, _ []byte) ([]byte, error) {
			return []byte("not json"), nil
		},
	}
	eval := newSeedEvaluator("policy-pdp", func() Caller { return fake })
	_, err := eval.Allow(context.Background(), Request{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal")
}

// Consecutive failures trip the breaker; cooldown admits exactly one
// probe that — on success — closes the breaker and restores normal flow.
func TestSeedEvaluator_CircuitBreakerTripsAndRecovers(t *testing.T) {
	var calls atomic.Int32
	respond := make(chan func([]byte) ([]byte, error), 16)
	fake := &fakeSeedCaller{
		handler: func(_, _ string, input []byte) ([]byte, error) {
			calls.Add(1)
			fn := <-respond
			return fn(input)
		},
	}
	eval := newSeedEvaluator("policy-pdp", func() Caller { return fake })

	clock := time.Unix(1_000, 0)
	eval.now = func() time.Time { return clock }

	// Exceed the default failure threshold (5) to trip the breaker.
	for range 5 {
		respond <- func([]byte) ([]byte, error) { return nil, errors.New("pdp unreachable") }
		_, err := eval.Allow(context.Background(), Request{})
		require.Error(t, err)
	}
	require.Equal(t, int32(5), calls.Load(), "first five calls reach the caller")

	// Still within cooldown — short-circuit without invoking the caller.
	_, err := eval.Allow(context.Background(), Request{})
	require.ErrorIs(t, err, ErrCircuitOpen)
	require.Equal(t, int32(5), calls.Load(), "circuit open — caller not invoked")

	// Advance past cooldown — one probe is admitted and on success closes
	// the breaker, then normal traffic flows again.
	clock = clock.Add(11 * time.Second)
	allow := Decision{Decision: true}
	respond <- func([]byte) ([]byte, error) { return json.Marshal(allow) }
	got, err := eval.Allow(context.Background(), Request{})
	require.NoError(t, err)
	require.True(t, got.Decision)
	require.Equal(t, int32(6), calls.Load(), "probe reaches the caller")

	respond <- func([]byte) ([]byte, error) { return json.Marshal(allow) }
	_, err = eval.Allow(context.Background(), Request{})
	require.NoError(t, err)
	require.Equal(t, int32(7), calls.Load(), "breaker closed, calls flow again")
}

// After the cooldown elapses, only the first caller through the gate is
// admitted as the probe. Concurrent callers see ErrCircuitOpen until the
// probe resolves, preventing a thundering-herd on a recovering PDP.
func TestSeedEvaluator_HalfOpenAdmitsOneProbe(t *testing.T) {
	var calls atomic.Int32
	probeStarted := make(chan struct{})
	probeRelease := make(chan struct{})
	fake := &fakeSeedCaller{
		handler: func(_, _ string, _ []byte) ([]byte, error) {
			calls.Add(1)
			// Signal that the probe is in flight, then block so concurrent
			// callers observe breakerHalfOpen (not breakerClosed).
			if calls.Load() <= 5 {
				return nil, errors.New("pdp unreachable")
			}
			close(probeStarted)
			<-probeRelease
			return json.Marshal(Decision{Decision: true})
		},
	}
	eval := newSeedEvaluator("policy-pdp", func() Caller { return fake })

	clock := time.Unix(1_000, 0)
	eval.now = func() time.Time { return clock }

	for range 5 {
		_, err := eval.Allow(context.Background(), Request{})
		require.Error(t, err)
	}
	require.Equal(t, int32(5), calls.Load())

	clock = clock.Add(11 * time.Second)

	const concurrent = 8
	var wg sync.WaitGroup
	errs := make(chan error, concurrent)
	wg.Add(concurrent)
	probeResolved := make(chan struct{})
	go func() {
		defer wg.Done()
		_, err := eval.Allow(context.Background(), Request{})
		errs <- err
		close(probeResolved)
	}()

	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("probe never reached the caller")
	}

	for range concurrent - 1 {
		go func() {
			defer wg.Done()
			_, err := eval.Allow(context.Background(), Request{})
			errs <- err
		}()
	}

	require.Eventually(t, func() bool {
		return len(errs) >= concurrent-1
	}, time.Second, time.Millisecond)
	require.Equal(t, int32(6), calls.Load(), "only the probe reaches the caller")

	close(probeRelease)
	<-probeResolved
	wg.Wait()

	close(errs)
	var circuitOpen, noErr int
	for err := range errs {
		switch {
		case err == nil:
			noErr++
		case errors.Is(err, ErrCircuitOpen):
			circuitOpen++
		default:
			require.Failf(t, "unexpected error", "%v", err)
		}
	}
	require.Equal(t, 1, noErr, "only the probe succeeds")
	require.Equal(t, concurrent-1, circuitOpen, "everyone else sees ErrCircuitOpen")
}
