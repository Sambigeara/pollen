// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/evaluator"
)

type fakeEval struct {
	err       error
	decisions []evaluator.Decision
	calls     int
}

func (f *fakeEval) Allow(_ context.Context, _ evaluator.Request) (evaluator.Decision, error) {
	f.calls++
	if f.err != nil {
		return evaluator.Decision{}, f.err
	}
	if len(f.decisions) == 0 {
		return evaluator.Decision{Decision: true}, nil
	}
	d := f.decisions[0]
	if len(f.decisions) > 1 {
		f.decisions = f.decisions[1:]
	}
	return d, nil
}

func routerWith(t *testing.T, eval evaluator.Evaluator, opts evaluator.GateOptions) *evaluator.Router {
	t.Helper()
	r, err := evaluator.NewRouter(
		evaluator.Config{
			Gates:       map[evaluator.GateName]string{evaluator.GateBlobFetch: "fake"},
			GateOptions: opts,
		},
		evaluator.WithFactory("fake", func(string) (evaluator.Evaluator, error) { return eval, nil }),
	)
	require.NoError(t, err)
	return r
}

func TestAllowAll(t *testing.T) {
	d, err := evaluator.AllowAll{}.Allow(context.Background(), evaluator.Request{})
	require.NoError(t, err)
	require.True(t, d.Decision)
}

func TestGateAllow(t *testing.T) {
	e := &fakeEval{decisions: []evaluator.Decision{{Decision: true}}}
	r := routerWith(t, e, evaluator.GateOptions{})
	require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, evaluator.Request{}))
	require.Equal(t, 1, e.calls)
}

func TestGateDenyProducesDeniedError(t *testing.T) {
	e := &fakeEval{decisions: []evaluator.Decision{{Decision: false, Context: map[string]any{"reason_user": "banned"}}}}
	r := routerWith(t, e, evaluator.GateOptions{})

	err := r.Allow(context.Background(), evaluator.GateBlobFetch, evaluator.Request{})
	require.ErrorIs(t, err, evaluator.ErrDenied)

	var denied *evaluator.DeniedError
	require.ErrorAs(t, err, &denied)
	require.Equal(t, "banned", denied.Reason)
}

func TestGateCacheHit(t *testing.T) {
	e := &fakeEval{decisions: []evaluator.Decision{{Decision: true}}}
	opts := evaluator.GateOptions{TTL: time.Minute, MaxCacheEntries: 10}
	r := routerWith(t, e, opts)

	req := evaluator.Request{
		Subject:  evaluator.Subject{Type: "peer", ID: "abc"},
		Action:   evaluator.Action{Name: "fetch"},
		Resource: evaluator.NewResource(evaluator.ResourceBlob, "hash", nil),
	}
	require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, req))
	require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, req))
	require.Equal(t, 1, e.calls, "second call should be served from cache")
}

func TestGateCacheExpires(t *testing.T) {
	e := &fakeEval{}
	clock := time.Unix(1000, 0)
	opts := evaluator.GateOptions{
		TTL:             time.Second,
		MaxCacheEntries: 10,
		Now:             func() time.Time { return clock },
	}
	r := routerWith(t, e, opts)

	req := evaluator.Request{Subject: evaluator.Subject{ID: "abc"}, Action: evaluator.Action{Name: "fetch"}}
	require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, req))
	clock = clock.Add(2 * time.Second)
	require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, req))
	require.Equal(t, 2, e.calls, "expired cache entry should force re-evaluation")
}

func TestRouterInvalidateSubject(t *testing.T) {
	e := &fakeEval{}
	opts := evaluator.GateOptions{TTL: time.Minute, MaxCacheEntries: 10}
	r := routerWith(t, e, opts)

	req := evaluator.Request{Subject: evaluator.Subject{ID: "banned"}, Action: evaluator.Action{Name: "fetch"}}
	require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, req))
	r.InvalidateSubject("banned")
	require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, req))
	require.Equal(t, 2, e.calls, "invalidation should drop the cached decision")
}

func TestGateFallbackOnEvaluatorError(t *testing.T) {
	e := &fakeEval{err: errors.New("pdp down")}
	metrics := &recordingMetrics{}
	opts := evaluator.GateOptions{
		Fallback: evaluator.Decision{Decision: false, Context: map[string]any{"reason_user": "fallback deny"}},
		Metrics:  metrics,
	}
	r := routerWith(t, e, opts)

	err := r.Allow(context.Background(), evaluator.GateBlobFetch, evaluator.Request{})
	require.Error(t, err)
	require.ErrorIs(t, err, evaluator.ErrDenied)
	require.Equal(t, []string{"internal"}, metrics.errorReasons,
		"fallback must be distinguishable from always-deny via the metric reason label")
}

// A transient evaluator error must not poison the cache with the
// fallback decision — the next call after recovery should see the real
// decision.
func TestGateDoesNotCacheFallback(t *testing.T) {
	e := &flipEval{errOnce: errors.New("pdp flapped")}
	opts := evaluator.GateOptions{
		TTL:             time.Minute,
		MaxCacheEntries: 10,
		Fallback:        evaluator.Decision{Decision: false, Context: map[string]any{"reason_user": "fallback deny"}},
	}
	r := routerWith(t, e, opts)

	req := evaluator.Request{Subject: evaluator.Subject{ID: "x"}, Action: evaluator.Action{Name: "fetch"}}

	err := r.Allow(context.Background(), evaluator.GateBlobFetch, req)
	require.ErrorIs(t, err, evaluator.ErrDenied, "fallback deny observed after PDP error")

	require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, req),
		"next call must re-evaluate and see allow — fallback must not be cached")
}

// flipEval returns errOnce on the first call, then a permanent allow.
type flipEval struct {
	errOnce error
	calls   int
}

func (f *flipEval) Allow(context.Context, evaluator.Request) (evaluator.Decision, error) {
	f.calls++
	if f.calls == 1 {
		return evaluator.Decision{}, f.errOnce
	}
	return evaluator.Decision{Decision: true}, nil
}

type recordingMetrics struct {
	errorReasons []string
}

func (r *recordingMetrics) ObserveDecision(string, bool, bool)   {}
func (r *recordingMetrics) ObserveLatency(string, time.Duration) {}
func (r *recordingMetrics) ObserveError(_, reason string) {
	r.errorReasons = append(r.errorReasons, reason)
}

func TestGateObserveError_ClosedSetClassification(t *testing.T) {
	cases := []struct {
		err    error
		name   string
		reason string
	}{
		{context.DeadlineExceeded, "timeout", "timeout"},
		{context.Canceled, "canceled", "canceled"},
		{evaluator.ErrCircuitOpen, "circuit_open", "circuit_open"},
		{fmt.Errorf("seed pdp: %w", evaluator.ErrCircuitOpen), "wrapped_circuit_open", "circuit_open"},
		{errors.New("anything else"), "internal_fallback", "internal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			metrics := &recordingMetrics{}
			e := &fakeEval{err: tc.err}
			r := routerWith(t, e, evaluator.GateOptions{Metrics: metrics})
			_ = r.Allow(context.Background(), evaluator.GateBlobFetch, evaluator.Request{})
			require.Equal(t, []string{tc.reason}, metrics.errorReasons)
		})
	}
}

// TestGateDenyObserverFires pins the three code paths that must emit a
// DenyEvent: a fresh deny from the evaluator, a cached deny served on a
// repeat call, and a fallback deny when the evaluator errors.
func TestGateDenyObserverFires(t *testing.T) {
	t.Run("fresh deny emits event", func(t *testing.T) {
		var got []evaluator.DenyEvent
		e := &fakeEval{decisions: []evaluator.Decision{{Decision: false, Context: map[string]any{"reason_user": "nope"}}}}
		r := routerWith(t, e, evaluator.GateOptions{
			OnDeny: func(ev evaluator.DenyEvent) { got = append(got, ev) },
		})

		err := r.Allow(context.Background(), evaluator.GateBlobFetch, evaluator.Request{
			Subject:  evaluator.Subject{Type: "peer", ID: "abc"},
			Resource: evaluator.NewResource(evaluator.ResourceBlob, "h", nil),
		})
		require.ErrorIs(t, err, evaluator.ErrDenied)
		require.Len(t, got, 1)
		require.Equal(t, evaluator.GateBlobFetch, got[0].Gate)
		require.Equal(t, "nope", got[0].Reason)
		require.Equal(t, "abc", got[0].Request.Subject.ID)
		require.False(t, got[0].Cached)
		require.False(t, got[0].Fallback)
	})

	t.Run("cached deny still emits event", func(t *testing.T) {
		var got []evaluator.DenyEvent
		e := &fakeEval{decisions: []evaluator.Decision{{Decision: false, Context: map[string]any{"reason_user": "banned"}}}}
		r := routerWith(t, e, evaluator.GateOptions{
			TTL:             time.Minute,
			MaxCacheEntries: 10,
			OnDeny:          func(ev evaluator.DenyEvent) { got = append(got, ev) },
		})
		req := evaluator.Request{Subject: evaluator.Subject{ID: "x"}, Action: evaluator.Action{Name: "fetch"}}

		require.ErrorIs(t, r.Allow(context.Background(), evaluator.GateBlobFetch, req), evaluator.ErrDenied)
		require.ErrorIs(t, r.Allow(context.Background(), evaluator.GateBlobFetch, req), evaluator.ErrDenied)
		require.Equal(t, 1, e.calls, "second call is cached")
		require.Len(t, got, 2, "observer fires on every deny including cache hits")
		require.False(t, got[0].Cached)
		require.True(t, got[1].Cached)
	})

	t.Run("fallback deny emits event with Fallback=true", func(t *testing.T) {
		var got []evaluator.DenyEvent
		e := &fakeEval{err: errors.New("pdp down")}
		r := routerWith(t, e, evaluator.GateOptions{
			Fallback: evaluator.Decision{Decision: false, Context: map[string]any{"reason_user": "fallback"}},
			OnDeny:   func(ev evaluator.DenyEvent) { got = append(got, ev) },
		})

		require.ErrorIs(t, r.Allow(context.Background(), evaluator.GateBlobFetch, evaluator.Request{}), evaluator.ErrDenied)
		require.Len(t, got, 1)
		require.True(t, got[0].Fallback)
		require.Equal(t, "fallback", got[0].Reason)
	})

	t.Run("allow decisions do not emit", func(t *testing.T) {
		var got []evaluator.DenyEvent
		e := &fakeEval{decisions: []evaluator.Decision{{Decision: true}}}
		r := routerWith(t, e, evaluator.GateOptions{
			OnDeny: func(ev evaluator.DenyEvent) { got = append(got, ev) },
		})

		require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, evaluator.Request{}))
		require.Empty(t, got, "allow decisions must not emit deny events")
	})
}

func TestNewResource(t *testing.T) {
	got := evaluator.NewResource(evaluator.ResourceBlob, "hash", map[string]any{"tier": "cold"})
	require.Equal(t, evaluator.Resource{
		Type:       evaluator.ResourceBlob,
		ID:         "hash",
		Properties: map[string]any{"tier": "cold"},
	}, got)
}

func TestRouterPanicsOnFactoryReturnsNil(t *testing.T) {
	require.Panics(t, func() {
		_, _ = evaluator.NewRouter(
			evaluator.Config{Gates: map[evaluator.GateName]string{evaluator.GateBlobFetch: "bogus"}},
			evaluator.WithFactory("bogus", func(string) (evaluator.Evaluator, error) { return nil, nil }),
		)
	})
}

func TestRouterDefaultAllowAll(t *testing.T) {
	r, err := evaluator.NewRouter(evaluator.Config{})
	require.NoError(t, err)

	for _, name := range evaluator.AllGateNames() {
		require.NoError(t, r.Allow(context.Background(), name, evaluator.Request{}))
	}
}

func TestRouterRejectsUnknownGate(t *testing.T) {
	_, err := evaluator.NewRouter(evaluator.Config{
		Gates: map[evaluator.GateName]string{"nonsense": "allow_all"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown gate")
}

func TestRouterRejectsUnknownEvaluatorKind(t *testing.T) {
	_, err := evaluator.NewRouter(evaluator.Config{Default: "no_such_kind"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown evaluator kind")
}

func TestRouterFactoryRegistration(t *testing.T) {
	called := false
	factory := func(spec string) (evaluator.Evaluator, error) {
		called = true
		require.Equal(t, "my-pdp", spec)
		return evaluator.AllowAll{}, nil
	}
	r, err := evaluator.NewRouter(
		evaluator.Config{Gates: map[evaluator.GateName]string{evaluator.GateBlobFetch: "seed/my-pdp"}},
		evaluator.WithFactory("seed", factory),
	)
	require.NoError(t, err)
	require.True(t, called)
	require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, evaluator.Request{}))
}

// InvalidateSubject on the router must fan out to every cached gate —
// a peer denial should propagate without waiting for TTL.
func TestRouterInvalidateSubjectPropagates(t *testing.T) {
	e := &fakeEval{}
	r := routerWith(t, e, evaluator.GateOptions{TTL: time.Minute, MaxCacheEntries: 10})

	req := evaluator.Request{Subject: evaluator.Subject{ID: "x"}, Action: evaluator.Action{Name: "fetch"}}
	require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, req))
	require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, req))
	require.Equal(t, 1, e.calls, "second call is served from cache")

	r.InvalidateSubject("x")
	require.NoError(t, r.Allow(context.Background(), evaluator.GateBlobFetch, req))
	require.Equal(t, 2, e.calls, "router.InvalidateSubject drops the cached decision")
}

func TestRouterAllowPanicsOnUnknownGate(t *testing.T) {
	r, err := evaluator.NewRouter(evaluator.Config{})
	require.NoError(t, err)
	require.Panics(t, func() {
		_ = r.Allow(context.Background(), evaluator.GateName("nonsense"), evaluator.Request{})
	})
}

func TestRequestMarshalsAsConventionalShape(t *testing.T) {
	req := evaluator.Request{
		Subject:  evaluator.Subject{Type: "peer", ID: "abc", Properties: map[string]any{"role": "editor"}},
		Action:   evaluator.Action{Name: "fetch"},
		Resource: evaluator.NewResource(evaluator.ResourceBlob, "hash", nil),
	}
	b, err := json.Marshal(req)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(b, &got))
	require.Contains(t, got, "subject")
	require.Contains(t, got, "action")
	require.Contains(t, got, "resource")

	subject := got["subject"].(map[string]any)
	require.Equal(t, "peer", subject["type"])
	require.Equal(t, "editor", subject["properties"].(map[string]any)["role"])

	resource := got["resource"].(map[string]any)
	require.Equal(t, "blob", resource["type"], "ResourceType must marshal as its string form")
}

// TestRequestRoundTripsResourceType verifies Resource.Type survives a
// JSON round-trip despite its underlying kind being ResourceType rather
// than string — PDP wire format compatibility.
func TestRequestRoundTripsResourceType(t *testing.T) {
	req := evaluator.Request{
		Subject:  evaluator.Subject{Type: "peer", ID: "abc"},
		Action:   evaluator.Action{Name: "fetch"},
		Resource: evaluator.NewResource(evaluator.ResourceBlob, "hash", nil),
	}
	b, err := json.Marshal(req)
	require.NoError(t, err)

	var back evaluator.Request
	require.NoError(t, json.Unmarshal(b, &back))
	require.Equal(t, evaluator.ResourceBlob, back.Resource.Type)
}
