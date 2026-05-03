// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("evaluator: circuit breaker open")

// Metrics receives gate hot-path observations. ObserveError's reason
// comes from a closed set ("timeout", "canceled", "circuit_open",
// "internal") so label cardinality is bounded.
type Metrics interface {
	ObserveDecision(gate string, allow, cached bool)
	ObserveLatency(gate string, d time.Duration)
	ObserveError(gate, reason string)
}

type noopMetrics struct{}

func (noopMetrics) ObserveDecision(string, bool, bool)   {}
func (noopMetrics) ObserveLatency(string, time.Duration) {}
func (noopMetrics) ObserveError(string, string)          {}

type DenyEvent struct {
	When     time.Time
	Gate     GateName
	Request  Request
	Reason   string
	Cached   bool
	Fallback bool
}

// DenyObserver must be goroutine-safe and non-blocking.
type DenyObserver func(DenyEvent)

const (
	defaultCacheEntries = 10_000
	defaultCacheTTL     = time.Second
	// Long to account for cold start on hefty seeds.
	defaultTimeout = 5 * time.Second
)

type GateOptions struct {
	Now             func() time.Time
	Metrics         Metrics
	OnDeny          DenyObserver
	Fallback        Decision
	TTL             time.Duration
	MaxCacheEntries int
	Timeout         time.Duration
}

func DefaultGateOptions() GateOptions {
	return GateOptions{
		Fallback:        Decision{Decision: false, Context: map[string]any{"reason_user": "evaluator unreachable"}},
		TTL:             defaultCacheTTL,
		MaxCacheEntries: defaultCacheEntries,
		Timeout:         defaultTimeout,
	}
}

type Config struct {
	Gates       map[GateName]string
	Default     string
	GateOptions GateOptions
}

type Factory func(spec string) (Evaluator, error)

type Router struct {
	gates map[GateName]*boundEvaluator
}

type RouterOption func(*routerBuild)

type routerBuild struct {
	extras map[string]Factory
}

func WithFactory(kind string, f Factory) RouterOption {
	return func(b *routerBuild) { b.extras[kind] = f }
}

func NewRouter(cfg Config, opts ...RouterOption) (*Router, error) {
	build := routerBuild{extras: make(map[string]Factory)}
	for _, o := range opts {
		o(&build)
	}

	factories := map[string]Factory{
		"allow_all": func(string) (Evaluator, error) { return AllowAll{}, nil },
	}
	maps.Copy(factories, build.extras)

	defaultKind := cfg.Default
	if defaultKind == "" {
		defaultKind = "allow_all"
	}
	defaultEval, err := resolveEvaluator(factories, defaultKind)
	if err != nil {
		return nil, fmt.Errorf("evaluator default: %w", err)
	}

	for name := range cfg.Gates {
		if !name.Valid() {
			known := make([]string, 0, len(AllGateNames()))
			for _, g := range AllGateNames() {
				known = append(known, string(g))
			}
			sort.Strings(known)
			return nil, fmt.Errorf("evaluator: unknown gate %q (known: %v)", name, known)
		}
	}

	gates := make(map[GateName]*boundEvaluator, len(AllGateNames()))
	for _, name := range AllGateNames() {
		eval := defaultEval
		if spec, ok := cfg.Gates[name]; ok && spec != "" {
			e, err := resolveEvaluator(factories, spec)
			if err != nil {
				return nil, fmt.Errorf("evaluator gate %q = %q: %w", name, spec, err)
			}
			eval = e
		}
		gates[name] = newBoundEvaluator(name, eval, cfg.GateOptions)
	}

	return &Router{gates: gates}, nil
}

func (r *Router) Allow(ctx context.Context, name GateName, req Request) error {
	be, ok := r.gates[name]
	if !ok {
		panic(fmt.Sprintf("evaluator: no gate registered for %q (missing from AllGateNames?)", name))
	}
	return be.check(ctx, req)
}

func (r *Router) InvalidateSubject(subjectID string) {
	for _, be := range r.gates {
		be.invalidate(subjectID)
	}
}

func resolveEvaluator(factories map[string]Factory, spec string) (Evaluator, error) {
	kind, sub, _ := strings.Cut(spec, "/")
	f, ok := factories[kind]
	if !ok {
		known := make([]string, 0, len(factories))
		for k := range factories {
			known = append(known, k)
		}
		sort.Strings(known)
		return nil, fmt.Errorf("unknown evaluator kind %q (known: %v)", kind, known)
	}
	return f(sub)
}

type boundEvaluator struct {
	eval     Evaluator
	metrics  Metrics
	onDeny   DenyObserver
	cache    *decisionCache
	now      func() time.Time
	name     GateName
	fallback Decision
	timeout  time.Duration
}

func newBoundEvaluator(name GateName, eval Evaluator, opts GateOptions) *boundEvaluator {
	if eval == nil {
		panic(fmt.Sprintf("evaluator: gate %q constructed with nil evaluator", name))
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics = noopMetrics{}
	}
	be := &boundEvaluator{
		name:     name,
		eval:     eval,
		fallback: opts.Fallback,
		timeout:  opts.Timeout,
		metrics:  metrics,
		onDeny:   opts.OnDeny,
		now:      now,
	}
	if evalCacheable(eval) && opts.MaxCacheEntries > 0 && opts.TTL > 0 {
		be.cache = newDecisionCache(opts.MaxCacheEntries, opts.TTL, now)
	}
	return be
}

func evalCacheable(e Evaluator) bool {
	if c, ok := e.(Cacheable); ok {
		return c.Cacheable()
	}
	return true
}

func (be *boundEvaluator) check(ctx context.Context, req Request) error {
	start := be.now()
	defer func() { be.metrics.ObserveLatency(string(be.name), be.now().Sub(start)) }()

	key := ""
	if be.cache != nil {
		k, err := cacheKey(req)
		if err == nil {
			key = k
			if d, ok := be.cache.Get(key); ok {
				be.metrics.ObserveDecision(string(be.name), d.Decision, true)
				be.emitDeny(req, d, true, false)
				return decisionError(d)
			}
		}
	}

	callCtx, cancel := context.WithTimeout(ctx, be.timeout)
	defer cancel()

	d, err := be.eval.Allow(callCtx, req)
	if err != nil {
		be.metrics.ObserveError(string(be.name), classifyError(err))
		be.metrics.ObserveDecision(string(be.name), be.fallback.Decision, false)
		be.emitDeny(req, be.fallback, false, true)
		return decisionError(be.fallback)
	}

	if key != "" {
		be.cache.Put(key, d, req.Subject.ID)
	}
	be.metrics.ObserveDecision(string(be.name), d.Decision, false)
	be.emitDeny(req, d, false, false)
	return decisionError(d)
}

func (be *boundEvaluator) emitDeny(req Request, d Decision, cached, fallback bool) {
	if be.onDeny == nil || d.Decision {
		return
	}
	be.onDeny(DenyEvent{
		When:     be.now(),
		Gate:     be.name,
		Request:  req,
		Reason:   reasonFrom(d.Context),
		Cached:   cached,
		Fallback: fallback,
	})
}

func (be *boundEvaluator) invalidate(subjectID string) {
	if be.cache != nil {
		be.cache.InvalidateSubject(subjectID)
	}
}

func decisionError(d Decision) error {
	if d.Decision {
		return nil
	}
	return &DeniedError{Reason: reasonFrom(d.Context)}
}

func classifyError(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, ErrCircuitOpen):
		return "circuit_open"
	default:
		return "internal"
	}
}

func reasonFrom(ctx map[string]any) string {
	if v, ok := ctx["reason_user"]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func cacheKey(req Request) (string, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type decisionCache struct {
	entries map[string]cacheEntry
	bySub   map[string]map[string]struct{}
	now     func() time.Time
	max     int
	ttl     time.Duration
	mu      sync.Mutex
}

type cacheEntry struct {
	decision  Decision
	expires   time.Time
	subjectID string
}

func newDecisionCache(maxEntries int, ttl time.Duration, now func() time.Time) *decisionCache {
	return &decisionCache{
		entries: make(map[string]cacheEntry),
		bySub:   make(map[string]map[string]struct{}),
		max:     maxEntries,
		ttl:     ttl,
		now:     now,
	}
}

func (c *decisionCache) Get(key string) (Decision, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return Decision{}, false
	}
	if c.now().After(e.expires) {
		c.removeLocked(key)
		return Decision{}, false
	}
	return e.decision, true
}

func (c *decisionCache) Put(key string, d Decision, subjectID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.max {
		for k := range c.entries {
			c.removeLocked(k)
			break
		}
	}
	c.entries[key] = cacheEntry{
		decision:  d,
		expires:   c.now().Add(c.ttl),
		subjectID: subjectID,
	}
	if subjectID != "" {
		set, ok := c.bySub[subjectID]
		if !ok {
			set = make(map[string]struct{})
			c.bySub[subjectID] = set
		}
		set[key] = struct{}{}
	}
}

func (c *decisionCache) InvalidateSubject(subjectID string) {
	if subjectID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	keys, ok := c.bySub[subjectID]
	if !ok {
		return
	}
	for k := range keys {
		delete(c.entries, k)
	}
	delete(c.bySub, subjectID)
}

func (c *decisionCache) removeLocked(key string) {
	e, ok := c.entries[key]
	if !ok {
		return
	}
	delete(c.entries, key)
	if e.subjectID != "" {
		if set, ok := c.bySub[e.subjectID]; ok {
			delete(set, key)
			if len(set) == 0 {
				delete(c.bySub, e.subjectID)
			}
		}
	}
}
