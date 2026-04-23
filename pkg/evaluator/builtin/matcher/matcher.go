// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// Package matcher implements a tiny YAML-driven evaluator. Each rule
// matches on dot-separated paths into the Request; a list value in the
// rule is any-of, a scalar is equality. The first rule whose match
// passes returns its decision — there's no CEL, no negation, no nesting.
// Users who outgrow this move to a PDP seed.
package matcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"

	"gopkg.in/yaml.v3"

	"github.com/sambigeara/pollen/pkg/evaluator"
)

type rule struct {
	Match    map[string]any `yaml:"match"`
	Decision string         `yaml:"decision"`
	Reason   string         `yaml:"reason,omitempty"`
}

type ruleFile struct {
	Rules []rule `yaml:"rules"`
}

// compiledRule is the runtime form of rule: decision is pre-parsed to a
// bool so the hot path dodges the string compare on every check.
type compiledRule struct {
	match  map[string]any
	reason string
	allow  bool
}

// Matcher is the Evaluator implementation. Safe for concurrent use;
// the rule slice is stored behind an atomic.Pointer so Reload can swap
// it in under running callers without a mutex.
type Matcher struct {
	rules    atomic.Pointer[[]compiledRule]
	fallback evaluator.Decision
}

// NewFromFile loads rules from a YAML file. A missing or empty file is
// an error — silently running with zero rules would default-deny every
// request, which is surprising behaviour for a misconfigured path.
func NewFromFile(path string) (*Matcher, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("matcher: read %s: %w", path, err)
	}
	return NewFromBytes(data)
}

// NewFromBytes loads rules from a byte slice. Useful for tests and
// embedded configurations.
func NewFromBytes(data []byte) (*Matcher, error) {
	compiled, err := compileRules(data)
	if err != nil {
		return nil, err
	}
	m := &Matcher{
		fallback: evaluator.Decision{Decision: false, Context: map[string]any{"reason_user": "no matching rule"}},
	}
	m.rules.Store(&compiled)
	return m, nil
}

// Reload re-reads the rules file and swaps the in-memory rule slice
// atomically. A malformed file leaves the previous rules in place and
// returns the parse error — so a bad edit can't brick authz.
func (m *Matcher) Reload(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("matcher: read %s: %w", path, err)
	}
	compiled, err := compileRules(data)
	if err != nil {
		return err
	}
	m.rules.Store(&compiled)
	return nil
}

func compileRules(data []byte) ([]compiledRule, error) {
	var f ruleFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("matcher: parse rules: %w", err)
	}
	if len(f.Rules) == 0 {
		return nil, errors.New("matcher: no rules defined — silent default-deny would surprise operators")
	}
	compiled := make([]compiledRule, len(f.Rules))
	for i, r := range f.Rules {
		switch r.Decision {
		case "allow":
			compiled[i] = compiledRule{match: r.Match, allow: true, reason: r.Reason}
		case "deny":
			compiled[i] = compiledRule{match: r.Match, allow: false, reason: r.Reason}
		default:
			return nil, fmt.Errorf("matcher: rule %d: decision must be \"allow\" or \"deny\", got %q", i, r.Decision)
		}
	}
	return compiled, nil
}

// Cacheable opts the matcher out of decision caching: the JSON-marshal
// cache-key cost exceeds the rule walk for the shapes dispatch sites
// actually see, so caching is net-negative here. Operators with
// expensive evaluators (PDP seeds, remote policy services) leave the
// default on.
func (*Matcher) Cacheable() bool { return false }

// Allow walks rules in order. The first rule whose match clause is
// satisfied returns its decision; if no rule matches, the fallback
// (deny) applies.
func (m *Matcher) Allow(_ context.Context, req evaluator.Request) (evaluator.Decision, error) {
	addr := requestMap(req)
	rules := *m.rules.Load()
	for _, r := range rules {
		if matchesRequest(r.match, addr) {
			d := evaluator.Decision{Decision: r.allow}
			if r.reason != "" {
				d.Context = map[string]any{"reason_user": r.reason}
			}
			return d, nil
		}
	}
	return m.fallback, nil
}

// requestMap builds the addressable shape once per Allow call so rule
// walks don't re-allocate the subject/action/resource/context maps per
// rule.
func requestMap(req evaluator.Request) map[string]any {
	return map[string]any{
		"subject": map[string]any{
			"type":       req.Subject.Type,
			"id":         req.Subject.ID,
			"properties": req.Subject.Properties,
		},
		"action": map[string]any{
			"name":       req.Action.Name,
			"properties": req.Action.Properties,
		},
		"resource": map[string]any{
			"type":       string(req.Resource.Type),
			"id":         req.Resource.ID,
			"properties": req.Resource.Properties,
		},
		"context": req.Context,
	}
}

// matchesRequest returns true when every (path, expected) pair in match
// satisfies the pre-built request map. Scalar expected values check
// equality; list-valued expected checks any-of.
func matchesRequest(match, req map[string]any) bool {
	for path, expected := range match {
		actual, ok := lookupPath(req, path)
		if !ok {
			return false
		}
		if !matchValue(expected, actual) {
			return false
		}
	}
	return true
}

func matchValue(expected, actual any) bool {
	if list, ok := expected.([]any); ok {
		for _, v := range list {
			if equalValues(v, actual) {
				return true
			}
		}
		return false
	}
	return equalValues(expected, actual)
}

// equalValues performs literal equality on strings, bools, and numbers.
// Numeric types are normalised through float64 so YAML's int/float
// duality doesn't surprise rule authors. Non-comparable values (maps,
// slices) fall through to reflect.DeepEqual so `a == b` can't panic at
// runtime on a rule that targets a structured context value.
func equalValues(a, b any) bool {
	if af, aok := toFloat(a); aok {
		if bf, bok := toFloat(b); bok {
			return af == bf
		}
	}
	if !comparableKind(a) || !comparableKind(b) {
		return reflect.DeepEqual(a, b)
	}
	return a == b
}

// comparableKind rejects kinds that panic under `==`. reflect.Kind on a
// nil any is Invalid, which is comparable (nil == nil is fine).
func comparableKind(v any) bool {
	if v == nil {
		return true
	}
	switch reflect.TypeOf(v).Kind() {
	case reflect.Map, reflect.Slice, reflect.Func:
		return false
	default:
		return true
	}
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	}
	return 0, false
}

// lookupPath resolves a dot-separated path against the pre-built
// request map. Only the known top-level fields (subject, action,
// resource, context) are addressable — unknown prefixes return false
// rather than silently matching nothing.
func lookupPath(req map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	head, ok := req[parts[0]]
	if !ok {
		return nil, false
	}
	return walk(head, parts[1:])
}

func walk(v any, parts []string) (any, bool) {
	for _, p := range parts {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := m[p]
		if !ok {
			return nil, false
		}
		v = next
	}
	return v, true
}
