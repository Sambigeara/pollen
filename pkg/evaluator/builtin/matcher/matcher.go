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

type compiledRule struct {
	match  map[string]any
	reason string
	allow  bool
}

type Matcher struct {
	rules    atomic.Pointer[[]compiledRule]
	fallback evaluator.Decision
}

func NewFromFile(path string) (*Matcher, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("matcher: read %s: %w", path, err)
	}
	return NewFromBytes(data)
}

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

// Reload swaps the rules atomically. A malformed file leaves the
// previous rules in place.
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

func (*Matcher) Cacheable() bool { return false }

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

// equalValues normalises numerics through float64 (YAML int/float duality)
// and falls through to reflect.DeepEqual for non-comparable types to avoid
// panics on structured context values.
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
