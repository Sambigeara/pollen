// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import (
	"errors"
	"fmt"
	"time"
)

// ConfigSpec bundles the supervisor-facing inputs the authz router
// needs to be assembled. External built-ins (e.g. attribute_matcher)
// plug in via AttributeMatcher to avoid import cycles.
type ConfigSpec struct {
	Gates            map[GateName]string
	Default          string
	AttributeMatcher Evaluator
	SeedCaller       Caller
	Metrics          Metrics
	OnDeny           DenyObserver

	// Fallback is assigned as-is: an explicit Decision{false, nil} is
	// honoured, not silently replaced with the package default.
	Fallback Decision

	TTL             time.Duration
	MaxCacheEntries int
	Timeout         time.Duration
}

func NewRouterFromConfig(cfg ConfigSpec, opts ...RouterOption) (*Router, error) {
	gateOpts := DefaultGateOptions()
	if cfg.TTL > 0 {
		gateOpts.TTL = cfg.TTL
	}
	if cfg.MaxCacheEntries > 0 {
		gateOpts.MaxCacheEntries = cfg.MaxCacheEntries
	}
	if cfg.Timeout > 0 {
		gateOpts.Timeout = cfg.Timeout
	}
	gateOpts.Fallback = cfg.Fallback
	gateOpts.Metrics = cfg.Metrics
	gateOpts.OnDeny = cfg.OnDeny

	builtinOpts := []RouterOption{
		WithFactory("seed", func(name string) (Evaluator, error) {
			if name == "" {
				return nil, errors.New("seed evaluator requires a seed name (seed/<name>)")
			}
			if cfg.SeedCaller == nil {
				return nil, fmt.Errorf("seed evaluator %q configured but no caller wired", name)
			}
			return newSeedEvaluator(name, cfg.SeedCaller), nil
		}),
	}
	if cfg.AttributeMatcher != nil {
		matcher := cfg.AttributeMatcher
		builtinOpts = append(builtinOpts, WithFactory("attribute_matcher", func(string) (Evaluator, error) {
			return matcher, nil
		}))
	}

	return NewRouter(Config{
		Default:     cfg.Default,
		Gates:       cfg.Gates,
		GateOptions: gateOpts,
	}, append(builtinOpts, opts...)...)
}
