// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import (
	"errors"
	"fmt"
	"time"
)

// ConfigSpec bundles the supervisor-facing inputs the authz router
// needs to be assembled. Built-in factory wiring (allow_all,
// seed/<name>) lives here so external built-ins (e.g. attribute_matcher,
// which imports pkg/evaluator and can't be imported back without a
// cycle) plug in via AttributeMatcher. Supervisor pre-constructs them
// and NewRouterFromConfig registers the kind.
type ConfigSpec struct {
	// Gates binds gate names to evaluator specs. Unknown names fail at
	// startup via the router's validation pass.
	Gates map[GateName]string

	// Default is the evaluator spec applied when a gate isn't listed in
	// Gates. Empty selects "allow_all".
	Default string

	// AttributeMatcher is an optional pre-built evaluator registered
	// under the "attribute_matcher" kind. Left nil when the matcher
	// built-in isn't configured.
	AttributeMatcher Evaluator

	// SeedCaller supplies the placement.Call dispatcher for seed-backed
	// PDPs. Required when any gate (or Default) resolves to "seed/<name>".
	SeedCaller Caller

	// Metrics records gate-level observations. Left nil, a no-op
	// recorder is wired.
	Metrics Metrics

	// OnDeny receives one DenyEvent per gate denial across every gate
	// the router registers. Left nil, denials emit no external signal.
	// See DenyObserver for concurrency and non-blocking requirements.
	OnDeny DenyObserver

	// Fallback is applied when the configured evaluator errors or times
	// out. Assigned as-is to the gate options: an explicit
	// Decision{false, nil} is honoured, not silently replaced with the
	// package default. Callers wanting the package default (deny with
	// "evaluator unreachable") should seed from DefaultGateOptions.
	Fallback Decision

	// TTL, MaxCacheEntries, Timeout override GateOptions defaults. Zero
	// selects the package defaults.
	TTL             time.Duration
	MaxCacheEntries int
	Timeout         time.Duration
}

// NewRouterFromConfig assembles the authz router, registering the
// built-in factories (allow_all, seed/<name>, optional
// attribute_matcher) and returning a ready-to-wire *Router.
//
// Additional factory bindings (e.g. test doubles) plug in via opts, the
// same RouterOption-shaped extensibility NewRouter exposes.
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
	// Fallback is assigned unconditionally: an explicit Decision{false,
	// nil} (deny with no reason) is a legitimate operator choice and
	// must not collide with the default's reason-bearing deny.
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
