// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/sambigeara/pollen/pkg/evaluator"
	"github.com/sambigeara/pollen/pkg/evaluator/builtin/matcher"
)

// preloadAuthz validates AuthzOptions and loads the matcher rules file
// if configured. Failure-prone authz setup runs here so supervisor.New
// can fail fast before allocating resources that would leak on a later
// authz error.
func preloadAuthz(opts AuthzOptions, relayOnly bool) (*matcher.Matcher, error) {
	if opts.Default == "" && len(opts.Gates) == 0 && opts.MatcherRules == "" {
		return nil, nil
	}
	if relayOnly && hasSeedGate(opts) {
		return nil, errors.New("authz: seed-backed PDP gates require workload hosting; incompatible with relay-only mode")
	}
	for k := range opts.Gates {
		if !evaluator.GateName(k).Valid() {
			return nil, fmt.Errorf("authz: unknown gate %q", k)
		}
	}
	if opts.MatcherRules == "" {
		return nil, nil
	}
	m, err := matcher.NewFromFile(opts.MatcherRules)
	if err != nil {
		return nil, fmt.Errorf("authz: load matcher rules: %w", err)
	}
	return m, nil
}

// buildAuthzRouter wires the router with the live placement caller and
// the preloaded matcher. Called after preloadAuthz validates the config
// and after placement is constructed. Residual failure modes (typo in
// an evaluator kind) are rare; preloadAuthz catches the common ones.
func buildAuthzRouter(opts AuthzOptions, mtr *matcher.Matcher, caller evaluator.Caller, log *zap.SugaredLogger) (*evaluator.Router, error) {
	if opts.Default == "" && len(opts.Gates) == 0 && opts.MatcherRules == "" {
		return evaluator.NewRouter(evaluator.Config{})
	}
	var matcherIface evaluator.Evaluator
	if mtr != nil {
		matcherIface = mtr
	}
	gates := make(map[evaluator.GateName]string, len(opts.Gates))
	for k, v := range opts.Gates {
		gates[evaluator.GateName(k)] = v
	}
	return evaluator.NewRouterFromConfig(evaluator.ConfigSpec{
		Default:          opts.Default,
		Gates:            gates,
		AttributeMatcher: matcherIface,
		SeedCaller:       caller,
		OnDeny:           logDeny(log),
	})
}

func hasSeedGate(opts AuthzOptions) bool {
	if isSeedSpec(opts.Default) {
		return true
	}
	for _, spec := range opts.Gates {
		if isSeedSpec(spec) {
			return true
		}
	}
	return false
}

func isSeedSpec(spec string) bool {
	kind, _, _ := strings.Cut(spec, "/")
	return kind == "seed"
}

// logDeny structured-logs every gate denial. Info level: denies are
// normal policy operation, not errors. service_connect has no other
// operator-visible signal short of the status API. Fallback denials
// (evaluator errored, gate applied its configured fallback) log at warn
// so a broken PDP surfaces distinctly from a legitimate deny.
func logDeny(log *zap.SugaredLogger) evaluator.DenyObserver {
	return func(e evaluator.DenyEvent) {
		fields := []any{
			"gate", string(e.Gate),
			"subject_type", e.Request.Subject.Type,
			"subject_id", e.Request.Subject.ID,
			"action", e.Request.Action.Name,
			"resource_type", string(e.Request.Resource.Type),
			"resource_id", e.Request.Resource.ID,
			"reason", e.Reason,
			"cached", e.Cached,
		}
		if e.Fallback {
			log.Warnw("authz fallback deny (evaluator unreachable)", fields...)
			return
		}
		log.Infow("authz deny", fields...)
	}
}

// ErrNoMatcher is returned by ReloadAuthzMatcher when no matcher rules
// were configured at supervisor.New time — there's nothing to reload.
var ErrNoMatcher = errors.New("supervisor: authz matcher not configured")

// ReloadAuthzMatcher re-reads the matcher rules file. A failed reload
// leaves the previous rules in place — a broken YAML must never brick
// authz. Daemon SIGHUP handlers call this directly.
func (n *Supervisor) ReloadAuthzMatcher() error {
	if n.authzMatcher == nil {
		return ErrNoMatcher
	}
	return n.authzMatcher.Reload(n.authzMatcherPath)
}
