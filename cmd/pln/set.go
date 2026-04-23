// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"cmp"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/observability/logging"
)

// settableKey describes a config key that can be edited via `pln set`/`pln unset`.
// apply normalises and stores the value, returning the canonical form so the
// caller can echo exactly what landed in config.yaml.
type settableKey struct {
	apply       func(*config.Config, string) (string, error)
	clear       func(*config.Config)
	name        string
	description string
	defaultVal  string
}

func settableKeys() []settableKey {
	return []settableKey{
		{
			name:        "http",
			description: "HTTP address for Prometheus metrics",
			defaultVal:  config.DefaultHTTPAddr,
			apply:       func(c *config.Config, v string) (string, error) { return setAddr(&c.HTTP, v) },
			clear:       func(c *config.Config) { c.HTTP = "" },
		},
		{
			name:        "static-http",
			description: "HTTP address for serving static sites",
			defaultVal:  config.DefaultStaticHTTPAddr,
			apply:       func(c *config.Config, v string) (string, error) { return setAddr(&c.StaticHTTP, v) },
			clear:       func(c *config.Config) { c.StaticHTTP = "" },
		},
		{
			name:        "control-addr",
			description: "TCP address for the control API (requires $PLN_DIR/control.token)",
			defaultVal:  config.DefaultControlAddr,
			apply:       func(c *config.Config, v string) (string, error) { return setAddr(&c.ControlAddr, v) },
			clear:       func(c *config.Config) { c.ControlAddr = "" },
		},
		{
			name:        "log-level",
			description: "Log verbosity: debug, info, warn, error",
			defaultVal:  config.DefaultLogLevel,
			apply:       func(c *config.Config, v string) (string, error) { return setLogLevel(&c.LogLevel, v) },
			clear:       func(c *config.Config) { c.LogLevel = "" },
		},
	}
}

func findKey(name string) (settableKey, bool) {
	for _, k := range settableKeys() {
		if k.name == name {
			return k, true
		}
	}
	return settableKey{}, false
}

func newSetCmds() []*cobra.Command {
	setCmd := &cobra.Command{
		Use:   "set [key] [value]",
		Short: "Set a daemon config value",
		Long: "Set a daemon config value in config.yaml. Works offline and online; the " +
			"daemon must be restarted to rebind listeners. Run `pln set` with no arguments " +
			"to list available keys.",
		Example: "  pln set                       # list available keys\n  pln set http :9100            # change Prometheus bind\n  pln set log-level debug",
		Args:    cobra.MaximumNArgs(2), //nolint:mnd
		RunE:    withEnv(runSet, localOnly()),
	}

	unsetCmd := &cobra.Command{
		Use:   "unset <key>",
		Short: "Clear a daemon config value",
		Long:  "Clears a daemon config value, restoring the default behaviour. Restart the daemon to rebind listeners.",
		Args:  cobra.ExactArgs(1),
		RunE:  withEnv(runUnset, localOnly()),
	}

	return []*cobra.Command{setCmd, unsetCmd}
}

func runSet(cmd *cobra.Command, args []string, env *cliEnv) error {
	if len(args) == 0 {
		printSettableKeys(cmd.OutOrStdout())
		return nil
	}

	key, ok := findKey(args[0])
	if !ok {
		return fmt.Errorf("unknown key %q; run `pln set` to list available keys", args[0])
	}

	value := key.defaultVal
	if len(args) > 1 {
		value = args[1]
	}
	canonical, err := key.apply(env.cfg, value)
	if err != nil {
		return fmt.Errorf("%s: %w", key.name, err)
	}
	if err := config.Save(env.dir, env.cfg); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "set %s=%s %s\n", key.name, canonical, applyHint(env.dir))
	return nil
}

func runUnset(cmd *cobra.Command, args []string, env *cliEnv) error {
	key, ok := findKey(args[0])
	if !ok {
		return fmt.Errorf("unknown key %q; run `pln set` to list available keys", args[0])
	}
	key.clear(env.cfg)
	if err := config.Save(env.dir, env.cfg); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "unset %s %s\n", key.name, applyHint(env.dir))
	return nil
}

// applyHint returns a parenthesised note describing how the change will take
// effect, tailored to whether the daemon is currently running.
func applyHint(dir string) string {
	if nodeSocketActive(filepath.Join(dir, socketName)) {
		return "(run `pln restart` to apply)"
	}
	return "(will apply on next `pln up`)"
}

func printSettableKeys(w io.Writer) {
	keys := settableKeys()
	slices.SortFunc(keys, func(a, b settableKey) int { return cmp.Compare(a.name, b.name) })
	fmt.Fprintln(w, "Available keys:")
	for _, k := range keys {
		fmt.Fprintf(w, "  %-14s %s (default %s)\n", k.name, k.description, k.defaultVal)
	}
}

func setLogLevel(dst *string, value string) (string, error) {
	if _, err := logging.ParseLevel(value); err != nil {
		return "", err
	}
	*dst = value
	return value, nil
}

// setAddr validates that value is a port, ":port", or "host:port" and stores
// the canonical ":port" / "host:port" form into dst, returning it for display.
func setAddr(dst *string, value string) (string, error) {
	normalised := value
	if _, err := strconv.Atoi(value); err == nil {
		normalised = ":" + value
	}
	host, port, err := net.SplitHostPort(normalised)
	if err != nil {
		return "", fmt.Errorf("invalid address %q: %w", value, err)
	}
	if p, perr := strconv.Atoi(port); perr != nil || p < 1 || p > maxPort {
		return "", fmt.Errorf("invalid port %q in address", port)
	}
	canonical := net.JoinHostPort(host, port)
	*dst = canonical
	return canonical, nil
}
