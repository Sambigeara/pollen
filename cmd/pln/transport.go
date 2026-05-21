// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

type transportKind int

const (
	transportLocal transportKind = iota
	transportWire
	transportSSHBridge
)

type transportSelection struct {
	wireAddr string
	sshHost  string
	kind     transportKind
}

func (t transportSelection) IsLocal() bool     { return t.kind == transportLocal }
func (t transportSelection) IsWire() bool      { return t.kind == transportWire }
func (t transportSelection) IsSSHBridge() bool { return t.kind == transportSSHBridge }

func (t transportSelection) WireAddr() string { return t.wireAddr }
func (t transportSelection) SSHHost() string  { return t.sshHost }

type transportOverride int

const (
	overrideNone transportOverride = iota
	overrideLocal
	overrideWire
)

// sockProbe defaults to nodeSocketActive; tests inject a fake.
var sockProbe = nodeSocketActive

// resolveTransport picks the transport. SSH-bridge ctxs win first and
// ignore overrides. Otherwise --local forces sock and --wire forces
// wire fallback (both error if unavailable); the default policy prefers
// sock if up, then wire fallback, then errors with an actionable
// message. Callers that don't dial (localOnly commands like `pln up`)
// must short-circuit before calling this; see cmd/pln/main.go withEnv.
func resolveTransport(entry contextEntry, override transportOverride) (transportSelection, error) {
	if entry.isSSHBridge() {
		return transportSelection{kind: transportSSHBridge, sshHost: entry.Host}, nil
	}
	if override == overrideLocal {
		if entry.Dir == "" {
			return transportSelection{}, errors.New("--local requires an identity dir for this ctx")
		}
		if !sockProbe(filepath.Join(entry.Dir, socketName)) {
			return transportSelection{}, fmt.Errorf("no local daemon running in %s; run `pln up`", entry.Dir)
		}
		return transportSelection{kind: transportLocal}, nil
	}
	if override == overrideWire {
		if entry.Wire == "" {
			return transportSelection{}, errors.New("--wire requires a wire fallback (pln://host:port) on this ctx")
		}
		return transportSelection{kind: transportWire, wireAddr: trimWireScheme(entry.Wire)}, nil
	}
	if entry.Dir != "" && sockProbe(filepath.Join(entry.Dir, socketName)) {
		return transportSelection{kind: transportLocal}, nil
	}
	if entry.Wire != "" {
		return transportSelection{kind: transportWire, wireAddr: trimWireScheme(entry.Wire)}, nil
	}
	if entry.Dir != "" {
		return transportSelection{}, fmt.Errorf("no daemon running in %s and no wire fallback configured; run `pln up` or set a wire fallback", entry.Dir)
	}
	return transportSelection{}, errors.New("ctx has no dir, wire fallback, or SSH host configured")
}

func trimWireScheme(s string) string { return strings.TrimPrefix(s, plnTargetScheme) }

func transportOverrideFromFlags(cmd *cobra.Command) transportOverride {
	if local, _ := cmd.Flags().GetBool("local"); local {
		return overrideLocal
	}
	if wire, _ := cmd.Flags().GetBool("wire"); wire {
		return overrideWire
	}
	return overrideNone
}
