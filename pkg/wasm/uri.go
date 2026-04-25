// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package wasm

import (
	"errors"
	"fmt"
	"strings"
)

// URIScheme identifies the target type in a pln:// URI.
type URIScheme string

const (
	SchemeSeed    URIScheme = "seed"
	SchemeService URIScheme = "service"
)

// ErrTargetNotFound means the URI named a seed or service that no peer has
// published. Distinct from placement.ErrNotRunning (spec exists, no claimants).
var ErrTargetNotFound = errors.New("target not found")

// URI is a parsed pln:// address.
type URI struct {
	Scheme   URIScheme
	Name     string // seed name or service name
	Function string // only for seed URIs
}

const plnPrefix = "pln://"

// ParseURI parses a pln:// URI string.
//
// Supported forms:
//
//	pln://seed/<name>/<function>
//	pln://service/<name>
func ParseURI(raw string) (URI, error) {
	if !strings.HasPrefix(raw, plnPrefix) {
		return URI{}, fmt.Errorf("invalid pln URI: missing pln:// prefix: %q", raw)
	}
	rest := raw[len(plnPrefix):]
	parts := strings.SplitN(rest, "/", 3) //nolint:mnd
	if len(parts) < 2 || parts[1] == "" {
		return URI{}, fmt.Errorf("invalid pln URI: missing name: %q", raw)
	}

	switch URIScheme(parts[0]) {
	case SchemeSeed:
		if len(parts) < 3 || parts[2] == "" {
			return URI{}, fmt.Errorf("invalid pln URI: seed requires function: %q", raw)
		}
		return URI{Scheme: SchemeSeed, Name: parts[1], Function: parts[2]}, nil
	case SchemeService:
		return URI{Scheme: SchemeService, Name: parts[1]}, nil
	default:
		return URI{}, fmt.Errorf("invalid pln URI: unknown scheme %q: %q", parts[0], raw)
	}
}
