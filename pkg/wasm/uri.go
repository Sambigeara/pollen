// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package wasm

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type URIScheme string

const (
	SchemeSeed    URIScheme = "seed"
	SchemeService URIScheme = "service"
)

var ErrTargetNotFound = errors.New("target not found")

var ErrInvalidTailCall = errors.New("invalid tail call marker")

type URI struct {
	Scheme   URIScheme
	Name     string
	Function string
}

type TailCall struct {
	URI   URI
	Input []byte
}

const plnPrefix = "pln://"

const (
	tailCallKind       = "tail_call"
	uriSchemeNameLen   = 2
	uriSchemeFnNameLen = 3
)

func ParseURI(raw string) (URI, error) {
	if !strings.HasPrefix(raw, plnPrefix) {
		return URI{}, fmt.Errorf("invalid pln URI: missing pln:// prefix: %q", raw)
	}
	rest := raw[len(plnPrefix):]
	parts := strings.SplitN(rest, "/", uriSchemeFnNameLen)
	if len(parts) < uriSchemeNameLen || parts[1] == "" {
		return URI{}, fmt.Errorf("invalid pln URI: missing name: %q", raw)
	}

	switch URIScheme(parts[0]) {
	case SchemeSeed:
		if len(parts) < uriSchemeFnNameLen || parts[2] == "" {
			return URI{}, fmt.Errorf("invalid pln URI: seed requires function: %q", raw)
		}
		return URI{Scheme: SchemeSeed, Name: parts[1], Function: parts[2]}, nil
	case SchemeService:
		if len(parts) > uriSchemeNameLen {
			return URI{}, fmt.Errorf("invalid pln URI: service must not include path: %q", raw)
		}
		return URI{Scheme: SchemeService, Name: parts[1]}, nil
	default:
		return URI{}, fmt.Errorf("invalid pln URI: unknown scheme %q: %q", parts[0], raw)
	}
}

type tailCallProbe struct {
	Input json.RawMessage `json:"input"`
	Kind  json.RawMessage `json:"kind"`
	URI   json.RawMessage `json:"uri"`
}

type tailCallWire struct {
	Input *string `json:"input,omitempty"`
	URI   string  `json:"uri"`
}

func ParseTailCallMarker(output []byte) (TailCall, bool, error) {
	trimmed := bytes.TrimSpace(output)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return TailCall{}, false, nil
	}
	if !json.Valid(trimmed) {
		if bytes.Contains(trimmed, []byte(tailCallKind)) {
			return TailCall{}, false, fmt.Errorf("%w: malformed JSON", ErrInvalidTailCall)
		}
		return TailCall{}, false, nil
	}

	var probe tailCallProbe
	if err := json.Unmarshal(trimmed, &probe); err != nil {
		return TailCall{}, false, fmt.Errorf("%w: %w", ErrInvalidTailCall, err)
	}
	isTail, err := isTailCallKind(probe)
	if err != nil {
		return TailCall{}, false, err
	}
	if !isTail {
		return TailCall{}, false, nil
	}

	var wire tailCallWire
	if err := json.Unmarshal(trimmed, &wire); err != nil {
		return TailCall{}, false, fmt.Errorf("%w: %w", ErrInvalidTailCall, err)
	}
	if wire.URI == "" {
		return TailCall{}, false, fmt.Errorf("%w: missing uri", ErrInvalidTailCall)
	}
	uri, err := ParseURI(wire.URI)
	if err != nil {
		return TailCall{}, false, fmt.Errorf("%w: %w", ErrInvalidTailCall, err)
	}

	var input []byte
	if wire.Input != nil {
		decoded, err := base64.StdEncoding.Strict().DecodeString(*wire.Input)
		if err != nil {
			return TailCall{}, false, fmt.Errorf("%w: decode input: %w", ErrInvalidTailCall, err)
		}
		input = decoded
	}
	return TailCall{URI: uri, Input: input}, true, nil
}

func isTailCallKind(probe tailCallProbe) (bool, error) {
	if len(probe.Kind) == 0 {
		return false, nil
	}
	var kind string
	if err := json.Unmarshal(probe.Kind, &kind); err != nil {
		if len(probe.URI) > 0 || len(probe.Input) > 0 {
			return false, fmt.Errorf("%w: kind must be a string", ErrInvalidTailCall)
		}
		return false, nil
	}
	return kind == tailCallKind, nil
}
