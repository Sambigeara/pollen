// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package wasm

import (
	"context"
	"encoding/json"

	"github.com/sambigeara/pollen/pkg/types"
)

type (
	callerInfoKey        struct{}
	executingSeedKey     struct{}
	executingFunctionKey struct{}
)

// WithExecutingSeed stamps ctx with the hash of the seed currently being
// invoked. Host functions read this to attribute outbound dials to the
// caller seed.
func WithExecutingSeed(ctx context.Context, hash string) context.Context {
	return context.WithValue(ctx, executingSeedKey{}, hash)
}

// ExecutingSeedFromContext returns the seed hash currently executing, or
// "" if none is stamped.
func ExecutingSeedFromContext(ctx context.Context) string {
	if h, ok := ctx.Value(executingSeedKey{}).(string); ok {
		return h
	}
	return ""
}

// WithExecutingFunction stamps ctx with the exported function currently
// being invoked. Paired with WithExecutingSeed so host functions can
// attribute observations to the specific (hash, function) admission unit.
func WithExecutingFunction(ctx context.Context, function string) context.Context {
	return context.WithValue(ctx, executingFunctionKey{}, function)
}

// ExecutingFunctionFromContext returns the executing function name, or ""
// if none is stamped.
func ExecutingFunctionFromContext(ctx context.Context) string {
	if f, ok := ctx.Value(executingFunctionKey{}).(string); ok {
		return f
	}
	return ""
}

// CallerInfo carries the peer's identity, cert attributes, and propagated
// deadline. OnBehalfOf names the seed the caller is acting for on this hop,
// letting the receiver gate on seed identity rather than the host peer; it
// is empty on the first hop and validated against cluster state before any
// seed-typed attribution is trusted (see evaluator.SubjectFromCallerInfo).
type CallerInfo struct {
	Attributes     map[string]any
	OnBehalfOf     string
	DeadlineUnixMs int64
	PeerKey        types.PeerKey
}

// WithCallerInfo returns a context carrying the given caller metadata.
func WithCallerInfo(ctx context.Context, info CallerInfo) context.Context {
	return context.WithValue(ctx, callerInfoKey{}, info)
}

// CallerInfoFromContext extracts CallerInfo from a context.
func CallerInfoFromContext(ctx context.Context) (CallerInfo, bool) {
	info, ok := ctx.Value(callerInfoKey{}).(CallerInfo)
	return info, ok
}

type callerInfoJSON struct {
	Attributes     map[string]any `json:"attributes,omitempty"`
	PeerKey        string         `json:"peerKey,omitempty"`
	OnBehalfOf     string         `json:"onBehalfOf,omitempty"`
	DeadlineUnixMs int64          `json:"deadlineUnixMs,omitempty"`
}

// MarshalCallerInfo serialises CallerInfo to JSON. Returns nil if every
// field is zero — callers with nothing to say send no caller block.
func MarshalCallerInfo(info CallerInfo) []byte {
	if info.PeerKey == (types.PeerKey{}) && info.Attributes == nil && info.DeadlineUnixMs == 0 && info.OnBehalfOf == "" {
		return nil
	}
	j := callerInfoJSON{
		Attributes:     info.Attributes,
		OnBehalfOf:     info.OnBehalfOf,
		DeadlineUnixMs: info.DeadlineUnixMs,
	}
	if info.PeerKey != (types.PeerKey{}) {
		j.PeerKey = info.PeerKey.String()
	}
	b, err := json.Marshal(j)
	if err != nil {
		return nil
	}
	return b
}

// CallerInfoFromJSON deserialises CallerInfo from JSON produced by
// MarshalCallerInfo. The peer key is optional — an absent or empty value
// yields a zero PeerKey, which the transport-authenticated peer then
// overrides on the server side.
func CallerInfoFromJSON(data []byte) (CallerInfo, bool) {
	var j callerInfoJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return CallerInfo{}, false
	}
	info := CallerInfo{
		Attributes:     j.Attributes,
		OnBehalfOf:     j.OnBehalfOf,
		DeadlineUnixMs: j.DeadlineUnixMs,
	}
	if j.PeerKey != "" {
		pk, err := types.PeerKeyFromString(j.PeerKey)
		if err != nil {
			return CallerInfo{}, false
		}
		info.PeerKey = pk
	}
	return info, true
}
