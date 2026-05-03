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

func WithExecutingSeed(ctx context.Context, hash string) context.Context {
	return context.WithValue(ctx, executingSeedKey{}, hash)
}

func ExecutingSeedFromContext(ctx context.Context) string {
	if h, ok := ctx.Value(executingSeedKey{}).(string); ok {
		return h
	}
	return ""
}

func WithExecutingFunction(ctx context.Context, function string) context.Context {
	return context.WithValue(ctx, executingFunctionKey{}, function)
}

func ExecutingFunctionFromContext(ctx context.Context) string {
	if f, ok := ctx.Value(executingFunctionKey{}).(string); ok {
		return f
	}
	return ""
}

type CallerInfo struct {
	Attributes     map[string]any
	DeadlineUnixMs int64
	PeerKey        types.PeerKey
}

func WithCallerInfo(ctx context.Context, info CallerInfo) context.Context {
	return context.WithValue(ctx, callerInfoKey{}, info)
}

func CallerInfoFromContext(ctx context.Context) (CallerInfo, bool) {
	info, ok := ctx.Value(callerInfoKey{}).(CallerInfo)
	return info, ok
}

type callerInfoJSON struct {
	Attributes     map[string]any `json:"attributes,omitempty"`
	PeerKey        string         `json:"peerKey,omitempty"`
	DeadlineUnixMs int64          `json:"deadlineUnixMs,omitempty"`
}

func MarshalCallerInfo(info CallerInfo) []byte {
	if info.PeerKey == (types.PeerKey{}) && info.Attributes == nil && info.DeadlineUnixMs == 0 {
		return nil
	}
	j := callerInfoJSON{
		Attributes:     info.Attributes,
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

func CallerInfoFromJSON(data []byte) (CallerInfo, bool) {
	var j callerInfoJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return CallerInfo{}, false
	}
	info := CallerInfo{
		Attributes:     j.Attributes,
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
