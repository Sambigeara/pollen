// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package wasm

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCallerInfoContextRoundTrip(t *testing.T) {
	info := CallerInfo{
		PeerKey:    types.PeerKeyFromBytes([]byte("01234567890123456789012345678901")),
		Attributes: map[string]any{"role": "worker"},
	}
	ctx := WithCallerInfo(context.Background(), info)
	got, ok := CallerInfoFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, info.PeerKey, got.PeerKey)
	require.Equal(t, "worker", got.Attributes["role"])
}

func TestCallerInfoContextMissing(t *testing.T) {
	_, ok := CallerInfoFromContext(context.Background())
	require.False(t, ok)
}

func TestMarshalCallerInfo(t *testing.T) {
	info := CallerInfo{
		PeerKey:    types.PeerKeyFromBytes([]byte("01234567890123456789012345678901")),
		Attributes: map[string]any{"env": "prod"},
	}
	b := MarshalCallerInfo(info)
	require.NotNil(t, b)

	var j map[string]any
	require.NoError(t, json.Unmarshal(b, &j))
	require.Equal(t, info.PeerKey.String(), j["peerKey"])
	require.Equal(t, "prod", j["attributes"].(map[string]any)["env"])
}

func TestMarshalCallerInfoNoAttributes(t *testing.T) {
	info := CallerInfo{
		PeerKey: types.PeerKeyFromBytes([]byte("01234567890123456789012345678901")),
	}
	b := MarshalCallerInfo(info)
	require.NotNil(t, b)

	var j map[string]any
	require.NoError(t, json.Unmarshal(b, &j))
	require.Equal(t, info.PeerKey.String(), j["peerKey"])
	_, hasAttrs := j["attributes"]
	require.False(t, hasAttrs)
}

func TestMarshalCallerInfoZeroPeerKey(t *testing.T) {
	require.Nil(t, MarshalCallerInfo(CallerInfo{}))
}

func TestCallerInfoJSONRoundTrip(t *testing.T) {
	original := CallerInfo{
		PeerKey:    types.PeerKeyFromBytes([]byte("01234567890123456789012345678901")),
		Attributes: map[string]any{"role": "relay", "tier": "edge"},
	}
	b := MarshalCallerInfo(original)
	require.NotNil(t, b)

	got, ok := CallerInfoFromJSON(b)
	require.True(t, ok)
	require.Equal(t, original.PeerKey, got.PeerKey)
	require.Equal(t, "relay", got.Attributes["role"])
	require.Equal(t, "edge", got.Attributes["tier"])
}

// OnBehalfOf round-trips so the receiver's dispatch loop can attribute
// the call to the caller seed rather than the host peer.
func TestCallerInfoJSONRoundTrip_OnBehalfOf(t *testing.T) {
	original := CallerInfo{
		PeerKey:    types.PeerKeyFromBytes([]byte("01234567890123456789012345678901")),
		OnBehalfOf: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
	}
	b := MarshalCallerInfo(original)
	require.NotNil(t, b)

	got, ok := CallerInfoFromJSON(b)
	require.True(t, ok)
	require.Equal(t, original.OnBehalfOf, got.OnBehalfOf)
}

// OnBehalfOf alone (no peer key, no attributes, no deadline) is still
// worth marshalling — the hop is a seed-to-seed invocation where the
// wire identity comes from the OBO attribution.
func TestMarshalCallerInfo_OnBehalfOfOnly(t *testing.T) {
	b := MarshalCallerInfo(CallerInfo{OnBehalfOf: "abc"})
	require.NotNil(t, b, "OnBehalfOf alone must still emit a caller block")
	got, ok := CallerInfoFromJSON(b)
	require.True(t, ok)
	require.Equal(t, "abc", got.OnBehalfOf)
}

func TestCallerInfoFromJSONInvalid(t *testing.T) {
	_, ok := CallerInfoFromJSON([]byte("not json"))
	require.False(t, ok)
}

func TestCallerInfoFromJSONBadPeerKey(t *testing.T) {
	_, ok := CallerInfoFromJSON([]byte(`{"peerKey":"not-hex"}`))
	require.False(t, ok)
}
