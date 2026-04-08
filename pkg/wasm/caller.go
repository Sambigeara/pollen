package wasm

import (
	"context"
	"encoding/json"

	"github.com/sambigeara/pollen/pkg/types"
)

type callerInfoKey struct{}

// CallerInfo holds the identity and cert attributes of the peer that
// initiated a workload invocation.
type CallerInfo struct {
	Attributes map[string]any
	PeerKey    types.PeerKey
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
	Attributes map[string]any `json:"attributes,omitempty"`
	PeerKey    string         `json:"peerKey"`
}

// MarshalCallerInfo serialises CallerInfo to JSON. Returns nil if PeerKey is zero.
func MarshalCallerInfo(info CallerInfo) []byte {
	if info.PeerKey == (types.PeerKey{}) {
		return nil
	}
	b, err := json.Marshal(callerInfoJSON{
		PeerKey:    info.PeerKey.String(),
		Attributes: info.Attributes,
	})
	if err != nil {
		return nil
	}
	return b
}

// CallerInfoFromJSON deserialises CallerInfo from JSON produced by MarshalCallerInfo.
func CallerInfoFromJSON(data []byte) (CallerInfo, bool) {
	var j callerInfoJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return CallerInfo{}, false
	}
	pk, err := types.PeerKeyFromString(j.PeerKey)
	if err != nil {
		return CallerInfo{}, false
	}
	return CallerInfo{PeerKey: pk, Attributes: j.Attributes}, true
}
