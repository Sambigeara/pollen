// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"errors"
	"testing"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/stretchr/testify/require"
)

// TestRouteServiceRequest_NoProvider pins that a pln://service/<name> dial
// for an unregistered service surfaces wasm.ErrTargetNotFound, so the host
// can demote the log and gRPC callers can map it to NotFound.
func TestRouteServiceRequest_NoProvider(t *testing.T) {
	store := state.New(types.PeerKey{})
	n := &Supervisor{store: store}

	_, err := n.routeServiceRequest(context.Background(), "missing", nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, wasm.ErrTargetNotFound), "want ErrTargetNotFound, got %v", err)
}
