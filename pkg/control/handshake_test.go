// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"
	"testing"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/wire"
	"github.com/stretchr/testify/require"
)

// Handshake is unauthenticated and side-effect free: it must report
// this build's protocol range regardless of caller, so the client can
// decide compatibility.
func TestHandshakeReportsServerRange(t *testing.T) {
	resp, err := (&Service{}).Handshake(context.Background(), &controlv1.HandshakeRequest{
		ClientMin: 1,
		ClientMax: 1,
	})
	require.NoError(t, err)
	require.Equal(t, wire.ProtocolMin, resp.GetServerMin())
	require.Equal(t, wire.ProtocolMax, resp.GetServerMax())
}
