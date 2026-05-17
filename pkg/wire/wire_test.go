// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package wire_test

import (
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"github.com/sambigeara/pollen/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestCheckRangeCompatible(t *testing.T) {
	// This build's own range always overlaps itself.
	require.NoError(t, wire.CheckRange(wire.ProtocolMin, wire.ProtocolMax))
	// A daemon that also speaks a wider range still overlaps.
	require.NoError(t, wire.CheckRange(wire.ProtocolMin, wire.ProtocolMax+5))
}

func TestCheckRangeClientBehind(t *testing.T) {
	// Daemon requires strictly newer than this build supports.
	err := wire.CheckRange(wire.ProtocolMax+1, wire.ProtocolMax+3)
	require.Error(t, err)

	var ode *wire.OutOfDateError
	require.True(t, errors.As(err, &ode))
	require.Equal(t, wire.ProtocolMax+1, ode.ServerMin)
	require.Equal(t, wire.ProtocolMax+3, ode.ServerMax)
	require.Equal(t, wire.ProtocolMin, ode.ClientMin)
	require.Equal(t, wire.ProtocolMax, ode.ClientMax)
	require.Contains(t, err.Error(), "pln is out of date")
}

func TestCheckRangeDaemonBehind(t *testing.T) {
	// Daemon tops out below this build's minimum (server range [0,0]
	// with ProtocolMin >= 1).
	err := wire.CheckRange(0, wire.ProtocolMin-1)
	require.Error(t, err)

	var ode *wire.OutOfDateError
	require.True(t, errors.As(err, &ode))
	require.Contains(t, err.Error(), "daemon is out of date")
}

func TestErrDaemonNoHandshake(t *testing.T) {
	require.Error(t, wire.ErrDaemonNoHandshake)
	require.Contains(t, wire.ErrDaemonNoHandshake.Error(), "out of date")
}

func TestDaemonLacksHandshake(t *testing.T) {
	unimpl := connect.NewError(connect.CodeUnimplemented, errors.New("no handler"))

	require.True(t, wire.DaemonLacksHandshake(unimpl),
		"a bare Unimplemented is a too-old daemon")
	require.True(t, wire.DaemonLacksHandshake(fmt.Errorf("dial: %w", unimpl)),
		"Unimplemented wrapped by fmt.Errorf is still recognised")
	require.True(t, wire.DaemonLacksHandshake(connect.NewError(connect.CodeUnknown, unimpl)),
		"Unimplemented rewrapped by an interceptor as Unknown is still recognised")

	require.False(t, wire.DaemonLacksHandshake(nil))
	require.False(t, wire.DaemonLacksHandshake(errors.New("dial tcp: connection refused")),
		"a genuine transport error is not a handshake-absence signal")
	require.False(t, wire.DaemonLacksHandshake(connect.NewError(connect.CodeUnavailable, errors.New("down"))),
		"availability errors pass through for the command to surface")
}
