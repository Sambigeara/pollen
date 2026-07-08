// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
)

// TestErrorLine pins the operator-facing rendering to the exact shapes
// the docs corpus quotes. The connect cases lose their gRPC code
// envelope down to the daemon's own message, matching the admission
// budget and not-found terminal blocks in troubleshoot.html and
// how-to.html. The local cases use the real production constructors so
// the troubleshoot #daemon-down and #socket-permission symptom blocks
// are literally what the binary prints.
func TestErrorLine(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			"connect failed-precondition drops the code envelope",
			connect.NewError(connect.CodeFailedPrecondition,
				errors.New("admission: functions budget exhausted: authority holds 5, limit 5")),
			"Error: admission: functions budget exhausted: authority holds 5, limit 5",
		},
		{
			"connect not-found drops the code envelope",
			connect.NewError(connect.CodeNotFound, errors.New("no such workload")),
			"Error: no such workload",
		},
		{
			"unreachable daemon prints verbatim",
			unreachableErr("daemon is not running"),
			"Error: daemon is not running",
		},
		{
			"socket permission hint prints verbatim across its lines",
			permissionErr("cannot reach daemon — are you in the pln group?\n  fix: sudo usermod -aG pln $(whoami) && newgrp pln"),
			"Error: cannot reach daemon — are you in the pln group?\n  fix: sudo usermod -aG pln $(whoami) && newgrp pln",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, errorLine(tc.err))
		})
	}
}
