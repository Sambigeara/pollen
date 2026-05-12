// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/auth"
)

func TestInjectCallerPopulatesContext(t *testing.T) {
	creds := mustNodeCredentials(t)
	srv := New(nil, nil, nil, nil, nil, nil, WithCredentials(creds))

	ctx := srv.injectCaller(context.Background())
	caller, ok := auth.RPCCallerFromContext(ctx)
	require.True(t, ok, "interceptor must inject a caller")
	require.Same(t, creds.Cert(), caller.Cert(),
		"caller cert must match the daemon's own when no inbound peer cert is presented")
}

func TestInjectCallerNoCredsLeavesContextEmpty(t *testing.T) {
	srv := New(nil, nil, nil, nil, nil, nil)

	ctx := srv.injectCaller(context.Background())
	_, ok := auth.RPCCallerFromContext(ctx)
	require.False(t, ok, "interceptor must skip injection when daemon has no creds")
}

func mustNodeCredentials(t *testing.T) *auth.NodeCredentials {
	t.Helper()
	dir := t.TempDir()
	nodePub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	creds, err := auth.EnsureLocalRootCredentials(dir, nodePub, nil, time.Now(), 24*time.Hour)
	require.NoError(t, err)
	return creds
}
