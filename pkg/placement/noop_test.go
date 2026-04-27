// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

func TestNoopServiceRejectsHosting(t *testing.T) {
	noop := placement.NewNoopService()

	require.NoError(t, noop.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, noop.Stop()) })

	require.ErrorIs(t, noop.Seed([]byte("wasm"), state.WorkloadSpec{}), placement.ErrRelayOnly)
	require.ErrorIs(t, noop.Unseed("hash"), placement.ErrRelayOnly)

	out, err := noop.Call(context.Background(), "hash", "fn", []byte("input"))
	require.ErrorIs(t, err, placement.ErrRelayOnly)
	require.Nil(t, out)

	require.Empty(t, noop.Status())
	require.Empty(t, noop.PlacementInfo())

	noop.RecordParkedTime("hash", "fn", time.Second)
	noop.Signal()
}

func TestNoopServiceServeClosesStream(t *testing.T) {
	noop := placement.NewNoopService()
	stream := &countingCloser{}
	noop.Serve(stream, types.PeerKey{})
	require.Equal(t, 1, stream.closes)
}

type countingCloser struct {
	closes int
}

func (c *countingCloser) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *countingCloser) Write(p []byte) (int, error) { return len(p), nil }
func (c *countingCloser) Close() error {
	c.closes++
	return nil
}

func TestErrRelayOnlyIsSentinel(t *testing.T) {
	wrapped := errors.Join(errors.New("outer"), placement.ErrRelayOnly)
	require.ErrorIs(t, wrapped, placement.ErrRelayOnly)
}
