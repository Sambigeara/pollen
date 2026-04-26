// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/wasm"
)

var ErrRelayOnly = errors.New("placement disabled: relay-only mode")

// NoopService satisfies PlacementAPI for nodes that don't host workloads.
// Membership and tunneling still run, so the node gossips topology and
// forwards routed streams; only the placement axis is disabled.
type NoopService struct{}

var _ PlacementAPI = (*NoopService)(nil)

func NewNoopService() *NoopService { return &NoopService{} }

func (*NoopService) Start(context.Context) error { return nil }

func (*NoopService) Stop() error { return nil }

func (*NoopService) Seed([]byte, state.WorkloadSpec) error { return ErrRelayOnly }

func (*NoopService) Unseed(string) error { return ErrRelayOnly }

func (*NoopService) Call(context.Context, string, string, []byte) ([]byte, error) {
	return nil, ErrRelayOnly
}

func (*NoopService) Status() []WorkloadSummary { return nil }

func (*NoopService) PlacementInfo() map[string]PlacementInfo { return nil }

func (*NoopService) RecordParkedTime(string, string, time.Duration) {}

// Serve should never fire: relay-only nodes never claim workloads, so peers
// never select them as a placement target. Close defensively if it does.
func (*NoopService) Serve(stream io.ReadWriteCloser, _ wasm.CallerInfo, _, _ string) {
	_ = stream.Close()
}

func (*NoopService) Signal() {}
