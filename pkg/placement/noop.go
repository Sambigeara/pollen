// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"context"
	"errors"
	"io"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

var ErrRelayOnly = errors.New("placement disabled: relay-only mode")

// NoopService is the relay-only PlacementAPI: membership and tunneling
// still run, but every workload-hosting operation rejects.
type NoopService struct{}

var _ PlacementAPI = (*NoopService)(nil)

func NewNoopService() *NoopService { return &NoopService{} }

func (*NoopService) Start(context.Context) error { return nil }

func (*NoopService) Stop() error { return nil }

func (*NoopService) Seed([]byte, state.WorkloadSpec, *admissionv1.Predicate) error {
	return ErrRelayOnly
}

func (*NoopService) Unseed(string) error { return ErrRelayOnly }

func (*NoopService) Call(context.Context, string, string, []byte) ([]byte, error) {
	return nil, ErrRelayOnly
}

func (*NoopService) Status() []WorkloadSummary { return nil }

// Serve should be unreachable on a relay-only node — peers won't pick a
// non-claimant target. Close defensively if it does fire.
func (*NoopService) Serve(stream io.ReadWriteCloser, _ types.PeerKey) {
	_ = stream.Close()
}

func (*NoopService) Signal() {}
