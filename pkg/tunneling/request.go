// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package tunneling

import (
	"context"
	"fmt"
	"io"

	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

func (s *Service) RequestService(ctx context.Context, peer types.PeerKey, port uint32, payload []byte) ([]byte, error) {
	sCtx, cancel := context.WithTimeout(ctx, streamOpenTimeout)
	defer cancel()

	stream, err := s.streams.OpenStream(sCtx, peer, transport.StreamTypeTunnel)
	if err != nil {
		return nil, fmt.Errorf("open tunnel stream: %w", err)
	}
	defer stream.Close()

	if err := writePort(stream, port); err != nil {
		return nil, fmt.Errorf("write port header: %w", err)
	}

	if _, err := stream.Write(payload); err != nil {
		return nil, fmt.Errorf("write payload: %w", err)
	}

	resp, err := io.ReadAll(stream)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	return resp, nil
}
