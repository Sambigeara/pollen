// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"strconv"

	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/state"
)

// dialLocalService connects directly to a service on the local node, bypassing
// the tunnel. This is used when the selected provider is the local peer.
func dialLocalService(ctx context.Context, port uint32, input []byte) ([]byte, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", "127.0.0.1:"+strconv.Itoa(int(port)))
	if err != nil {
		return nil, fmt.Errorf("dial local service: %w", err)
	}
	defer conn.Close()
	if _, err := conn.Write(input); err != nil {
		return nil, fmt.Errorf("write to local service: %w", err)
	}
	resp, err := io.ReadAll(conn)
	if err != nil {
		return nil, fmt.Errorf("read local service response: %w", err)
	}
	return resp, nil
}

// pickNearestService selects a service provider using power-of-two-choices with
// Vivaldi distance as the comparison metric. If fewer than two candidates exist
// or coordinates are unavailable, it falls back to an arbitrary pick.
func pickNearestService(snap state.Snapshot, candidates []state.ServiceInfo) state.ServiceInfo {
	if len(candidates) == 1 {
		return candidates[0]
	}

	localNV, ok := snap.Nodes[snap.LocalID]
	if !ok || localNV.VivaldiCoord == nil {
		return candidates[rand.IntN(len(candidates))] //nolint:gosec
	}

	// P2C: pick two distinct random candidates, choose the closer one.
	// j is drawn from [0, len-2] then shifted past i to guarantee i != j.
	i := rand.IntN(len(candidates))     //nolint:gosec
	j := rand.IntN(len(candidates) - 1) //nolint:gosec
	if j >= i {
		j++
	}

	distI := serviceDistance(snap, *localNV.VivaldiCoord, candidates[i])
	distJ := serviceDistance(snap, *localNV.VivaldiCoord, candidates[j])

	if distI <= distJ {
		return candidates[i]
	}
	return candidates[j]
}

func serviceDistance(snap state.Snapshot, local coords.Coord, svc state.ServiceInfo) float64 {
	nv, ok := snap.Nodes[svc.Peer]
	if !ok || nv.VivaldiCoord == nil {
		return math.MaxFloat64
	}
	return coords.Distance(local, *nv.VivaldiCoord)
}
