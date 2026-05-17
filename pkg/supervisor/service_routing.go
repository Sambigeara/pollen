// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strconv"

	"github.com/sambigeara/pollen/pkg/route"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

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

// serviceK caps the candidate set after Vivaldi-distance narrowing. It
// matches placement dispatch's dispatchK so service routing and seed
// dispatch make the same locality-aware choice through the one selector.
const serviceK = 2

// pickNearestService selects a provider for one service name through the
// shared locality selector. Callers guarantee a non-empty candidate set,
// so the selector always yields a peer.
func pickNearestService(snap state.Snapshot, candidates []state.ServiceInfo) state.ServiceInfo {
	byPeer := make(map[types.PeerKey]state.ServiceInfo, len(candidates))
	peers := make([]types.PeerKey, 0, len(candidates))
	for _, c := range candidates {
		if _, seen := byPeer[c.Peer]; !seen {
			peers = append(peers, c.Peer)
		}
		byPeer[c.Peer] = c
	}
	pick, _ := route.PowerOfTwo(snap, snap.LocalID, peers, serviceK, nil,
		rand.IntN) //nolint:gosec
	return byPeer[pick]
}
