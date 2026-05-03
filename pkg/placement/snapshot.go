// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"time"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/types"
)

func replicasOf(snap state.Snapshot, seed string) []types.PeerKey {
	set := snap.Claims[seed]
	out := make([]types.PeerKey, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}

func ownedBy(snap state.Snapshot, self types.PeerKey) []string {
	out := make([]string, 0)
	for hash, claimants := range snap.Claims {
		if _, ok := claimants[self]; ok {
			out = append(out, hash)
		}
	}
	return out
}

func allSeeds(snap state.Snapshot) []string {
	out := make([]string, 0, len(snap.Specs))
	for hash := range snap.Specs {
		out = append(out, hash)
	}
	return out
}

func isBackedOff(snap state.Snapshot, peer types.PeerKey, now time.Time) bool {
	nv, ok := snap.Nodes[peer]
	if !ok {
		return false
	}
	return now.Before(nv.BackoffExpiry)
}

func backedOffSet(snap state.Snapshot, now time.Time) map[types.PeerKey]struct{} {
	out := make(map[types.PeerKey]struct{})
	for pk, nv := range snap.Nodes {
		if now.Before(nv.BackoffExpiry) {
			out[pk] = struct{}{}
		}
	}
	return out
}

type sourceCount struct {
	caller types.PeerKey
	count  uint64
}

func sourcesOf(snap state.Snapshot, seed string) []sourceCount {
	out := make([]sourceCount, 0, len(snap.Nodes))
	for pk, nv := range snap.Nodes {
		if c := nv.CallCounts[seed]; c > 0 {
			out = append(out, sourceCount{caller: pk, count: c})
		}
	}
	return out
}
