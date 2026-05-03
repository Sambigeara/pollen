// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package placement

import (
	"github.com/sambigeara/pollen/pkg/types"
)

type actionKind int

const (
	actionClaim actionKind = iota
	actionRelease
)

type action struct {
	Hash string
	Kind actionKind
}

type spec struct {
	MemoryBytes uint64
	MinReplicas uint32
	Spread      float32
}

func effectiveTarget(minReplicas uint32, spread float32, clusterSize int) uint32 {
	if spread >= 1.0 {
		return uint32(clusterSize)
	}
	return min(minReplicas, uint32(clusterSize))
}

type evaluateInput struct {
	specs     map[string]spec
	claims    map[string]map[types.PeerKey]struct{}
	draining  map[string]map[types.PeerKey]struct{}
	isRunning func(string) bool
	allPeers  []types.PeerKey
	localID   types.PeerKey
}

// activeClaimCount counts non-draining claimants. Draining peers stay
// in the full set so callers keep routing to them, but don't count
// toward the replica target — that's the make-before-break overlap.
func activeClaimCount(claimants, drainSet map[types.PeerKey]struct{}) uint32 {
	if len(drainSet) == 0 {
		return uint32(len(claimants))
	}
	var n uint32
	for pk := range claimants {
		if _, drain := drainSet[pk]; !drain {
			n++
		}
	}
	return n
}

func evaluate(in evaluateInput) []action {
	var actions []action

	for hash, sp := range in.specs {
		claimants := in.claims[hash]
		drainSet := in.draining[hash]
		_, iClaimed := claimants[in.localID]
		_, iDraining := drainSet[in.localID]
		target := effectiveTarget(sp.MinReplicas, sp.Spread, len(in.allPeers))
		claimCount := activeClaimCount(claimants, drainSet)

		if iDraining {
			if claimCount < target {
				actions = append(actions, action{Hash: hash, Kind: actionClaim})
			} else {
				actions = append(actions, action{Hash: hash, Kind: actionRelease})
			}
			continue
		}

		if iClaimed && !in.isRunning(hash) {
			actions = append(actions, action{Hash: hash, Kind: actionClaim})
		}
	}

	for hash, claimants := range in.claims {
		if _, iClaimed := claimants[in.localID]; !iClaimed {
			continue
		}
		if _, hasSpec := in.specs[hash]; !hasSpec {
			actions = append(actions, action{Hash: hash, Kind: actionRelease})
		}
	}

	return actions
}
