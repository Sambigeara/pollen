// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import (
	"errors"
	"fmt"

	"github.com/sambigeara/pollen/pkg/types"
)

// CallerInput carries the fields SubjectFromCallerInfo needs from the
// wire-level caller envelope. Defined here (not in pkg/wasm) so
// evaluator stays a leaf foundation package with no sibling imports.
type CallerInput struct {
	OnBehalfOf string
	PeerKey    types.PeerKey
}

// ErrSeedUnknown is returned when a CallerInfo envelope claims an
// OnBehalfOf hash that doesn't resolve to a known seed in cluster
// state. Callers map it to codes.PermissionDenied.
var ErrSeedUnknown = errors.New("unknown seed in on_behalf_of envelope")

// ErrSeedNotClaimedByPeer is returned when the seed named by OnBehalfOf
// exists in cluster state but the transport-authenticated peer does not
// hold a live WorkloadClaim for it. Without this check any member peer
// could attribute a call to any seed and inherit its policy context.
var ErrSeedNotClaimedByPeer = errors.New("peer does not claim on_behalf_of seed")

// SubjectFromPeerKey builds a peer-typed Subject whose properties come
// from the supplied lookup. peerProps may be nil — the resulting Subject
// then carries no Properties.
func SubjectFromPeerKey(pk types.PeerKey, peerProps func(types.PeerKey) map[string]any) Subject {
	s := Subject{Type: "peer", ID: pk.String()}
	if peerProps != nil {
		s.Properties = peerProps(pk)
	}
	return s
}

// SubjectFromCallerInfo builds the Subject for an incoming workload
// invocation. When the wire CallerInfo carries an OnBehalfOf hash the
// call is attributed to that seed, but only after two checks: the seed
// must exist in cluster state (so properties come from a signed
// publisher claim, not envelope data) and the transport-authenticated
// peer must hold a live WorkloadClaim for it (so a member peer cannot
// forge attribution to a seed it does not run). Unknown seeds return
// ErrSeedUnknown; seeds claimed by someone else return
// ErrSeedNotClaimedByPeer. The dispatch site maps both to
// PermissionDenied.
//
// seedProps returning (nil, true) is legitimate — a seed with no
// published properties. (nil, false) means the seed is unknown.
func SubjectFromCallerInfo(
	info CallerInput,
	peerProps func(types.PeerKey) map[string]any,
	seedProps func(string) (map[string]any, bool),
	seedOwnedBy func(string, types.PeerKey) bool,
) (Subject, error) {
	if info.OnBehalfOf != "" {
		if seedProps == nil || seedOwnedBy == nil {
			return Subject{}, fmt.Errorf("%w: on_behalf_of lookup not wired", ErrSeedUnknown)
		}
		props, exists := seedProps(info.OnBehalfOf)
		if !exists {
			return Subject{}, fmt.Errorf("%w: %s", ErrSeedUnknown, types.ShortHash(info.OnBehalfOf))
		}
		if !seedOwnedBy(info.OnBehalfOf, info.PeerKey) {
			return Subject{}, fmt.Errorf("%w: seed %s not claimed by peer %s", ErrSeedNotClaimedByPeer, types.ShortHash(info.OnBehalfOf), info.PeerKey.Short())
		}
		return Subject{Type: "seed", ID: info.OnBehalfOf, Properties: props}, nil
	}
	return SubjectFromPeerKey(info.PeerKey, peerProps), nil
}
