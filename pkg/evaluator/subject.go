// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator

import "github.com/sambigeara/pollen/pkg/types"

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
