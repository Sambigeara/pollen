// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package evaluator_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sambigeara/pollen/pkg/evaluator"
	"github.com/sambigeara/pollen/pkg/types"
)

func TestSubjectFromPeerKey_WithProps(t *testing.T) {
	peer := types.PeerKey{1}
	peerProps := func(pk types.PeerKey) map[string]any {
		require.Equal(t, peer, pk)
		return map[string]any{"role": "admin"}
	}
	got := evaluator.SubjectFromPeerKey(peer, peerProps)
	require.Equal(t, evaluator.Subject{
		Type:       "peer",
		ID:         peer.String(),
		Properties: map[string]any{"role": "admin"},
	}, got)
}

func TestSubjectFromPeerKey_NilLookup(t *testing.T) {
	peer := types.PeerKey{2}
	got := evaluator.SubjectFromPeerKey(peer, nil)
	require.Equal(t, evaluator.Subject{Type: "peer", ID: peer.String()}, got)
	require.Nil(t, got.Properties)
}
