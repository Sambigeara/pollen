package node

import (
	"context"
	"errors"
	"testing"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBroadcastGossipBatchesStopsRetryingFailedPeer(t *testing.T) {
	n := newMinimalNode(t, false)
	failPeer := testPeerKey(1)
	okPeer := testPeerKey(2)
	wrapper := &countingMesh{Mesh: n.mesh, failPeer: failPeer, failAfter: 1, err: errors.New("boom")}
	n.mesh = wrapper

	batches := [][]*statev1.GossipEvent{{{}}, {{}}}
	n.broadcastGossipBatches(context.Background(), []types.PeerKey{failPeer, okPeer, n.store.LocalID()}, batches)

	require.Equal(t, 1, wrapper.calls[failPeer])
	require.Equal(t, 2, wrapper.calls[okPeer])
	_, calledLocal := wrapper.calls[n.store.LocalID()]
	require.False(t, calledLocal)
}

type countingMesh struct {
	mesh.Mesh
	calls     map[types.PeerKey]int
	failPeer  types.PeerKey
	failAfter int
	err       error
}

func (m *countingMesh) Send(_ context.Context, peerKey types.PeerKey, _ *meshv1.Envelope) error {
	if m.calls == nil {
		m.calls = make(map[types.PeerKey]int)
	}
	m.calls[peerKey]++
	if peerKey == m.failPeer && m.calls[peerKey] >= m.failAfter {
		return m.err
	}
	return nil
}
