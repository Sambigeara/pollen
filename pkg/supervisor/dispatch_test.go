package supervisor

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/sambigeara/pollen/pkg/state"
	"github.com/stretchr/testify/require"
)

func TestDispatchPeerDeniedForgetsAndDisconnects(t *testing.T) {
	n := newMinimalNode(t, false)
	pk := testPeerKey(1)

	addKnownPeerForTopology(t, n.store, pk, 1)
	n.meshInternal.DiscoverPeer(pk, []net.IP{net.ParseIP("10.0.0.1")}, 9001, nil, false, false)
	require.True(t, n.meshInternal.HasPeer(pk))

	n.dispatchEvents(context.Background(), []state.Event{state.PeerDenied{Key: pk}})

	require.False(t, n.meshInternal.HasPeer(pk))
}

func TestSignalRouteInvalidateIsNonBlocking(t *testing.T) {
	n := newMinimalNode(t, false)

	n.signalRouteInvalidate()
	n.signalRouteInvalidate() // must not block

	select {
	case <-n.routeInvalidate:
	default:
		t.Fatal("expected at least one signal")
	}
}

func TestSignalRouteInvalidateCoalesces(t *testing.T) {
	n := newMinimalNode(t, false)

	// Two signals should coalesce into one buffered signal.
	n.signalRouteInvalidate()
	n.signalRouteInvalidate()

	// Drain the one coalesced signal.
	select {
	case <-n.routeInvalidate:
	default:
		t.Fatal("expected signal")
	}

	// Channel should be empty now.
	select {
	case <-n.routeInvalidate:
		t.Fatal("expected no second signal")
	default:
	}
}

func TestSaveStatePersistsToDisk(t *testing.T) {
	n := newMinimalNode(t, false)
	n.saveState()

	data, err := os.ReadFile(filepath.Join(n.pollenDir, "state.pb"))
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestRecomputeRoutesDoesNotPanic(t *testing.T) {
	n := newMinimalNode(t, false)
	addKnownPeerForTopology(t, n.store, testPeerKey(1), 1)
	addKnownPeerForTopology(t, n.store, testPeerKey(2), 2)

	require.NotPanics(t, func() {
		n.recomputeRoutes()
	})
}
