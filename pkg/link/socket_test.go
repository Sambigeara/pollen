package link

import (
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestSocketStore_Close_DoesNotHangAfterPeerSocketReplaced(t *testing.T) {
	store, err := NewSocketStore(0)
	require.NoError(t, err)

	peerKey := types.PeerKeyFromBytes([]byte("01234567890123456789012345678901"))

	oldSock, err := store.CreateEphemeral()
	require.NoError(t, err)
	newSock, err := store.CreateEphemeral()
	require.NoError(t, err)

	store.AssociatePeerSocket(peerKey, oldSock)
	store.AssociatePeerSocket(peerKey, newSock)

	require.Eventually(t, func() bool {
		return oldSock.IsClosed()
	}, time.Second, 10*time.Millisecond)

	done := make(chan error, 1)
	go func() {
		done <- store.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		_ = oldSock.Close()
		_ = newSock.Close()
		_ = store.Base().Close()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		t.Fatalf("SocketStore.Close hung after peer socket replacement")
	}
}
