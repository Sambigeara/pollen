package link

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/flynn/noise"
	"github.com/sambigeara/pollen/internal/testutil/memtransport"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ed25519"
)

func TestLink_UpdatesPeerAddrFromAuthenticatedTraffic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	network := memtransport.NewNetwork()

	addrA := "127.0.0.1:11001"
	addrB := "127.0.0.2:11002"

	storeA, err := network.Bind(addrA)
	require.NoError(t, err)
	storeB, err := network.Bind(addrB)
	require.NoError(t, err)

	cs := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)
	keyA, _ := cs.GenerateKeypair(rand.Reader)
	keyB, _ := cs.GenerateKeypair(rand.Reader)

	pubA, _, _ := ed25519.GenerateKey(rand.Reader)
	pubB, _, _ := ed25519.GenerateKey(rand.Reader)

	invitesA, err := admission.Load(t.TempDir())
	require.NoError(t, err)
	inviteB := newInvite(t, addrA)
	invitesA.AddInvite(inviteB)

	linkA, err := NewLink(storeA, &cs, keyA, testCrypto{noisePub: keyA.Public, identityPub: pubA}, invitesA)
	require.NoError(t, err)
	linkB, err := NewLink(storeB, &cs, keyB, testCrypto{noisePub: keyB.Public, identityPub: pubB}, mustAdmission(t))
	require.NoError(t, err)

	require.NoError(t, linkA.Start(ctx))
	require.NoError(t, linkB.Start(ctx))

	peerA := types.PeerKeyFromBytes(keyA.Public)
	peerB := types.PeerKeyFromBytes(keyB.Public)

	require.NoError(t, linkB.JoinWithInvite(ctx, inviteB))

	awaitConnects(t, linkA.Events(), peerB)
	awaitConnects(t, linkB.Events(), peerA)

	recvA := make(chan []byte, 1)
	linkA.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, payload []byte) error {
		select {
		case recvA <- payload:
		default:
		}
		return nil
	})

	recvB := make(chan []byte, 1)
	linkB.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, payload []byte) error {
		select {
		case recvB <- payload:
		default:
		}
		return nil
	})

	implB, ok := linkB.(*impl)
	require.True(t, ok)
	sessB, ok := implB.sessionStore.getByPeer(peerA)
	require.True(t, ok)

	ephemeralB, err := linkB.SocketStore().CreateEphemeral()
	require.NoError(t, err)

	payload := []byte("rebinding")
	ct, _, err := sessB.Encrypt(payload)
	require.NoError(t, err)

	require.NoError(t, ephemeralB.Send(addrA, encodeFrame(&frame{
		payload:    ct,
		typ:        types.MsgTypeTest,
		senderID:   sessB.localSessionID,
		receiverID: sessB.peerSessionID,
	})))

	awaitPayload(t, recvA, payload)

	addr, ok := linkA.GetActivePeerAddress(peerB)
	require.True(t, ok)
	require.Equal(t, ephemeralB.LocalAddr().String(), addr)

	reply := []byte("reply")
	require.NoError(t, linkA.Send(ctx, peerB, types.Envelope{Type: types.MsgTypeTest, Payload: reply}))
	awaitPayload(t, recvB, reply)

	require.Eventually(t, func() bool {
		sock, ok := linkB.SocketStore().GetPeerSocket(peerA)
		return ok && sock == ephemeralB
	}, time.Second, 10*time.Millisecond)
}
