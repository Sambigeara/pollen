package link

import (
	"context"
	"crypto/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/flynn/noise"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ed25519"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/internal/testutil/memtransport"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/types"
)

func TestPunchCoordination_ConnectsPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	network := memtransport.NewNetwork()

	addrA := "127.0.0.1:11001"
	addrB := "127.0.0.2:11002"
	addrC := "127.0.0.3:11003"

	storeA, err := network.Bind(addrA)
	require.NoError(t, err)
	storeB, err := network.Bind(addrB)
	require.NoError(t, err)
	storeC, err := network.Bind(addrC)
	require.NoError(t, err)

	cs := noise.NewCipherSuite(noise.DH25519, noise.CipherAESGCM, noise.HashSHA256)
	keyA, _ := cs.GenerateKeypair(rand.Reader)
	keyB, _ := cs.GenerateKeypair(rand.Reader)
	keyC, _ := cs.GenerateKeypair(rand.Reader)

	pubA, _, _ := ed25519.GenerateKey(rand.Reader)
	pubB, _, _ := ed25519.GenerateKey(rand.Reader)
	pubC, _, _ := ed25519.GenerateKey(rand.Reader)

	invitesA, err := admission.Load(t.TempDir())
	require.NoError(t, err)

	inviteB := newInvite(t, addrA)
	inviteC := newInvite(t, addrA)
	invitesA.AddInvite(inviteB)
	invitesA.AddInvite(inviteC)

	linkA, err := NewLink(storeA, &cs, keyA, testCrypto{noisePub: keyA.Public, identityPub: pubA}, invitesA,
		WithEnsurePeerInterval(5*time.Millisecond),
		WithEnsurePeerTimeout(250*time.Millisecond),
		WithHolepunchAttempts(2),
	)
	require.NoError(t, err)
	linkB, err := NewLink(storeB, &cs, keyB, testCrypto{noisePub: keyB.Public, identityPub: pubB}, mustAdmission(t),
		WithEnsurePeerInterval(5*time.Millisecond),
		WithEnsurePeerTimeout(250*time.Millisecond),
		WithHolepunchAttempts(2),
	)
	require.NoError(t, err)
	linkC, err := NewLink(storeC, &cs, keyC, testCrypto{noisePub: keyC.Public, identityPub: pubC}, mustAdmission(t),
		WithEnsurePeerInterval(5*time.Millisecond),
		WithEnsurePeerTimeout(250*time.Millisecond),
		WithHolepunchAttempts(2),
	)
	require.NoError(t, err)

	require.NoError(t, linkA.Start(ctx))
	require.NoError(t, linkB.Start(ctx))
	require.NoError(t, linkC.Start(ctx))

	peerA := types.PeerKeyFromBytes(keyA.Public)
	peerB := types.PeerKeyFromBytes(keyB.Public)
	peerC := types.PeerKeyFromBytes(keyC.Public)

	require.NoError(t, linkB.JoinWithInvite(ctx, inviteB))
	require.NoError(t, linkC.JoinWithInvite(ctx, inviteC))

	awaitConnects(t, linkA.Events(), peerB, peerC)
	awaitConnects(t, linkB.Events(), peerA)
	awaitConnects(t, linkC.Events(), peerA)

	req := &peerv1.PunchCoordRequest{
		PeerId: peerC.Bytes(),
		Mode:   peerv1.PunchMode_PUNCH_MODE_DIRECT,
	}
	reqBytes, err := req.MarshalVT()
	require.NoError(t, err)

	require.NoError(t, linkB.Send(ctx, peerA, types.Envelope{
		Type:    types.MsgTypeUDPPunchCoordRequest,
		Payload: reqBytes,
	}))

	awaitConnects(t, linkB.Events(), peerC)
	awaitConnects(t, linkC.Events(), peerB)

	_, ok := linkB.GetActivePeerAddress(peerC)
	require.True(t, ok)
	_, ok = linkC.GetActivePeerAddress(peerB)
	require.True(t, ok)

	recvB := make(chan []byte, 1)
	linkB.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, payload []byte) error {
		select {
		case recvB <- payload:
		default:
		}
		return nil
	})
	recvC := make(chan []byte, 1)
	linkC.Handle(types.MsgTypeTest, func(_ context.Context, _ types.PeerKey, payload []byte) error {
		select {
		case recvC <- payload:
		default:
		}
		return nil
	})

	payload := []byte("hello")
	require.NoError(t, linkB.Send(ctx, peerC, types.Envelope{Type: types.MsgTypeTest, Payload: payload}))
	require.NoError(t, linkC.Send(ctx, peerB, types.Envelope{Type: types.MsgTypeTest, Payload: payload}))

	awaitPayload(t, recvC, payload)
	awaitPayload(t, recvB, payload)
}

type testCrypto struct {
	noisePub    []byte
	identityPub []byte
}

func (c testCrypto) NoisePub() []byte {
	return c.noisePub
}

func (c testCrypto) IdentityPub() []byte {
	return c.identityPub
}

func newInvite(t *testing.T, addr string) *peerv1.Invite {
	t.Helper()
	psk := make([]byte, 32)
	_, err := rand.Read(psk)
	require.NoError(t, err)

	return &peerv1.Invite{
		Id:   uuid.NewString(),
		Psk:  psk,
		Addr: []string{addr},
	}
}

func mustAdmission(t *testing.T) admission.Store {
	t.Helper()
	store, err := admission.Load(t.TempDir())
	require.NoError(t, err)
	return store
}

const connectTimeout = 2 * time.Second

func awaitConnects(t *testing.T, events <-chan peer.Input, peers ...types.PeerKey) {
	t.Helper()
	if len(peers) == 0 {
		return
	}

	pending := make(map[types.PeerKey]struct{}, len(peers))
	for _, p := range peers {
		pending[p] = struct{}{}
	}

	deadline := time.After(connectTimeout)
	for len(pending) > 0 {
		select {
		case in := <-events:
			if cp, ok := in.(peer.ConnectPeer); ok {
				delete(pending, cp.PeerKey)
			}
		case <-deadline:
			t.Fatalf("timeout waiting for peers: %v", pending)
		}
	}
}

func awaitPayload(t *testing.T, ch <-chan []byte, expected []byte) {
	t.Helper()
	select {
	case got := <-ch:
		require.Equal(t, expected, got)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for payload")
	}
}

func portFromAddr(t *testing.T, addr string) int {
	t.Helper()
	_, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	return port
}
