package link

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/flynn/noise"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ed25519"

	peerv1 "github.com/sambigeara/pollen/api/genpb/pollen/peer/v1"
	"github.com/sambigeara/pollen/internal/testutil/nattransport"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/types"
)

func TestPunchCoordination_BirthdayParadoxSymmetricNAT(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	network := nattransport.NewNetwork()

	addrA := "203.0.113.1:11001"
	internalB := "10.0.0.2:11002"
	internalC := "10.0.0.3:11003"

	trA, err := network.Bind(addrA)
	require.NoError(t, err)
	trB, err := network.BindNAT(internalB, nattransport.NATConfig{
		PublicIP: "198.51.100.2",
		PortMin:  40000,
		PortMax:  40063,
		Seed:     1,
	})
	require.NoError(t, err)
	trC, err := network.BindNAT(internalC, nattransport.NATConfig{
		PublicIP: "198.51.100.3",
		PortMin:  40000,
		PortMax:  40063,
		Seed:     2,
	})
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

	opts := []Option{
		WithEnsurePeerInterval(5 * time.Millisecond),
		WithEnsurePeerTimeout(500 * time.Millisecond),
		WithHolepunchAttempts(2),
		WithBirthdayPunchPortRange(40000, 40063),
		WithBirthdayPunchPortsPerAttempt(256),
	}

	linkA, err := NewLinkWithTransport(trA, &cs, keyA, testCrypto{noisePub: keyA.Public, identityPub: pubA}, invitesA, opts...)
	require.NoError(t, err)
	linkB, err := NewLinkWithTransport(trB, &cs, keyB, testCrypto{noisePub: keyB.Public, identityPub: pubB}, mustAdmission(t),
		append(opts, WithBirthdayPunchSeed(1))...)
	require.NoError(t, err)
	linkC, err := NewLinkWithTransport(trC, &cs, keyC, testCrypto{noisePub: keyC.Public, identityPub: pubC}, mustAdmission(t),
		append(opts, WithBirthdayPunchSeed(2))...)
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
		PeerId:    peerC.Bytes(),
		LocalPort: int32(portFromAddr(t, internalB)),
		Mode:      peerv1.PunchMode_PUNCH_MODE_BIRTHDAY,
	}
	reqBytes, err := req.MarshalVT()
	require.NoError(t, err)

	require.NoError(t, linkB.Send(ctx, peerA, types.Envelope{
		Type:    types.MsgTypeUDPPunchCoordRequest,
		Payload: reqBytes,
	}))

	awaitConnects(t, linkB.Events(), peerC)
	awaitConnects(t, linkC.Events(), peerB)

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
