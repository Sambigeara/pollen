package mesh_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net"
	"testing"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/admission"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

type meshHarness struct {
	mesh    mesh.Mesh
	invites admission.Store
	peerKey types.PeerKey
	port    int
	cancel  context.CancelFunc
}

func TestJoinWithInviteHappyPath(t *testing.T) {
	bootstrap := startMeshHarness(t, freeUDPPort(t))
	joiner := startMeshHarness(t, freeUDPPort(t))

	inviteToken := "invite-happy-path"
	bootstrap.invites.AddInvite(&admissionv1.Invite{Id: inviteToken})

	invite := &admissionv1.Invite{
		Id:          inviteToken,
		Addr:        []string{fmt.Sprintf("127.0.0.1:%d", bootstrap.port)},
		Fingerprint: bootstrap.peerKey.Bytes(),
	}

	joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, joiner.mesh.JoinWithInvite(joinCtx, invite))

	require.Eventually(t, func() bool {
		_, ok := joiner.mesh.GetConn(bootstrap.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

	require.Eventually(t, func() bool {
		_, ok := bootstrap.mesh.GetConn(joiner.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

	ok, err := bootstrap.invites.ConsumeToken(inviteToken)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestConnectHappyPath(t *testing.T) {
	bootstrap := startMeshHarness(t, freeUDPPort(t))
	joiner := startMeshHarness(t, freeUDPPort(t))

	connectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addrs := []*net.UDPAddr{{IP: net.IPv4(127, 0, 0, 1), Port: bootstrap.port}}
	require.NoError(t, joiner.mesh.Connect(connectCtx, bootstrap.peerKey, addrs))

	require.Eventually(t, func() bool {
		_, ok := joiner.mesh.GetConn(bootstrap.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)

	require.Eventually(t, func() bool {
		_, ok := bootstrap.mesh.GetConn(joiner.peerKey)
		return ok
	}, 5*time.Second, 25*time.Millisecond)
}

func startMeshHarness(t *testing.T, port int) *meshHarness {
	t.Helper()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	invites, err := admission.Load(t.TempDir())
	require.NoError(t, err)

	m, err := mesh.NewMesh(port, priv, invites)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, m.Start(ctx))

	h := &meshHarness{
		mesh:    m,
		invites: invites,
		peerKey: types.PeerKeyFromBytes(priv.Public().(ed25519.PublicKey)),
		port:    port,
		cancel:  cancel,
	}

	t.Cleanup(func() {
		h.cancel()
		_ = h.mesh.Close()
	})

	return h
}

func freeUDPPort(t *testing.T) int {
	t.Helper()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer conn.Close()

	return conn.LocalAddr().(*net.UDPAddr).Port
}
