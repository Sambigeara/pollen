package supervisor

import (
	"context"
	"net"
	"net/netip"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"

	"github.com/quic-go/quic-go"
)

type Transport interface {
	Start(ctx context.Context) error
	Stop() error
	Send(ctx context.Context, peer types.PeerKey, data []byte) error
	Recv(ctx context.Context) (transport.Packet, error)
	PeerEvents() <-chan transport.PeerEvent
	OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (transport.Stream, error)
	AcceptStream(ctx context.Context) (transport.Stream, transport.StreamType, types.PeerKey, error)
	Connect(ctx context.Context, peer types.PeerKey, addrs []netip.AddrPort) error
	Disconnect(peer types.PeerKey) error
	ConnectedPeers() []types.PeerKey
	GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)
	PeerStateCounts() transport.PeerStateCounts
}

type TransportInternal interface {
	DiscoverPeer(pk types.PeerKey, ips []net.IP, port int, lastAddr *net.UDPAddr, privatelyRoutable, publiclyAccessible bool)
	ForgetPeer(pk types.PeerKey)
	RetryPeer(pk types.PeerKey)
	ConnectFailed(pk types.PeerKey)
	HasPeer(pk types.PeerKey) bool
	IsPeerConnected(pk types.PeerKey) bool
	IsPeerConnecting(pk types.PeerKey) bool
	SetPeerMetrics(pm *metrics.PeerMetrics)
	MarkPeerConnected(pk types.PeerKey, ip net.IP, port int)
	ClosePeerSession(peer types.PeerKey, reason string)
	Punch(ctx context.Context, peer types.PeerKey, addr *net.UDPAddr, localNAT nat.Type) error
	IsOutbound(peer types.PeerKey) bool
	ListenPort() int
	GetConn(peer types.PeerKey) (*quic.Conn, bool)
	JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error)
	SupervisorEvents() <-chan transport.PeerEvent
}
