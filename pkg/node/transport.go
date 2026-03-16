package node

import (
	"context"
	"crypto/tls"
	"io"
	"net"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/mesh"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/traces"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/traffic"
	"github.com/sambigeara/pollen/pkg/types"

	"github.com/quic-go/quic-go"
)

// Transport abstracts the mesh layer for the node. It contains only the
// methods the node actually calls at runtime. The interface is defined by
// the consumer (pkg/node), not the provider (pkg/mesh).
type Transport interface {
	// Lifecycle
	Start(ctx context.Context) error
	Close() error

	// Messaging
	Send(ctx context.Context, peer types.PeerKey, env *meshv1.Envelope) error
	Recv(ctx context.Context) (mesh.Packet, error)
	Events() <-chan peer.Input

	// Stream open/accept
	OpenStream(ctx context.Context, peer types.PeerKey) (io.ReadWriteCloser, error)
	AcceptAllStreams(ctx context.Context) (io.ReadWriteCloser, mesh.StreamType, types.PeerKey, error)
	OpenClockStream(ctx context.Context, peer types.PeerKey) (mesh.Stream, error)
	OpenArtifactStream(ctx context.Context, peer types.PeerKey) (io.ReadWriteCloser, error)
	OpenWorkloadStream(ctx context.Context, peer types.PeerKey) (io.ReadWriteCloser, error)

	// Connection management
	Connect(ctx context.Context, peer types.PeerKey, addrs []*net.UDPAddr) error
	Punch(ctx context.Context, peer types.PeerKey, addr *net.UDPAddr, localNAT nat.Type) error
	ClosePeerSession(peer types.PeerKey, reason mesh.CloseReason)
	BroadcastDisconnect() error
	ConnectedPeers() []types.PeerKey
	IsOutbound(peer types.PeerKey) bool
	GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)
	GetConn(peer types.PeerKey) (*quic.Conn, bool)
	ListenPort() int

	// Certificate management
	UpdateMeshCert(cert tls.Certificate)
	RequestCertRenewal(ctx context.Context, peer types.PeerKey) (*admissionv1.DelegationCert, error)
	PeerDelegationCert(peer types.PeerKey) (*admissionv1.DelegationCert, bool)

	// Configuration — called before Start
	SetRouter(r mesh.Router)
	SetTracer(t *traces.Tracer)
	SetTrafficTracker(t traffic.Recorder)
	SetPacketConn(conn net.PacketConn)
	SetDisableNATPunch(disable bool)

	// Auth
	JoinWithInvite(ctx context.Context, token *admissionv1.InviteToken) (*admissionv1.JoinToken, error)
	JoinWithToken(ctx context.Context, token *admissionv1.JoinToken) error
}

// Verify mesh.Mesh satisfies Transport at compile time.
// The current mesh.Mesh interface is a superset of Transport.
var _ Transport = mesh.Mesh(nil)
