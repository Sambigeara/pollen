package membership

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/netip"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"

	"github.com/quic-go/quic-go"
)

var ErrCertExpired = errors.New("delegation certificate has expired")

const maxDatagramPayload = 1100

const eventBufSize = 64

var envelopeOverhead = (&meshv1.Envelope{Body: &meshv1.Envelope_Events{
	Events: &statev1.GossipEventBatch{},
}}).SizeVT()

const (
	certCheckInterval     = 5 * time.Minute
	CertWarnThreshold     = 1 * time.Hour
	CertCriticalThreshold = 15 * time.Minute
	certRenewalTimeout    = 10 * time.Second

	gossipStreamTimeout = 5 * time.Second

	maxResponseSize = 4 << 20 // 4 MB

	vivaldiWarmupDuration = 5 * time.Second

	VivaldiErrAlpha = 0.2

	eagerSyncCooldown = 5 * time.Second
	eagerSyncTimeout  = 5 * time.Second

	expirySweepInterval = 30 * time.Second
	ipRefreshInterval   = 5 * time.Minute
)

type MembershipAPI interface {
	Start(ctx context.Context) error
	Stop() error

	DenyPeer(key types.PeerKey) error
	Invite(subject string) (string, error)

	HandleDigestStream(ctx context.Context, stream transport.Stream, peer types.PeerKey)
	HandleCertRenewalStream(stream transport.Stream, peer types.PeerKey)

	Events() <-chan state.Event
	ControlMetrics() ControlMetrics
}

var _ MembershipAPI = (*Service)(nil)

type ClusterState interface {
	Snapshot() state.Snapshot
	ApplyDelta(from types.PeerKey, data []byte) ([]state.Event, []byte, error)
	EncodeDelta(since state.Digest) []byte
	EncodeFull() []byte
	PendingNotify() <-chan struct{}
	FlushPendingGossip() []*statev1.GossipEvent
	DenyPeer(key types.PeerKey) []state.Event
	SetLocalAddresses([]netip.AddrPort) []state.Event
	SetLocalCoord(coords.Coord, float64) []state.Event
	SetLocalNAT(nat.Type) []state.Event
	SetLocalReachable([]types.PeerKey) []state.Event
	SetLocalObservedAddress(string, uint32) []state.Event
}

type Network interface {
	Connect(ctx context.Context, key types.PeerKey, addrs []netip.AddrPort) error
	Disconnect(key types.PeerKey) error
	Send(ctx context.Context, peer types.PeerKey, data []byte) error
	Recv(ctx context.Context) (transport.Packet, error)
	ConnectedPeers() []types.PeerKey
	PeerEvents() <-chan transport.PeerEvent
}

type StreamOpener interface {
	OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (transport.Stream, error)
}

type RTTSource interface {
	GetConn(peer types.PeerKey) (*quic.Conn, bool)
}

type CertManager interface {
	UpdateMeshCert(cert tls.Certificate)
	RequestCertRenewal(ctx context.Context, peer types.PeerKey) (*admissionv1.DelegationCert, error)
	PeerDelegationCert(peer types.PeerKey) (*admissionv1.DelegationCert, bool)
}

type PeerAddressSource interface {
	GetActivePeerAddress(peer types.PeerKey) (*net.UDPAddr, bool)
}

type PeerSessionCloser interface {
	ClosePeerSession(peer types.PeerKey, reason string)
}

type ControlMetrics struct {
	LocalCoord        coords.Coord
	SmoothedErr       float64
	VivaldiSamples    int64
	EagerSyncs        int64
	EagerSyncFailures int64
}
