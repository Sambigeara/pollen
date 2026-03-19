package membership

import (
	"context"
	"crypto/ed25519"
	"time"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/types"
	"go.opentelemetry.io/otel/trace"

	"go.uber.org/zap"
)

type Option func(*Service)

func WithGossipInterval(d time.Duration) Option {
	return func(s *Service) { s.gossipInterval = d }
}

func WithGossipJitter(f float64) Option {
	return func(s *Service) { s.gossipJitter = f }
}

func WithPeerTickInterval(d time.Duration) Option {
	return func(s *Service) { s.peerTickInterval = d }
}

func WithAdvertisedIPs(ips []string) Option {
	return func(s *Service) { s.advertisedIPs = ips }
}

func WithShutdownSignal(ch chan<- struct{}) Option {
	return func(s *Service) { s.shutdownCh = ch }
}

func WithDatagramHandler(fn func(ctx context.Context, from types.PeerKey, env *meshv1.Envelope)) Option {
	return func(s *Service) { s.datagramHandler = fn }
}

func WithTracer(tp trace.TracerProvider) Option {
	return func(s *Service) { s.tracer = tp.Tracer("pollen/membership") }
}

func WithNATDetector(d *nat.Detector) Option {
	return func(s *Service) { s.natDetector = d }
}

func WithNodeMetrics(m *metrics.NodeMetrics) Option {
	return func(s *Service) { s.nodeMetrics = m }
}

func WithSigningKey(k ed25519.PrivateKey) Option {
	return func(s *Service) { s.signPriv = k }
}

func WithPollenDir(d string) Option {
	return func(s *Service) { s.pollenDir = d }
}

func WithLogger(l *zap.SugaredLogger) Option {
	return func(s *Service) { s.log = l }
}

func WithPort(p int) Option {
	return func(s *Service) { s.port = p }
}

func WithTLSIdentityTTL(d time.Duration) Option {
	return func(s *Service) { s.tlsIdentityTTL = d }
}

func WithMembershipTTL(d time.Duration) Option {
	return func(s *Service) { s.membershipTTL = d }
}

func WithReconnectWindow(d time.Duration) Option {
	return func(s *Service) { s.reconnectWindow = d }
}

func WithStreamOpener(so StreamOpener) Option {
	return func(s *Service) { s.streams = so }
}

func WithRTTSource(r RTTSource) Option {
	return func(s *Service) { s.rtt = r }
}

func WithCertManager(cm CertManager) Option {
	return func(s *Service) { s.certs = cm }
}

func WithPeerAddressSource(pa PeerAddressSource) Option {
	return func(s *Service) { s.peerAddrs = pa }
}

func WithPeerSessionCloser(pc PeerSessionCloser) Option {
	return func(s *Service) { s.sessionCloser = pc }
}

func WithSmoothedErr(e *metrics.EWMA) Option {
	return func(s *Service) { s.smoothedErr = e }
}
