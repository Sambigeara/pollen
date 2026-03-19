package supervisor

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/sambigeara/pollen/pkg/control"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
)

type streamOpenAdapter struct{ t Transport }

func (a *streamOpenAdapter) OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error) {
	return a.t.OpenStream(ctx, peer, st)
}

type trafficCountedStream struct {
	inner    io.ReadWriteCloser
	recorder transport.TrafficRecorder
	peer     types.PeerKey
}

func (s *trafficCountedStream) Read(p []byte) (int, error) {
	n, err := s.inner.Read(p)
	if n > 0 {
		s.recorder.Record(s.peer, uint64(n), 0)
	}
	return n, err
}

func (s *trafficCountedStream) Write(p []byte) (int, error) {
	n, err := s.inner.Write(p)
	if n > 0 {
		s.recorder.Record(s.peer, 0, uint64(n))
	}
	return n, err
}

func (s *trafficCountedStream) Close() error { return s.inner.Close() }

func wrapTrafficStream(stream io.ReadWriteCloser, recorder transport.TrafficRecorder, peer types.PeerKey) io.ReadWriteCloser {
	return &trafficCountedStream{inner: stream, recorder: recorder, peer: peer}
}

type trafficCountedOpener struct {
	inner    placement.StreamOpener
	recorder transport.TrafficRecorder
}

func (a *trafficCountedOpener) OpenStream(ctx context.Context, peer types.PeerKey, st transport.StreamType) (io.ReadWriteCloser, error) {
	stream, err := a.inner.OpenStream(ctx, peer, st)
	if err != nil {
		return nil, err
	}
	return wrapTrafficStream(stream, a.recorder, peer), nil
}

type supervisorTransportInfo struct{ n *Supervisor }

func (t *supervisorTransportInfo) PeerStateCounts() transport.PeerStateCounts {
	return t.n.mesh.PeerStateCounts()
}

func (t *supervisorTransportInfo) GetActivePeerAddress(pk types.PeerKey) (*net.UDPAddr, bool) {
	return t.n.mesh.GetActivePeerAddress(pk)
}

func (t *supervisorTransportInfo) PeerRTT(pk types.PeerKey) (time.Duration, bool) {
	conn, ok := t.n.meshInternal.GetConn(pk)
	if !ok {
		return 0, false
	}
	rtt := conn.ConnectionStats().SmoothedRTT
	if rtt <= 0 {
		return 0, false
	}
	return rtt, true
}

func (t *supervisorTransportInfo) ReconnectWindowDuration() time.Duration {
	return t.n.conf.ReconnectWindow
}

type supervisorMetricsSource struct {
	providers  *metrics.Providers
	membership membership.MembershipAPI
}

func (m *supervisorMetricsSource) ControlMetrics() control.Metrics {
	snap := m.providers.CollectSnapshot(context.Background())
	cm := m.membership.ControlMetrics()
	return control.Metrics{
		CertExpirySeconds:  snap.Float64s["pollen.node.cert.expiry.seconds"],
		CertRenewals:       uint64(snap.Int64s["pollen.node.cert.renewals"]),        //nolint:gosec
		CertRenewalsFailed: uint64(snap.Int64s["pollen.node.cert.renewals.failed"]), //nolint:gosec
		PunchAttempts:      uint64(snap.Int64s["pollen.node.punch.attempts"]),       //nolint:gosec
		PunchFailures:      uint64(snap.Int64s["pollen.node.punch.failures"]),       //nolint:gosec
		GossipApplied:      uint64(snap.Int64s["pollen.gossip.events.applied"]),     //nolint:gosec
		GossipStale:        uint64(snap.Int64s["pollen.gossip.events.stale"]),       //nolint:gosec
		SmoothedVivaldiErr: cm.SmoothedErr,
		VivaldiSamples:     uint64(cm.VivaldiSamples),    //nolint:gosec
		EagerSyncs:         uint64(cm.EagerSyncs),        //nolint:gosec
		EagerSyncFailures:  uint64(cm.EagerSyncFailures), //nolint:gosec
	}
}
