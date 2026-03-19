package metrics

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

// MeshMetrics instruments the transport layer.
type MeshMetrics struct {
	DatagramsSent      metric.Int64Counter
	DatagramsRecv      metric.Int64Counter
	DatagramBytesSent  metric.Int64Counter
	DatagramBytesRecv  metric.Int64Counter
	DatagramErrors     metric.Int64Counter
	SessionConnects    metric.Int64Counter
	SessionDisconnects metric.Int64Counter
	SessionsActive     metric.Float64Gauge
}

func NewMeshMetrics(mp metric.MeterProvider) *MeshMetrics {
	m := mp.Meter("pollen/mesh")
	mm := &MeshMetrics{}
	mm.DatagramsSent, _ = m.Int64Counter("pollen.mesh.datagrams.sent")
	mm.DatagramsRecv, _ = m.Int64Counter("pollen.mesh.datagrams.recv")
	mm.DatagramBytesSent, _ = m.Int64Counter("pollen.mesh.datagram.bytes.sent")
	mm.DatagramBytesRecv, _ = m.Int64Counter("pollen.mesh.datagram.bytes.recv")
	mm.DatagramErrors, _ = m.Int64Counter("pollen.mesh.datagram.errors")
	mm.SessionConnects, _ = m.Int64Counter("pollen.mesh.session.connects")
	mm.SessionDisconnects, _ = m.Int64Counter("pollen.mesh.session.disconnects")
	mm.SessionsActive, _ = m.Float64Gauge("pollen.mesh.sessions.active")
	return mm
}

// PeerMetrics instruments the peer state machine.
type PeerMetrics struct {
	Connections      metric.Int64Counter
	Disconnects      metric.Int64Counter
	StageEscalations metric.Int64Counter
	StateTransitions metric.Int64Counter
	PeersDiscovered  metric.Float64Gauge
	PeersConnecting  metric.Float64Gauge
	PeersConnected   metric.Float64Gauge
	PeersUnreachable metric.Float64Gauge
}

func NewPeerMetrics(mp metric.MeterProvider) *PeerMetrics {
	m := mp.Meter("pollen/peer")
	pm := &PeerMetrics{}
	pm.Connections, _ = m.Int64Counter("pollen.peer.connections")
	pm.Disconnects, _ = m.Int64Counter("pollen.peer.disconnects")
	pm.StageEscalations, _ = m.Int64Counter("pollen.peer.stage.escalations")
	pm.StateTransitions, _ = m.Int64Counter("pollen.peer.state.transitions")
	pm.PeersDiscovered, _ = m.Float64Gauge("pollen.peer.discovered")
	pm.PeersConnecting, _ = m.Float64Gauge("pollen.peer.connecting")
	pm.PeersConnected, _ = m.Float64Gauge("pollen.peer.connected")
	pm.PeersUnreachable, _ = m.Float64Gauge("pollen.peer.unreachable")
	return pm
}

// Enabled returns true if metrics are actively being collected (not noop).
func (pm *PeerMetrics) Enabled(ctx context.Context) bool {
	return pm.PeersConnected.Enabled(ctx)
}

// GossipMetrics instruments the gossip/state layer.
type GossipMetrics struct {
	EventsReceived metric.Int64Counter
	EventsApplied  metric.Int64Counter
	EventsStale    metric.Int64Counter
	SelfConflicts  metric.Int64Counter
	Revocations    metric.Int64Counter
	BatchSize      metric.Float64Gauge
	StaleRatio     *EWMA
}

func NewGossipMetrics(mp metric.MeterProvider) *GossipMetrics {
	m := mp.Meter("pollen/gossip")
	gm := &GossipMetrics{
		StaleRatio: NewEWMA(0.01), //nolint:mnd
	}
	gm.EventsReceived, _ = m.Int64Counter("pollen.gossip.events.received")
	gm.EventsApplied, _ = m.Int64Counter("pollen.gossip.events.applied")
	gm.EventsStale, _ = m.Int64Counter("pollen.gossip.events.stale")
	gm.SelfConflicts, _ = m.Int64Counter("pollen.gossip.self.conflicts")
	gm.Revocations, _ = m.Int64Counter("pollen.gossip.revocations")
	gm.BatchSize, _ = m.Float64Gauge("pollen.gossip.batch.size")

	_, _ = m.Float64ObservableGauge("pollen.gossip.stale.ratio",
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(gm.StaleRatio.Value())
			return nil
		}))
	return gm
}

// TopologyMetrics instruments topology selection.
type TopologyMetrics struct {
	VivaldiError       metric.Float64Gauge
	HMACNearestEnabled metric.Float64Gauge
	TopologyPrunes     metric.Int64Counter
}

func NewTopologyMetrics(mp metric.MeterProvider) *TopologyMetrics {
	m := mp.Meter("pollen/topology")
	tm := &TopologyMetrics{}
	tm.VivaldiError, _ = m.Float64Gauge("pollen.topology.vivaldi.error")
	tm.HMACNearestEnabled, _ = m.Float64Gauge("pollen.topology.hmac.nearest.enabled")
	tm.TopologyPrunes, _ = m.Int64Counter("pollen.topology.prunes")
	return tm
}

// NodeMetrics instruments node-level operations.
type NodeMetrics struct {
	CertExpirySeconds  metric.Float64Gauge
	CertRenewals       metric.Int64Counter
	CertRenewalsFailed metric.Int64Counter
	PunchAttempts      metric.Int64Counter
	PunchFailures      metric.Int64Counter
}

func NewNodeMetrics(mp metric.MeterProvider) *NodeMetrics {
	m := mp.Meter("pollen/node")
	nm := &NodeMetrics{}
	nm.CertExpirySeconds, _ = m.Float64Gauge("pollen.node.cert.expiry.seconds")
	nm.CertRenewals, _ = m.Int64Counter("pollen.node.cert.renewals")
	nm.CertRenewalsFailed, _ = m.Int64Counter("pollen.node.cert.renewals.failed")
	nm.PunchAttempts, _ = m.Int64Counter("pollen.node.punch.attempts")
	nm.PunchFailures, _ = m.Int64Counter("pollen.node.punch.failures")
	return nm
}
