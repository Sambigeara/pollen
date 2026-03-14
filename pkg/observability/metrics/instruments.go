package metrics

// MeshMetrics holds pre-registered instruments for the mesh layer.
type MeshMetrics struct {
	DatagramsSent      *Counter
	DatagramsRecv      *Counter
	DatagramBytesSent  *Counter
	DatagramBytesRecv  *Counter
	DatagramErrors     *Counter
	SessionConnects    *Counter
	SessionDisconnects *Counter
	SessionsActive     *Gauge
}

// NewMeshMetrics registers all mesh instruments on c.
func NewMeshMetrics(c *Collector) *MeshMetrics {
	if c == nil {
		return &MeshMetrics{}
	}
	return &MeshMetrics{
		DatagramsSent:      c.Counter("pollen_mesh_datagrams_sent_total"),
		DatagramsRecv:      c.Counter("pollen_mesh_datagrams_recv_total"),
		DatagramBytesSent:  c.Counter("pollen_mesh_datagram_bytes_sent_total"),
		DatagramBytesRecv:  c.Counter("pollen_mesh_datagram_bytes_recv_total"),
		DatagramErrors:     c.Counter("pollen_mesh_datagram_errors_total"),
		SessionConnects:    c.Counter("pollen_mesh_session_connects_total"),
		SessionDisconnects: c.Counter("pollen_mesh_session_disconnects_total"),
		SessionsActive:     c.Gauge("pollen_mesh_sessions_active"),
	}
}

// PeerMetrics holds pre-registered instruments for the peer state machine.
type PeerMetrics struct {
	Connections      *Counter
	Disconnects      *Counter
	PeersDiscovered  *Gauge
	PeersConnecting  *Gauge
	PeersConnected   *Gauge
	PeersUnreachable *Gauge
	StageEscalations *Counter
	StateTransitions *Counter
}

// Enabled reports whether any gauge is wired (non-nil), so callers can skip
// iterating peers when metrics collection is disabled.
func (m *PeerMetrics) Enabled() bool {
	return m.PeersDiscovered != nil
}

// NewPeerMetrics registers all peer instruments on c.
func NewPeerMetrics(c *Collector) *PeerMetrics {
	if c == nil {
		return &PeerMetrics{}
	}
	return &PeerMetrics{
		Connections:      c.Counter("pollen_peer_connections_total"),
		Disconnects:      c.Counter("pollen_peer_disconnects_total"),
		PeersDiscovered:  c.Gauge("pollen_peers_discovered"),
		PeersConnecting:  c.Gauge("pollen_peers_connecting"),
		PeersConnected:   c.Gauge("pollen_peers_connected"),
		PeersUnreachable: c.Gauge("pollen_peers_unreachable"),
		StageEscalations: c.Counter("pollen_peer_stage_escalations_total"),
		StateTransitions: c.Counter("pollen_peer_state_transitions_total"),
	}
}

const staleRatioAlpha = 0.01 // ~100-event EWMA window

// GossipMetrics holds pre-registered instruments for the gossip/store layer.
type GossipMetrics struct {
	EventsReceived *Counter
	EventsApplied  *Counter
	EventsStale    *Counter
	SelfConflicts  *Counter
	Revocations    *Counter
	BatchSize      *Gauge
	StaleRatio     *EWMA
}

// NewGossipMetrics registers all gossip instruments on c.
func NewGossipMetrics(c *Collector) *GossipMetrics {
	if c == nil {
		return &GossipMetrics{StaleRatio: NewEWMA(staleRatioAlpha)}
	}
	return &GossipMetrics{
		EventsReceived: c.Counter("pollen_gossip_events_received_total"),
		EventsApplied:  c.Counter("pollen_gossip_events_applied_total"),
		EventsStale:    c.Counter("pollen_gossip_events_stale_total"),
		SelfConflicts:  c.Counter("pollen_gossip_self_conflicts_total"),
		Revocations:    c.Counter("pollen_gossip_revocations_total"),
		BatchSize:      c.Gauge("pollen_gossip_batch_size"),
		StaleRatio:     NewEWMA(staleRatioAlpha),
	}
}

// TopologyMetrics holds pre-registered instruments for topology selection.
type TopologyMetrics struct {
	VivaldiError       *Gauge
	HMACNearestEnabled *Gauge
	TopologyPrunes     *Counter
}

// NewTopologyMetrics registers all topology instruments on c.
func NewTopologyMetrics(c *Collector) *TopologyMetrics {
	if c == nil {
		return &TopologyMetrics{}
	}
	return &TopologyMetrics{
		VivaldiError:       c.Gauge("pollen_topology_vivaldi_error"),
		HMACNearestEnabled: c.Gauge("pollen_topology_hmac_nearest_enabled"),
		TopologyPrunes:     c.Counter("pollen_topology_prunes_total"),
	}
}

// NodeMetrics holds pre-registered instruments for the node orchestrator.
type NodeMetrics struct {
	CertExpirySeconds  *Gauge
	CertRenewals       *Counter
	CertRenewalsFailed *Counter
	PunchAttempts      *Counter
	PunchFailures      *Counter
}

// NewNodeMetrics registers all node instruments on c.
func NewNodeMetrics(c *Collector) *NodeMetrics {
	if c == nil {
		return &NodeMetrics{}
	}
	return &NodeMetrics{
		CertExpirySeconds:  c.Gauge("pollen_node_cert_expiry_seconds"),
		CertRenewals:       c.Counter("pollen_node_cert_renewals_total"),
		CertRenewalsFailed: c.Counter("pollen_node_cert_renewals_failed_total"),
		PunchAttempts:      c.Counter("pollen_node_punch_attempts_total"),
		PunchFailures:      c.Counter("pollen_node_punch_failures_total"),
	}
}
