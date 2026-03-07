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
}

// NewMeshMetrics registers all mesh instruments on c.
func NewMeshMetrics(c *Collector) *MeshMetrics {
	if c == nil {
		return &MeshMetrics{}
	}
	return &MeshMetrics{
		DatagramsSent:      c.Counter("pollen_mesh_datagrams_sent_total", Labels{}),
		DatagramsRecv:      c.Counter("pollen_mesh_datagrams_recv_total", Labels{}),
		DatagramBytesSent:  c.Counter("pollen_mesh_datagram_bytes_sent_total", Labels{}),
		DatagramBytesRecv:  c.Counter("pollen_mesh_datagram_bytes_recv_total", Labels{}),
		DatagramErrors:     c.Counter("pollen_mesh_datagram_errors_total", Labels{}),
		SessionConnects:    c.Counter("pollen_mesh_session_connects_total", Labels{}),
		SessionDisconnects: c.Counter("pollen_mesh_session_disconnects_total", Labels{}),
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
		Connections:      c.Counter("pollen_peer_connections_total", Labels{}),
		Disconnects:      c.Counter("pollen_peer_disconnects_total", Labels{}),
		PeersDiscovered:  c.Gauge("pollen_peers_discovered", Labels{}),
		PeersConnecting:  c.Gauge("pollen_peers_connecting", Labels{}),
		PeersConnected:   c.Gauge("pollen_peers_connected", Labels{}),
		PeersUnreachable: c.Gauge("pollen_peers_unreachable", Labels{}),
		StageEscalations: c.Counter("pollen_peer_stage_escalations_total", Labels{}),
	}
}

// GossipMetrics holds pre-registered instruments for the gossip/store layer.
type GossipMetrics struct {
	EventsReceived    *Counter
	EventsApplied     *Counter
	EventsStale       *Counter
	EventsRebroadcast *Counter
	SelfConflicts     *Counter
	Revocations       *Counter
}

// NewGossipMetrics registers all gossip instruments on c.
func NewGossipMetrics(c *Collector) *GossipMetrics {
	if c == nil {
		return &GossipMetrics{}
	}
	return &GossipMetrics{
		EventsReceived:    c.Counter("pollen_gossip_events_received_total", Labels{}),
		EventsApplied:     c.Counter("pollen_gossip_events_applied_total", Labels{}),
		EventsStale:       c.Counter("pollen_gossip_events_stale_total", Labels{}),
		EventsRebroadcast: c.Counter("pollen_gossip_events_rebroadcast_total", Labels{}),
		SelfConflicts:     c.Counter("pollen_gossip_self_conflicts_total", Labels{}),
		Revocations:       c.Counter("pollen_gossip_revocations_total", Labels{}),
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
		VivaldiError:       c.Gauge("pollen_topology_vivaldi_error", Labels{}),
		HMACNearestEnabled: c.Gauge("pollen_topology_hmac_nearest_enabled", Labels{}),
		TopologyPrunes:     c.Counter("pollen_topology_prunes_total", Labels{}),
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
		CertExpirySeconds:  c.Gauge("pollen_node_cert_expiry_seconds", Labels{}),
		CertRenewals:       c.Counter("pollen_node_cert_renewals_total", Labels{}),
		CertRenewalsFailed: c.Counter("pollen_node_cert_renewals_failed_total", Labels{}),
		PunchAttempts:      c.Counter("pollen_node_punch_attempts_total", Labels{}),
		PunchFailures:      c.Counter("pollen_node_punch_failures_total", Labels{}),
	}
}
