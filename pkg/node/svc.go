package node

import (
	"cmp"
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/peer"
	"github.com/sambigeara/pollen/pkg/store"
	"github.com/sambigeara/pollen/pkg/tunnel"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/sambigeara/pollen/pkg/workload"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// controlMetrics aggregates observability counters the control service exposes
// via GetMetrics, decoupling svc.go from internal metric instruments.
type controlMetrics struct {
	CertExpirySeconds  float64
	CertRenewals       uint64
	CertRenewalsFailed uint64
	PunchAttempts      uint64
	PunchFailures      uint64
	GossipApplied      uint64
	GossipStale        uint64
	SmoothedVivaldiErr float64
	VivaldiSamples     uint64
	EagerSyncs         uint64
	EagerSyncFailures  uint64
}

// nodeController is the narrow interface the gRPC control service uses to
// interact with the node. *Node satisfies it.
type nodeController interface {
	// State reads
	Snapshot() store.Snapshot
	ListConnections() []tunnel.ConnectionInfo
	PeerStateCounts() peer.PeerStateCounts
	GetActivePeerAddress(types.PeerKey) (*net.UDPAddr, bool)
	PeerRTT(types.PeerKey) (time.Duration, bool)
	ReconnectWindowDuration() time.Duration

	// Service / peer mutations
	UpsertService(port uint32, name string)
	RemoveService(name string, port uint32)
	ConnectService(peerID types.PeerKey, remotePort, localPort uint32) (uint32, error)
	DisconnectService(localPort uint32) error
	DenyPeer(pk types.PeerKey)
	MeshConnect(ctx context.Context, peer types.PeerKey, addrs []*net.UDPAddr) error

	// Workload lifecycle
	HasWorkloads() bool
	WorkloadList() []workload.Summary
	WorkloadSeed(wasmBytes []byte, cfg wasm.PluginConfig) (string, error)
	WorkloadUnseed(hash string) error
	WorkloadIsRunning(hash string) bool
	RouteCall(ctx context.Context, hash, function string, input []byte) ([]byte, error)

	// Workload store
	ResolveWorkloadPrefix(prefix string) (hash string, ambiguous, found bool)
	SetWorkloadSpec(hash string, replicas, memoryPages, timeoutMs uint32) error
	SetLocalWorkloadClaim(hash string, claimed bool)
	RemoveLocalWorkloadSpec(hash string)

	// Observability
	ControlMetrics() controlMetrics
	Log() *zap.SugaredLogger
}

var (
	_ controlv1.ControlServiceServer = (*NodeService)(nil)
	_ nodeController                 = (*Node)(nil)
)

type NodeService struct {
	controlv1.UnimplementedControlServiceServer
	node     nodeController
	shutdown func()
	creds    *auth.NodeCredentials
}

func NewNodeService(n nodeController, shutdown func(), creds *auth.NodeCredentials) *NodeService {
	return &NodeService{node: n, shutdown: shutdown, creds: creds}
}

func (s *NodeService) Shutdown(_ context.Context, _ *controlv1.ShutdownRequest) (*controlv1.ShutdownResponse, error) {
	if s.shutdown == nil {
		return &controlv1.ShutdownResponse{}, status.Error(codes.FailedPrecondition, "shutdown callback not configured")
	}

	go s.shutdown()

	return &controlv1.ShutdownResponse{}, nil
}

func (s *NodeService) GetBootstrapInfo(_ context.Context, _ *controlv1.GetBootstrapInfoRequest) (*controlv1.GetBootstrapInfoResponse, error) {
	snap := s.node.Snapshot()
	local := snap.LocalID
	rec, ok := snap.Nodes[local]
	if !ok {
		return &controlv1.GetBootstrapInfoResponse{}, nil
	}

	resp := &controlv1.GetBootstrapInfoResponse{
		Self: nodeBootstrapInfo(local, rec.IPs, rec.LocalPort),
	}

	resp.Recommended = s.pickRecommendedPeer(snap)

	return resp, nil
}

func (s *NodeService) pickRecommendedPeer(snap store.Snapshot) *controlv1.BootstrapPeerInfo {
	var candidates []types.PeerKey
	for peerID, nv := range snap.Nodes {
		if peerID == snap.LocalID || !nv.PubliclyAccessible {
			continue
		}
		if len(nv.IPs) == 0 || nv.LocalPort == 0 {
			continue
		}
		candidates = append(candidates, peerID)
	}

	if len(candidates) > 0 {
		slices.SortFunc(candidates, types.PeerKey.Compare)
		best := candidates[0]
		nv := snap.Nodes[best]
		return nodeBootstrapInfo(best, nv.IPs, nv.LocalPort)
	}

	localNV, ok := snap.Nodes[snap.LocalID]
	if !ok || len(localNV.IPs) == 0 || localNV.LocalPort == 0 {
		return nil
	}
	return nodeBootstrapInfo(snap.LocalID, localNV.IPs, localNV.LocalPort)
}

func nodeBootstrapInfo(peerID types.PeerKey, ips []string, port uint32) *controlv1.BootstrapPeerInfo {
	addrs := make([]string, 0, len(ips))
	for _, ip := range ips {
		addrs = append(addrs, net.JoinHostPort(ip, strconv.Itoa(int(port))))
	}
	return &controlv1.BootstrapPeerInfo{
		Peer:  &controlv1.NodeRef{PeerId: peerID.Bytes()},
		Addrs: addrs,
	}
}

func (s *NodeService) GetStatus(_ context.Context, _ *controlv1.GetStatusRequest) (*controlv1.GetStatusResponse, error) {
	snap := s.node.Snapshot()
	nodes := snap.Nodes
	localID := snap.LocalID

	selfAddr := ""
	selfPubliclyAccessible := false
	if rec, ok := nodes[localID]; ok {
		selfPubliclyAccessible = rec.PubliclyAccessible
		if len(rec.IPs) > 0 {
			port := rec.LocalPort
			ip := net.ParseIP(rec.IPs[0])
			if ip != nil && rec.ExternalPort != 0 && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
				port = rec.ExternalPort
			}
			selfAddr = net.JoinHostPort(rec.IPs[0], strconv.Itoa(int(port)))
		}
	}

	// Report degraded when cert is expired but within the reconnect window.
	degraded := false
	if s.creds != nil && s.creds.Cert != nil {
		now := time.Now()
		if auth.IsCertExpired(s.creds.Cert, now) &&
			auth.IsCertWithinReconnectWindow(s.creds.Cert, now, s.node.ReconnectWindowDuration()) {
			degraded = true
		}
	}

	out := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{
			Node:               &controlv1.NodeRef{PeerId: localID.Bytes()},
			Status:             controlv1.NodeStatus_NODE_STATUS_ONLINE,
			Addr:               selfAddr,
			PubliclyAccessible: selfPubliclyAccessible,
		},
		Services:    []*controlv1.ServiceSummary{},
		Nodes:       make([]*controlv1.NodeSummary, 0, len(nodes)),
		Connections: []*controlv1.ConnectionSummary{},
		Degraded:    degraded,
	}

	if s.creds != nil && s.creds.Cert != nil {
		claims := s.creds.Cert.GetClaims()
		caps := claims.GetCapabilities()
		health := controlv1.CertHealth_CERT_HEALTH_OK
		remaining := time.Until(auth.CertExpiresAt(s.creds.Cert))
		switch {
		case remaining <= 0:
			health = controlv1.CertHealth_CERT_HEALTH_EXPIRED
		case remaining <= certCriticalThreshold:
			health = controlv1.CertHealth_CERT_HEALTH_EXPIRING_SOON
		case remaining <= certWarnThreshold:
			health = controlv1.CertHealth_CERT_HEALTH_RENEWING
		}
		out.Certificates = append(out.Certificates, &controlv1.CertInfo{
			NotBeforeUnix:      claims.GetNotBeforeUnix(),
			NotAfterUnix:       claims.GetNotAfterUnix(),
			Serial:             claims.GetSerial(),
			Health:             health,
			CanDelegate:        caps.GetCanDelegate(),
			CanAdmit:           caps.GetCanAdmit(),
			MaxDepth:           caps.GetMaxDepth(),
			AccessDeadlineUnix: claims.GetAccessDeadlineUnix(),
		})
	}

	connections := s.node.ListConnections()
	tunnelCounts := make(map[types.PeerKey]uint32, len(connections))
	for _, c := range connections {
		tunnelCounts[c.PeerID]++
	}

	if rec, ok := nodes[localID]; ok {
		out.Self.CpuPercent = rec.CPUPercent
		out.Self.MemPercent = rec.MemPercent
		out.Self.NumCpu = rec.NumCPU
	}
	out.Self.TunnelCount = uint32(len(connections))

	// Fetch mesh-wide traffic heatmaps for aggregate traffic display.
	// Each node's published heatmap is summed across all peers to give
	// a total bytes-in / bytes-out for that node.
	allTraffic := snap.Heatmaps
	for _, ts := range allTraffic[localID] {
		out.Self.TrafficBytesIn += ts.BytesIn
		out.Self.TrafficBytesOut += ts.BytesOut
	}

	// Determine which peers have direct QUIC sessions (ONLINE).
	// Store the address so we don't call GetActivePeerAddress again per peer
	// (avoids a TOCTOU race where a session appears/disappears between calls).
	directPeers := make(map[types.PeerKey]*net.UDPAddr)
	for key := range nodes {
		if key == localID {
			continue
		}
		if addr, ok := s.node.GetActivePeerAddress(key); ok {
			directPeers[key] = addr
		}
	}

	// BFS from directly-connected peers through gossip Reachable edges to
	// find the indirectly-reachable component. Only trust edges from nodes
	// that are themselves proven reachable (direct or already discovered
	// via traversal), so stale edges from partitioned nodes are ignored.
	indirectPeers := make(map[types.PeerKey]struct{})
	queue := make([]types.PeerKey, 0, len(directPeers))
	for p := range directPeers {
		queue = append(queue, p)
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		rec, ok := nodes[cur]
		if !ok {
			continue
		}
		for peer := range rec.Reachable {
			if peer == localID {
				continue
			}
			if _, ok := directPeers[peer]; ok {
				continue
			}
			if _, ok := indirectPeers[peer]; ok {
				continue
			}
			indirectPeers[peer] = struct{}{}
			queue = append(queue, peer)
		}
	}

	log := s.node.Log()
	for key, node := range nodes {
		if key == localID {
			continue
		}

		addr, isDirect := directPeers[key]
		peerStatus := controlv1.NodeStatus_NODE_STATUS_OFFLINE
		if isDirect {
			peerStatus = controlv1.NodeStatus_NODE_STATUS_ONLINE
		} else if _, ok := indirectPeers[key]; ok {
			peerStatus = controlv1.NodeStatus_NODE_STATUS_INDIRECT
		}

		addrStr := ""
		if addr != nil {
			addrStr = addr.String()
		}
		if addrStr == "" && len(node.IPs) > 0 {
			ip := net.ParseIP(node.IPs[0])
			if ip == nil {
				log.Warnw("skipping unparseable peer IP", "peer", key.Short(), "ip", node.IPs[0])
			} else {
				port := int(node.LocalPort)
				if node.ExternalPort != 0 && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
					port = int(node.ExternalPort)
				}
				addrStr = (&net.UDPAddr{
					IP:   ip,
					Port: port,
				}).String()
			}
		}

		ns := &controlv1.NodeSummary{
			Node:               &controlv1.NodeRef{PeerId: key.Bytes()},
			Status:             peerStatus,
			Addr:               addrStr,
			PubliclyAccessible: node.PubliclyAccessible,
			TunnelCount:        tunnelCounts[key],
			CpuPercent:         node.CPUPercent,
			MemPercent:         node.MemPercent,
			NumCpu:             node.NumCPU,
		}
		for _, ts := range allTraffic[key] {
			ns.TrafficBytesIn += ts.BytesIn
			ns.TrafficBytesOut += ts.BytesOut
		}

		if isDirect {
			if rtt, ok := s.node.PeerRTT(key); ok {
				ns.LatencyMs = float64(rtt.Microseconds()) / 1000 //nolint:mnd
			}
		}

		out.Nodes = append(out.Nodes, ns)
	}

	for key, node := range nodes {
		for _, svc := range node.Services {
			name := svc.GetName()
			if name == "" {
				name = strconv.FormatUint(uint64(svc.GetPort()), 10)
			}
			out.Services = append(out.Services, &controlv1.ServiceSummary{
				Name:     name,
				Provider: &controlv1.NodeRef{PeerId: key.Bytes()},
				Port:     svc.GetPort(),
			})
		}
	}

	for _, c := range connections {
		name := ""
		if node, ok := nodes[c.PeerID]; ok {
			for _, svc := range node.Services {
				if svc.GetPort() == c.RemotePort {
					name = svc.GetName()
					break
				}
			}
		}
		out.Connections = append(out.Connections, &controlv1.ConnectionSummary{
			Peer:        &controlv1.NodeRef{PeerId: c.PeerID.Bytes()},
			RemotePort:  c.RemotePort,
			LocalPort:   c.LocalPort,
			ServiceName: name,
		})
	}

	slices.SortFunc(out.Nodes, func(a, b *controlv1.NodeSummary) int {
		if ra, rb := nodeStatusRank(a.Status), nodeStatusRank(b.Status); ra != rb {
			return ra - rb
		}
		return types.PeerKeyFromBytes(a.Node.PeerId).Compare(types.PeerKeyFromBytes(b.Node.PeerId))
	})

	slices.SortFunc(out.Services, func(a, b *controlv1.ServiceSummary) int {
		if a.Name != b.Name {
			return cmp.Compare(a.Name, b.Name)
		}
		if a.Port != b.Port {
			return cmp.Compare(a.Port, b.Port)
		}
		return types.PeerKeyFromBytes(a.Provider.PeerId).Compare(types.PeerKeyFromBytes(b.Provider.PeerId))
	})

	slices.SortFunc(out.Connections, func(a, b *controlv1.ConnectionSummary) int {
		if a.LocalPort != b.LocalPort {
			return cmp.Compare(a.LocalPort, b.LocalPort)
		}
		return types.PeerKeyFromBytes(a.Peer.PeerId).Compare(types.PeerKeyFromBytes(b.Peer.PeerId))
	})

	// Merge local workload state with cluster-wide spec/claim data.
	specs := snap.Specs
	claims := snap.Claims

	seen := make(map[string]struct{})
	for _, w := range s.node.WorkloadList() {
		seen[w.Hash] = struct{}{}
		ws := &controlv1.WorkloadSummary{
			Hash:          w.Hash,
			Status:        workloadStatusProto(w.Status),
			StartedAtUnix: w.CompiledAt.Unix(),
			Local:         true,
		}
		if sv, ok := specs[w.Hash]; ok {
			ws.DesiredReplicas = sv.Spec.GetReplicas()
		}
		ws.ActiveReplicas = uint32(len(claims[w.Hash]))
		out.Workloads = append(out.Workloads, ws)
	}
	// Add remote-only workloads (specs we know about but aren't running locally).
	for hash, sv := range specs {
		if _, ok := seen[hash]; ok {
			continue
		}
		out.Workloads = append(out.Workloads, &controlv1.WorkloadSummary{
			Hash:            hash,
			DesiredReplicas: sv.Spec.GetReplicas(),
			ActiveReplicas:  uint32(len(claims[hash])),
		})
	}

	return out, nil
}

func (s *NodeService) RegisterService(_ context.Context, req *controlv1.RegisterServiceRequest) (*controlv1.RegisterServiceResponse, error) {
	name := req.GetName()
	if name == "" {
		name = strconv.FormatUint(uint64(req.Port), 10)
	}
	s.node.UpsertService(req.Port, name)
	return &controlv1.RegisterServiceResponse{}, nil
}

func (s *NodeService) UnregisterService(_ context.Context, req *controlv1.UnregisterServiceRequest) (*controlv1.UnregisterServiceResponse, error) {
	s.node.RemoveService(req.GetName(), req.GetPort())
	return &controlv1.UnregisterServiceResponse{}, nil
}

func (s *NodeService) ConnectPeer(ctx context.Context, req *controlv1.ConnectPeerRequest) (*controlv1.ConnectPeerResponse, error) {
	peerKey := types.PeerKeyFromBytes(req.PeerId)
	addrs := make([]*net.UDPAddr, 0, len(req.Addrs))
	for _, a := range req.Addrs {
		addr, err := net.ResolveUDPAddr("udp", a)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid address %q", a)
		}
		addrs = append(addrs, addr)
	}
	if err := s.node.MeshConnect(ctx, peerKey, addrs); err != nil {
		s.node.Log().Warnw("connect peer failed", "peer", peerKey.Short(), "err", err)
		return nil, status.Error(codes.Internal, "failed to connect to peer")
	}
	return &controlv1.ConnectPeerResponse{}, nil
}

func (s *NodeService) ConnectService(_ context.Context, req *controlv1.ConnectServiceRequest) (*controlv1.ConnectServiceResponse, error) {
	localPort, err := s.node.ConnectService(types.PeerKeyFromBytes(req.Node.PeerId), req.RemotePort, req.LocalPort)
	if err != nil {
		s.node.Log().Warnw("connect service failed", "err", err)
		return nil, status.Error(codes.Internal, "failed to connect service")
	}

	return &controlv1.ConnectServiceResponse{LocalPort: localPort}, nil
}

func (s *NodeService) DisconnectService(_ context.Context, req *controlv1.DisconnectServiceRequest) (*controlv1.DisconnectServiceResponse, error) {
	if err := s.node.DisconnectService(req.GetLocalPort()); err != nil {
		s.node.Log().Warnw("disconnect service failed", "err", err)
		return nil, status.Error(codes.Internal, "failed to disconnect service")
	}
	return &controlv1.DisconnectServiceResponse{}, nil
}

func (s *NodeService) DenyPeer(_ context.Context, req *controlv1.DenyPeerRequest) (*controlv1.DenyPeerResponse, error) {
	if s.creds.DelegationKey == nil {
		return nil, status.Error(codes.FailedPrecondition, "delegation key not available; this node cannot deny peers")
	}
	s.node.DenyPeer(types.PeerKeyFromBytes(req.GetPeerId()))
	return &controlv1.DenyPeerResponse{}, nil
}

func (s *NodeService) GetMetrics(_ context.Context, _ *controlv1.GetMetricsRequest) (*controlv1.GetMetricsResponse, error) {
	counts := s.node.PeerStateCounts()
	m := s.node.ControlMetrics()

	certExpiry := m.CertExpirySeconds

	// When metrics are disabled the gauge stays at zero; compute expiry
	// directly from the credential so the health check is always accurate.
	if certExpiry == 0 && s.creds != nil && s.creds.Cert != nil {
		certExpiry = time.Until(auth.CertExpiresAt(s.creds.Cert)).Seconds()
	}

	health := controlv1.HealthStatus_HEALTH_STATUS_HEALTHY
	switch {
	case certExpiry <= 0 && s.creds != nil && s.creds.Cert != nil:
		health = controlv1.HealthStatus_HEALTH_STATUS_UNHEALTHY
	case counts.Connected == 0:
		health = controlv1.HealthStatus_HEALTH_STATUS_UNHEALTHY
	case m.SmoothedVivaldiErr > vivaldiDegradedThreshold:
		health = controlv1.HealthStatus_HEALTH_STATUS_DEGRADED
	}

	return &controlv1.GetMetricsResponse{
		PeersDiscovered:    counts.Discovered,
		PeersConnecting:    counts.Connecting,
		PeersConnected:     counts.Connected,
		PeersUnreachable:   counts.Unreachable,
		EventsApplied:      m.GossipApplied,
		EventsStale:        m.GossipStale,
		VivaldiError:       m.SmoothedVivaldiErr,
		CertExpirySeconds:  certExpiry,
		CertRenewals:       m.CertRenewals,
		CertRenewalsFailed: m.CertRenewalsFailed,
		PunchAttempts:      m.PunchAttempts,
		PunchFailures:      m.PunchFailures,
		Health:             health,
		VivaldiSamples:     m.VivaldiSamples,
		EagerSyncs:         m.EagerSyncs,
		EagerSyncFailures:  m.EagerSyncFailures,
	}, nil
}

func (s *NodeService) SeedWorkload(_ context.Context, req *controlv1.SeedWorkloadRequest) (*controlv1.SeedWorkloadResponse, error) {
	if !s.node.HasWorkloads() {
		return nil, status.Error(codes.FailedPrecondition, "workload manager not initialized")
	}

	cfg := wasm.PluginConfig{
		MemoryPages: req.GetMemoryPages(),
		Timeout:     time.Duration(req.GetTimeoutMs()) * time.Millisecond,
	}
	hash, err := s.node.WorkloadSeed(req.GetWasmBytes(), cfg)
	newlySeeded := err == nil
	if err != nil && !errors.Is(err, workload.ErrAlreadyRunning) {
		log := s.node.Log()
		switch {
		case errors.Is(err, workload.ErrCompile):
			log.Warnw("seed workload failed", "err", err)
			return nil, status.Error(codes.InvalidArgument, "failed to compile workload")
		default:
			log.Errorw("seed workload failed", "err", err)
			return nil, status.Error(codes.Internal, "failed to seed workload")
		}
	}

	replicas := req.GetReplicas()
	if replicas == 0 {
		replicas = 1
	}

	// Publish spec atomically — the store rejects under its lock if a remote
	// node already owns this hash, closing the TOCTOU race.
	if err := s.node.SetWorkloadSpec(hash, replicas, req.GetMemoryPages(), req.GetTimeoutMs()); err != nil {
		// Only tear down the runtime if we just compiled it — don't destroy
		// a previously healthy workload on a spec rejection.
		if newlySeeded {
			_ = s.node.WorkloadUnseed(hash)
		}
		s.node.Log().Infow("spec rejected", "hash", hash, "err", err)
		return nil, status.Error(codes.AlreadyExists, "workload already published by another node")
	}
	s.node.SetLocalWorkloadClaim(hash, true)

	return &controlv1.SeedWorkloadResponse{Hash: hash}, nil
}

func (s *NodeService) UnseedWorkload(_ context.Context, req *controlv1.UnseedWorkloadRequest) (*controlv1.UnseedWorkloadResponse, error) {
	if !s.node.HasWorkloads() {
		return nil, status.Error(codes.FailedPrecondition, "workload manager not initialized")
	}

	// Resolve prefix from cluster-wide specs, not just local workloads.
	hash, ambiguous, found := s.node.ResolveWorkloadPrefix(req.GetHash())
	if ambiguous {
		return nil, status.Error(codes.InvalidArgument, "prefix matches multiple workloads; use a longer prefix")
	}
	if !found {
		return nil, status.Error(codes.NotFound, "no workload matches that prefix")
	}

	// Only the spec owner can unseed — reject on non-owner nodes.
	snap := s.node.Snapshot()
	sv, ok := snap.Specs[hash]
	if ok && sv.Publisher != snap.LocalID {
		return nil, status.Error(codes.PermissionDenied, "workload owned by another node; unseed from the publishing node")
	}

	// Stop the local runtime only if this node is running it.
	if s.node.WorkloadIsRunning(hash) {
		if err := s.node.WorkloadUnseed(hash); err != nil {
			s.node.Log().Warnw("unseed workload failed", "hash", hash, "err", err)
			return nil, status.Error(codes.Internal, "failed to unseed workload")
		}
	}

	// Remove spec + claim from local gossip record.
	s.node.RemoveLocalWorkloadSpec(hash)
	s.node.SetLocalWorkloadClaim(hash, false)

	return &controlv1.UnseedWorkloadResponse{}, nil
}

func (s *NodeService) CallWorkload(ctx context.Context, req *controlv1.CallWorkloadRequest) (*controlv1.CallWorkloadResponse, error) {
	if !s.node.HasWorkloads() {
		return nil, status.Error(codes.FailedPrecondition, "workload manager not initialized")
	}

	// Resolve prefix from cluster-wide specs.
	hash, ambiguous, found := s.node.ResolveWorkloadPrefix(req.GetHash())
	if ambiguous {
		return nil, status.Error(codes.InvalidArgument, "prefix matches multiple workloads; use a longer prefix")
	}
	if !found {
		return nil, status.Error(codes.NotFound, "no workload matches that prefix")
	}

	output, err := s.node.RouteCall(ctx, hash, req.GetFunction(), req.GetInput())
	if err != nil {
		s.node.Log().Warnw("call workload failed", "hash", hash, "function", req.GetFunction(), "err", err)
		if errors.Is(err, workload.ErrNotRunning) {
			return nil, status.Error(codes.NotFound, "workload not running on any reachable node")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, "workload invocation timed out")
		}
		return nil, status.Error(codes.Internal, "workload invocation failed")
	}
	return &controlv1.CallWorkloadResponse{Output: output}, nil
}

const vivaldiDegradedThreshold = 0.9

const offlineRank = 3

func nodeStatusRank(status controlv1.NodeStatus) int {
	switch status {
	case controlv1.NodeStatus_NODE_STATUS_ONLINE:
		return 0
	case controlv1.NodeStatus_NODE_STATUS_INDIRECT:
		return 1
	case controlv1.NodeStatus_NODE_STATUS_RELAY:
		return 2 //nolint:mnd
	case controlv1.NodeStatus_NODE_STATUS_OFFLINE,
		controlv1.NodeStatus_NODE_STATUS_UNSPECIFIED:
		return offlineRank
	}
	return offlineRank
}

func workloadStatusProto(s workload.Status) controlv1.WorkloadStatus {
	switch s {
	case workload.StatusRunning:
		return controlv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
	case workload.StatusStopped:
		return controlv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
	case workload.StatusErrored:
		return controlv1.WorkloadStatus_WORKLOAD_STATUS_ERRORED
	}
	return controlv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED
}
