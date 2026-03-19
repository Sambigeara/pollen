package control

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/config"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/tunneling"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Metrics struct {
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

type MembershipControl interface {
	DenyPeer(key types.PeerKey) error
	Invite(subject string) (string, error)
}

type PlacementControl interface {
	Seed(hash string, binary []byte, replicas, memoryPages, timeoutMs uint32) error
	Unseed(hash string) error
	Call(ctx context.Context, hash, fn string, input []byte) ([]byte, error)
	Status() []placement.WorkloadSummary
}

type TunnelingControl interface {
	Connect(ctx context.Context, service, peer string) error
	Disconnect(service string) error
	ExposeService(port uint32, name string) error
	UnexposeService(name string) error
	ListConnections() []tunneling.ConnectionInfo
}

type StateReader interface {
	Snapshot() state.Snapshot
}

type TransportInfo interface {
	PeerStateCounts() transport.PeerStateCounts
	GetActivePeerAddress(types.PeerKey) (*net.UDPAddr, bool)
	PeerRTT(types.PeerKey) (time.Duration, bool)
	ReconnectWindowDuration() time.Duration
}

type MetricsSource interface {
	ControlMetrics() Metrics
}

type MeshConnector interface {
	Connect(ctx context.Context, peer types.PeerKey, addrs []netip.AddrPort) error
}

var _ controlv1.ControlServiceServer = (*Service)(nil)

type Service struct {
	controlv1.UnimplementedControlServiceServer
	membership MembershipControl
	placement  PlacementControl
	tunneling  TunnelingControl
	state      StateReader
	shutdown   func()
	creds      *auth.NodeCredentials
	transport  TransportInfo
	metrics    MetricsSource
	connector  MeshConnector
	log        *zap.SugaredLogger
}

type Option func(*Service)

func WithShutdown(fn func()) Option                  { return func(s *Service) { s.shutdown = fn } }
func WithCredentials(c *auth.NodeCredentials) Option { return func(s *Service) { s.creds = c } }
func WithTransportInfo(t TransportInfo) Option       { return func(s *Service) { s.transport = t } }
func WithMetricsSource(m MetricsSource) Option       { return func(s *Service) { s.metrics = m } }
func WithMeshConnector(c MeshConnector) Option       { return func(s *Service) { s.connector = c } }

func NewService(membership MembershipControl, placement PlacementControl, tunneling TunnelingControl, state StateReader, opts ...Option) *Service {
	s := &Service{
		membership: membership,
		placement:  placement,
		tunneling:  tunneling,
		state:      state,
		log:        zap.S().Named("control"),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

type Server struct {
	svc *Service
	gs  *grpc.Server
	log *zap.SugaredLogger
}

func New(membership MembershipControl, placement PlacementControl, tunneling TunnelingControl, state StateReader, opts ...Option) *Server {
	svc := NewService(membership, placement, tunneling, state, opts...)
	gs := grpc.NewServer()
	controlv1.RegisterControlServiceServer(gs, svc)
	return &Server{
		svc: svc,
		gs:  gs,
		log: zap.S().Named("grpc"),
	}
}

func (s *Server) Start(socketPath string) error {
	if _, err := os.Stat(socketPath); err == nil {
		dialCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		conn, dialErr := (&net.Dialer{Timeout: time.Second}).DialContext(dialCtx, "unix", socketPath)
		if dialErr == nil {
			_ = conn.Close()
			return nil
		}
		_ = os.Remove(socketPath)
	}

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", socketPath)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return fmt.Errorf("failed to listen: %w", err)
	}
	defer os.Remove(socketPath)

	if err := config.SetGroupSocket(socketPath); err != nil {
		s.log.Warnw("socket group permissions", "err", err)
	}

	return s.gs.Serve(l)
}

func (s *Server) Stop()             { s.gs.GracefulStop() }
func (s *Server) Service() *Service { return s.svc }

func (s *Service) Shutdown(_ context.Context, _ *controlv1.ShutdownRequest) (*controlv1.ShutdownResponse, error) {
	if s.shutdown == nil {
		return &controlv1.ShutdownResponse{}, status.Error(codes.FailedPrecondition, "shutdown callback not configured")
	}
	go s.shutdown()
	return &controlv1.ShutdownResponse{}, nil
}

func (s *Service) GetBootstrapInfo(_ context.Context, _ *controlv1.GetBootstrapInfoRequest) (*controlv1.GetBootstrapInfoResponse, error) {
	snap := s.state.Snapshot()
	local := snap.LocalID
	rec, ok := snap.Nodes[local]
	if !ok {
		return &controlv1.GetBootstrapInfoResponse{}, nil
	}
	return &controlv1.GetBootstrapInfoResponse{
		Self:        nodeBootstrapInfo(local, rec.IPs, rec.LocalPort),
		Recommended: s.pickRecommendedPeer(snap),
	}, nil
}

func (s *Service) pickRecommendedPeer(snap state.Snapshot) *controlv1.BootstrapPeerInfo {
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

func (s *Service) GetStatus(_ context.Context, _ *controlv1.GetStatusRequest) (*controlv1.GetStatusResponse, error) {
	snap := s.state.Snapshot()
	nodes := snap.Nodes
	localID := snap.LocalID
	localNode := nodes[localID]

	degraded := false
	if s.creds != nil && s.creds.Cert != nil && s.transport != nil {
		now := time.Now()
		if auth.IsCertExpired(s.creds.Cert, now) &&
			auth.IsCertWithinReconnectWindow(s.creds.Cert, now, s.transport.ReconnectWindowDuration()) {
			degraded = true
		}
	}

	out := &controlv1.GetStatusResponse{
		Self: &controlv1.NodeSummary{
			Node:               &controlv1.NodeRef{PeerId: localID.Bytes()},
			Status:             controlv1.NodeStatus_NODE_STATUS_ONLINE,
			Addr:               nodeViewAddr(localNode),
			PubliclyAccessible: localNode.PubliclyAccessible,
			CpuPercent:         localNode.CPUPercent,
			MemPercent:         localNode.MemPercent,
			NumCpu:             localNode.NumCPU,
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
		case remaining <= membership.CertCriticalThreshold:
			health = controlv1.CertHealth_CERT_HEALTH_EXPIRING_SOON
		case remaining <= membership.CertWarnThreshold:
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

	connections := s.tunneling.ListConnections()
	tunnelCounts := make(map[types.PeerKey]uint32, len(connections))
	for _, c := range connections {
		tunnelCounts[c.PeerID]++
	}
	out.Self.TunnelCount = uint32(len(connections))

	for _, ts := range snap.Heatmaps[localID] {
		out.Self.TrafficBytesIn += ts.BytesIn
		out.Self.TrafficBytesOut += ts.BytesOut
	}

	directPeers := make(map[types.PeerKey]*net.UDPAddr)
	if s.transport != nil {
		for key := range nodes {
			if key == localID {
				continue
			}
			if addr, ok := s.transport.GetActivePeerAddress(key); ok {
				directPeers[key] = addr
			}
		}
	}

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
			_, direct := directPeers[peer]
			_, seen := indirectPeers[peer]
			if peer == localID || direct || seen {
				continue
			}
			indirectPeers[peer] = struct{}{}
			queue = append(queue, peer)
		}
	}

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

		addrStr := nodeViewAddr(node)
		if isDirect {
			addrStr = addr.String()
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
		for _, ts := range snap.Heatmaps[key] {
			ns.TrafficBytesIn += ts.BytesIn
			ns.TrafficBytesOut += ts.BytesOut
		}
		if isDirect && s.transport != nil {
			if rtt, ok := s.transport.PeerRTT(key); ok {
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

	specs := snap.Specs
	claims := snap.Claims

	seen := make(map[string]struct{})
	for _, w := range s.placement.Status() {
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

func (s *Service) RegisterService(_ context.Context, req *controlv1.RegisterServiceRequest) (*controlv1.RegisterServiceResponse, error) {
	name := req.GetName()
	if name == "" {
		name = strconv.FormatUint(uint64(req.Port), 10)
	}
	if err := s.tunneling.ExposeService(req.Port, name); err != nil {
		return nil, s.fail(err, "register service failed")
	}
	return &controlv1.RegisterServiceResponse{}, nil
}

func (s *Service) UnregisterService(_ context.Context, req *controlv1.UnregisterServiceRequest) (*controlv1.UnregisterServiceResponse, error) {
	name := req.GetName()
	if name == "" {
		name = strconv.FormatUint(uint64(req.GetPort()), 10)
	}
	if err := s.tunneling.UnexposeService(name); err != nil {
		return nil, s.fail(err, "unregister service failed")
	}
	return &controlv1.UnregisterServiceResponse{}, nil
}

func (s *Service) ConnectPeer(ctx context.Context, req *controlv1.ConnectPeerRequest) (*controlv1.ConnectPeerResponse, error) {
	if s.connector == nil {
		return nil, status.Error(codes.FailedPrecondition, "mesh connector not configured")
	}
	peerKey := types.PeerKeyFromBytes(req.PeerId)
	addrs := make([]netip.AddrPort, 0, len(req.Addrs))
	for _, a := range req.Addrs {
		ap, err := netip.ParseAddrPort(a)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid address %q", a)
		}
		addrs = append(addrs, ap)
	}
	if err := s.connector.Connect(ctx, peerKey, addrs); err != nil {
		return nil, s.fail(err, "connect peer failed", "peer", peerKey.Short())
	}
	return &controlv1.ConnectPeerResponse{}, nil
}

func (s *Service) ConnectService(ctx context.Context, req *controlv1.ConnectServiceRequest) (*controlv1.ConnectServiceResponse, error) {
	peerKey := types.PeerKeyFromBytes(req.Node.PeerId)
	serviceName := resolveServiceName(s.state.Snapshot(), peerKey, req.RemotePort)
	if serviceName == "" {
		serviceName = strconv.FormatUint(uint64(req.RemotePort), 10)
	}
	if err := s.tunneling.Connect(ctx, serviceName, peerKey.String()); err != nil {
		return nil, s.fail(err, "connect service failed")
	}
	return &controlv1.ConnectServiceResponse{}, nil
}

func (s *Service) DisconnectService(_ context.Context, req *controlv1.DisconnectServiceRequest) (*controlv1.DisconnectServiceResponse, error) {
	localPort := req.GetLocalPort()
	snap := s.state.Snapshot()
	var serviceName string
	for _, c := range s.tunneling.ListConnections() {
		if c.LocalPort == localPort {
			serviceName = resolveServiceName(snap, c.PeerID, c.RemotePort)
			break
		}
	}
	if serviceName == "" {
		return nil, status.Error(codes.NotFound, "no connection on that local port")
	}
	if err := s.tunneling.Disconnect(serviceName); err != nil {
		return nil, s.fail(err, "disconnect service failed")
	}
	return &controlv1.DisconnectServiceResponse{}, nil
}

func (s *Service) DenyPeer(_ context.Context, req *controlv1.DenyPeerRequest) (*controlv1.DenyPeerResponse, error) {
	if s.creds == nil || s.creds.DelegationKey == nil {
		return nil, status.Error(codes.FailedPrecondition, "delegation key not available; this node cannot deny peers")
	}
	if err := s.membership.DenyPeer(types.PeerKeyFromBytes(req.GetPeerId())); err != nil {
		return nil, s.fail(err, "deny peer failed")
	}
	return &controlv1.DenyPeerResponse{}, nil
}

func (s *Service) GetMetrics(_ context.Context, _ *controlv1.GetMetricsRequest) (*controlv1.GetMetricsResponse, error) {
	var counts transport.PeerStateCounts
	if s.transport != nil {
		counts = s.transport.PeerStateCounts()
	}
	var m Metrics
	if s.metrics != nil {
		m = s.metrics.ControlMetrics()
	}

	certExpiry := m.CertExpirySeconds
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
		PeersDiscovered:    counts.Backoff,
		PeersConnecting:    counts.Connecting,
		PeersConnected:     counts.Connected,
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

func (s *Service) SeedWorkload(_ context.Context, req *controlv1.SeedWorkloadRequest) (*controlv1.SeedWorkloadResponse, error) {
	wasmBytes := req.GetWasmBytes()
	h := sha256.Sum256(wasmBytes)
	hash := hex.EncodeToString(h[:])

	if err := s.placement.Seed(hash, wasmBytes, req.GetReplicas(), req.GetMemoryPages(), req.GetTimeoutMs()); err != nil && !errors.Is(err, placement.ErrAlreadyRunning) {
		if errors.Is(err, placement.ErrCompile) {
			s.log.Warnw("seed workload failed", "err", err)
			return nil, status.Error(codes.InvalidArgument, "failed to compile workload")
		}
		s.log.Errorw("seed workload failed", "err", err)
		return nil, status.Error(codes.Internal, "failed to seed workload")
	}

	return &controlv1.SeedWorkloadResponse{Hash: hash}, nil
}

func (s *Service) UnseedWorkload(_ context.Context, req *controlv1.UnseedWorkloadRequest) (*controlv1.UnseedWorkloadResponse, error) {
	if err := s.placement.Unseed(req.GetHash()); err != nil {
		return nil, s.fail(err, "unseed workload failed", "hash", req.GetHash())
	}
	return &controlv1.UnseedWorkloadResponse{}, nil
}

func (s *Service) CallWorkload(ctx context.Context, req *controlv1.CallWorkloadRequest) (*controlv1.CallWorkloadResponse, error) {
	output, err := s.placement.Call(ctx, req.GetHash(), req.GetFunction(), req.GetInput())
	if err != nil {
		s.log.Warnw("call workload failed", "hash", req.GetHash(), "function", req.GetFunction(), "err", err)
		if errors.Is(err, placement.ErrNotRunning) {
			return nil, status.Error(codes.NotFound, "workload not running on any reachable node")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, "workload invocation timed out")
		}
		return nil, status.Error(codes.Internal, "workload invocation failed")
	}
	return &controlv1.CallWorkloadResponse{Output: output}, nil
}

func (s *Service) fail(err error, msg string, kv ...any) error {
	s.log.Warnw(msg, append(kv, "err", err)...)
	return status.Error(codes.Internal, msg)
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

func nodeViewAddr(nv state.NodeView) string {
	if len(nv.IPs) == 0 {
		return ""
	}
	port := nv.LocalPort
	if ip := net.ParseIP(nv.IPs[0]); ip != nil && nv.ExternalPort != 0 && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
		port = nv.ExternalPort
	}
	return net.JoinHostPort(nv.IPs[0], strconv.Itoa(int(port)))
}

func resolveServiceName(snap state.Snapshot, peerKey types.PeerKey, port uint32) string {
	nv, ok := snap.Nodes[peerKey]
	if !ok {
		return ""
	}
	for _, svc := range nv.Services {
		if svc.GetPort() == port {
			return svc.GetName()
		}
	}
	return ""
}

const vivaldiDegradedThreshold = 0.9

const offlineRank = 3

func nodeStatusRank(s controlv1.NodeStatus) int {
	switch s {
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

func workloadStatusProto(s placement.Status) controlv1.WorkloadStatus {
	switch s {
	case placement.StatusRunning:
		return controlv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
	case placement.StatusStopped:
		return controlv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
	case placement.StatusErrored:
		return controlv1.WorkloadStatus_WORKLOAD_STATUS_ERRORED
	}
	return controlv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED
}
