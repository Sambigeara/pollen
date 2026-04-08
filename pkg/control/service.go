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
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/plnfs"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/tunneling"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/wasm"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
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
	IssueCert(ctx context.Context, peerKey types.PeerKey, admin bool, attributes *structpb.Struct) error
}

type PlacementControl interface {
	Seed(name, hash string, binary []byte, replicas, memoryPages, timeoutMs uint32, spread float32) error
	Unseed(hash string) error
	Call(ctx context.Context, hash, fn string, input []byte) ([]byte, error)
	Status() []placement.WorkloadSummary
	PlacementInfo() map[string][2]float64
}

type TunnelingControl interface {
	Connect(ctx context.Context, peer types.PeerKey, remotePort, localPort uint32, protocol statev1.ServiceProtocol) (uint32, error)
	Disconnect(service string) error
	ExposeService(port uint32, name string, protocol statev1.ServiceProtocol) error
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
		if conn, dialErr := net.DialTimeout("unix", socketPath, time.Second); dialErr == nil { //nolint:noctx
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

	if err := plnfs.SetGroupSocket(socketPath); err != nil {
		s.log.Warnw("socket group permissions", "err", err)
	}

	return s.gs.Serve(l)
}

func (s *Server) Stop()             { s.gs.GracefulStop() }
func (s *Server) Service() *Service { return s.svc }

func (s *Service) Shutdown(_ context.Context, _ *controlv1.ShutdownRequest) (*controlv1.ShutdownResponse, error) {
	if s.shutdown == nil {
		return nil, status.Error(codes.FailedPrecondition, "shutdown callback not configured")
	}
	go s.shutdown()
	return &controlv1.ShutdownResponse{}, nil
}

func (s *Service) GetBootstrapInfo(_ context.Context, _ *controlv1.GetBootstrapInfoRequest) (*controlv1.GetBootstrapInfoResponse, error) {
	snap := s.state.Snapshot()
	rec, ok := snap.Nodes[snap.LocalID]
	if !ok {
		return &controlv1.GetBootstrapInfoResponse{}, nil
	}
	return &controlv1.GetBootstrapInfoResponse{
		Self:        nodeBootstrapInfo(snap.LocalID, rec),
		Recommended: s.pickRecommendedPeer(snap),
	}, nil
}

func (s *Service) pickRecommendedPeer(snap state.Snapshot) *controlv1.BootstrapPeerInfo {
	var candidates []types.PeerKey
	for peerID, nv := range snap.Nodes {
		hasAddr := len(nv.IPs) > 0 || nv.ObservedExternalIP != ""
		if peerID != snap.LocalID && nv.PubliclyAccessible && hasAddr && nv.LocalPort > 0 {
			candidates = append(candidates, peerID)
		}
	}
	if len(candidates) > 0 {
		slices.SortFunc(candidates, types.PeerKey.Compare)
		best := candidates[0]
		return nodeBootstrapInfo(best, snap.Nodes[best])
	}

	localNV, ok := snap.Nodes[snap.LocalID]
	if !ok {
		return nil
	}
	hasAddr := len(localNV.IPs) > 0 || localNV.ObservedExternalIP != ""
	if !hasAddr || localNV.LocalPort == 0 {
		return nil
	}
	return nodeBootstrapInfo(snap.LocalID, localNV)
}

func (s *Service) GetStatus(_ context.Context, _ *controlv1.GetStatusRequest) (*controlv1.GetStatusResponse, error) {
	snap := s.state.Snapshot()
	now := time.Now()

	activeNodes := filterActiveNodes(snap.Nodes, snap.LocalID, now)
	connections := s.tunneling.ListConnections()

	out := &controlv1.GetStatusResponse{
		Degraded:     s.isDegraded(now),
		Certificates: s.buildCertificates(),
		Self:         s.buildSelfSummary(snap.LocalID, snap.Nodes[snap.LocalID], connections),
		Nodes:        s.buildNodeSummaries(snap, activeNodes, connections),
		Services:     buildServiceSummaries(activeNodes),
		Connections:  buildConnectionSummaries(activeNodes, connections),
		Workloads:    s.buildWorkloadSummaries(snap),
	}

	sortStatusResponse(out)
	return out, nil
}

func filterActiveNodes(nodes map[types.PeerKey]state.NodeView, localID types.PeerKey, now time.Time) map[types.PeerKey]state.NodeView {
	active := make(map[types.PeerKey]state.NodeView, len(nodes))
	for key, nv := range nodes {
		if key != localID && nv.CertExpiry != 0 && auth.IsExpiredAt(time.Unix(nv.CertExpiry, 0), now) {
			continue
		}
		active[key] = nv
	}
	return active
}

func (s *Service) isDegraded(now time.Time) bool {
	if s.creds == nil || s.creds.Cert() == nil || s.transport == nil {
		return false
	}
	cert := s.creds.Cert()
	window := s.transport.ReconnectWindowDuration()
	return auth.IsCertExpired(cert, now) && now.Before(auth.CertExpiresAt(cert).Add(window))
}

func (s *Service) buildCertificates() []*controlv1.CertInfo {
	if s.creds == nil || s.creds.Cert() == nil {
		return nil
	}
	cert := s.creds.Cert()
	claims := cert.GetClaims()
	caps := claims.GetCapabilities()
	health := controlv1.CertHealth_CERT_HEALTH_OK
	remaining := time.Until(auth.CertExpiresAt(cert))

	switch {
	case remaining <= 0:
		health = controlv1.CertHealth_CERT_HEALTH_EXPIRED
	case remaining <= membership.CertCriticalThreshold:
		health = controlv1.CertHealth_CERT_HEALTH_EXPIRING_SOON
	case remaining <= membership.CertWarnThreshold:
		health = controlv1.CertHealth_CERT_HEALTH_RENEWING
	}

	return []*controlv1.CertInfo{{
		NotBeforeUnix:      claims.GetNotBeforeUnix(),
		NotAfterUnix:       claims.GetNotAfterUnix(),
		Serial:             claims.GetSerial(),
		Health:             health,
		CanDelegate:        caps.GetCanDelegate(),
		CanAdmit:           caps.GetCanAdmit(),
		MaxDepth:           caps.GetMaxDepth(),
		AccessDeadlineUnix: claims.GetAccessDeadlineUnix(),
		Attributes:         caps.GetAttributes(),
	}}
}

func (s *Service) buildSelfSummary(localID types.PeerKey, localNode state.NodeView, connections []tunneling.ConnectionInfo) *controlv1.NodeSummary {
	in, out := sumTraffic(localNode.TrafficRates)
	return &controlv1.NodeSummary{
		Node:               &controlv1.NodeRef{PeerPub: localID.Bytes()},
		Name:               localNode.Name,
		Status:             controlv1.NodeStatus_NODE_STATUS_ONLINE,
		Addr:               nodeViewAddr(localNode),
		PubliclyAccessible: localNode.PubliclyAccessible,
		CpuPercent:         localNode.CPUPercent,
		MemPercent:         localNode.MemPercent,
		NumCpu:             localNode.NumCPU,
		TunnelCount:        uint32(len(connections)),
		TrafficBytesIn:     in,
		TrafficBytesOut:    out,
	}
}

func (s *Service) buildNodeSummaries(snap state.Snapshot, nodes map[types.PeerKey]state.NodeView, connections []tunneling.ConnectionInfo) []*controlv1.NodeSummary {
	liveSet := make(map[types.PeerKey]struct{}, len(snap.PeerKeys))
	for _, pk := range snap.PeerKeys {
		liveSet[pk] = struct{}{}
	}

	tunnelCounts := make(map[types.PeerKey]uint32, len(connections))
	for _, c := range connections {
		tunnelCounts[c.PeerID]++
	}

	out := make([]*controlv1.NodeSummary, 0, len(nodes))
	for key, node := range nodes {
		if key == snap.LocalID {
			continue
		}

		var isDirect bool
		var addr *net.UDPAddr
		if s.transport != nil {
			addr, isDirect = s.transport.GetActivePeerAddress(key)
		}

		peerStatus := controlv1.NodeStatus_NODE_STATUS_OFFLINE
		addrStr := nodeViewAddr(node)

		if isDirect {
			peerStatus = controlv1.NodeStatus_NODE_STATUS_ONLINE
			addrStr = addr.String()
		} else if _, ok := liveSet[key]; ok {
			peerStatus = controlv1.NodeStatus_NODE_STATUS_INDIRECT
		}

		in, outBytes := sumTraffic(node.TrafficRates)
		ns := &controlv1.NodeSummary{
			Node:               &controlv1.NodeRef{PeerPub: key.Bytes()},
			Name:               node.Name,
			Status:             peerStatus,
			Addr:               addrStr,
			PubliclyAccessible: node.PubliclyAccessible,
			TunnelCount:        tunnelCounts[key],
			CpuPercent:         node.CPUPercent,
			MemPercent:         node.MemPercent,
			NumCpu:             node.NumCPU,
			TrafficBytesIn:     in,
			TrafficBytesOut:    outBytes,
		}
		if isDirect && s.transport != nil {
			if rtt, ok := s.transport.PeerRTT(key); ok {
				ns.LatencyMs = float64(rtt.Microseconds()) / 1000.0 //nolint:mnd
			}
		}
		out = append(out, ns)
	}
	return out
}

func buildServiceSummaries(nodes map[types.PeerKey]state.NodeView) []*controlv1.ServiceSummary {
	var out []*controlv1.ServiceSummary
	for key, node := range nodes {
		for _, svc := range node.Services {
			out = append(out, &controlv1.ServiceSummary{
				Name:     serviceNameOrDefault(svc.Name, svc.Port),
				Provider: &controlv1.NodeRef{PeerPub: key.Bytes()},
				Port:     svc.Port,
				Protocol: svc.Protocol,
			})
		}
	}
	return out
}

func buildConnectionSummaries(nodes map[types.PeerKey]state.NodeView, connections []tunneling.ConnectionInfo) []*controlv1.ConnectionSummary {
	out := make([]*controlv1.ConnectionSummary, 0, len(connections))
	for _, c := range connections {
		var name string
		if node, ok := nodes[c.PeerID]; ok {
			for _, svc := range node.Services {
				if svc.Port == c.RemotePort && svc.Protocol == c.Protocol {
					name = svc.Name
					break
				}
			}
		}
		out = append(out, &controlv1.ConnectionSummary{
			Peer:        &controlv1.NodeRef{PeerPub: c.PeerID.Bytes()},
			RemotePort:  c.RemotePort,
			LocalPort:   c.LocalPort,
			ServiceName: name,
			Protocol:    c.Protocol,
		})
	}
	return out
}

func (s *Service) buildWorkloadSummaries(snap state.Snapshot) []*controlv1.WorkloadSummary {
	var out []*controlv1.WorkloadSummary
	seen := make(map[string]struct{})
	pinfo := s.placement.PlacementInfo()

	for _, w := range s.placement.Status() {
		seen[w.Hash] = struct{}{}
		ws := &controlv1.WorkloadSummary{
			Hash:            w.Hash,
			Name:            w.Name,
			Status:          workloadStatusProto(w.Status),
			StartedAtUnix:   w.CompiledAt.Unix(),
			Local:           true,
			ActiveReplicas:  uint32(len(snap.Claims[w.Hash])),
			EffectiveTarget: w.EffectiveTarget,
			Pressure:        float32(w.Pressure),
		}
		if sv, ok := snap.Specs[w.Hash]; ok {
			ws.MinReplicas = sv.Spec.GetMinReplicas()
			ws.Spread = sv.Spec.GetSpread()
		}
		out = append(out, ws)
	}

	for hash, sv := range snap.Specs {
		if _, ok := seen[hash]; ok {
			continue
		}
		ws := &controlv1.WorkloadSummary{
			Hash:           hash,
			Name:           sv.Spec.GetName(),
			MinReplicas:    sv.Spec.GetMinReplicas(),
			Spread:         sv.Spec.GetSpread(),
			ActiveReplicas: uint32(len(snap.Claims[hash])),
		}
		if info, ok := pinfo[hash]; ok {
			ws.EffectiveTarget = uint32(info[0])
			ws.Pressure = float32(info[1])
		}
		out = append(out, ws)
	}
	return out
}

func sortStatusResponse(out *controlv1.GetStatusResponse) {
	slices.SortFunc(out.Nodes, func(a, b *controlv1.NodeSummary) int {
		if ra, rb := nodeStatusRank(a.Status), nodeStatusRank(b.Status); ra != rb {
			return ra - rb
		}
		return types.PeerKeyFromBytes(a.Node.PeerPub).Compare(types.PeerKeyFromBytes(b.Node.PeerPub))
	})
	slices.SortFunc(out.Services, func(a, b *controlv1.ServiceSummary) int {
		if a.Name != b.Name {
			return cmp.Compare(a.Name, b.Name)
		}
		if a.Port != b.Port {
			return cmp.Compare(a.Port, b.Port)
		}
		return types.PeerKeyFromBytes(a.Provider.PeerPub).Compare(types.PeerKeyFromBytes(b.Provider.PeerPub))
	})
	slices.SortFunc(out.Connections, func(a, b *controlv1.ConnectionSummary) int {
		if a.LocalPort != b.LocalPort {
			return cmp.Compare(a.LocalPort, b.LocalPort)
		}
		return types.PeerKeyFromBytes(a.Peer.PeerPub).Compare(types.PeerKeyFromBytes(b.Peer.PeerPub))
	})
}

func (s *Service) RegisterService(_ context.Context, req *controlv1.RegisterServiceRequest) (*controlv1.RegisterServiceResponse, error) {
	name := serviceNameOrDefault(req.GetName(), req.Port)
	if err := s.tunneling.ExposeService(req.Port, name, normaliseProtocol(req.GetProtocol())); err != nil {
		return nil, s.fail(err, "register service failed")
	}
	return &controlv1.RegisterServiceResponse{}, nil
}

func (s *Service) UnregisterService(_ context.Context, req *controlv1.UnregisterServiceRequest) (*controlv1.UnregisterServiceResponse, error) {
	name := serviceNameOrDefault(req.GetName(), req.GetPort())
	if err := s.tunneling.UnexposeService(name); err != nil {
		return nil, s.fail(err, "unregister service failed")
	}
	return &controlv1.UnregisterServiceResponse{}, nil
}

func (s *Service) ConnectPeer(ctx context.Context, req *controlv1.ConnectPeerRequest) (*controlv1.ConnectPeerResponse, error) {
	if s.connector == nil {
		return nil, status.Error(codes.FailedPrecondition, "mesh connector not configured")
	}
	peerKey := types.PeerKeyFromBytes(req.PeerPub)
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
	peerKey := types.PeerKeyFromBytes(req.Node.PeerPub)
	boundPort, err := s.tunneling.Connect(ctx, peerKey, req.GetRemotePort(), req.GetLocalPort(), normaliseProtocol(req.GetProtocol()))
	if err != nil {
		return nil, s.fail(err, "connect service failed")
	}
	return &controlv1.ConnectServiceResponse{LocalPort: boundPort}, nil
}

func (s *Service) DisconnectService(_ context.Context, req *controlv1.DisconnectServiceRequest) (*controlv1.DisconnectServiceResponse, error) {
	localPort := req.GetLocalPort()
	snap := s.state.Snapshot()
	var serviceName string
	for _, c := range s.tunneling.ListConnections() {
		if c.LocalPort == localPort {
			serviceName = resolveServiceName(snap, c.PeerID, c.RemotePort, c.Protocol)
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
	if s.creds == nil || s.creds.DelegationKey() == nil {
		return nil, status.Error(codes.FailedPrecondition, "delegation key not available; this node cannot deny peers")
	}
	if err := s.membership.DenyPeer(types.PeerKeyFromBytes(req.GetPeerPub())); err != nil {
		return nil, s.fail(err, "deny peer failed")
	}
	return &controlv1.DenyPeerResponse{}, nil
}

func (s *Service) IssueCert(ctx context.Context, req *controlv1.IssueCertRequest) (*controlv1.IssueCertResponse, error) {
	if s.creds == nil || s.creds.DelegationKey() == nil {
		return nil, status.Error(codes.FailedPrecondition, "only root admin can issue certificates")
	}
	if err := auth.ValidateAttributes(req.GetAttributes()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.membership.IssueCert(ctx, types.PeerKeyFromBytes(req.GetPeerPub()), req.GetAdmin(), req.GetAttributes()); err != nil {
		return nil, s.fail(err, "issue cert failed")
	}
	return &controlv1.IssueCertResponse{}, nil
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
	if certExpiry == 0 && s.creds != nil && s.creds.Cert() != nil {
		certExpiry = time.Until(auth.CertExpiresAt(s.creds.Cert())).Seconds()
	}

	health := controlv1.HealthStatus_HEALTH_STATUS_HEALTHY
	switch {
	case (certExpiry <= 0 && s.creds != nil && s.creds.Cert() != nil) || counts.Connected == 0:
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

	name := req.GetName()
	if name == "" {
		name = hash
	}

	if err := s.placement.Seed(name, hash, wasmBytes, req.GetMinReplicas(), req.GetMemoryPages(), req.GetTimeoutMs(), req.GetSpread()); err != nil && !errors.Is(err, placement.ErrAlreadyRunning) {
		if errors.Is(err, placement.ErrCompile) {
			s.log.Warnw("seed workload failed", "err", err)
			return nil, status.Error(codes.InvalidArgument, "failed to compile workload")
		}
		return nil, s.fail(err, "failed to seed workload")
	}

	return &controlv1.SeedWorkloadResponse{Hash: hash, Name: name}, nil
}

func (s *Service) UnseedWorkload(_ context.Context, req *controlv1.UnseedWorkloadRequest) (*controlv1.UnseedWorkloadResponse, error) {
	if err := s.placement.Unseed(req.GetHash()); err != nil {
		return nil, s.fail(err, "unseed workload failed", "hash", req.GetHash())
	}
	return &controlv1.UnseedWorkloadResponse{}, nil
}

func (s *Service) CallWorkload(ctx context.Context, req *controlv1.CallWorkloadRequest) (*controlv1.CallWorkloadResponse, error) {
	ctx = s.localCallerContext(ctx)
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

func (s *Service) localCallerContext(ctx context.Context) context.Context {
	if s.creds == nil {
		return ctx
	}
	cert := s.creds.Cert()
	if cert == nil {
		return ctx
	}
	info := wasm.CallerInfo{
		PeerKey: types.PeerKeyFromBytes(cert.GetClaims().GetSubjectPub()),
	}
	if attrs := cert.GetClaims().GetCapabilities().GetAttributes(); attrs != nil {
		info.Attributes = attrs.AsMap()
	}
	return wasm.WithCallerInfo(ctx, info)
}

func (s *Service) fail(err error, msg string, kv ...any) error {
	s.log.Warnw(msg, append(kv, "err", err)...)
	return status.Error(codes.Internal, msg)
}

func sumTraffic(rates map[types.PeerKey]state.TrafficSnapshot) (uint64, uint64) {
	var in, out uint64
	for _, ts := range rates {
		in += ts.BytesIn
		out += ts.BytesOut
	}
	return in, out
}

func serviceNameOrDefault(name string, port uint32) string {
	if name != "" {
		return name
	}
	return strconv.FormatUint(uint64(port), 10)
}

func nodeBootstrapInfo(peerID types.PeerKey, nv state.NodeView) *controlv1.BootstrapPeerInfo {
	var addrs []string
	if nv.ObservedExternalIP != "" {
		port := nv.LocalPort
		if nv.ExternalPort != 0 {
			port = nv.ExternalPort
		}
		addrs = append(addrs, net.JoinHostPort(nv.ObservedExternalIP, strconv.Itoa(int(port))))
	}
	for _, ip := range nv.IPs {
		addrs = append(addrs, net.JoinHostPort(ip, strconv.Itoa(int(nv.LocalPort))))
	}
	return &controlv1.BootstrapPeerInfo{
		Peer:  &controlv1.NodeRef{PeerPub: peerID.Bytes()},
		Addrs: addrs,
	}
}

func nodeViewAddr(nv state.NodeView) string {
	if nv.ObservedExternalIP != "" {
		port := nv.LocalPort
		if nv.ExternalPort != 0 {
			port = nv.ExternalPort
		}
		return net.JoinHostPort(nv.ObservedExternalIP, strconv.Itoa(int(port)))
	}
	if len(nv.IPs) == 0 {
		return ""
	}
	return net.JoinHostPort(nv.IPs[0], strconv.Itoa(int(nv.LocalPort)))
}

func resolveServiceName(snap state.Snapshot, peerKey types.PeerKey, port uint32, protocol statev1.ServiceProtocol) string {
	if nv, ok := snap.Nodes[peerKey]; ok {
		for _, svc := range nv.Services {
			if svc.Port == port && svc.Protocol == protocol {
				return svc.Name
			}
		}
	}
	return ""
}

// TODO(saml): remove once all persisted state and in-flight gossip carry an explicit protocol.
func normaliseProtocol(p statev1.ServiceProtocol) statev1.ServiceProtocol {
	if p == statev1.ServiceProtocol_SERVICE_PROTOCOL_UNSPECIFIED {
		return statev1.ServiceProtocol_SERVICE_PROTOCOL_TCP
	}
	return p
}

const (
	vivaldiDegradedThreshold = 0.9
	offlineRank              = 3
)

func nodeStatusRank(s controlv1.NodeStatus) int {
	switch s { //nolint:exhaustive
	case controlv1.NodeStatus_NODE_STATUS_ONLINE:
		return 0
	case controlv1.NodeStatus_NODE_STATUS_INDIRECT:
		return 1
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
