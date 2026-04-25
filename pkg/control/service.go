// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/blobs"
	claimspkg "github.com/sambigeara/pollen/pkg/claims"
	"github.com/sambigeara/pollen/pkg/evaluator"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// ControlTokenMetadataKey is the gRPC metadata key expected to carry the
// shared secret for non-unix control RPCs.
const ControlTokenMetadataKey = "x-pln-token"

type Metrics struct {
	CertExpirySeconds  float64
	CertRenewals       uint64
	CertRenewalsFailed uint64
	PunchAttempts      uint64
	PunchFailures      uint64
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
	Seed(binary []byte, spec state.WorkloadSpec) error
	Unseed(hash string) error
	Call(ctx context.Context, hash, fn string, input []byte) ([]byte, error)
	Status() []placement.WorkloadSummary
	PlacementInfo() map[string]placement.PlacementInfo
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

type BlobsControl interface {
	Fetch(ctx context.Context, hash string, peers []types.PeerKey) error
	Put(r io.Reader) (string, error)
	SetName(hash, name string, claim *claimspkg.PublisherClaim) error
	Remove(hash string) error
}

type StaticControl interface {
	SeedStatic(name string, manifestDigest []byte, minReplicas uint32, claim *claimspkg.PublisherClaim) error
	UnseedStatic(name string) error
	StaticBlobs() map[string]struct{}
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
	blobs      BlobsControl
	static     StaticControl
	state      StateReader
	authz      *evaluator.Router
	shutdown   func()
	creds      *auth.NodeCredentials
	transport  TransportInfo
	metrics    MetricsSource
	connector  MeshConnector
	log        *zap.SugaredLogger
	signPriv   ed25519.PrivateKey
}

type Option func(*Service)

func WithShutdown(fn func()) Option                  { return func(s *Service) { s.shutdown = fn } }
func WithCredentials(c *auth.NodeCredentials) Option { return func(s *Service) { s.creds = c } }
func WithTransportInfo(t TransportInfo) Option       { return func(s *Service) { s.transport = t } }
func WithMetricsSource(m MetricsSource) Option       { return func(s *Service) { s.metrics = m } }
func WithMeshConnector(c MeshConnector) Option       { return func(s *Service) { s.connector = c } }
func WithAuthzRouter(r *evaluator.Router) Option     { return func(s *Service) { s.authz = r } }
func WithSigningKey(p ed25519.PrivateKey) Option     { return func(s *Service) { s.signPriv = p } }

func NewService(membership MembershipControl, placement PlacementControl, tunneling TunnelingControl, blobs BlobsControl, sc StaticControl, state StateReader, opts ...Option) *Service {
	s := &Service{
		membership: membership,
		placement:  placement,
		tunneling:  tunneling,
		blobs:      blobs,
		static:     sc,
		state:      state,
		log:        zap.S().Named("control"),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// authorise is the single spec_publish gate entry point for every
// admin-side publish RPC. A nil router (test constructor without the
// option) bypasses the gate — the gate itself is the policy surface, so
// "not configured" means "no policy enforced," not "deny." Denied
// publishes surface as codes.PermissionDenied; the detailed reason is
// logged locally and not leaked to the caller.
func (s *Service) authorise(ctx context.Context, rt evaluator.ResourceType, id string, props *structpb.Struct) error {
	if s.authz == nil {
		return nil
	}
	var resourceProps map[string]any
	if props != nil {
		resourceProps = props.AsMap()
	}
	req := evaluator.Request{
		Subject:  evaluator.Subject{Type: "admin", ID: "local"},
		Action:   evaluator.Action{Name: "publish"},
		Resource: evaluator.NewResource(rt, id, resourceProps),
	}
	if err := s.authz.Allow(ctx, evaluator.GateSpecPublish, req); err != nil {
		s.log.Warnw("spec_publish denied", "resource_type", string(rt), "resource_id", id, zap.Error(err))
		return status.Error(codes.PermissionDenied, "publish denied")
	}
	return nil
}

// signClaim produces a PublisherClaim for the given resource, binding
// properties to the publisher's signature. Returns nil when no signing
// key is wired (test constructor or a node without creds) — unsigned
// specs pass gossip-ingest verification untouched, so this degrades
// gracefully.
func (s *Service) signClaim(rt evaluator.ResourceType, id string, props *structpb.Struct) (*claimspkg.PublisherClaim, error) {
	if s.signPriv == nil {
		return nil, nil
	}
	sig, err := claimspkg.Sign(s.signPriv, rt, id, props)
	if err != nil {
		return nil, fmt.Errorf("sign publisher claim: %w", err)
	}
	var propMap map[string]any
	if props != nil {
		propMap = props.AsMap()
	}
	return claimspkg.New(propMap, sig), nil
}

type Server struct {
	svc   *Service
	gs    *grpc.Server
	log   *zap.SugaredLogger
	token string
}

func New(membership MembershipControl, placement PlacementControl, tunneling TunnelingControl, blobs BlobsControl, sc StaticControl, state StateReader, opts ...Option) *Server {
	svc := NewService(membership, placement, tunneling, blobs, sc, state, opts...)
	s := &Server{
		svc: svc,
		log: zap.S().Named("grpc"),
	}
	s.gs = grpc.NewServer(
		grpc.UnaryInterceptor(s.authInterceptor),
		grpc.StreamInterceptor(s.streamAuthInterceptor),
	)
	controlv1.RegisterControlServiceServer(s.gs, svc)
	return s
}

// SetToken installs the shared secret required on non-unix control RPCs.
// Unix socket connections bypass this check. Must be called before Start
// or StartTCP so every request goes through a configured interceptor.
func (s *Server) SetToken(token string) { s.token = token }

// checkToken is a no-op when the token is empty; unix callers always bypass.
func (s *Server) checkToken(ctx context.Context) error {
	if s.token == "" {
		return nil
	}
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil && p.Addr.Network() == "unix" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get(ControlTokenMetadataKey)
	if len(vals) == 0 || subtle.ConstantTimeCompare([]byte(vals[0]), []byte(s.token)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid control token")
	}
	return nil
}

func (s *Server) authInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := s.checkToken(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (s *Server) streamAuthInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.checkToken(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
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

	return s.Serve(l)
}

// StartTCP serves the control API on the given TCP address. Blocks until
// the listener closes. When a token is configured via SetToken, non-unix
// callers must present it via the x-pln-token metadata header.
func (s *Server) StartTCP(addr string) error {
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		return fmt.Errorf("control tcp listen: %w", err)
	}
	s.log.Infow("control tcp listener", "addr", l.Addr().String(), "auth", s.token != "")
	return s.Serve(l)
}

// Serve runs the gRPC handler on an already-established listener. All public
// Start/StartTCP paths funnel through here; tests use it to drive the server
// on an ephemeral listener.
func (s *Server) Serve(l net.Listener) error { return s.gs.Serve(l) }

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
		Sites:        buildStaticSummaries(snap),
		Blobs:        s.buildBlobSummaries(snap),
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
		TrafficRateIn:      in,
		TrafficRateOut:     out,
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
			TrafficRateIn:      in,
			TrafficRateOut:     outBytes,
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
			ws.MinReplicas = sv.Spec.MinReplicas
			ws.Spread = sv.Spec.Spread
		}
		out = append(out, ws)
	}

	for hash, sv := range snap.Specs {
		if _, ok := seen[hash]; ok {
			continue
		}
		ws := &controlv1.WorkloadSummary{
			Hash:           hash,
			Name:           sv.Spec.Name,
			MinReplicas:    sv.Spec.MinReplicas,
			Spread:         sv.Spec.Spread,
			ActiveReplicas: uint32(len(snap.Claims[hash])),
		}
		if info, ok := pinfo[hash]; ok {
			ws.EffectiveTarget = info.EffectiveTarget
			ws.Pressure = float32(info.SLOBurnRatio)
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
	slices.SortFunc(out.Workloads, func(a, b *controlv1.WorkloadSummary) int {
		if a.Name != b.Name {
			return cmp.Compare(a.Name, b.Name)
		}
		return cmp.Compare(a.Hash, b.Hash)
	})
	slices.SortFunc(out.Sites, func(a, b *controlv1.StaticSummary) int {
		return cmp.Compare(a.Name, b.Name)
	})
	slices.SortFunc(out.Blobs, func(a, b *controlv1.BlobSummary) int {
		if a.Replicas != b.Replicas {
			return cmp.Compare(b.Replicas, a.Replicas)
		}
		return cmp.Compare(a.Hash, b.Hash)
	})
}

func (s *Service) RegisterService(ctx context.Context, req *controlv1.RegisterServiceRequest) (*controlv1.RegisterServiceResponse, error) {
	name := serviceNameOrDefault(req.GetName(), req.Port)
	if err := s.authorise(ctx, evaluator.ResourceService, name, nil); err != nil {
		return nil, err
	}
	if err := s.tunneling.ExposeService(req.Port, name, state.NormaliseProtocol(req.GetProtocol())); err != nil {
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
	boundPort, err := s.tunneling.Connect(ctx, peerKey, req.GetRemotePort(), req.GetLocalPort(), state.NormaliseProtocol(req.GetProtocol()))
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

// CheckPolicy runs the request through the live router and returns the
// decision without dispatching the primitive — a policy-authoring aid. With
// no router configured, the answer is always allow.
func (s *Service) CheckPolicy(ctx context.Context, req *controlv1.CheckPolicyRequest) (*controlv1.CheckPolicyResponse, error) {
	gate := evaluator.GateName(req.GetGate())
	if !gate.Valid() {
		return nil, status.Errorf(codes.InvalidArgument, "unknown gate %q", req.GetGate())
	}
	if s.authz == nil {
		return &controlv1.CheckPolicyResponse{Allow: true, Reason: "no router configured; allow-all"}, nil
	}
	authReq := evaluator.Request{
		Subject: evaluator.Subject{
			Type:       req.GetSubject().GetType(),
			ID:         req.GetSubject().GetId(),
			Properties: req.GetSubject().GetProperties().AsMap(),
		},
		Action: evaluator.Action{
			Name:       req.GetAction().GetName(),
			Properties: req.GetAction().GetProperties().AsMap(),
		},
		Resource: evaluator.NewResource(
			evaluator.ResourceType(req.GetResource().GetType()),
			req.GetResource().GetId(),
			req.GetResource().GetProperties().AsMap(),
		),
		Context: req.GetContext().AsMap(),
	}
	err := s.authz.Allow(ctx, gate, authReq)
	if err == nil {
		return &controlv1.CheckPolicyResponse{Allow: true}, nil
	}
	var denied *evaluator.DeniedError
	if errors.As(err, &denied) {
		return &controlv1.CheckPolicyResponse{Allow: false, Reason: denied.Reason}, nil
	}
	return &controlv1.CheckPolicyResponse{Allow: false, Reason: err.Error()}, nil
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

func (s *Service) SeedWorkload(stream grpc.ClientStreamingServer[controlv1.SeedWorkloadRequest, controlv1.SeedWorkloadResponse]) error {
	if s.creds == nil || s.creds.DelegationKey() == nil {
		return status.Error(codes.PermissionDenied, "only admin nodes can seed workloads")
	}

	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "missing seed header")
		}
		return status.Error(codes.InvalidArgument, "receive seed header")
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "first message must carry header")
	}

	// Gate on the header before consuming the WASM chunk stream so a
	// denied caller can't force the daemon to buffer the upload.
	if err := s.authorise(stream.Context(), evaluator.ResourceSeed, header.GetName(), header.GetProperties()); err != nil {
		return err
	}

	var buf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return status.Error(codes.InvalidArgument, "receive seed chunk")
		}
		chunk := msg.GetChunk()
		if chunk == nil {
			return status.Error(codes.InvalidArgument, "expected chunk after header")
		}
		buf.Write(chunk)
	}

	wasmBytes := buf.Bytes()
	if len(wasmBytes) == 0 {
		return status.Error(codes.InvalidArgument, "empty workload binary")
	}

	h := sha256.Sum256(wasmBytes)
	hash := hex.EncodeToString(h[:])

	name := header.GetName()
	if name == "" {
		name = hash
	}

	claim, err := s.signClaim(evaluator.ResourceSeed, hash, header.GetProperties())
	if err != nil {
		return s.fail(err, "sign workload claim")
	}
	spec := state.WorkloadSpec{
		Hash:        hash,
		Name:        name,
		MinReplicas: header.GetMinReplicas(),
		MemoryBytes: header.GetMemoryBytes(),
		Timeout:     time.Duration(header.GetTimeoutMs()) * time.Millisecond,
		Spread:      header.GetSpread(),
		LatencySLO:  time.Duration(header.GetLatencySloMs()) * time.Millisecond,
		Claim:       claim,
	}
	if err := s.placement.Seed(wasmBytes, spec); err != nil {
		if errors.Is(err, placement.ErrCompile) {
			s.log.Warnw("seed workload failed", "name", name, "hash", types.ShortHash(hash), "err", err)
			return status.Error(codes.InvalidArgument, "failed to compile workload")
		}
		return s.fail(err, "failed to seed workload")
	}

	return stream.SendAndClose(&controlv1.SeedWorkloadResponse{Hash: hash, Name: name})
}

func (s *Service) FetchBlob(ctx context.Context, req *controlv1.FetchBlobRequest) (*controlv1.FetchBlobResponse, error) {
	hash := req.GetHash()
	var peers []types.PeerKey
	if pub := req.GetPeerPub(); len(pub) > 0 {
		peers = []types.PeerKey{types.PeerKeyFromBytes(pub)}
	} else {
		peers = s.state.Snapshot().PeersWithBlob(hash)
		if len(peers) == 0 {
			return nil, status.Error(codes.NotFound, "no peers advertise blob")
		}
	}
	if err := s.blobs.Fetch(ctx, hash, peers); err != nil {
		s.log.Warnw("fetch blob failed", "hash", types.ShortHash(hash), "err", err)
		return nil, status.Error(codes.NotFound, "fetch blob")
	}
	return &controlv1.FetchBlobResponse{}, nil
}

func (s *Service) UploadBlob(stream grpc.ClientStreamingServer[controlv1.UploadBlobRequest, controlv1.UploadBlobResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "missing upload header")
		}
		return status.Error(codes.InvalidArgument, "receive upload header")
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "first message must carry header")
	}

	// Anonymous uploads (no name) write local CAS only and stay
	// ungated — the content is addressable but not advertised, so
	// there's nothing for a publish policy to match on. Named
	// uploads go through the spec_publish gate before the chunk loop
	// so denial stops the upload early.
	if name := header.GetName(); name != "" {
		if err := s.authorise(stream.Context(), evaluator.ResourceBlob, name, header.GetProperties()); err != nil {
			return err
		}
	}

	var buf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return status.Error(codes.InvalidArgument, "receive upload chunk")
		}
		chunk := msg.GetChunk()
		if chunk == nil {
			return status.Error(codes.InvalidArgument, "expected chunk after header")
		}
		buf.Write(chunk)
	}

	hash, err := s.blobs.Put(&buf)
	if err != nil {
		return s.fail(err, "upload blob")
	}

	if name := header.GetName(); name != "" {
		claim, err := s.signClaim(evaluator.ResourceBlob, hash, header.GetProperties())
		if err != nil {
			return s.fail(err, "sign blob claim")
		}
		if err := s.blobs.SetName(hash, name, claim); err != nil {
			s.log.Warnw("set blob name failed", "hash", types.ShortHash(hash), "name", name, "err", err)
			return status.Error(codes.Internal, "set blob name")
		}
	}

	return stream.SendAndClose(&controlv1.UploadBlobResponse{Hash: hash})
}

func (s *Service) RemoveBlob(_ context.Context, req *controlv1.RemoveBlobRequest) (*controlv1.RemoveBlobResponse, error) {
	if err := s.blobs.Remove(req.GetHash()); err != nil {
		if errors.Is(err, blobs.ErrNotLocal) {
			return nil, status.Error(codes.FailedPrecondition, "blob not present locally")
		}
		s.log.Warnw("remove blob failed", "hash", types.ShortHash(req.GetHash()), "err", err)
		return nil, status.Error(codes.Internal, "remove blob")
	}
	return &controlv1.RemoveBlobResponse{}, nil
}

func (s *Service) SeedStatic(ctx context.Context, req *controlv1.SeedStaticRequest) (*controlv1.SeedStaticResponse, error) {
	if s.creds == nil || s.creds.DelegationKey() == nil {
		return nil, status.Error(codes.PermissionDenied, "only admin nodes can seed static sites")
	}
	if err := s.authorise(ctx, evaluator.ResourceStatic, req.GetName(), req.GetProperties()); err != nil {
		return nil, err
	}
	claim, err := s.signClaim(evaluator.ResourceStatic, req.GetName(), req.GetProperties())
	if err != nil {
		return nil, s.fail(err, "sign static claim")
	}
	if err := s.static.SeedStatic(req.GetName(), req.GetManifestDigest(), req.GetMinReplicas(), claim); err != nil {
		return nil, s.fail(err, "seed static")
	}
	return &controlv1.SeedStaticResponse{}, nil
}

func (s *Service) UnseedStatic(_ context.Context, req *controlv1.UnseedStaticRequest) (*controlv1.UnseedStaticResponse, error) {
	if s.creds == nil || s.creds.DelegationKey() == nil {
		return nil, status.Error(codes.PermissionDenied, "only admin nodes can unseed static sites")
	}
	if err := s.static.UnseedStatic(req.GetName()); err != nil {
		return nil, s.fail(err, "unseed static")
	}
	return &controlv1.UnseedStaticResponse{}, nil
}

func (s *Service) ListStatic(_ context.Context, _ *controlv1.ListStaticRequest) (*controlv1.ListStaticResponse, error) {
	return &controlv1.ListStaticResponse{Sites: buildStaticSummaries(s.state.Snapshot())}, nil
}

func buildStaticSummaries(snap state.Snapshot) []*controlv1.StaticSummary {
	var capacity uint32
	for _, nv := range snap.Nodes {
		if nv.CanServeStatic {
			capacity++
		}
	}
	out := make([]*controlv1.StaticSummary, 0, len(snap.StaticSpecs))
	for name, spec := range snap.StaticSpecs {
		digest, _ := hex.DecodeString(spec.Spec.ManifestDigest)
		claimants := snap.StaticClaims[name]
		_, local := claimants[snap.LocalID]
		summary := &controlv1.StaticSummary{
			Name:            name,
			ManifestDigest:  digest,
			MinReplicas:     spec.Spec.MinReplicas,
			Publisher:       &controlv1.NodeRef{PeerPub: spec.Publisher.Bytes()},
			Local:           local,
			ServingCapacity: capacity,
		}
		for pk := range claimants {
			summary.Claimants = append(summary.Claimants, &controlv1.NodeRef{PeerPub: pk.Bytes()})
		}
		out = append(out, summary)
	}
	return out
}

// buildBlobSummaries tags unreferenced blobs as orphan and restricts holders
// to live peers — stale BlobAvailability from offline peers would inflate
// replicas and surface phantom orphans. Workload- and static-backed blobs
// are surfaced under their own summaries and skipped here.
func (s *Service) buildBlobSummaries(snap state.Snapshot) []*controlv1.BlobSummary {
	liveSet := make(map[types.PeerKey]struct{}, len(snap.PeerKeys))
	for _, pk := range snap.PeerKeys {
		liveSet[pk] = struct{}{}
	}
	counts := make(map[string]uint32)
	for pk, nv := range snap.Nodes {
		if _, live := liveSet[pk]; !live {
			continue
		}
		for hash := range nv.Blobs {
			counts[hash]++
		}
	}
	staticBlobs := s.static.StaticBlobs()
	localBlobs := snap.Nodes[snap.LocalID].Blobs
	out := make([]*controlv1.BlobSummary, 0, len(counts))
	for hash, n := range counts {
		if _, ok := snap.Specs[hash]; ok {
			continue
		}
		if _, ok := staticBlobs[hash]; ok {
			continue
		}
		_, local := localBlobs[hash]
		summary := &controlv1.BlobSummary{
			Hash:     hash,
			Replicas: n,
			Local:    local,
		}
		if view, ok := snap.BlobSpecs[hash]; ok {
			summary.Name = view.Spec.Name
			summary.Publisher = &controlv1.NodeRef{PeerPub: view.Publisher.Bytes()}
		} else {
			summary.Orphan = true
		}
		out = append(out, summary)
	}
	return out
}

func (s *Service) UnseedWorkload(_ context.Context, req *controlv1.UnseedWorkloadRequest) (*controlv1.UnseedWorkloadResponse, error) {
	if s.creds == nil || s.creds.DelegationKey() == nil {
		return nil, status.Error(codes.PermissionDenied, "only admin nodes can unseed workloads")
	}
	if err := s.placement.Unseed(req.GetHash()); err != nil {
		return nil, s.fail(err, "unseed workload failed", "hash", req.GetHash())
	}
	return &controlv1.UnseedWorkloadResponse{}, nil
}

func (s *Service) CallWorkload(ctx context.Context, req *controlv1.CallWorkloadRequest) (*controlv1.CallWorkloadResponse, error) {
	hash, function := req.GetHash(), req.GetFunction()
	if uri := req.GetUri(); uri != "" {
		parsed, err := wasm.ParseURI(uri)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if parsed.Scheme != wasm.SchemeSeed {
			return nil, status.Errorf(codes.InvalidArgument, "unsupported uri scheme %q; CallWorkload only supports 'seed'", parsed.Scheme)
		}
		hash, function = parsed.Name, parsed.Function
	}
	if hash == "" || function == "" {
		return nil, status.Error(codes.InvalidArgument, "either (hash, function) or uri must be set")
	}

	ctx = s.localCallerContext(ctx)
	output, err := s.placement.Call(ctx, hash, function, req.GetInput())
	if err != nil {
		s.log.Warnw("call workload failed", "hash", hash, "function", function, "err", err)
		switch {
		case errors.Is(err, wasm.ErrTargetNotFound):
			return nil, status.Error(codes.NotFound, "no such workload")
		case errors.Is(err, placement.ErrNotRunning):
			return nil, status.Error(codes.NotFound, "workload not running on any reachable node")
		case errors.Is(err, placement.ErrCycle):
			return nil, status.Error(codes.FailedPrecondition, "call cycle detected")
		case errors.Is(err, context.DeadlineExceeded):
			return nil, status.Error(codes.DeadlineExceeded, "workload invocation timed out")
		default:
			return nil, status.Error(codes.Internal, "workload invocation failed")
		}
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
		in += ts.RateIn
		out += ts.RateOut
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
	if s == placement.StatusRunning {
		return controlv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
	}
	return controlv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED
}
