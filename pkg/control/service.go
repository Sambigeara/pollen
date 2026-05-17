// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package control

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	admissionv1 "github.com/sambigeara/pollen/api/genpb/pollen/admission/v1"
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	factv1 "github.com/sambigeara/pollen/api/genpb/pollen/fact/v1"
	identityv1 "github.com/sambigeara/pollen/api/genpb/pollen/identity/v1"
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/blobs"
	"github.com/sambigeara/pollen/pkg/identity"
	"github.com/sambigeara/pollen/pkg/membership"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/placement"
	"github.com/sambigeara/pollen/pkg/plnfs"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/tunneling"
	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/view"
	"github.com/sambigeara/pollen/pkg/wasm"
	"github.com/sambigeara/pollen/pkg/wire"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type Metrics struct {
	CertExpirySeconds  float64
	PunchAttempts      uint64
	PunchFailures      uint64
	SmoothedVivaldiErr float64
	VivaldiSamples     uint64
	EagerSyncs         uint64
	EagerSyncFailures  uint64
}

type MembershipControl interface {
	DenyPeer(key types.PeerKey) error
	IssueGrant(ctx context.Context, peerKey types.PeerKey, caps *identityv1.Capabilities, budget *identityv1.Budget) (*identityv1.Grant, error)
	RenewalFailing() bool
}

type PlacementControl interface {
	Seed(binary []byte, spec state.WorkloadSpec, policy *admissionv1.Predicate) error
	SeedPresigned(binary []byte, spec state.WorkloadSpec, presignedFact *factv1.Fact) error
	Unseed(hash string) error
	UnseedPresigned(hash string, presignedFact *factv1.Fact) error
	Call(ctx context.Context, hash, fn string, input []byte) ([]byte, error)
	Status() []placement.WorkloadSummary
}

type TunnelingControl interface {
	Connect(ctx context.Context, peer types.PeerKey, remotePort, localPort uint32, protocol statev1.ServiceProtocol) (uint32, error)
	Disconnect(service string) error
	ExposeService(port uint32, name string, protocol statev1.ServiceProtocol, policy *admissionv1.Predicate) error
	UnexposeService(name string) error
	ListConnections() []tunneling.ConnectionInfo
}

type StateReader interface {
	Snapshot() state.Snapshot
}

type BlobsControl interface {
	Fetch(ctx context.Context, hash string, peers []types.PeerKey) error
	FetchPlaintext(ctx context.Context, hash string) (io.ReadCloser, error)
	Put(r io.Reader) (string, error)
	Publish(hash, name string, policy *admissionv1.Predicate) error
	PublishPresigned(hash, name string, presignedFact *factv1.Fact) error
	Remove(hash string) error
	RemovePresigned(hash string, presignedFact *factv1.Fact) error
}

type StaticControl interface {
	SeedStatic(name string, manifestDigest []byte, policy *admissionv1.Predicate) error
	SeedStaticPresigned(name string, manifestDigest []byte, presignedFact *factv1.Fact) error
	UnseedStatic(name string) error
	UnseedStaticPresigned(name string, presignedFact *factv1.Fact) error
	StaticBlobs() map[string]struct{}
}

type TransportInfo interface {
	PeerStateCounts() transport.PeerStateCounts
	GetActivePeerAddress(types.PeerKey) (*net.UDPAddr, bool)
	PeerRTT(types.PeerKey) (time.Duration, bool)
}

type MetricsSource interface {
	ControlMetrics() Metrics
}

type MeshConnector interface {
	Connect(ctx context.Context, peer types.PeerKey, addrs []netip.AddrPort) error
}

// OperatorGate authorises Connect and Fetch. Workload invocations are
// gated in placement.Call because that path catches remote dispatch and
// seed-to-seed tail calls as well as operator RPCs.
//
// Connect and Fetch take the caller's grant directly so wire-mode
// tenants (whose grants aren't gossiped into the mesh snapshot) can be
// authorised against their own authority. Mesh-peer call sites resolve
// the grant via LookupGrant from the snapshot before calling.
type OperatorGate interface {
	Connect(caller *identityv1.Grant, hostPeer types.PeerKey, port uint32) error
	Fetch(caller *identityv1.Grant, hash string) error
}

var _ controlv1.ControlServiceServer = (*Service)(nil)

type Service struct {
	controlv1.UnimplementedControlServiceServer
	state        StateReader
	metrics      MetricsSource
	tunneling    TunnelingControl
	blobs        BlobsControl
	static       StaticControl
	membership   MembershipControl
	gate         OperatorGate
	connector    MeshConnector
	placement    PlacementControl
	transport    TransportInfo
	creds        *identity.Credentials
	shutdown     func()
	log          *zap.SugaredLogger
	staticDomain string
	signPriv     ed25519.PrivateKey
}

// grantCanPublish reports whether a grant permits publishing any
// resource kind. Per-kind enforcement is the admission pipeline's job;
// this coarse predicate only populates the informational CanPublish
// field in status and certificate summaries.
func grantCanPublish(g *identityv1.Grant) bool {
	p := g.GetClaims().GetCapabilities().GetPublish()
	return p.GetFunctions() || p.GetBlobs() || p.GetSites() || p.GetServices()
}

func (s *Service) localPeerKey() types.PeerKey {
	if s.creds == nil {
		return types.PeerKey{}
	}
	grant := s.creds.Grant()
	if grant == nil {
		return types.PeerKey{}
	}
	return types.PeerKeyFromBytes(grant.GetClaims().GetSubjectPub())
}

type Option func(*Service)

func WithShutdown(fn func()) Option                  { return func(s *Service) { s.shutdown = fn } }
func WithCredentials(c *identity.Credentials) Option { return func(s *Service) { s.creds = c } }
func WithTransportInfo(t TransportInfo) Option       { return func(s *Service) { s.transport = t } }
func WithMetricsSource(m MetricsSource) Option       { return func(s *Service) { s.metrics = m } }
func WithMeshConnector(c MeshConnector) Option       { return func(s *Service) { s.connector = c } }
func WithOperatorGate(g OperatorGate) Option         { return func(s *Service) { s.gate = g } }
func WithSignPriv(priv ed25519.PrivateKey) Option    { return func(s *Service) { s.signPriv = priv } }
func WithStaticDomain(d string) Option {
	return func(s *Service) {
		if d != "" && d[0] != '.' {
			d = "." + d
		}
		s.staticDomain = strings.ToLower(d)
	}
}

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

type Server struct {
	svc   *Service
	gs    *grpc.Server
	tlsGS *grpc.Server
	log   *zap.SugaredLogger
}

func New(membership MembershipControl, placement PlacementControl, tunneling TunnelingControl, blobs BlobsControl, sc StaticControl, state StateReader, opts ...Option) *Server {
	svc := NewService(membership, placement, tunneling, blobs, sc, state, opts...)
	s := &Server{
		svc: svc,
		log: zap.S().Named("grpc"),
	}
	s.gs = grpc.NewServer(
		grpc.ChainUnaryInterceptor(s.callerInterceptor),
		grpc.ChainStreamInterceptor(s.streamCallerInterceptor),
	)
	controlv1.RegisterControlServiceServer(s.gs, svc)
	return s
}

// callerInterceptor injects the resolved caller identity.Principal into
// the request context for every unary RPC, resolved by injectCaller from
// whatever identity the inbound transport carries.
func (s *Server) callerInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return handler(s.injectCaller(ctx), req)
}

func (s *Server) streamCallerInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := s.injectCaller(ss.Context())
	return handler(srv, &callerStream{ServerStream: ss, ctx: ctx})
}

func (s *Server) injectCaller(ctx context.Context) context.Context {
	if p := identity.PrincipalFromGrant(wire.CallerGrantFromContext(ctx)); p.Valid() {
		return auth.WithCaller(ctx, p)
	}
	// Only fall back to the daemon's own grant when the inbound
	// transport is a local credential (unix socket). On TLS paths a
	// missing peer cert means the mTLS handshake didn't populate
	// peer.AuthInfo as expected — leaking daemon-self privileges to
	// such a caller would erase the wire-mode security boundary.
	if !isLocalCallerCtx(ctx) {
		return ctx
	}
	if s.svc == nil || s.svc.creds == nil {
		return ctx
	}
	return auth.WithCaller(ctx, identity.PrincipalFromGrant(s.svc.creds.Grant()))
}

// isLocalCallerCtx reports whether the inbound RPC arrived over the
// local unix socket. We detect it by inspecting peer.Peer's addr: TLS
// streams expose a credentials.TLSInfo with a SAN-bearing AuthInfo and
// always have a network addr; the unix-socket path uses a stdlib
// *net.UnixAddr (or no addr at all for in-process tests).
func isLocalCallerCtx(ctx context.Context) bool {
	p, ok := peer.FromContext(ctx)
	if !ok {
		// In-process tests dial via grpc.NewServer in-process without
		// populating peer.Peer. Treat the absence as local so the
		// existing test surface keeps working; production transports
		// always populate peer.Peer.
		return true
	}
	if _, isTLS := p.AuthInfo.(credentials.TLSInfo); isTLS {
		return false
	}
	if _, ok := p.Addr.(*net.UnixAddr); ok {
		return true
	}
	return p.Addr == nil
}

type callerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (c *callerStream) Context() context.Context { return c.ctx }

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

// StartTLS opens a public TLS+mTLS listener for the control RPC at the
// given address. Inbound clients must present an x509 cert whose pollen
// session extension chains back to the cluster root.
func (s *Server) StartTLS(addr string) error {
	tcp, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		return fmt.Errorf("control tls listen: %w", err)
	}
	return s.ServeTLS(tcp)
}

// ServeTLS wraps a pre-bound TCP listener with the control TLS config
// and serves it. Useful when the caller needs the bound address before
// serving (dynamic-port tests, integration smokes).
func (s *Server) ServeTLS(l net.Listener) error {
	if s.svc == nil || s.svc.creds == nil {
		return errors.New("control tls: no credentials configured")
	}
	if len(s.svc.signPriv) == 0 {
		return errors.New("control tls: no signing private key configured")
	}
	if len(s.svc.creds.RootPub()) == 0 {
		return errors.New("control tls: credentials missing root pub")
	}
	session, err := s.svc.creds.EnsureFreshSession(time.Now(), controlTLSIdentityTTL, controlTLSIdentityTTL/2) //nolint:mnd
	if err != nil {
		return fmt.Errorf("control tls session: %w", err)
	}
	serverCert, err := transport.GenerateIdentityCert(s.svc.signPriv, session, controlTLSIdentityTTL)
	if err != nil {
		return fmt.Errorf("control tls identity cert: %w", err)
	}
	denied := func(sub []byte) bool {
		return s.svc.state.Snapshot().IsDenied(types.PeerKeyFromBytes(sub))
	}
	cfg := wire.ServerTLSConfig(serverCert, s.svc.creds.RootPub(), denied)
	// gRPC's TLS credentials drive both the handshake and the population
	// of peer.AuthInfo; pre-wrapping the listener with tls.NewListener
	// leaves AuthInfo nil, which strips the caller cert from every RPC.
	tlsGS := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(cfg)),
		grpc.ChainUnaryInterceptor(s.callerInterceptor),
		grpc.ChainStreamInterceptor(s.streamCallerInterceptor),
	)
	controlv1.RegisterControlServiceServer(tlsGS, s.svc)
	s.tlsGS = tlsGS
	s.log.Infow("control tls listener", "addr", l.Addr().String())
	return tlsGS.Serve(l)
}

const controlTLSIdentityTTL = 24 * time.Hour

func (s *Server) Serve(l net.Listener) error { return s.gs.Serve(l) }

func (s *Server) Stop() {
	if s.tlsGS != nil {
		s.tlsGS.GracefulStop()
	}
	s.gs.GracefulStop()
}
func (s *Server) Service() *Service { return s.svc }

// Handshake reports this daemon's protocol-version range. It is
// deliberately unauthenticated and side-effect free: the client decides
// compatibility from the returned range, so a mismatch becomes an
// explicit "out of date" message instead of an opaque failure.
func (s *Service) Handshake(_ context.Context, _ *controlv1.HandshakeRequest) (*controlv1.HandshakeResponse, error) {
	return &controlv1.HandshakeResponse{
		ServerMin: wire.ProtocolMin,
		ServerMax: wire.ProtocolMax,
	}, nil
}

func (s *Service) Shutdown(ctx context.Context, _ *controlv1.ShutdownRequest) (*controlv1.ShutdownResponse, error) {
	if err := s.requireDaemonSelf(ctx, "shutdown is daemon-self only"); err != nil {
		return nil, err
	}
	if s.shutdown == nil {
		return nil, status.Error(codes.FailedPrecondition, "shutdown callback not configured")
	}
	go s.shutdown()
	return &controlv1.ShutdownResponse{}, nil
}

func (s *Service) GetBootstrapInfo(_ context.Context, _ *controlv1.GetBootstrapInfoRequest) (*controlv1.GetBootstrapInfoResponse, error) {
	snap := s.state.Snapshot()
	return &controlv1.GetBootstrapInfoResponse{
		Peers: pickBootstrapPeers(snap),
	}, nil
}

func (s *Service) GetStatus(ctx context.Context, _ *controlv1.GetStatusRequest) (*controlv1.GetStatusResponse, error) {
	snap := s.state.Snapshot()
	connections := s.tunneling.ListConnections()
	lens := s.callerPrincipal(ctx)
	scoped := view.Project(snap, lens)
	operator := s.operatorRequest(ctx, lens)

	out := &controlv1.GetStatusResponse{
		Degraded:      s.isDegraded(),
		Certificates:  s.buildCertificates(ctx, snap, lens),
		Self:          s.buildSelfSummary(snap, lens, operator, connections),
		Nodes:         s.buildNodeSummaries(snap, scoped, lens, operator, connections),
		Services:      buildServiceSummaries(scoped.Nodes, lens),
		Connections:   buildConnectionSummaries(scoped.Nodes, connections),
		Workloads:     s.buildWorkloadSummaries(snap, scoped, lens),
		Sites:         s.buildStaticSummaries(snap, scoped, operator),
		Blobs:         s.buildBlobSummaries(snap, scoped, lens, operator),
		GatewayDomain: strings.TrimPrefix(s.staticDomain, "."),
	}

	sortStatusResponse(out)
	return out, nil
}

func (s *Service) Inspect(ctx context.Context, req *controlv1.InspectRequest) (*controlv1.InspectResponse, error) {
	lens := s.callerPrincipal(ctx)
	switch t := req.GetTarget().(type) {
	case *controlv1.InspectRequest_NodePub:
		peerKey := types.PeerKeyFromBytes(t.NodePub)
		snap := s.state.Snapshot()
		scoped := view.Project(snap, lens)
		detail, err := s.inspectNode(snap, scoped, peerKey, lens, s.operatorRequest(ctx, lens))
		if err != nil {
			return nil, err
		}
		return &controlv1.InspectResponse{Detail: &controlv1.InspectResponse_Node{Node: detail}}, nil
	case *controlv1.InspectRequest_WorkloadHash,
		*controlv1.InspectRequest_StaticName,
		*controlv1.InspectRequest_BlobHash,
		*controlv1.InspectRequest_Service:
		return nil, status.Error(codes.Unimplemented, "resource inspect not yet implemented")
	case nil:
		return nil, status.Error(codes.InvalidArgument, "inspect target required")
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unrecognised inspect target type %T", req.GetTarget())
	}
}

func (s *Service) inspectNode(snap state.Snapshot, scoped view.ScopedView, peerKey types.PeerKey, lens view.Lens, operator bool) (*controlv1.NodeDetail, error) {
	nv, ok := scoped.Nodes[peerKey]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no peer %s in cluster view", peerKey.String())
	}

	connections := s.tunneling.ListConnections()
	var summary *controlv1.NodeSummary
	if peerKey == snap.LocalID && operator {
		summary = s.buildSelfSummary(snap, lens, operator, connections)
	} else {
		summary = s.buildPeerSummary(snap, peerKey, nv, connections)
	}
	if !lens.Admin() {
		redactNodeTelemetry(summary)
	}

	detail := &controlv1.NodeDetail{
		Summary:     summary,
		Cert:        nodeCertInfo(nv.Grant, time.Now(), snap.IsDenied(peerKey)),
		IssuerChain: issuerChain(nv.Grant),
	}
	// Mesh topology (NAT class, Vivaldi position, host memory, reachable
	// peers) is cluster-operator data. A tenant inspecting a node that
	// merely holds its fact must not learn the host's mesh position or
	// which other peers it can reach.
	if lens.Admin() {
		detail.NatType = natTypeLabel(nv.NatType)
		detail.MemTotalBytes = nv.MemTotalBytes
		if nv.VivaldiCoord != nil {
			detail.VivaldiX = nv.VivaldiCoord.X
			detail.VivaldiY = nv.VivaldiCoord.Y
			detail.VivaldiHeight = nv.VivaldiCoord.Height
			detail.VivaldiError = nv.VivaldiErr
		}
		detail.ReachablePeers = sortedReachableRefs(nv.Reachable)
	}
	fillPublishedResources(detail, scoped, nv, peerKey, lens)
	return detail, nil
}

func sortedReachableRefs(reachable map[types.PeerKey]struct{}) []*controlv1.NodeRef {
	if len(reachable) == 0 {
		return nil
	}
	keys := make([]types.PeerKey, 0, len(reachable))
	for pk := range reachable {
		keys = append(keys, pk)
	}
	slices.SortFunc(keys, types.PeerKey.Compare)
	refs := make([]*controlv1.NodeRef, len(keys))
	for i, k := range keys {
		refs[i] = &controlv1.NodeRef{PeerPub: k.Bytes()}
	}
	return refs
}

// fillPublishedResources populates the published_* slices on detail from
// the already-projected view, narrowed to resources whose publisher is
// peerKey. Because scoped is the caller's projection, a non-admin caller
// only ever sees resources it published itself, so inspecting a shared
// holder never enumerates another tenant's facts. The anonymous-publish
// fallback labels hash-only entries by their hash.
func fillPublishedResources(detail *controlv1.NodeDetail, scoped view.ScopedView, nv state.NodeView, peerKey types.PeerKey, lens view.Lens) {
	for name, svc := range nv.Services {
		if !lens.Admin() && (!hasServicePublisher(svc) || !lens.Permits(servicePublisher(svc))) {
			continue
		}
		detail.PublishedServices = append(detail.PublishedServices, name)
	}
	slices.Sort(detail.PublishedServices)

	for hash, sv := range scoped.Workloads {
		if sv.Publisher != peerKey {
			continue
		}
		label := sv.Spec.Name
		if label == "" {
			label = hash
		}
		detail.PublishedWorkloads = append(detail.PublishedWorkloads, label)
	}
	slices.Sort(detail.PublishedWorkloads)

	for name, sv := range scoped.Statics {
		if sv.Publisher == peerKey {
			detail.PublishedStatics = append(detail.PublishedStatics, name)
		}
	}
	slices.Sort(detail.PublishedStatics)

	for digest, bv := range scoped.Blobs {
		if bv.Publisher != peerKey {
			continue
		}
		label := bv.Spec.Name
		if label == "" {
			label = digest
		}
		detail.PublishedBlobs = append(detail.PublishedBlobs, label)
	}
	slices.Sort(detail.PublishedBlobs)
}

// buildPeerSummary builds a NodeSummary for one peer. It walks
// snap.PeerKeys and connections directly; callers iterating the full
// node set (buildNodeSummaries) precompute those into maps and use
// peerSummary instead to avoid quadratic cost.
func (s *Service) buildPeerSummary(snap state.Snapshot, peerKey types.PeerKey, nv state.NodeView, connections []tunneling.ConnectionInfo) *controlv1.NodeSummary {
	isLive := slices.Contains(snap.PeerKeys, peerKey)
	var tunnels uint32
	for _, c := range connections {
		if c.PeerID == peerKey {
			tunnels++
		}
	}
	return s.peerSummary(peerKey, nv, tunnels, isLive)
}

// peerSummary is the shared per-peer body used by buildNodeSummaries and
// buildPeerSummary. Tunnels and liveness are precomputed by the caller
// so this function carries no map lookups of its own.
func (s *Service) peerSummary(peerKey types.PeerKey, nv state.NodeView, tunnels uint32, isLive bool) *controlv1.NodeSummary {
	var isDirect bool
	var addr *net.UDPAddr
	if s.transport != nil {
		addr, isDirect = s.transport.GetActivePeerAddress(peerKey)
	}
	peerStatus := controlv1.NodeStatus_NODE_STATUS_OFFLINE
	addrStr := nodeViewAddr(nv)
	if isDirect {
		peerStatus = controlv1.NodeStatus_NODE_STATUS_ONLINE
		addrStr = addr.String()
	} else if isLive {
		peerStatus = controlv1.NodeStatus_NODE_STATUS_INDIRECT
	}
	in, outBytes := sumTraffic(nv.TrafficRates)
	ns := &controlv1.NodeSummary{
		Node:               &controlv1.NodeRef{PeerPub: peerKey.Bytes()},
		Name:               nv.Name,
		Status:             peerStatus,
		Addr:               addrStr,
		PubliclyAccessible: nv.PubliclyAccessible,
		TunnelCount:        tunnels,
		CpuPercent:         nv.CPUPercent,
		MemPercent:         nv.MemPercent,
		NumCpu:             nv.NumCPU,
		TrafficRateIn:      in,
		TrafficRateOut:     outBytes,
	}
	if isDirect && s.transport != nil {
		if rtt, ok := s.transport.PeerRTT(peerKey); ok {
			ns.LatencyMs = float64(rtt.Microseconds()) / 1000.0 //nolint:mnd
		}
	}
	return ns
}

// nodeCertInfo derives CertInfo from a peer's gossiped Grant. Health is
// computed against the grant's own deadline; the local-node version in
// buildCertificates uses the credentials store directly because it
// applies the grant-horizon warn/critical thresholds, which only matter
// for the local node's own grant. denied reflects whether the cluster
// has revoked this peer; callers must source it from the same snapshot
// they read the grant from.
func nodeCertInfo(grant *identityv1.Grant, now time.Time, denied bool) *controlv1.CertInfo {
	if grant == nil {
		return nil
	}
	claims := grant.GetClaims()
	caps := claims.GetCapabilities()
	dl := claims.GetGrantDeadlineUnix()
	health := controlv1.CertHealth_CERT_HEALTH_OK
	if denied || (dl > 0 && now.After(time.Unix(dl, 0))) {
		health = controlv1.CertHealth_CERT_HEALTH_EXPIRED
	}
	return &controlv1.CertInfo{
		NotBeforeUnix:     claims.GetNotBeforeUnix(),
		GrantDeadlineUnix: dl,
		Serial:            claims.GetSerial(),
		Health:            health,
		CanDelegate:       caps.GetCanDelegate(),
		CanAdmit:          caps.GetCanAdmit(),
		CanPublish:        grantCanPublish(grant),
		MaxDepth:          caps.GetMaxDepth(),
		Attributes:        caps.GetAttributes(),
		Denied:            denied,
	}
}

// issuerChain returns the grant chain root-down, ending at the peer
// immediately above the inspected node. Empty for a self-issued root.
//
// grant.Chain is a flattened leaf-to-root list (the issuer clears
// nested chains at issuance), so we iterate it in reverse to surface
// root first.
func issuerChain(grant *identityv1.Grant) []*controlv1.NodeRef {
	chain := grant.GetChain()
	if len(chain) == 0 {
		return nil
	}
	refs := make([]*controlv1.NodeRef, 0, len(chain))
	for i := len(chain) - 1; i >= 0; i-- {
		sub := chain[i].GetClaims().GetSubjectPub()
		refs = append(refs, &controlv1.NodeRef{PeerPub: bytes.Clone(sub)})
	}
	return refs
}

func natTypeLabel(t nat.Type) string {
	switch t {
	case nat.Easy:
		return "easy"
	case nat.Hard:
		return "hard"
	default:
		return ""
	}
}

// isDegraded reports the truthful pre-shutdown state: the node holds a
// horizon-bound grant and its most recent proactive renewal attempt
// failed, so it is acting before a hard stop. It is never a
// post-deadline grace: once the deadline passes membership shuts the
// node down, so there is no live post-deadline window to report.
// Admin/root grants carry no horizon and are never degraded.
func (s *Service) isDegraded() bool {
	if s.creds == nil || s.creds.Grant() == nil {
		return false
	}
	if s.creds.Grant().GetClaims().GetGrantDeadlineUnix() == 0 {
		return false
	}
	return s.membership.RenewalFailing()
}

// buildCertificates reports the credential the caller cares about. An
// admin operator or the daemon itself sees the serving node's own grant
// with renewal-horizon health, since that node is the one that renews. A
// wire tenant sees its OWN grant, never the serving daemon's: surfacing
// the daemon's credential to a tenant is both a leak and the wrong
// answer (a tenant wants its own expiry, not the host's).
func (s *Service) buildCertificates(ctx context.Context, snap state.Snapshot, lens view.Lens) []*controlv1.CertInfo {
	if s.operatorRequest(ctx, lens) {
		return s.localCertificates(snap)
	}
	ci := nodeCertInfo(s.callerPrincipal(ctx).Grant, time.Now(), snap.IsDenied(lens.Subject()))
	if ci == nil {
		return nil
	}
	return []*controlv1.CertInfo{ci}
}

func (s *Service) localCertificates(snap state.Snapshot) []*controlv1.CertInfo {
	if s.creds == nil || s.creds.Grant() == nil {
		return nil
	}
	grant := s.creds.Grant()
	claims := grant.GetClaims()
	caps := claims.GetCapabilities()
	health := controlv1.CertHealth_CERT_HEALTH_OK
	var remaining time.Duration
	if dl := claims.GetGrantDeadlineUnix(); dl > 0 {
		remaining = time.Until(time.Unix(dl, 0))
	} else {
		// Admin/root grants carry no horizon: always healthy.
		remaining = membership.GrantWarnThreshold + time.Hour
	}

	switch {
	case remaining <= 0:
		health = controlv1.CertHealth_CERT_HEALTH_EXPIRED
	case remaining <= membership.GrantWarnThreshold:
		health = controlv1.CertHealth_CERT_HEALTH_EXPIRING_SOON
	}

	return []*controlv1.CertInfo{{
		NotBeforeUnix:     claims.GetNotBeforeUnix(),
		GrantDeadlineUnix: claims.GetGrantDeadlineUnix(),
		Serial:            claims.GetSerial(),
		Health:            health,
		CanDelegate:       caps.GetCanDelegate(),
		CanAdmit:          caps.GetCanAdmit(),
		CanPublish:        grantCanPublish(grant),
		MaxDepth:          caps.GetMaxDepth(),
		Attributes:        caps.GetAttributes(),
		Denied:            snap.IsDenied(snap.LocalID),
	}}
}

func (s *Service) buildSelfSummary(snap state.Snapshot, lens view.Lens, operator bool, connections []tunneling.ConnectionInfo) *controlv1.NodeSummary {
	if operator {
		localID := snap.LocalID
		localNode := snap.Nodes[localID]
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
	// A wire tenant is not a mesh node; surface its own identity so the
	// status header is the caller, never the serving daemon.
	return &controlv1.NodeSummary{
		Node:   &controlv1.NodeRef{PeerPub: lens.Subject().Bytes()},
		Status: controlv1.NodeStatus_NODE_STATUS_OFFLINE,
	}
}

func (s *Service) buildNodeSummaries(snap state.Snapshot, scoped view.ScopedView, lens view.Lens, operator bool, connections []tunneling.ConnectionInfo) []*controlv1.NodeSummary {
	liveSet := make(map[types.PeerKey]struct{}, len(snap.PeerKeys))
	for _, pk := range snap.PeerKeys {
		liveSet[pk] = struct{}{}
	}

	tunnelCounts := make(map[types.PeerKey]uint32, len(connections))
	for _, c := range connections {
		tunnelCounts[c.PeerID]++
	}

	// The serving node is rendered as Self only on the operator/daemon
	// path. A tenant has no Self node, so the serving node (when it
	// holds the tenant's fact) belongs in the node list like any other
	// holder.
	skipSelf := operator
	out := make([]*controlv1.NodeSummary, 0, len(scoped.Nodes))
	for key, node := range scoped.Nodes {
		if skipSelf && key == snap.LocalID {
			continue
		}
		_, isLive := liveSet[key]
		ns := s.peerSummary(key, node, tunnelCounts[key], isLive)
		if !lens.Admin() {
			redactNodeTelemetry(ns)
		}
		out = append(out, ns)
	}
	return out
}

// redactNodeTelemetry strips a node's operational metrics from a summary
// shown to a non-admin caller. A tenant may see WHERE its facts run
// (identity, status, address) but not the host's load or topology, which
// would expose other tenants sharing the machine.
func redactNodeTelemetry(ns *controlv1.NodeSummary) {
	ns.CpuPercent = 0
	ns.MemPercent = 0
	ns.NumCpu = 0
	ns.TrafficRateIn = 0
	ns.TrafficRateOut = 0
	ns.LatencyMs = 0
	ns.TunnelCount = 0
}

func buildServiceSummaries(nodes map[types.PeerKey]state.NodeView, lens view.Lens) []*controlv1.ServiceSummary {
	var out []*controlv1.ServiceSummary
	for slot, node := range nodes {
		for _, svc := range node.Services {
			if hasServicePublisher(svc) {
				if !lens.Permits(servicePublisher(svc)) {
					continue
				}
			} else if !lens.Admin() {
				continue
			}
			out = append(out, &controlv1.ServiceSummary{
				Name:     serviceNameOrDefault(svc.Name, svc.Port),
				Provider: &controlv1.NodeRef{PeerPub: slot.Bytes()},
				Port:     svc.Port,
				Protocol: svc.Protocol,
			})
		}
	}
	return out
}

func hasServicePublisher(svc *state.Service) bool {
	return svc != nil && svc.Fact != nil && len(svc.Fact.GetAuthorityPub()) > 0
}

func servicePublisher(svc *state.Service) types.PeerKey {
	return types.PeerKeyFromBytes(svc.Fact.GetAuthorityPub())
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

func (s *Service) buildWorkloadSummaries(snap state.Snapshot, scoped view.ScopedView, lens view.Lens) []*controlv1.WorkloadSummary {
	var out []*controlv1.WorkloadSummary
	seen := make(map[string]struct{})

	for _, w := range s.placement.Status() {
		sv, inScope := scoped.Workloads[w.Hash]
		// inScope means a named spec the caller may see. Not in scope and
		// not admin: either another tenant's spec or an unattributed local
		// workload with no publisher to own; neither is the caller's.
		if !inScope && !lens.Admin() {
			continue
		}
		seen[w.Hash] = struct{}{}
		ws := &controlv1.WorkloadSummary{
			Hash:           w.Hash,
			Name:           w.Name,
			Status:         controlv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
			StartedAtUnix:  w.CompiledAt.Unix(),
			Local:          true,
			ActiveReplicas: uint32(len(snap.Claims[w.Hash])),
		}
		if inScope {
			fillWorkloadSpecFields(ws, sv)
		}
		out = append(out, ws)
	}

	for hash, sv := range scoped.Workloads {
		if _, ok := seen[hash]; ok {
			continue
		}
		ws := &controlv1.WorkloadSummary{
			Hash:           hash,
			Name:           sv.Spec.Name,
			ActiveReplicas: uint32(len(snap.Claims[hash])),
		}
		fillWorkloadSpecFields(ws, sv)
		out = append(out, ws)
	}
	return out
}

func fillWorkloadSpecFields(ws *controlv1.WorkloadSummary, sv state.WorkloadSpecView) {
	ws.MinReplicas = sv.Spec.MinReplicas
	ws.Spread = sv.Spec.Spread
	ws.MemoryBytes = sv.Spec.MemoryBytes
	ws.TimeoutMs = uint32(sv.Spec.Timeout / time.Millisecond)
	ws.Publisher = &controlv1.NodeRef{PeerPub: sv.Publisher.Bytes()}
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
	if caller, ok := auth.CallerFromContext(ctx); ok && caller.Subject() != s.localPeerKey() {
		return nil, status.Error(codes.InvalidArgument, "service exposure is not supported for wire-mode callers")
	}
	name := serviceNameOrDefault(req.GetName(), req.Port)
	protocol := state.NormaliseProtocol(req.GetProtocol())
	if err := s.tunneling.ExposeService(req.Port, name, protocol, req.GetPolicy()); err != nil {
		return nil, s.fail(err, "register service failed")
	}
	return &controlv1.RegisterServiceResponse{}, nil
}

func (s *Service) UnregisterService(ctx context.Context, req *controlv1.UnregisterServiceRequest) (*controlv1.UnregisterServiceResponse, error) {
	if caller, ok := auth.CallerFromContext(ctx); ok && caller.Subject() != s.localPeerKey() {
		return nil, status.Error(codes.InvalidArgument, "service exposure is not supported for wire-mode callers")
	}
	name := serviceNameOrDefault(req.GetName(), req.GetPort())
	if svc := s.lookupLocalService(name); hasServicePublisher(svc) {
		if err := s.authoriseOwnership(ctx, servicePublisher(svc)); err != nil {
			return nil, err
		}
	}
	if err := s.tunneling.UnexposeService(name); err != nil {
		return nil, s.fail(err, "unregister service failed")
	}
	return &controlv1.UnregisterServiceResponse{}, nil
}

// authorisePresignedTombstone validates that a presigned tombstone
// comes from the caller's grant and carries Deleted=true. Shared shape
// across UnseedWorkload/UnseedStatic/RemoveBlob/UnregisterService.
func (s *Service) authorisePresignedTombstone(ctx context.Context, presigned *factv1.Fact) error {
	caller, ok := auth.CallerFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "caller grant required for presigned tombstone")
	}
	if !presigned.GetDeleted() {
		return status.Error(codes.InvalidArgument, "presigned tombstone must have Deleted=true")
	}
	publisher := types.PeerKeyFromBytes(presigned.GetAuthorityPub())
	if publisher != caller.Subject() {
		return status.Error(codes.PermissionDenied, "pre_signed_fact authority must match caller grant")
	}
	return nil
}

func (s *Service) lookupLocalService(name string) *state.Service {
	snap := s.state.Snapshot()
	nv, ok := snap.Nodes[snap.LocalID]
	if !ok {
		return nil
	}
	return nv.Services[name]
}

func (s *Service) ConnectPeer(ctx context.Context, req *controlv1.ConnectPeerRequest) (*controlv1.ConnectPeerResponse, error) {
	if err := s.requireCallerCap(ctx, admitCap, "admit"); err != nil {
		return nil, err
	}
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
	if s.gate != nil {
		if err := s.gate.Connect(s.callerGrant(ctx), peerKey, req.GetRemotePort()); err != nil {
			return nil, status.Error(codes.PermissionDenied, "connect denied")
		}
	}
	boundPort, err := s.tunneling.Connect(ctx, peerKey, req.GetRemotePort(), req.GetLocalPort(), state.NormaliseProtocol(req.GetProtocol()))
	if err != nil {
		return nil, s.fail(err, "connect service failed")
	}
	return &controlv1.ConnectServiceResponse{LocalPort: boundPort}, nil
}

func (s *Service) DisconnectService(ctx context.Context, req *controlv1.DisconnectServiceRequest) (*controlv1.DisconnectServiceResponse, error) {
	if err := s.requireCallerCap(ctx, admitCap, "admit"); err != nil {
		return nil, err
	}
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

func (s *Service) DenyPeer(ctx context.Context, req *controlv1.DenyPeerRequest) (*controlv1.DenyPeerResponse, error) {
	if err := s.requireCallerCap(ctx, admitCap, "admit"); err != nil {
		return nil, err
	}
	if err := s.membership.DenyPeer(types.PeerKeyFromBytes(req.GetPeerPub())); err != nil {
		return nil, s.fail(err, "deny peer failed")
	}
	return &controlv1.DenyPeerResponse{}, nil
}

func (s *Service) IssueGrant(ctx context.Context, req *controlv1.IssueGrantRequest) (*controlv1.IssueGrantResponse, error) {
	caller, ok := auth.CallerFromContext(ctx)
	if !ok || !caller.CanDelegate() {
		return nil, status.Error(codes.PermissionDenied, "delegate capability required")
	}
	caps := req.GetCapabilities()
	if caps == nil {
		return nil, status.Error(codes.InvalidArgument, "capabilities required")
	}
	if err := identity.ValidateAttributes(caps.GetAttributes()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// Caller cannot grant capabilities they don't hold themselves.
	// Bool caps gate cluster-admin escalation; MaxDepth and Attributes
	// gate downstream delegation reach and runtime policy clauses. The
	// membership signer only enforces child ≤ this node's parent chain,
	// so a CanDelegate tenant could otherwise request a child with
	// MaxDepth=255 / attrs={role:"admin"} via a higher-cap relay daemon.
	if err := enforceGrantCeiling(caps, caller.Capabilities); err != nil {
		return nil, err
	}
	if err := enforceBudgetCeiling(req.GetBudget(), caller.Budget); err != nil {
		return nil, err
	}
	grant, err := s.membership.IssueGrant(ctx, types.PeerKeyFromBytes(req.GetPeerPub()), caps, req.GetBudget())
	if err != nil {
		if errors.Is(err, membership.ErrNotDelegating) {
			return nil, status.Error(codes.FailedPrecondition, "this node has no delegation authority; target a delegating node")
		}
		return nil, s.fail(err, "issue grant failed")
	}
	return &controlv1.IssueGrantResponse{Grant: grant}, nil
}

// RenewGrant re-mints the caller's own grant with a fresh horizon. It
// is deliberately distinct from IssueGrant: the caller renews itself
// and need not hold delegate; the serving node supplies the delegating
// authority. The mTLS handshake has already verified the caller's
// session, its chain to root and the denylist before this handler
// runs, so a revoked or expired key cannot reach here. The re-mint
// copies the caller's current capabilities and budget verbatim;
// applyParent on the serving node's chain reclamps them, so renewal
// can never escalate. Grants with no horizon (admin/root) are refused:
// they have nothing to renew and re-parenting them would only obscure
// their chain.
func (s *Service) RenewGrant(ctx context.Context, _ *controlv1.RenewGrantRequest) (*controlv1.RenewGrantResponse, error) {
	caller, ok := auth.CallerFromContext(ctx)
	if !ok || !caller.Valid() {
		return nil, status.Error(codes.Unauthenticated, "no verified caller identity")
	}
	if caller.Grant.GetClaims().GetGrantDeadlineUnix() == 0 {
		return nil, status.Error(codes.FailedPrecondition, "grant has no renewal horizon")
	}
	grant, err := s.membership.IssueGrant(ctx, caller.Subject(), caller.Capabilities, caller.Budget)
	if err != nil {
		if errors.Is(err, membership.ErrNotDelegating) {
			return nil, status.Error(codes.FailedPrecondition, "this node has no delegation authority; target a delegating node")
		}
		return nil, s.fail(err, "renew grant failed")
	}
	return &controlv1.RenewGrantResponse{Grant: grant}, nil
}

// enforceGrantCeiling rejects a requested capability set that exceeds
// the caller's own in any dimension. The membership signer only
// enforces child <= this node's parent chain, so without this a
// CanDelegate tenant could request a child with admit / extra publish
// kinds / MaxDepth=255 / attrs={role:"admin"} via a higher-cap relay
// daemon. Returns a gRPC status error so the handler propagates it
// verbatim.
func enforceGrantCeiling(reqCaps, callerCaps *identityv1.Capabilities) error {
	if reqCaps.GetCanAdmit() && !callerCaps.GetCanAdmit() {
		return status.Error(codes.PermissionDenied, "cannot grant admit; caller lacks admit")
	}
	if reqCaps.GetCanDelegate() && !callerCaps.GetCanDelegate() {
		return status.Error(codes.PermissionDenied, "cannot grant delegate; caller lacks delegate")
	}
	if grantCapsPublishExceeds(reqCaps, callerCaps) {
		return status.Error(codes.PermissionDenied, "cannot grant publish; caller lacks publish")
	}
	if reqCaps.GetMaxDepth() > callerCaps.GetMaxDepth() {
		return status.Errorf(codes.PermissionDenied, "cannot grant max_depth %d; caller's max_depth is %d", reqCaps.GetMaxDepth(), callerCaps.GetMaxDepth())
	}
	if err := attributesSubsetOf(reqCaps.GetAttributes(), callerCaps.GetAttributes()); err != nil {
		return status.Errorf(codes.PermissionDenied, "cannot grant attributes: %v", err)
	}
	return nil
}

// enforceBudgetCeiling rejects a requested per-Principal Budget that
// exceeds the caller's own in any count dimension. A zero dimension
// means unlimited, so a caller limited in a dimension cannot mint a
// child that is unlimited or larger there. A caller unlimited in a
// dimension (zero) imposes no constraint for it.
func enforceBudgetCeiling(req, caller *identityv1.Budget) error {
	check := func(kind string, reqV, callerV uint32) error {
		if callerV == 0 {
			return nil
		}
		if reqV == 0 {
			return status.Errorf(codes.PermissionDenied, "cannot grant unlimited %s; caller's limit is %d", kind, callerV)
		}
		if reqV > callerV {
			return status.Errorf(codes.PermissionDenied, "cannot grant %s budget %d; caller's limit is %d", kind, reqV, callerV)
		}
		return nil
	}
	if err := check("functions", req.GetMaxFunctions(), caller.GetMaxFunctions()); err != nil {
		return err
	}
	if err := check("blobs", req.GetMaxBlobs(), caller.GetMaxBlobs()); err != nil {
		return err
	}
	return check("sites", req.GetMaxSites(), caller.GetMaxSites())
}

// grantCapsPublishExceeds reports whether child requests any publish
// kind the parent does not hold. Publish is per-kind now, so the old
// single CanPublish bool becomes a per-kind subset check.
func grantCapsPublishExceeds(child, parent *identityv1.Capabilities) bool {
	cp, pp := child.GetPublish(), parent.GetPublish()
	return (cp.GetFunctions() && !pp.GetFunctions()) ||
		(cp.GetBlobs() && !pp.GetBlobs()) ||
		(cp.GetSites() && !pp.GetSites()) ||
		(cp.GetServices() && !pp.GetServices())
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
	if certExpiry == 0 && s.creds != nil && s.creds.Grant() != nil {
		if dl := s.creds.Grant().GetClaims().GetGrantDeadlineUnix(); dl > 0 {
			certExpiry = time.Until(time.Unix(dl, 0)).Seconds()
		}
	}

	health := controlv1.HealthStatus_HEALTH_STATUS_HEALTHY
	switch {
	case (certExpiry < 0 && s.creds != nil && s.creds.Grant() != nil && s.creds.Grant().GetClaims().GetGrantDeadlineUnix() > 0) || (counts.Connected == 0 && (counts.Connecting > 0 || counts.Backoff > 0)):
		health = controlv1.HealthStatus_HEALTH_STATUS_UNHEALTHY
	case m.SmoothedVivaldiErr > vivaldiDegradedThreshold:
		health = controlv1.HealthStatus_HEALTH_STATUS_DEGRADED
	}

	return &controlv1.GetMetricsResponse{
		PeersDiscovered:   counts.Backoff,
		PeersConnecting:   counts.Connecting,
		PeersConnected:    counts.Connected,
		VivaldiError:      m.SmoothedVivaldiErr,
		CertExpirySeconds: certExpiry,
		PunchAttempts:     m.PunchAttempts,
		PunchFailures:     m.PunchFailures,
		Health:            health,
		VivaldiSamples:    m.VivaldiSamples,
		EagerSyncs:        m.EagerSyncs,
		EagerSyncFailures: m.EagerSyncFailures,
	}, nil
}

func (s *Service) SeedWorkload(stream grpc.ClientStreamingServer[controlv1.SeedWorkloadRequest, controlv1.SeedWorkloadResponse]) error {
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

	spec := state.WorkloadSpec{
		Hash:        hash,
		Name:        name,
		MinReplicas: header.GetMinReplicas(),
		MemoryBytes: header.GetMemoryBytes(),
		Timeout:     time.Duration(header.GetTimeoutMs()) * time.Millisecond,
		Spread:      header.GetSpread(),
	}

	if presigned := header.GetPreSignedFact(); presigned != nil {
		if err := s.seedWorkloadPresigned(stream.Context(), wasmBytes, spec, presigned); err != nil {
			return err
		}
		publisher := types.PeerKeyFromBytes(presigned.GetAuthorityPub())
		return stream.SendAndClose(&controlv1.SeedWorkloadResponse{Hash: hash, Name: name, PublicUrl: s.pathBasedURL("fn", name, publisher, presigned.GetPolicy().GetPublic())})
	}

	if caller, ok := auth.CallerFromContext(stream.Context()); ok && caller.Subject() != s.localPeerKey() {
		return status.Error(codes.InvalidArgument, "wire-mode callers must supply pre_signed_auth")
	}
	if err := s.placement.Seed(wasmBytes, spec, header.GetPolicy()); err != nil {
		switch {
		case errors.Is(err, placement.ErrCompile):
			s.log.Warnw("seed workload failed", "name", name, "hash", types.ShortHash(hash), "err", err)
			return status.Error(codes.InvalidArgument, "failed to compile workload")
		case errors.Is(err, placement.ErrRelayOnly):
			return status.Error(codes.FailedPrecondition, "node is relay-only; workload hosting disabled")
		case errors.Is(err, placement.ErrPublishDenied):
			return status.Error(codes.FailedPrecondition, err.Error())
		default:
			return s.fail(err, "failed to seed workload")
		}
	}

	return stream.SendAndClose(&controlv1.SeedWorkloadResponse{Hash: hash, Name: name, PublicUrl: s.pathBasedURL("fn", name, s.localPeerKey(), header.GetPolicy().GetPublic())})
}

func (s *Service) seedWorkloadPresigned(ctx context.Context, wasmBytes []byte, spec state.WorkloadSpec, presigned *factv1.Fact) error {
	caller, ok := auth.CallerFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "caller cert required for presigned spec")
	}
	publisher := types.PeerKeyFromBytes(presigned.GetAuthorityPub())
	if publisher != caller.Subject() {
		return status.Error(codes.PermissionDenied, "pre_signed_auth publisher must match caller cert")
	}
	if err := s.placement.SeedPresigned(wasmBytes, spec, presigned); err != nil {
		return s.fail(err, "failed to seed workload")
	}
	return nil
}

// fetchChunkSize is the plaintext payload per FetchBlobResponse frame.
// 32 KiB keeps gRPC framing overhead amortised while staying well below
// the default 4 MiB message-size limit.
const fetchChunkSize = 32 * 1024

func (s *Service) FetchBlob(req *controlv1.FetchBlobRequest, stream grpc.ServerStreamingServer[controlv1.FetchBlobResponse]) error {
	hash := req.GetHash()
	if s.gate != nil {
		if err := s.gate.Fetch(s.callerGrant(stream.Context()), hash); err != nil {
			return status.Error(codes.PermissionDenied, "fetch denied")
		}
	}
	rc, err := s.blobs.FetchPlaintext(stream.Context(), hash)
	if err != nil {
		s.log.Warnw("fetch blob failed", "hash", types.ShortHash(hash), "err", err)
		switch {
		case errors.Is(err, blobs.ErrNoPublisher):
			return status.Error(codes.NotFound, "no publisher known for blob")
		case errors.Is(err, blobs.ErrNotLocal):
			return status.Error(codes.Unavailable, "publisher does not have blob")
		default:
			return status.Error(codes.Unavailable, "fetch blob from publisher")
		}
	}
	defer rc.Close()

	buf := make([]byte, fetchChunkSize)
	for {
		n, readErr := rc.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&controlv1.FetchBlobResponse{Chunk: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			s.log.Warnw("fetch blob stream error", "hash", types.ShortHash(hash), "err", readErr)
			return status.Error(codes.Internal, "fetch blob stream")
		}
	}
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

	if err := s.authoriseBlobUpload(stream.Context(), header); err != nil {
		return err
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

	name := header.GetName()
	if name == "" && header.GetAnchor() {
		name = types.ShortHash(hash)
	}
	var publisher types.PeerKey
	if name != "" {
		if err := s.publishUploadedBlob(hash, name, header); err != nil {
			return err
		}
		if presigned := header.GetPreSignedFact(); presigned != nil {
			publisher = types.PeerKeyFromBytes(presigned.GetAuthorityPub())
		} else {
			publisher = s.localPeerKey()
		}
	}
	return stream.SendAndClose(&controlv1.UploadBlobResponse{Hash: hash, PublicUrl: s.pathBasedURL("blob", name, publisher, header.GetPolicy().GetPublic())})
}

func (s *Service) publishUploadedBlob(hash, name string, header *controlv1.UploadBlobHeader) error {
	if presigned := header.GetPreSignedFact(); presigned != nil {
		if err := s.blobs.PublishPresigned(hash, name, presigned); err != nil {
			s.log.Warnw("publish blob (presigned) failed", "hash", types.ShortHash(hash), "name", name, "err", err)
			return status.Error(codes.Internal, "publish blob")
		}
		return nil
	}
	if err := s.blobs.Publish(hash, name, header.GetPolicy()); err != nil {
		s.log.Warnw("publish blob failed", "hash", types.ShortHash(hash), "name", name, "err", err)
		return status.Error(codes.Internal, "publish blob")
	}
	return nil
}

// authoriseBlobUpload runs the auth dispatch for UploadBlob. A wire-mode
// caller either supplies a pre_signed_fact whose authority must match
// the caller, or uploads anchor/anonymous content-addressed bytes that
// create no durable spec. Per-kind publish capability is enforced by the
// admission pipeline when a presigned fact reaches the CRDT write; the
// caller's own Budget governs raw byte volume.
func (s *Service) authoriseBlobUpload(ctx context.Context, header *controlv1.UploadBlobHeader) error {
	caller, hasCaller := auth.CallerFromContext(ctx)
	presigned := header.GetPreSignedFact()
	if presigned != nil {
		if !hasCaller {
			return status.Error(codes.Unauthenticated, "caller cert required for presigned spec")
		}
		publisher := types.PeerKeyFromBytes(presigned.GetAuthorityPub())
		if publisher != caller.Subject() {
			return status.Error(codes.PermissionDenied, "pre_signed_auth publisher must match caller cert")
		}
		return nil
	}
	if hasCaller && caller.Subject() != s.localPeerKey() {
		if header.GetName() != "" || header.GetAnchor() {
			return status.Error(codes.InvalidArgument, "wire-mode named/anchor uploads must supply pre_signed_auth")
		}
		return nil
	}
	return nil
}

func (s *Service) RemoveBlob(ctx context.Context, req *controlv1.RemoveBlobRequest) (*controlv1.RemoveBlobResponse, error) {
	hash := req.GetHash()
	snap := s.state.Snapshot()
	if _, ok := snap.Specs[hash]; ok {
		return nil, status.Error(codes.FailedPrecondition, "blob is referenced by a workload spec; unseed the workload instead")
	}
	if _, ok := s.static.StaticBlobs()[hash]; ok {
		return nil, status.Error(codes.FailedPrecondition, "blob is referenced by a static manifest; unseed the static site instead")
	}
	if presigned := req.GetPreSignedFact(); presigned != nil {
		if err := s.authorisePresignedTombstone(ctx, presigned); err != nil {
			return nil, err
		}
		if err := s.blobs.RemovePresigned(hash, presigned); err != nil {
			return nil, s.failBlobRemove(hash, err)
		}
		return &controlv1.RemoveBlobResponse{}, nil
	}
	if bv, ok := snap.BlobSpecs[hash]; ok {
		if err := s.authoriseOwnership(ctx, bv.Publisher); err != nil {
			return nil, err
		}
	}
	if err := s.blobs.Remove(hash); err != nil {
		return nil, s.failBlobRemove(hash, err)
	}
	return &controlv1.RemoveBlobResponse{}, nil
}

func (s *Service) failBlobRemove(hash string, err error) error {
	if errors.Is(err, blobs.ErrNotLocal) {
		return status.Error(codes.FailedPrecondition, "blob not present locally")
	}
	s.log.Warnw("remove blob failed", "hash", types.ShortHash(hash), "err", err)
	return status.Error(codes.Internal, "remove blob")
}

func (s *Service) SeedStatic(ctx context.Context, req *controlv1.SeedStaticRequest) (*controlv1.SeedStaticResponse, error) {
	if presigned := req.GetPreSignedFact(); presigned != nil {
		return s.seedStaticPresigned(ctx, req, presigned)
	}
	if caller, ok := auth.CallerFromContext(ctx); ok && caller.Subject() != s.localPeerKey() {
		return nil, status.Error(codes.InvalidArgument, "wire-mode callers must supply pre_signed_auth")
	}
	if err := s.static.SeedStatic(req.GetName(), req.GetManifestDigest(), req.GetPolicy()); err != nil {
		return nil, s.fail(err, "seed static")
	}
	return &controlv1.SeedStaticResponse{PublicUrl: s.hostBasedURL(req.GetName(), s.localPeerKey())}, nil
}

func (s *Service) seedStaticPresigned(ctx context.Context, req *controlv1.SeedStaticRequest, presigned *factv1.Fact) (*controlv1.SeedStaticResponse, error) {
	caller, ok := auth.CallerFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "caller cert required for presigned spec")
	}
	publisher := types.PeerKeyFromBytes(presigned.GetAuthorityPub())
	if publisher != caller.Subject() {
		return nil, status.Error(codes.PermissionDenied, "pre_signed_auth publisher must match caller cert")
	}
	if err := s.static.SeedStaticPresigned(req.GetName(), req.GetManifestDigest(), presigned); err != nil {
		return nil, s.fail(err, "seed static")
	}
	return &controlv1.SeedStaticResponse{PublicUrl: s.hostBasedURL(req.GetName(), publisher)}, nil
}

func (s *Service) UnseedStatic(ctx context.Context, req *controlv1.UnseedStaticRequest) (*controlv1.UnseedStaticResponse, error) {
	if presigned := req.GetPreSignedFact(); presigned != nil {
		if err := s.authorisePresignedTombstone(ctx, presigned); err != nil {
			return nil, err
		}
		if err := s.static.UnseedStaticPresigned(req.GetName(), presigned); err != nil {
			return nil, s.fail(err, "unseed static")
		}
		return &controlv1.UnseedStaticResponse{}, nil
	}
	if sv, ok := s.state.Snapshot().StaticSpecs[req.GetName()]; ok {
		if err := s.authoriseOwnership(ctx, sv.Publisher); err != nil {
			return nil, err
		}
	}
	if err := s.static.UnseedStatic(req.GetName()); err != nil {
		return nil, s.fail(err, "unseed static")
	}
	return &controlv1.UnseedStaticResponse{}, nil
}

func (s *Service) ListStatic(ctx context.Context, _ *controlv1.ListStaticRequest) (*controlv1.ListStaticResponse, error) {
	snap := s.state.Snapshot()
	lens := s.callerPrincipal(ctx)
	return &controlv1.ListStaticResponse{Sites: s.buildStaticSummaries(snap, view.Project(snap, lens), s.operatorRequest(ctx, lens))}, nil
}

func (s *Service) buildStaticSummaries(snap state.Snapshot, scoped view.ScopedView, operator bool) []*controlv1.StaticSummary {
	var capacity uint32
	for _, nv := range scoped.Nodes {
		if nv.CanServeStatic {
			capacity++
		}
	}
	out := make([]*controlv1.StaticSummary, 0, len(scoped.Statics))
	for name, spec := range scoped.Statics {
		digest, _ := hex.DecodeString(spec.Spec.ManifestDigest)
		claimants := snap.StaticClaims[name]
		_, local := claimants[snap.LocalID]
		summary := &controlv1.StaticSummary{
			Name:            name,
			ManifestDigest:  digest,
			Publisher:       &controlv1.NodeRef{PeerPub: spec.Publisher.Bytes()},
			Local:           local && operator,
			ServingCapacity: capacity,
			PublicUrl:       s.hostBasedURL(name, spec.Publisher),
		}
		for pk := range claimants {
			if _, ok := scoped.Nodes[pk]; !ok {
				continue
			}
			summary.Claimants = append(summary.Claimants, &controlv1.NodeRef{PeerPub: pk.Bytes()})
		}
		out = append(out, summary)
	}
	return out
}

// hostBasedURL renders a static-style URL: `https://<name>-<slug>.<domain>`.
// Returns "" when no gateway domain is configured.
func (s *Service) hostBasedURL(name string, publisher types.PeerKey) string {
	if s.staticDomain == "" || name == "" {
		return ""
	}
	return "https://" + name + "-" + publisher.Slug() + s.staticDomain
}

// pathBasedURL renders a canonical fn/blob URL:
// `https://<subdomain>.<domain>/<slug>/<name>`. Anonymous callers reach
// it only when the spec's policy is public; the URL is suppressed for
// non-public specs to keep the CLI output truthful.
func (s *Service) pathBasedURL(subdomain, name string, publisher types.PeerKey, public bool) string {
	if !public || s.staticDomain == "" || name == "" {
		return ""
	}
	return "https://" + subdomain + s.staticDomain + "/" + publisher.Slug() + "/" + name
}

// Restricts holders to live peers; stale BlobAvailability from offline
// peers would inflate replicas and surface phantom orphans.
func (s *Service) buildBlobSummaries(snap state.Snapshot, scoped view.ScopedView, lens view.Lens, operator bool) []*controlv1.BlobSummary {
	liveSet := make(map[types.PeerKey]struct{}, len(snap.PeerKeys))
	for _, pk := range snap.PeerKeys {
		liveSet[pk] = struct{}{}
	}
	counts := make(map[string]uint32)
	for pk, nv := range scoped.Nodes {
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
		bv, hasSpec := snap.BlobSpecs[hash]
		// Orphan blobs (no named BlobSpec) carry no publisher attribution,
		// so non-admin callers can't claim ownership of them — only admins
		// see them. Named specs are gated by the caller's projection.
		if hasSpec {
			if _, ok := scoped.Blobs[hash]; !ok {
				continue
			}
		} else if !lens.Admin() {
			continue
		}
		_, local := localBlobs[hash]
		summary := &controlv1.BlobSummary{
			Hash:     hash,
			Replicas: n,
			Local:    local && operator,
		}
		if hasSpec {
			summary.Name = bv.Spec.Name
			summary.Publisher = &controlv1.NodeRef{PeerPub: bv.Publisher.Bytes()}
		} else {
			summary.Orphan = true
		}
		out = append(out, summary)
	}
	return out
}

func (s *Service) UnseedWorkload(ctx context.Context, req *controlv1.UnseedWorkloadRequest) (*controlv1.UnseedWorkloadResponse, error) {
	if presigned := req.GetPreSignedFact(); presigned != nil {
		if err := s.authorisePresignedTombstone(ctx, presigned); err != nil {
			return nil, err
		}
		if err := s.placement.UnseedPresigned(req.GetHash(), presigned); err != nil {
			return nil, s.fail(err, "unseed workload failed", "hash", req.GetHash())
		}
		return &controlv1.UnseedWorkloadResponse{}, nil
	}
	if sv, ok := s.state.Snapshot().Specs[req.GetHash()]; ok {
		if err := s.authoriseOwnership(ctx, sv.Publisher); err != nil {
			return nil, err
		}
	}
	if err := s.placement.Unseed(req.GetHash()); err != nil {
		if errors.Is(err, placement.ErrRelayOnly) {
			return nil, status.Error(codes.FailedPrecondition, "node is relay-only; workload hosting disabled")
		}
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

	ctx = s.callerWasmContext(ctx)
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
		case errors.Is(err, placement.ErrOverloaded):
			return nil, status.Error(codes.ResourceExhausted, "node overloaded; retry later")
		case errors.Is(err, placement.ErrRelayOnly):
			return nil, status.Error(codes.FailedPrecondition, "node is relay-only; workload invocation disabled")
		case errors.Is(err, context.DeadlineExceeded):
			return nil, status.Error(codes.DeadlineExceeded, "workload invocation timed out")
		default:
			return nil, status.Error(codes.Internal, "workload invocation failed")
		}
	}
	return &controlv1.CallWorkloadResponse{Output: output}, nil
}

// callerWasmContext seeds a wasm.CallerInfo on ctx from the caller's
// grant (set by the gRPC interceptor). Unix-socket and SSH-bridge paths
// hit the daemon-self fallback in injectCaller and naturally carry the
// daemon's own grant; wire-mode callers carry their own mTLS-validated
// grant. Either way the downstream placement layer sees the authentic
// caller identity, not a substituted daemon identity.
func (s *Service) callerWasmContext(ctx context.Context) context.Context {
	grant := s.callerGrant(ctx)
	if grant == nil {
		return ctx
	}
	info := wasm.CallerInfo{
		PeerKey: types.PeerKeyFromBytes(grant.GetClaims().GetSubjectPub()),
	}
	if attrs := grant.GetClaims().GetCapabilities().GetAttributes(); attrs != nil {
		info.Attributes = attrs.AsMap()
	}
	return wasm.WithCallerInfo(ctx, info)
}

// callerGrant returns the caller's grant from the RPC context. Returns
// nil if no caller is present — the interceptor (injectCaller) is the
// only legitimate source of a caller grant, and its TLS-path guard
// refuses the daemon-self fallback for wire-mode peers. Mirroring that
// refusal here keeps the security boundary at one well-defined edge.
// Downstream gate methods (Connect, Fetch, Invoke) fail closed on nil.
func (s *Service) callerGrant(ctx context.Context) *identityv1.Grant {
	rpc, ok := auth.CallerFromContext(ctx)
	if !ok {
		return nil
	}
	return rpc.Grant
}

type capabilityCheck func(identity.Principal) bool

func admitCap(c identity.Principal) bool { return c.Admin() }

// attributesSubsetOf returns nil if every key/value pair in child is
// present with the same value in parent. An empty child is a subset of
// anything (callers who request no attributes don't need to bound them).
// A missing parent attribute is treated as not-granted: the child can't
// introduce keys the parent doesn't carry.
func attributesSubsetOf(child, parent *structpb.Struct) error {
	if child == nil || len(child.GetFields()) == 0 {
		return nil
	}
	parentFields := parent.GetFields()
	for key, childVal := range child.GetFields() {
		parentVal, ok := parentFields[key]
		if !ok {
			return fmt.Errorf("attribute %q absent from caller's cert", key)
		}
		if !proto.Equal(childVal, parentVal) {
			return fmt.Errorf("attribute %q value %q does not match caller's %q", key, childVal.GetStringValue(), parentVal.GetStringValue())
		}
	}
	return nil
}

// requireCallerCap returns a PermissionDenied unless the caller's cert
// holds the named capability. Unlike the legacy s.canX() helpers it
// resolves the caller from the RPC context, so wire-mode tenants are
// gated by their OWN authority instead of riding the daemon's cert.
func (s *Service) requireCallerCap(ctx context.Context, want capabilityCheck, friendly string) error {
	caller, ok := auth.CallerFromContext(ctx)
	if !ok || !want(caller) {
		return status.Errorf(codes.PermissionDenied, "%s capability required", friendly)
	}
	return nil
}

// requireDaemonSelf restricts an RPC to callers whose cert subject pub
// matches the daemon's own. Used for verbs that are nonsensical or
// dangerous to expose to wire-mode tenants (Shutdown).
func (s *Service) requireDaemonSelf(ctx context.Context, friendly string) error {
	rpc, ok := auth.CallerFromContext(ctx)
	if !ok {
		return status.Error(codes.PermissionDenied, friendly)
	}
	if rpc.Subject() != s.localPeerKey() {
		return status.Error(codes.PermissionDenied, friendly)
	}
	return nil
}

func (s *Service) fail(err error, msg string, kv ...any) error {
	if errors.Is(err, state.ErrTombstoneNoLiveSpec) {
		return status.Error(codes.NotFound, "no live spec by this publisher matches; nothing to unseed")
	}
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
