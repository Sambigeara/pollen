package membership

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"maps"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	meshv1 "github.com/sambigeara/pollen/api/genpb/pollen/mesh/v1"
	"github.com/sambigeara/pollen/pkg/auth"
	"github.com/sambigeara/pollen/pkg/coords"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/state"
	"github.com/sambigeara/pollen/pkg/transport"
	"github.com/sambigeara/pollen/pkg/types"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

type Service struct {
	store             ClusterState
	mesh              Network
	streams           StreamOpener
	rtt               RTTSource
	certs             CertManager
	peerAddrs         PeerAddressSource
	sessionCloser     PeerSessionCloser
	datagramHandler   func(ctx context.Context, from types.PeerKey, env *meshv1.Envelope)
	peerConnectTime   map[types.PeerKey]time.Time
	natDetector       *nat.Detector
	creds             *auth.NodeCredentials
	nodeMetrics       *metrics.NodeMetrics
	smoothedErr       *metrics.EWMA
	log               *zap.SugaredLogger
	tracer            trace.Tracer
	events            chan state.Event
	lastEagerSync     map[types.PeerKey]time.Time
	shutdownCh        chan<- struct{}
	pollenDir         string
	signPriv          ed25519.PrivateKey
	advertisedIPs     []string
	localCoord        coords.Coord
	wg                sync.WaitGroup
	eagerSyncFailures atomic.Int64
	tlsIdentityTTL    time.Duration
	reconnectWindow   time.Duration
	vivaldiSamples    atomic.Int64
	eagerSyncs        atomic.Int64
	gossipJitter      float64
	port              int
	membershipTTL     time.Duration
	peerTickInterval  time.Duration
	gossipInterval    time.Duration
	localCoordErr     float64
	stopOnce          sync.Once
	mu                sync.Mutex
	renewalFailed     atomic.Bool
	localID           types.PeerKey
}

func New(
	self types.PeerKey,
	creds *auth.NodeCredentials,
	net Network,
	cluster ClusterState,
	opts ...Option,
) *Service {
	s := &Service{
		store:           cluster,
		mesh:            net,
		creds:           creds,
		localID:         self,
		log:             zap.S().Named("membership"),
		tracer:          tracenoop.NewTracerProvider().Tracer("pollen/membership"),
		nodeMetrics:     metrics.NewNodeMetrics(metricnoop.NewMeterProvider()),
		smoothedErr:     metrics.NewEWMA(VivaldiErrAlpha, 1.0),
		localCoord:      coords.RandomCoord(),
		localCoordErr:   1.0,
		peerConnectTime: make(map[types.PeerKey]time.Time),
		lastEagerSync:   make(map[types.PeerKey]time.Time),
		events:          make(chan state.Event, eventBufSize),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *Service) Start(ctx context.Context) error {
	s.store.SetLocalCoord(s.localCoord, s.localCoordErr)
	s.broadcastBatchBytes(ctx, s.store.EncodeFull())

	if s.checkCertExpiry() {
		return ErrCertExpired
	}

	s.wg.Go(func() { s.runGossipTicker(ctx) })
	s.wg.Go(func() { s.runRecvLoop(ctx) })
	s.wg.Go(func() { s.runPeerEventLoop(ctx) })
	s.wg.Go(func() { s.runPeerTick(ctx) })
	s.wg.Go(func() { s.runPendingGossipBroadcast(ctx) })
	s.wg.Go(func() { s.runCertCheckTicker(ctx) })

	if len(s.advertisedIPs) == 0 {
		s.wg.Go(func() { s.runIPRefresh(ctx) })
	}

	return nil
}

func (s *Service) Stop() error {
	s.wg.Wait()
	s.stopOnce.Do(func() { close(s.events) })
	return nil
}

func (s *Service) Events() <-chan state.Event {
	return s.events
}

func (s *Service) ControlMetrics() ControlMetrics {
	s.mu.Lock()
	coord := s.localCoord
	s.mu.Unlock()
	return ControlMetrics{
		LocalCoord:        coord,
		SmoothedErr:       s.smoothedErr.Value(),
		VivaldiSamples:    s.vivaldiSamples.Load(),
		EagerSyncs:        s.eagerSyncs.Load(),
		EagerSyncFailures: s.eagerSyncFailures.Load(),
	}
}

func (s *Service) DenyPeer(key types.PeerKey) error {
	events := s.store.DenyPeer(key)
	s.forwardEvents(events)
	return nil
}

func (s *Service) Invite(_ string) (string, error) {
	signer := s.creds.DelegationKey()
	if signer == nil {
		return "", fmt.Errorf("this node is not an admin")
	}
	token, err := signer.IssueInviteToken(
		nil, // open invite — any peer can claim
		nil, // bootstrap peers filled by the control layer
		time.Now(),
		s.membershipTTL,
		s.membershipTTL,
	)
	if err != nil {
		return "", fmt.Errorf("issue invite: %w", err)
	}
	return auth.EncodeInviteToken(token)
}

func (s *Service) sendEvent(ev state.Event) {
	select {
	case s.events <- ev:
	default:
	}
}

func (s *Service) forwardEvents(events []state.Event) {
	for _, ev := range events {
		if _, ok := ev.(state.GossipApplied); ok {
			continue
		}
		s.sendEvent(ev)
	}
}

func (s *Service) runGossipTicker(ctx context.Context) {
	ticker := newJitterTicker(ctx, s.gossipInterval, s.gossipJitter)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events := s.gossip(ctx)
			s.forwardEvents(events)
		}
	}
}

func (s *Service) runRecvLoop(ctx context.Context) {
	for {
		p, err := s.mesh.Recv(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Debugw("recv failed", "err", err)
			continue
		}
		s.handleDatagram(ctx, p.From, p.Data)
	}
}

func (s *Service) runPeerEventLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-s.mesh.PeerEvents():
			switch ev.Type {
			case transport.PeerEventConnected:
				s.mu.Lock()
				s.peerConnectTime[ev.Key] = time.Now()
				shouldEagerSync := time.Since(s.lastEagerSync[ev.Key]) >= eagerSyncCooldown
				s.mu.Unlock()

				if shouldEagerSync {
					s.doEagerSync(ctx, ev.Key)
				}
				if len(ev.Addrs) > 0 {
					s.sendObservedAddress(ev.Key, net.UDPAddrFromAddrPort(ev.Addrs[0]))
				}

			case transport.PeerEventDisconnected:
				s.mu.Lock()
				delete(s.peerConnectTime, ev.Key)
				delete(s.lastEagerSync, ev.Key)
				s.mu.Unlock()
			}
			s.forwardEvents(s.store.SetLocalReachable(s.mesh.ConnectedPeers()))
		}
	}
}

func (s *Service) runPeerTick(ctx context.Context) {
	ticker := time.NewTicker(s.peerTickInterval)
	defer ticker.Stop()
	var lastExpirySweep time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap := s.store.Snapshot()
			s.updateVivaldiCoords(snap)
			if time.Since(lastExpirySweep) >= expirySweepInterval {
				s.disconnectExpiredPeers()
				lastExpirySweep = time.Now()
			}
		}
	}
}

func (s *Service) runPendingGossipBroadcast(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.store.PendingNotify():
			events := s.store.FlushPendingGossip()
			s.broadcastEvents(ctx, events)
		}
	}
}

func (s *Service) runCertCheckTicker(ctx context.Context) {
	certInterval := certCheckInterval
	if auth.IsCertExpired(s.creds.Cert(), time.Now()) {
		certInterval = expirySweepInterval
	}
	ticker := time.NewTicker(certInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.checkCertExpiry() {
				if s.shutdownCh != nil {
					s.shutdownCh <- struct{}{}
				}
				return
			}
			newInterval := certCheckInterval
			if s.renewalFailed.Load() {
				newInterval = expirySweepInterval
			}
			if newInterval != certInterval {
				certInterval = newInterval
				ticker.Reset(certInterval)
			}
		}
	}
}

func (s *Service) runIPRefresh(ctx context.Context) {
	ticker := time.NewTicker(ipRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshIPs()
		}
	}
}

func (s *Service) sendObservedAddress(pk types.PeerKey, addr *net.UDPAddr) {
	data, err := (&meshv1.Envelope{
		Body: &meshv1.Envelope_ObservedAddress{
			ObservedAddress: &meshv1.ObservedAddress{Addr: addr.String()},
		},
	}).MarshalVT()
	if err != nil {
		return
	}
	if err := s.mesh.Send(context.Background(), pk, data); err != nil {
		s.log.Debugw("failed sending observed address", "peer", pk.Short(), "err", err)
	}
}

func (s *Service) handleObservedAddress(from types.PeerKey, oa *meshv1.ObservedAddress) {
	addr, err := net.ResolveUDPAddr("udp", oa.GetAddr())
	if err != nil {
		s.log.Debugw("observed address: parse failed", "addr", oa.GetAddr(), "err", err)
		return
	}

	if addr.IP.IsPrivate() || addr.IP.IsLoopback() || addr.IP.IsLinkLocalUnicast() {
		return
	}

	s.log.Debugw("observed address received", "addr", addr.String(), "from", from.Short())

	// Only accept address updates from publicly accessible peers. A NAT'd
	// peer's observation has a destination-dependent port mapping that is
	// only valid for traffic from that specific peer, not as a general
	// external address. NAT type detection still uses all observations.
	snap := s.store.Snapshot()
	if fromNV, ok := snap.Nodes[from]; ok && fromNV.PubliclyAccessible {
		events := s.store.SetLocalObservedAddress(addr.IP.String(), uint32(addr.Port))
		if len(events) > 0 {
			s.natDetector.Reset()
			s.log.Infow("external IP changed, reset NAT observations")
		}
		s.forwardEvents(events)
	}

	if peerAddr, ok := s.peerAddrs.GetActivePeerAddress(from); ok {
		if observerIP, ok := netip.AddrFromSlice(peerAddr.IP); ok {
			observerIP = observerIP.Unmap()
			if natType, changed := s.natDetector.AddObservation(observerIP, addr.Port); changed {
				s.log.Infow("NAT type detected", "type", natType)
				s.store.SetLocalNAT(natType)
			}
		}
	}
}

func (s *Service) refreshIPs() {
	newIPs, err := transport.GetAdvertisableAddrs(transport.DefaultExclusions)
	if err != nil {
		s.log.Debugw("ip refresh failed", "err", err)
		return
	}
	var addrs []netip.AddrPort
	for _, ip := range newIPs {
		addr, err := netip.ParseAddr(ip)
		if err != nil {
			continue
		}
		addrs = append(addrs, netip.AddrPortFrom(addr, uint16(s.port)))
	}
	if len(s.store.SetLocalAddresses(addrs)) > 0 {
		s.log.Infow("advertised IPs changed", "ips", newIPs)
	}
}

func (s *Service) updateVivaldiCoords(snap state.Snapshot) {
	updated := false
	now := time.Now()

	s.mu.Lock()
	localCoord := s.localCoord
	localCoordErr := s.localCoordErr
	peerConnectTimes := make(map[types.PeerKey]time.Time, len(s.peerConnectTime))
	maps.Copy(peerConnectTimes, s.peerConnectTime)
	s.mu.Unlock()

	for _, peerKey := range s.mesh.ConnectedPeers() {
		if ct, ok := peerConnectTimes[peerKey]; ok && now.Sub(ct) < vivaldiWarmupDuration {
			continue
		}
		conn, ok := s.rtt.GetConn(peerKey)
		if !ok {
			continue
		}
		rtt := conn.ConnectionStats().SmoothedRTT
		if rtt <= 0 {
			continue
		}
		nv, ok := snap.Nodes[peerKey]
		if !ok || nv.VivaldiCoord == nil {
			continue
		}
		peerCoord := nv.VivaldiCoord
		var newErr float64
		localCoord, newErr = coords.Update(
			localCoord, localCoordErr,
			coords.Sample{RTT: rtt, PeerCoord: *peerCoord, PeerErr: nv.VivaldiErr},
		)
		localCoordErr = newErr
		s.smoothedErr.Update(newErr)
		s.vivaldiSamples.Add(1)
		updated = true
	}

	if updated {
		s.mu.Lock()
		s.localCoord = localCoord
		s.localCoordErr = localCoordErr
		s.mu.Unlock()

		events := s.store.SetLocalCoord(localCoord, localCoordErr)
		s.forwardEvents(events)
	}
}

const jitterScale = 2

type jitterTicker struct {
	C    <-chan time.Time
	stop context.CancelFunc
}

func newJitterTicker(ctx context.Context, base time.Duration, percent float64) *jitterTicker {
	tickCh := make(chan time.Time)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(tickCh)
		timer := time.NewTimer(jitter(base, percent))
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-timer.C:
				select {
				case <-ctx.Done():
					return
				case tickCh <- t:
				}
				timer.Reset(jitter(base, percent))
			}
		}
	}()
	return &jitterTicker{C: tickCh, stop: cancel}
}

func (t *jitterTicker) Stop() {
	t.stop()
}

func jitter(d time.Duration, percent float64) time.Duration {
	if percent <= 0 {
		return d
	}
	delta := time.Duration(float64(d) * percent)
	n := int64(delta)*jitterScale + 1
	offset := time.Duration(rand.N(n)) - delta //nolint:gosec
	return d + offset
}
