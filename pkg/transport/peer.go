package transport

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/types"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

type peerState int

const (
	peerStateDiscovered peerState = iota + 1
	peerStateConnecting
	peerStateConnected
	peerStateUnreachable
)

type connectStage int

const (
	connectStageUnspecified connectStage = iota
	connectStageEagerRetry
	connectStageDirect
	connectStagePunch
)

type DisconnectReason int

const (
	disconnectUnknown DisconnectReason = iota
	disconnectIdleTimeout
	disconnectReset
	DisconnectGraceful
	DisconnectTopologyPrune
	DisconnectDenied
	DisconnectCertRotation
	DisconnectCertExpired
	disconnectDuplicate
	disconnectShutdown
)

func (r DisconnectReason) String() string {
	switch r {
	case disconnectIdleTimeout:
		return "idle_timeout"
	case disconnectReset:
		return "stateless_reset"
	case DisconnectGraceful:
		return "graceful"
	case DisconnectTopologyPrune:
		return "topology_prune"
	case DisconnectDenied:
		return "denied"
	case DisconnectCertRotation:
		return "cert_rotation"
	case DisconnectCertExpired:
		return "cert_expired"
	case disconnectDuplicate:
		return "duplicate"
	case disconnectShutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

const (
	baseBackoff  = 1 * time.Second
	maxBackoff   = 10 * time.Minute
	firstBackoff = 500 * time.Millisecond

	eagerRetryAttemptThreshold = 1
	directAttemptThreshold     = 2
	punchAttemptThreshold      = 2
	connectingTimeout          = 10 * time.Second

	unreachableRetryInterval        = 20 * time.Second
	idleTimeoutRetryInterval        = 1 * time.Second
	resetRetryInterval              = 5 * time.Second
	gracefulDisconnectRetryInterval = 1 * time.Hour
	unknownDisconnectRetryInterval  = 3 * time.Second
)

type peer struct {
	NextActionAt  time.Time
	ConnectingAt  time.Time
	ConnectedAt   time.Time
	LastAddr      *net.UDPAddr
	Ips           []net.IP
	State         peerState
	Stage         connectStage
	StageAttempts int
	ObservedPort  int
	ID            types.PeerKey
}

func (p *peer) resetStage() {
	if p.LastAddr != nil {
		p.Stage = connectStageEagerRetry
	} else {
		p.Stage = connectStageDirect
	}
}

type peerStore struct {
	m       map[types.PeerKey]*peer
	metrics *metrics.PeerMetrics
	trans   *QUICTransport
	mu      sync.RWMutex
}

func newPeerStore(t *QUICTransport) *peerStore {
	return &peerStore{
		m:       make(map[types.PeerKey]*peer),
		metrics: metrics.NewPeerMetrics(metricnoop.NewMeterProvider()),
		trans:   t,
	}
}

func (s *peerStore) runPeerTickLoop(ctx context.Context, tickInterval time.Duration) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *peerStore) tick(ctx context.Context) {
	s.mu.Lock()
	now := time.Now()
	var actions []func()

	for pk, p := range s.m {
		if now.Before(p.NextActionAt) || p.State == peerStateConnected {
			continue
		}

		if p.State == peerStateConnecting {
			if now.Sub(p.ConnectingAt) >= connectingTimeout {
				s.failConnectLocked(pk, now)
			}
			continue
		}

		if p.State == peerStateUnreachable {
			p.State = peerStateDiscovered
			p.resetStage()
			p.StageAttempts = 0
		}

		p.State = peerStateConnecting
		p.ConnectingAt = now

		// Capture current state for the async execution
		pk := p.ID
		stage := p.Stage
		lastAddr := p.LastAddr
		ips := p.Ips
		port := p.ObservedPort

		actions = append(actions, func() {
			switch stage {
			case connectStageEagerRetry:
				s.trans.acceptWG.Go(func() { _ = s.trans.Connect(ctx, pk, []netip.AddrPort{lastAddr.AddrPort()}) })
			case connectStageUnspecified, connectStageDirect:
				addrs := make([]netip.AddrPort, 0, len(ips))
				for _, ip := range ips {
					if addr, ok := netip.AddrFromSlice(ip); ok {
						addrs = append(addrs, netip.AddrPortFrom(addr, uint16(port)))
					}
				}
				s.trans.acceptWG.Go(func() { _ = s.trans.Connect(ctx, pk, addrs) })
			case connectStagePunch:
				s.trans.emitPeerEvent(PeerEvent{Key: pk, Type: peerEventNeedsPunch})
			}
		})
	}
	s.mu.Unlock()

	for _, act := range actions {
		act()
	}
}

func (s *peerStore) Discover(pk types.PeerKey, ips []net.IP, port int, lastAddr *net.UDPAddr, privatelyRoutable, publiclyAccessible bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()

	p, exists := s.m[pk]
	if !exists {
		p = &peer{
			ID:           pk,
			State:        peerStateDiscovered,
			LastAddr:     lastAddr,
			Ips:          ips,
			ObservedPort: port,
			NextActionAt: now,
		}
		p.resetStage()
		if !publiclyAccessible && !privatelyRoutable && p.LastAddr == nil {
			p.Stage = connectStagePunch
		}
		s.m[pk] = p
		return
	}

	p.Ips = ips
	if port != 0 {
		p.ObservedPort = port
	}
	if lastAddr != nil {
		p.LastAddr = lastAddr
	}

	if p.State == peerStateDiscovered && p.Stage == connectStagePunch && privatelyRoutable && p.LastAddr == nil {
		p.Stage = connectStageDirect
		p.StageAttempts = 0
		p.NextActionAt = now
	}
}

func (s *peerStore) MarkConnected(pk types.PeerKey, ip net.IP, port int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, exists := s.m[pk]
	if !exists {
		p = &peer{ID: pk}
		s.m[pk] = p
	}
	p.State = peerStateConnected
	p.Ips = []net.IP{ip}
	p.ObservedPort = port
	p.LastAddr = &net.UDPAddr{IP: ip, Port: port}
	p.ConnectedAt = time.Now()
	p.Stage = connectStageDirect
	p.StageAttempts = 0
}

func (s *peerStore) Disconnect(pk types.PeerKey, reason DisconnectReason) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, exists := s.m[pk]
	if !exists || (p.State != peerStateConnected && p.State != peerStateConnecting) {
		return
	}

	delay := unknownDisconnectRetryInterval
	switch reason { //nolint:exhaustive
	case disconnectIdleTimeout, DisconnectCertRotation:
		delay = idleTimeoutRetryInterval
	case disconnectReset, disconnectShutdown:
		delay = resetRetryInterval
	case DisconnectGraceful:
		delay = gracefulDisconnectRetryInterval
	case DisconnectTopologyPrune, DisconnectDenied:
		delay = unreachableRetryInterval
	}

	p.State = peerStateDiscovered
	p.resetStage()
	p.StageAttempts = 0
	p.NextActionAt = time.Now().Add(delay)
}

func (s *peerStore) failConnectLocked(pk types.PeerKey, now time.Time) {
	p, exists := s.m[pk]
	if !exists || p.State != peerStateConnecting {
		return
	}

	p.StageAttempts++
	switch p.Stage {
	case connectStageEagerRetry:
		if p.StageAttempts >= eagerRetryAttemptThreshold {
			p.Stage = connectStageDirect
			p.StageAttempts = 0
		}
	case connectStageDirect, connectStageUnspecified:
		if p.StageAttempts >= directAttemptThreshold {
			p.Stage = connectStagePunch
			p.StageAttempts = 0
		}
	case connectStagePunch:
		if p.StageAttempts >= punchAttemptThreshold {
			p.StageAttempts = 0
			p.State = peerStateUnreachable
			p.NextActionAt = now.Add(unreachableRetryInterval)
			return
		}
	}
	p.NextActionAt = now.Add(backoff(p.StageAttempts))
	p.State = peerStateDiscovered
}

func backoff(attempts int) time.Duration {
	if attempts == 0 {
		return firstBackoff
	}
	return min(baseBackoff*(1<<(attempts-1)), maxBackoff)
}

func (s *peerStore) stateCounts() PeerStateCounts {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var c PeerStateCounts
	for _, p := range s.m {
		switch p.State {
		case peerStateDiscovered:
			c.Backoff++
		case peerStateConnecting:
			c.Connecting++
		case peerStateConnected:
			c.Connected++
		case peerStateUnreachable:
			c.Backoff++
		}
	}
	return c
}

func (s *peerStore) Forget(pk types.PeerKey) {
	s.mu.Lock()
	delete(s.m, pk)
	s.mu.Unlock()
}
