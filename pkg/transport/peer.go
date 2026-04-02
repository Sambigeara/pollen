package transport

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/types"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

type peerState int

const (
	_ peerState = iota
	peerStateDiscovered
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

type peer struct {
	NextActionAt  time.Time
	ConnectedAt   time.Time
	ConnectingAt  time.Time
	LastAddr      *net.UDPAddr
	Ips           []net.IP
	ObservedPort  int
	State         peerState
	Stage         connectStage
	StageAttempts int
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
	log     *zap.SugaredLogger
	metrics *metrics.PeerMetrics
	m       map[types.PeerKey]*peer
	mu      sync.RWMutex
}

func newPeerStore() *peerStore {
	return &peerStore{
		log:     zap.S().Named("peers"),
		metrics: metrics.NewPeerMetrics(metricnoop.NewMeterProvider()),
		m:       make(map[types.PeerKey]*peer),
	}
}

func (s *peerStore) setPeerMetrics(m *metrics.PeerMetrics) {
	s.metrics = m
}

type input interface{ isInput() }

type discoverPeer struct {
	LastAddr           *net.UDPAddr
	Ips                []net.IP
	Port               int
	PeerKey            types.PeerKey
	PrivatelyRoutable  bool
	PubliclyAccessible bool
}

func (discoverPeer) isInput() {}

type tick struct{}

func (tick) isInput() {}

type connectPeer struct {
	IP           net.IP
	ObservedPort int
	PeerKey      types.PeerKey
}

func (connectPeer) isInput() {}

type connectFailed struct {
	PeerKey types.PeerKey
}

func (connectFailed) isInput() {}

type disconnectReason int

const (
	disconnectUnknown       disconnectReason = iota
	disconnectIdleTimeout                    // peer likely still alive, transient loss
	disconnectReset                          // peer rebooted (stateless reset)
	disconnectGraceful                       // clean app-level close
	disconnectTopologyPrune                  // peer intentionally pruned this edge
	disconnectDenied                         // peer denied our session/membership
	disconnectCertRotation                   // forced reconnection for cert rotation
	disconnectCertExpired                    // peer membership cert expired
	disconnectDuplicate                      // session lost tie-break; preferred session exists
	disconnectShutdown                       // peer is shutting down, will likely restart soon
)

func (r disconnectReason) String() string {
	switch r {
	case disconnectUnknown:
		return "unknown"
	case disconnectIdleTimeout:
		return "idle_timeout"
	case disconnectReset:
		return "stateless_reset"
	case disconnectGraceful:
		return "graceful"
	case disconnectTopologyPrune:
		return "topology_prune"
	case disconnectDenied:
		return "denied"
	case disconnectCertRotation:
		return "cert_rotation"
	case disconnectCertExpired:
		return "cert_expired"
	case disconnectDuplicate:
		return "duplicate"
	case disconnectShutdown:
		return "shutdown"
	}
	return "unknown"
}

type peerDisconnected struct {
	PeerKey types.PeerKey
	Reason  disconnectReason
}

func (peerDisconnected) isInput() {}

type retryPeer struct {
	PeerKey types.PeerKey
}

func (retryPeer) isInput() {}

type forgetPeer struct {
	PeerKey types.PeerKey
}

func (forgetPeer) isInput() {}

type output interface{ isOutput() }

type attemptConnect struct {
	Ips     []net.IP
	Port    int
	PeerKey types.PeerKey
}

func (attemptConnect) isOutput() {}

type attemptEagerConnect struct {
	Addr    *net.UDPAddr
	PeerKey types.PeerKey
}

func (attemptEagerConnect) isOutput() {}

type requestPunchCoordination struct {
	PeerKey types.PeerKey
}

func (requestPunchCoordination) isOutput() {}

const (
	baseBackoff  = 1 * time.Second
	maxBackoff   = 10 * time.Minute
	firstBackoff = 500 * time.Millisecond

	eagerRetryAttemptThreshold = 1
	directAttemptThreshold     = 2
	punchAttemptThreshold      = 2

	connectingTimeout = 10 * time.Second

	unreachableRetryInterval        = 20 * time.Second
	idleTimeoutRetryInterval        = 1 * time.Second
	resetRetryInterval              = 5 * time.Second
	gracefulDisconnectRetryInterval = 1 * time.Hour
	unknownDisconnectRetryInterval  = 3 * time.Second
)

func backoff(attempts int) time.Duration {
	if attempts == 0 {
		return firstBackoff
	}
	return min(baseBackoff*(1<<(attempts-1)), maxBackoff)
}

func (s *peerStore) step(now time.Time, in input) []output {
	s.mu.Lock()
	defer s.mu.Unlock()

	var before map[types.PeerKey]peerState
	if s.metrics.Enabled(context.Background()) {
		before = s.stateSnapshot()
	}

	var out []output
	switch e := in.(type) {
	case tick:
		out = s.tick(now)
	case discoverPeer:
		s.discoverPeer(now, e)
	case connectPeer:
		s.connectPeer(now, e)
	case connectFailed:
		s.connectFailed(now, e)
	case peerDisconnected:
		s.disconnectPeer(now, e)
	case retryPeer:
		s.retryPeer(now, e)
	case forgetPeer:
		delete(s.m, e.PeerKey)
	}

	if before != nil {
		s.countTransitions(before)
	}
	s.updateGauges()
	return out
}

func (s *peerStore) stateSnapshot() map[types.PeerKey]peerState {
	snap := make(map[types.PeerKey]peerState, len(s.m))
	for k, p := range s.m {
		snap[k] = p.State
	}
	return snap
}

func (s *peerStore) countTransitions(before map[types.PeerKey]peerState) {
	var transitions int64
	for k, p := range s.m {
		prev, existed := before[k]
		if !existed || prev != p.State {
			transitions++
		}
	}
	if transitions > 0 {
		s.metrics.StateTransitions.Add(context.Background(), transitions)
	}
}

func (s *peerStore) updateGauges() {
	ctx := context.Background()
	if !s.metrics.Enabled(ctx) {
		return
	}
	c := s.stateCountsLocked()
	s.metrics.PeersDiscovered.Record(ctx, float64(c.Backoff))
	s.metrics.PeersConnecting.Record(ctx, float64(c.Connecting))
	s.metrics.PeersConnected.Record(ctx, float64(c.Connected))
}

func (s *peerStore) discoverPeer(now time.Time, e discoverPeer) {
	p, exists := s.m[e.PeerKey]
	if !exists {
		p := &peer{
			ID:           e.PeerKey,
			State:        peerStateDiscovered,
			LastAddr:     e.LastAddr,
			Ips:          e.Ips,
			ObservedPort: e.Port,
			NextActionAt: now,
		}
		p.resetStage()
		if !e.PubliclyAccessible && !e.PrivatelyRoutable && p.LastAddr == nil {
			p.Stage = connectStagePunch
		}
		s.m[e.PeerKey] = p
		// TODO(saml): remove diagnostic logging once topology reconnection bug is resolved
		s.log.Debugw("peer discovered (new)",
			"peer", e.PeerKey.Short(),
			"has_last_addr", e.LastAddr != nil,
			"ips", len(e.Ips),
			"stage", p.Stage,
		)
		return
	}

	p.Ips = e.Ips
	if e.Port != 0 {
		p.ObservedPort = e.Port
	}
	if e.LastAddr != nil {
		p.LastAddr = e.LastAddr
	}
	if p.State == peerStateDiscovered && p.Stage == connectStagePunch && e.PrivatelyRoutable && p.LastAddr == nil {
		p.Stage = connectStageDirect
		p.StageAttempts = 0
		p.NextActionAt = now
	}
}

func (s *peerStore) tick(now time.Time) []output {
	var outputs []output //nolint:prealloc
	for _, p := range s.m {
		if now.Before(p.NextActionAt) {
			continue
		}

		switch p.State {
		case peerStateConnected:
			continue
		case peerStateConnecting:
			if now.Sub(p.ConnectingAt) < connectingTimeout {
				continue
			}
			s.connectFailed(now, connectFailed{PeerKey: p.ID})
			continue
		case peerStateUnreachable:
			p.State = peerStateDiscovered
			p.resetStage()
			p.StageAttempts = 0
		case peerStateDiscovered:
		}

		var out output
		switch p.Stage {
		case connectStageEagerRetry:
			out = attemptEagerConnect{PeerKey: p.ID, Addr: p.LastAddr}
		case connectStageUnspecified, connectStageDirect:
			out = attemptConnect{PeerKey: p.ID, Ips: p.Ips, Port: p.ObservedPort}
		case connectStagePunch:
			out = requestPunchCoordination{PeerKey: p.ID}
		}

		outputs = append(outputs, out)
		p.State = peerStateConnecting
		p.ConnectingAt = now
	}
	return outputs
}

func (s *peerStore) connectPeer(now time.Time, e connectPeer) {
	p, exists := s.m[e.PeerKey]
	if !exists {
		p = &peer{ID: e.PeerKey}
		s.m[e.PeerKey] = p
	}

	p.State = peerStateConnected
	p.Ips = []net.IP{e.IP}
	p.ObservedPort = e.ObservedPort
	p.LastAddr = &net.UDPAddr{IP: e.IP, Port: e.ObservedPort}
	p.ConnectedAt = now
	p.Stage = connectStageDirect
	p.StageAttempts = 0

	s.metrics.Connections.Add(context.Background(), 1)
}

func (s *peerStore) connectFailed(now time.Time, e connectFailed) {
	p, exists := s.m[e.PeerKey]
	if !exists || p.State != peerStateConnecting {
		return
	}

	p.StageAttempts++

	switch p.Stage {
	case connectStageEagerRetry:
		if p.StageAttempts >= eagerRetryAttemptThreshold {
			s.log.Debugw("eager retry failed, falling back to direct", "peer", e.PeerKey.Short())
			p.Stage = connectStageDirect
			p.StageAttempts = 0
			s.metrics.StageEscalations.Add(context.Background(), 1)
		}
	case connectStageDirect, connectStageUnspecified:
		if p.StageAttempts >= directAttemptThreshold {
			s.log.Debugw("escalating to punch", "peer", e.PeerKey.Short())
			p.Stage = connectStagePunch
			p.StageAttempts = 0
			s.metrics.StageEscalations.Add(context.Background(), 1)
		}
	case connectStagePunch:
		if p.StageAttempts >= punchAttemptThreshold {
			s.log.Debugw("marking unreachable", "peer", e.PeerKey.Short())
			p.StageAttempts = 0
			p.State = peerStateUnreachable
			p.NextActionAt = now.Add(unreachableRetryInterval)
			s.metrics.StageEscalations.Add(context.Background(), 1)
			return
		}
	}

	p.NextActionAt = now.Add(backoff(p.StageAttempts))
	p.State = peerStateDiscovered
}

func (s *peerStore) disconnectPeer(now time.Time, e peerDisconnected) {
	p, exists := s.m[e.PeerKey]
	if !exists || (p.State != peerStateConnected && p.State != peerStateConnecting) {
		return
	}

	delay := unknownDisconnectRetryInterval
	switch e.Reason {
	case disconnectDuplicate:
	case disconnectUnknown:
	case disconnectIdleTimeout:
		delay = idleTimeoutRetryInterval
	case disconnectReset:
		delay = resetRetryInterval
	case disconnectGraceful:
		delay = gracefulDisconnectRetryInterval
	case disconnectTopologyPrune:
		delay = unreachableRetryInterval
	case disconnectDenied:
		delay = unreachableRetryInterval
	case disconnectCertRotation:
		delay = idleTimeoutRetryInterval
	case disconnectCertExpired:
		delay = unknownDisconnectRetryInterval
	case disconnectShutdown:
		delay = resetRetryInterval
	}

	s.metrics.Disconnects.Add(context.Background(), 1)

	s.log.Debugw("scheduling reconnect",
		"peer", e.PeerKey.Short(),
		"retry_delay", delay,
	)

	p.State = peerStateDiscovered
	p.resetStage()
	p.StageAttempts = 0
	p.NextActionAt = now.Add(delay)
}

func (s *peerStore) retryPeer(now time.Time, e retryPeer) {
	p, exists := s.m[e.PeerKey]
	if !exists {
		p := &peer{
			ID:           e.PeerKey,
			State:        peerStateDiscovered,
			NextActionAt: now,
		}
		p.resetStage()
		s.m[e.PeerKey] = p
		return
	}

	if p.State == peerStateConnected || p.State == peerStateConnecting {
		return
	}

	p.State = peerStateDiscovered
	p.resetStage()
	p.StageAttempts = 0
	p.NextActionAt = now
}

func (s *peerStore) get(pk types.PeerKey) (peer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.m[pk]
	if !ok {
		return peer{}, false
	}
	return *p, ok
}

func (s *peerStore) inState(pk types.PeerKey, state peerState) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.m[pk]
	return ok && p.State == state
}

func (s *peerStore) stateCounts() PeerStateCounts {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stateCountsLocked()
}

func (s *peerStore) stateCountsLocked() PeerStateCounts {
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
