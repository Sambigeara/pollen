package peer

import (
	"net"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/observability/metrics"
	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

type State int

const (
	_ State = iota
	Discovered
	Connecting
	Connected
	Unreachable
)

type ConnectStage int

const (
	ConnectStageDirect ConnectStage = iota
	ConnectStageEagerRetry
	ConnectStagePunch
)

type Peer struct {
	NextActionAt  time.Time
	ConnectedAt   time.Time
	connectingAt  time.Time
	LastAddr      *net.UDPAddr
	Ips           []net.IP
	ObservedPort  int
	State         State
	stage         ConnectStage
	stageAttempts int
	ID            types.PeerKey
}

func (p *Peer) resetStage() {
	if p.LastAddr != nil {
		p.stage = ConnectStageEagerRetry
	} else {
		p.stage = ConnectStageDirect
	}
}

type Store struct {
	log     *zap.SugaredLogger
	metrics *metrics.PeerMetrics
	m       map[types.PeerKey]*Peer
	mu      sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		log:     zap.S().Named("peers"),
		metrics: &metrics.PeerMetrics{},
		m:       make(map[types.PeerKey]*Peer),
	}
}

// SetPeerMetrics replaces the no-op metrics with wired instruments.
func (s *Store) SetPeerMetrics(m *metrics.PeerMetrics) {
	s.metrics = m
}

// Input events.
type Input interface{ isInput() }

// DiscoverPeer adds a new peer or updates known addresses for an existing peer.
// Used on startup (from disk) or when learning about a peer from gossip.
type DiscoverPeer struct {
	LastAddr           *net.UDPAddr
	Ips                []net.IP
	Port               int
	PeerKey            types.PeerKey
	PrivatelyRoutable  bool
	PubliclyAccessible bool
}

func (DiscoverPeer) isInput() {}

// Tick triggers the state machine to evaluate all peers and emit pending actions.
// MaxConnect caps the number of new connection attempts per tick to prevent
// thundering-herd on cold start. Zero or negative disables the cap.
type Tick struct {
	MaxConnect int
}

func (Tick) isInput() {}

// ConnectPeer signals a successful connection (handshake complete).
type ConnectPeer struct {
	IP           net.IP
	ObservedPort int
	PeerKey      types.PeerKey
}

func (ConnectPeer) isInput() {}

// ConnectFailed signals a failed connection attempt.
type ConnectFailed struct {
	PeerKey types.PeerKey
}

func (ConnectFailed) isInput() {}

type DisconnectReason int

const (
	DisconnectUnknown       DisconnectReason = iota
	DisconnectIdleTimeout                    // peer likely still alive, transient loss
	DisconnectReset                          // peer rebooted (stateless reset)
	DisconnectGraceful                       // clean app-level close
	DisconnectTopologyPrune                  // peer intentionally pruned this edge
	DisconnectDenied                         // peer denied our session/membership
	DisconnectCertRotation                   // forced reconnection for cert rotation
	DisconnectCertExpired                    // peer membership cert expired
)

func (r DisconnectReason) String() string {
	switch r {
	case DisconnectUnknown:
		return "unknown"
	case DisconnectIdleTimeout:
		return "idle_timeout"
	case DisconnectReset:
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
	}
	panic("unreachable")
}

type PeerDisconnected struct {
	PeerKey types.PeerKey
	Reason  DisconnectReason
}

func (PeerDisconnected) isInput() {}

type RetryPeer struct {
	PeerKey types.PeerKey
}

func (RetryPeer) isInput() {}

// ForgetPeer removes a peer from the state machine entirely, preventing
// further dial attempts (e.g. after revocation or cert expiry).
type ForgetPeer struct {
	PeerKey types.PeerKey
}

func (ForgetPeer) isInput() {}

// Output effects.
type Output interface{ isOutput() }

// PeerConnected signals a peer has transitioned to connected state.
type PeerConnected struct {
	IP           net.IP
	ObservedPort int
	PeerKey      types.PeerKey
}

func (PeerConnected) isOutput() {}

// AttemptConnect signals the caller should try a direct connection to this peer.
type AttemptConnect struct {
	Ips     []net.IP
	Port    int
	PeerKey types.PeerKey
}

func (AttemptConnect) isOutput() {}

// AttemptEagerConnect signals the caller should try connecting to a previously-known address.
type AttemptEagerConnect struct {
	Addr    *net.UDPAddr
	PeerKey types.PeerKey
}

func (AttemptEagerConnect) isOutput() {}

// RequestPunchCoordination signals the caller should request NAT punch coordination.
type RequestPunchCoordination struct {
	Ips     []net.IP
	PeerKey types.PeerKey
}

func (RequestPunchCoordination) isOutput() {}

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

func (s *Store) Step(now time.Time, in Input) []Output {
	s.mu.Lock()
	defer s.mu.Unlock()

	var before map[types.PeerKey]State
	if s.metrics.StateTransitions != nil {
		before = s.stateSnapshot()
	}

	var out []Output
	switch e := in.(type) {
	case Tick:
		out = s.tick(now, e.MaxConnect)
	case DiscoverPeer:
		s.discoverPeer(now, e)
	case ConnectPeer:
		out = s.connectPeer(now, e)
	case ConnectFailed:
		s.connectFailed(now, e)
	case PeerDisconnected:
		s.disconnectPeer(now, e)
	case RetryPeer:
		s.retryPeer(now, e)
	case ForgetPeer:
		delete(s.m, e.PeerKey)
	}

	if before != nil {
		s.countTransitions(before)
	}
	s.updateGauges()
	return out
}

func (s *Store) stateSnapshot() map[types.PeerKey]State {
	snap := make(map[types.PeerKey]State, len(s.m))
	for k, p := range s.m {
		snap[k] = p.State
	}
	return snap
}

func (s *Store) countTransitions(before map[types.PeerKey]State) {
	var transitions int64
	for k, p := range s.m {
		prev, existed := before[k]
		if !existed || prev != p.State {
			transitions++
		}
	}
	if transitions > 0 {
		s.metrics.StateTransitions.Add(transitions)
	}
}

func (s *Store) updateGauges() {
	if !s.metrics.Enabled() {
		return
	}
	c := s.stateCountsLocked()
	s.metrics.PeersDiscovered.Set(float64(c.Discovered))
	s.metrics.PeersConnecting.Set(float64(c.Connecting))
	s.metrics.PeersConnected.Set(float64(c.Connected))
	s.metrics.PeersUnreachable.Set(float64(c.Unreachable))
}

func (s *Store) discoverPeer(now time.Time, e DiscoverPeer) {
	p, exists := s.m[e.PeerKey]
	if !exists {
		p := &Peer{
			ID:           e.PeerKey,
			State:        Discovered,
			LastAddr:     e.LastAddr,
			Ips:          e.Ips,
			ObservedPort: e.Port,
			NextActionAt: now, // eligible for connection immediately
		}
		p.resetStage()
		if !e.PubliclyAccessible && !e.PrivatelyRoutable && p.LastAddr == nil {
			p.stage = ConnectStagePunch
		}
		s.m[e.PeerKey] = p
		return
	}

	// Always update addresses — gossip may provide fresher IPs.
	p.Ips = e.Ips
	if e.Port != 0 {
		p.ObservedPort = e.Port
	}
	if e.LastAddr != nil {
		p.LastAddr = e.LastAddr
	}
	if p.State == Discovered && p.stage == ConnectStagePunch && e.PrivatelyRoutable && p.LastAddr == nil {
		p.stage = ConnectStageDirect
		p.stageAttempts = 0
		p.NextActionAt = now
	}
}

func (s *Store) tick(now time.Time, maxConnect int) []Output {
	var outputs []Output //nolint:prealloc
	for _, p := range s.m {
		if now.Before(p.NextActionAt) {
			continue
		}

		switch p.State {
		case Connected:
			continue
		case Connecting:
			if now.Sub(p.connectingAt) < connectingTimeout {
				continue
			}
			s.connectTimedOut(now, p)
			continue
		case Unreachable:
			p.State = Discovered
			p.resetStage()
			p.stageAttempts = 0
		case Discovered:
		}

		if maxConnect > 0 && len(outputs) >= maxConnect {
			continue
		}

		var out Output
		switch p.stage {
		case ConnectStageEagerRetry:
			out = AttemptEagerConnect{PeerKey: p.ID, Addr: p.LastAddr}
		case ConnectStageDirect:
			out = AttemptConnect{PeerKey: p.ID, Ips: p.Ips, Port: p.ObservedPort}
		case ConnectStagePunch:
			out = RequestPunchCoordination{PeerKey: p.ID, Ips: p.Ips}
		}

		outputs = append(outputs, out)
		p.State = Connecting
		p.connectingAt = now
	}
	return outputs
}

func (s *Store) connectPeer(now time.Time, e ConnectPeer) []Output {
	p, exists := s.m[e.PeerKey]
	if !exists {
		p = &Peer{ID: e.PeerKey}
		s.m[e.PeerKey] = p
	}

	// Always update state, even if already connected (peer may have restarted)
	p.State = Connected
	p.Ips = []net.IP{e.IP}
	p.ObservedPort = e.ObservedPort
	p.LastAddr = &net.UDPAddr{IP: e.IP, Port: e.ObservedPort}
	p.ConnectedAt = now
	p.stage = ConnectStageDirect // reset for next time
	p.stageAttempts = 0

	s.metrics.Connections.Inc()
	return []Output{PeerConnected(e)}
}

func (s *Store) connectFailed(now time.Time, e ConnectFailed) {
	p, exists := s.m[e.PeerKey]
	if !exists || p.State != Connecting {
		return
	}

	p.stageAttempts++

	// Check if we should escalate to next stage
	switch p.stage {
	case ConnectStageEagerRetry:
		if p.stageAttempts >= eagerRetryAttemptThreshold {
			s.log.Debugw("eager retry failed, falling back to direct", "peer", e.PeerKey.Short())
			p.stage = ConnectStageDirect
			p.stageAttempts = 0
			s.metrics.StageEscalations.Inc()
		}
	case ConnectStageDirect:
		if p.stageAttempts >= directAttemptThreshold {
			s.log.Debugw("escalating to punch", "peer", e.PeerKey.Short())
			p.stage = ConnectStagePunch
			p.stageAttempts = 0
			s.metrics.StageEscalations.Inc()
		}
	case ConnectStagePunch:
		if p.stageAttempts >= punchAttemptThreshold {
			s.log.Debugw("marking unreachable", "peer", e.PeerKey.Short())
			p.stageAttempts = 0
			p.State = Unreachable
			p.NextActionAt = now.Add(unreachableRetryInterval)
			s.metrics.StageEscalations.Inc()
			return
		}
	}

	p.NextActionAt = now.Add(backoff(p.stageAttempts))
	p.State = Discovered // eligible for retry after backoff
}

// connectTimedOut handles a peer that has been stuck in Connecting
// beyond connectingTimeout. It runs the same stage-escalation logic as
// connectFailed so that punch-stage peers eventually reach Unreachable.
func (s *Store) connectTimedOut(now time.Time, p *Peer) {
	s.connectFailed(now, ConnectFailed{PeerKey: p.ID})
}

func (s *Store) disconnectPeer(now time.Time, e PeerDisconnected) {
	p, exists := s.m[e.PeerKey]
	if !exists || (p.State != Connected && p.State != Connecting) {
		return
	}

	delay := unknownDisconnectRetryInterval
	switch e.Reason {
	case DisconnectUnknown:
	case DisconnectIdleTimeout:
		delay = idleTimeoutRetryInterval
	case DisconnectReset:
		delay = resetRetryInterval
	case DisconnectGraceful:
		delay = gracefulDisconnectRetryInterval
	case DisconnectTopologyPrune:
		delay = unreachableRetryInterval
	case DisconnectDenied:
		delay = unreachableRetryInterval
	case DisconnectCertRotation:
		delay = idleTimeoutRetryInterval
	case DisconnectCertExpired:
		delay = unknownDisconnectRetryInterval
	}

	s.metrics.Disconnects.Inc()

	s.log.Debugw("scheduling reconnect",
		"peer", e.PeerKey.Short(),
		"retry_delay", delay,
	)

	p.State = Discovered
	p.resetStage()
	p.stageAttempts = 0
	p.NextActionAt = now.Add(delay)
}

func (s *Store) retryPeer(now time.Time, e RetryPeer) {
	p, exists := s.m[e.PeerKey]
	if !exists {
		p := &Peer{
			ID:           e.PeerKey,
			State:        Discovered,
			NextActionAt: now,
		}
		p.resetStage()
		s.m[e.PeerKey] = p
		return
	}

	if p.State == Connected || p.State == Connecting {
		return
	}

	p.State = Discovered
	p.resetStage()
	p.stageAttempts = 0
	p.NextActionAt = now
}

func (s *Store) Get(peer types.PeerKey) (Peer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.m[peer]
	if !ok {
		return Peer{}, false
	}
	return *p, ok
}

func (s *Store) InState(peer types.PeerKey, state State) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.m[peer]
	return ok && p.State == state
}

func (s *Store) GetAll(state State) []types.PeerKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []types.PeerKey
	for k, p := range s.m {
		if p.State == state {
			out = append(out, k)
		}
	}
	return out
}

// StateCounts holds per-state peer counts.
type StateCounts struct {
	Discovered  uint32
	Connecting  uint32
	Connected   uint32
	Unreachable uint32
}

func (s *Store) StateCounts() StateCounts {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stateCountsLocked()
}

func (s *Store) stateCountsLocked() StateCounts {
	var c StateCounts
	for _, p := range s.m {
		switch p.State {
		case Discovered:
			c.Discovered++
		case Connecting:
			c.Connecting++
		case Connected:
			c.Connected++
		case Unreachable:
			c.Unreachable++
		}
	}
	return c
}
