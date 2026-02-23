package peer

import (
	"net"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

type PeerState int

const (
	PeerStateUnspecified PeerState = iota
	PeerStateDiscovered
	PeerStateConnecting
	PeerStateConnected
	PeerStateUnreachable
)

type ConnectStage int

const (
	ConnectStageUnspecified ConnectStage = iota
	ConnectStageDirect
	ConnectStagePunch
)

type Peer struct {
	NextActionAt  time.Time
	ConnectedAt   time.Time
	Ips           []net.IP
	ObservedPort  int
	State         PeerState
	Stage         ConnectStage
	StageAttempts int
	ID            types.PeerKey
}

type Store struct {
	log *zap.SugaredLogger
	m   map[types.PeerKey]*Peer
	mu  sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		log: zap.S().Named("peers"),
		m:   make(map[types.PeerKey]*Peer),
	}
}

// Input events.
type Input interface{ isInput() }

// DiscoverPeer adds a new peer or updates known addresses for an existing peer.
// Used on startup (from disk) or when learning about a peer from gossip.
type DiscoverPeer struct {
	Ips     []net.IP
	Port    int
	PeerKey types.PeerKey
}

func (DiscoverPeer) isInput() {}

// Tick triggers the state machine to evaluate all peers and emit pending actions.
type Tick struct{}

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
	DisconnectUnknown     DisconnectReason = iota
	DisconnectIdleTimeout                  // peer likely still alive, transient loss
	DisconnectReset                        // peer rebooted (stateless reset)
	DisconnectGraceful                     // clean app-level close
)

func (r DisconnectReason) String() string {
	switch r {
	case DisconnectIdleTimeout:
		return "idle_timeout"
	case DisconnectReset:
		return "stateless_reset"
	case DisconnectGraceful:
		return "graceful"
	default:
		return "unknown"
	}
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

// RequestPunchCoordination signals the caller should request NAT punch coordination.
type RequestPunchCoordination struct {
	Ips     []net.IP
	PeerKey types.PeerKey
}

func (RequestPunchCoordination) isOutput() {}

const (
	baseBackoff  = 1 * time.Second
	maxBackoff   = 60 * time.Second
	firstBackoff = 500 * time.Millisecond

	directAttemptThreshold = 2
	punchAttemptThreshold  = 2

	unreachableRetryInterval        = 20 * time.Second
	idleTimeoutRetryInterval        = 1 * time.Second
	resetRetryInterval              = 5 * time.Second
	gracefulDisconnectRetryInterval = 3 * time.Second
	unknownDisconnectRetryInterval  = 3 * time.Second
)

func (s *Store) backoff(attempts int) time.Duration {
	if attempts == 0 {
		return firstBackoff
	}
	d := min(baseBackoff*(1<<(attempts-1)), maxBackoff)
	return d
}

func (s *Store) Step(now time.Time, in Input) []Output {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch e := in.(type) {
	case Tick:
		return s.tick(now)
	case DiscoverPeer:
		s.discoverPeer(now, e)
		return nil
	case ConnectPeer:
		return s.connectPeer(now, e)
	case ConnectFailed:
		s.connectFailed(now, e)
		return nil
	case PeerDisconnected:
		s.disconnectPeer(now, e)
		return nil
	case RetryPeer:
		s.retryPeer(now, e)
		return nil
	}
	return nil
}

func (s *Store) discoverPeer(now time.Time, e DiscoverPeer) {
	p, exists := s.m[e.PeerKey]
	if !exists {
		s.m[e.PeerKey] = &Peer{
			ID:           e.PeerKey,
			State:        PeerStateDiscovered,
			Stage:        ConnectStageDirect,
			Ips:          e.Ips,
			ObservedPort: e.Port, // we don't know if the port is observed at this point
			NextActionAt: now,    // eligible for connection immediately
		}
		return
	}

	// Always update addresses — gossip may provide fresher IPs.
	p.Ips = e.Ips
	if e.Port != 0 {
		p.ObservedPort = e.Port
	}
}

func (s *Store) tick(now time.Time) []Output {
	var outputs []Output //nolint:prealloc
	for _, p := range s.m {
		if now.Before(p.NextActionAt) {
			continue
		}

		switch p.State { //nolint:exhaustive
		case PeerStateConnected, PeerStateConnecting:
			continue
		case PeerStateUnreachable:
			// Retry unreachable peers after backoff expires
			p.State = PeerStateDiscovered
			p.Stage = ConnectStageDirect
			p.StageAttempts = 0
		}

		var out Output
		switch p.Stage {
		case ConnectStageUnspecified, ConnectStageDirect:
			out = AttemptConnect{PeerKey: p.ID, Ips: p.Ips, Port: p.ObservedPort}
		case ConnectStagePunch:
			out = RequestPunchCoordination{PeerKey: p.ID, Ips: p.Ips}
		}

		outputs = append(outputs, out)
		p.State = PeerStateConnecting
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
	p.State = PeerStateConnected
	p.Ips = []net.IP{e.IP}
	p.ObservedPort = e.ObservedPort
	p.ConnectedAt = now
	p.Stage = ConnectStageDirect // reset for next time
	p.StageAttempts = 0

	return []Output{PeerConnected(e)}
}

func (s *Store) connectFailed(now time.Time, e ConnectFailed) {
	p, exists := s.m[e.PeerKey]
	if !exists || p.State != PeerStateConnecting {
		return
	}

	p.StageAttempts++

	// Check if we should escalate to next stage
	switch p.Stage {
	case ConnectStageDirect, ConnectStageUnspecified:
		if p.StageAttempts >= directAttemptThreshold {
			s.log.Debugw("escalating to punch", "peer", e.PeerKey.Short())
			p.Stage = ConnectStagePunch
			p.StageAttempts = 0
		}
	case ConnectStagePunch:
		if p.StageAttempts >= punchAttemptThreshold {
			s.log.Debugw("marking unreachable", "peer", e.PeerKey.Short())
			p.StageAttempts = 0
			p.State = PeerStateUnreachable
			p.NextActionAt = now.Add(unreachableRetryInterval)
			return
		}
	}

	p.NextActionAt = now.Add(s.backoff(p.StageAttempts))
	p.State = PeerStateDiscovered // eligible for retry after backoff
}

func (s *Store) disconnectPeer(now time.Time, e PeerDisconnected) {
	p, exists := s.m[e.PeerKey]
	if !exists || (p.State != PeerStateConnected && p.State != PeerStateConnecting) {
		return
	}

	delay := unknownDisconnectRetryInterval
	switch e.Reason { //nolint:exhaustive
	case DisconnectIdleTimeout:
		delay = idleTimeoutRetryInterval
	case DisconnectReset:
		delay = resetRetryInterval
	case DisconnectGraceful:
		delay = gracefulDisconnectRetryInterval
	}

	s.log.Debugw("scheduling reconnect",
		"peer", e.PeerKey.Short(),
		"retry_delay", delay,
	)

	p.State = PeerStateDiscovered
	p.Stage = ConnectStageDirect
	p.StageAttempts = 0
	p.NextActionAt = now.Add(delay)
}

func (s *Store) retryPeer(now time.Time, e RetryPeer) {
	p, exists := s.m[e.PeerKey]
	if !exists {
		s.m[e.PeerKey] = &Peer{
			ID:           e.PeerKey,
			State:        PeerStateDiscovered,
			Stage:        ConnectStageDirect,
			NextActionAt: now,
		}
		return
	}

	if p.State == PeerStateConnected || p.State == PeerStateConnecting {
		return
	}

	p.State = PeerStateDiscovered
	p.Stage = ConnectStageDirect
	p.StageAttempts = 0
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

func (s *Store) InState(peer types.PeerKey, state PeerState) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.m[peer]
	return ok && p.State == state
}

func (s *Store) GetAll(state PeerState) []types.PeerKey {
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
