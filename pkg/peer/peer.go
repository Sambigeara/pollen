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

// PeerDisconnected signals a peer's session has died (timeout or graceful disconnect).
// Transitions from Connected back to Discovered for reconnection.
type PeerDisconnected struct {
	PeerKey types.PeerKey
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

	unreachableRetryInterval  = 20 * time.Second
	disconnectedRetryInterval = 60 * time.Second
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
		return s.discoverPeer(now, e)
	case ConnectPeer:
		return s.connectPeer(now, e)
	case ConnectFailed:
		return s.connectFailed(now, e)
	case PeerDisconnected:
		return s.disconnectPeer(now, e)
	case RetryPeer:
		return s.retryPeer(now, e)
	}
	return nil
}

func (s *Store) discoverPeer(now time.Time, e DiscoverPeer) []Output {
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
		return nil
	}

	// Always update addresses — gossip may provide fresher IPs.
	p.Ips = e.Ips
	if e.Port != 0 {
		p.ObservedPort = e.Port
	}
	return nil
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

func (s *Store) connectFailed(now time.Time, e ConnectFailed) []Output {
	p, exists := s.m[e.PeerKey]
	if !exists || p.State != PeerStateConnecting {
		return nil
	}

	p.StageAttempts++

	// Check if we should escalate to next stage
	switch p.Stage {
	case ConnectStageDirect, ConnectStageUnspecified:
		if p.StageAttempts >= directAttemptThreshold {
			p.Stage = ConnectStagePunch
			p.StageAttempts = 0
		}
	case ConnectStagePunch:
		if p.StageAttempts >= punchAttemptThreshold {
			p.StageAttempts = 0
			p.State = PeerStateUnreachable
			p.NextActionAt = now.Add(unreachableRetryInterval)
			return nil
		}
	}

	p.NextActionAt = now.Add(s.backoff(p.StageAttempts))
	p.State = PeerStateDiscovered // eligible for retry after backoff
	return nil
}

func (s *Store) disconnectPeer(now time.Time, e PeerDisconnected) []Output {
	p, exists := s.m[e.PeerKey]
	if !exists || (p.State != PeerStateConnected && p.State != PeerStateConnecting) {
		return nil
	}

	// Transition back to Discovered for immediate reconnection attempt
	p.State = PeerStateDiscovered
	p.Stage = ConnectStageDirect
	p.StageAttempts = 0
	p.NextActionAt = now.Add(disconnectedRetryInterval)
	return nil
}

func (s *Store) retryPeer(now time.Time, e RetryPeer) []Output {
	p, exists := s.m[e.PeerKey]
	if !exists {
		s.m[e.PeerKey] = &Peer{
			ID:           e.PeerKey,
			State:        PeerStateDiscovered,
			Stage:        ConnectStageDirect,
			NextActionAt: now,
		}
		return nil
	}

	if p.State == PeerStateConnected || p.State == PeerStateConnecting {
		return nil
	}

	p.State = PeerStateDiscovered
	p.Stage = ConnectStageDirect
	p.StageAttempts = 0
	p.NextActionAt = now
	return nil
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
