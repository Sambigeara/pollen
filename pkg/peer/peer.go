package peer

import (
	"net"
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
	PeerStateStale
	PeerStateUnreachable
	PeerStateRevoked
)

type ConnectStage int

const (
	ConnectStageUnspecified ConnectStage = iota
	ConnectStageDirect
	ConnectStagePunch
	ConnectStageRelay
)

type Peer struct {
	nextActionAt  time.Time
	connectedAt   time.Time
	ips           []net.IP
	observedPort  int
	state         PeerState
	stage         ConnectStage
	stageAttempts int
	id            types.PeerKey
}

type Store struct {
	log *zap.SugaredLogger
	m   map[types.PeerKey]*Peer
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

// RemovePeer stops tracking a peer entirely. Used by the TCP FSM when no
// tunnels to a peer remain.
type RemovePeer struct {
	PeerKey types.PeerKey
}

func (RemovePeer) isInput() {}

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

// Configuration.
const (
	baseBackoff  = 1 * time.Second
	maxBackoff   = 60 * time.Second
	firstBackoff = 500 * time.Millisecond

	directAttemptThreshold = 1
	punchAttemptThreshold  = 1

	unreachableRetryInterval  = 60 * time.Second
	disconnectedRetryInterval = 60 * time.Second
)

func (s *Store) backoff(attempts int) time.Duration {
	if attempts == 0 {
		return firstBackoff
	}
	// exponential: 1s, 2s, 4s, 8s, ...
	d := min(baseBackoff*(1<<(attempts-1)), maxBackoff)
	return d
}

// Step progresses the state machine according to the input. It must only
// be called from a single thread.
func (s *Store) Step(now time.Time, in Input) []Output {
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
	case RemovePeer:
		return s.removePeer(e)
	}
	return nil
}

func (s *Store) discoverPeer(now time.Time, e DiscoverPeer) []Output {
	_, exists := s.m[e.PeerKey]
	if !exists {
		s.m[e.PeerKey] = &Peer{
			id:           e.PeerKey,
			state:        PeerStateDiscovered,
			stage:        ConnectStageDirect,
			ips:          e.Ips,
			observedPort: e.Port, // we don't know if the port is observed at this point
			nextActionAt: now,    // eligible for connection immediately
		}
		return nil
	}

	return nil
}

func (s *Store) tick(now time.Time) []Output {
	var outputs []Output //nolint:prealloc
	for _, p := range s.m {
		if now.Before(p.nextActionAt) {
			continue
		}

		switch p.state { //nolint:exhaustive
		case PeerStateConnected, PeerStateConnecting:
			continue
		case PeerStateUnreachable:
			// Retry unreachable peers after backoff expires
			p.state = PeerStateDiscovered
			p.stage = ConnectStageDirect
			p.stageAttempts = 0
		}

		var out Output
		switch p.stage {
		case ConnectStageUnspecified, ConnectStageDirect:
			out = AttemptConnect{PeerKey: p.id, Ips: p.ips, Port: p.observedPort}
		case ConnectStagePunch:
			out = RequestPunchCoordination{PeerKey: p.id, Ips: p.ips}
		case ConnectStageRelay:
			// TODO(saml): implement relay stage
			continue
		}

		outputs = append(outputs, out)
		p.state = PeerStateConnecting
	}
	return outputs
}

func (s *Store) connectPeer(now time.Time, e ConnectPeer) []Output {
	p, exists := s.m[e.PeerKey]
	if !exists {
		p = &Peer{id: e.PeerKey}
		s.m[e.PeerKey] = p
	}

	// Always update state, even if already connected (peer may have restarted)
	p.state = PeerStateConnected
	p.ips = []net.IP{e.IP}
	p.observedPort = e.ObservedPort
	p.connectedAt = now
	p.stage = ConnectStageDirect // reset for next time
	p.stageAttempts = 0

	return []Output{PeerConnected(e)}
}

func (s *Store) connectFailed(now time.Time, e ConnectFailed) []Output {
	p, exists := s.m[e.PeerKey]
	if !exists || p.state != PeerStateConnecting {
		return nil
	}

	p.stageAttempts++

	// Check if we should escalate to next stage
	switch p.stage {
	case ConnectStageDirect, ConnectStageUnspecified:
		if p.stageAttempts >= directAttemptThreshold {
			p.stage = ConnectStagePunch
			p.stageAttempts = 0
		}
	case ConnectStagePunch:
		if p.stageAttempts >= punchAttemptThreshold {
			// All connection methods exhausted, fall back to relay (currently unreachable)
			p.state = PeerStateUnreachable
			p.nextActionAt = now.Add(unreachableRetryInterval)
			return nil
		}
	case ConnectStageRelay:
		// TODO(saml) implement relay stage
	}

	p.nextActionAt = now.Add(s.backoff(p.stageAttempts))
	p.state = PeerStateDiscovered // eligible for retry after backoff
	return nil
}

func (s *Store) disconnectPeer(now time.Time, e PeerDisconnected) []Output {
	p, exists := s.m[e.PeerKey]
	if !exists || (p.state != PeerStateConnected && p.state != PeerStateConnecting) {
		return nil
	}

	// Transition back to Discovered for immediate reconnection attempt
	p.state = PeerStateDiscovered
	p.stage = ConnectStageDirect
	p.stageAttempts = 0
	p.nextActionAt = now.Add(disconnectedRetryInterval)
	return nil
}

func (s *Store) removePeer(e RemovePeer) []Output {
	delete(s.m, e.PeerKey)
	return nil
}

func (s *Store) retryPeer(now time.Time, e RetryPeer) []Output {
	p, exists := s.m[e.PeerKey]
	if !exists {
		s.m[e.PeerKey] = &Peer{
			id:           e.PeerKey,
			state:        PeerStateDiscovered,
			stage:        ConnectStageDirect,
			nextActionAt: now,
		}
		return nil
	}

	if p.state == PeerStateConnected || p.state == PeerStateConnecting {
		return nil
	}

	p.state = PeerStateDiscovered
	p.stage = ConnectStageDirect
	p.stageAttempts = 0
	p.nextActionAt = now
	return nil
}

// GetAll returns all peers in the given state.
func (s *Store) GetAll(state PeerState) []types.PeerKey {
	var out []types.PeerKey
	for k, p := range s.m {
		if p.state == state {
			out = append(out, k)
		}
	}
	return out
}
