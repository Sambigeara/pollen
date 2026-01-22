package peer

import (
	"time"

	"github.com/sambigeara/pollen/pkg/types"
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
	addrs         []string
	identityPub   []byte
	state         PeerState
	stage         ConnectStage
	stageAttempts int
	id            types.PeerKey
}

type Store struct {
	m map[types.PeerKey]*Peer
}

func NewStore() *Store {
	return &Store{
		m: make(map[types.PeerKey]*Peer),
	}
}

// Input events.
type Input interface{ isInput() }

// DiscoverPeer adds a new peer or updates known addresses for an existing peer.
// Used on startup (from disk) or when learning about a peer from gossip.
type DiscoverPeer struct {
	Addrs   []string
	PeerKey types.PeerKey
}

func (DiscoverPeer) isInput() {}

// Tick triggers the state machine to evaluate all peers and emit pending actions.
type Tick struct{}

func (Tick) isInput() {}

// ConnectPeer signals a successful connection (handshake complete).
type ConnectPeer struct {
	Addr        string
	IdentityPub []byte
	PeerKey     types.PeerKey
}

func (ConnectPeer) isInput() {}

// ConnectFailed signals a failed connection attempt.
type ConnectFailed struct {
	PeerKey types.PeerKey
}

func (ConnectFailed) isInput() {}

// UpdatePeerAddrs updates addresses for a peer, typically from gossip.
// Resets backoff to allow immediate retry with new addresses.
type UpdatePeerAddrs struct {
	Addrs   []string
	PeerKey types.PeerKey
}

func (UpdatePeerAddrs) isInput() {}

// PeerDisconnected signals a peer's session has died (timeout or graceful disconnect).
// Transitions from Connected back to Discovered for reconnection.
type PeerDisconnected struct {
	PeerKey types.PeerKey
}

func (PeerDisconnected) isInput() {}

// Output effects.
type Output interface{ isOutput() }

// PeerConnected signals a peer has transitioned to connected state.
type PeerConnected struct {
	PeerKey types.PeerKey
}

func (PeerConnected) isOutput() {}

// AttemptConnect signals the caller should try a direct connection to this peer.
type AttemptConnect struct {
	Addrs   []string
	PeerKey types.PeerKey
}

func (AttemptConnect) isOutput() {}

// RequestPunchCoordination signals the caller should request NAT punch coordination.
type RequestPunchCoordination struct {
	Addrs   []string
	PeerKey types.PeerKey
}

func (RequestPunchCoordination) isOutput() {}

// Configuration.
const (
	baseBackoff  = 1 * time.Second
	maxBackoff   = 60 * time.Second
	firstBackoff = 500 * time.Millisecond

	// Stage thresholds: how many failures before escalating to next stage.
	directAttemptThreshold = 1
	punchAttemptThreshold  = 3

	// How long to wait before retrying an unreachable peer.
	unreachableRetryInterval = 60 * time.Second
)

func backoff(attempts int) time.Duration {
	if attempts == 0 {
		return firstBackoff
	}
	// exponential: 1s, 2s, 4s, 8s, ...
	d := min(baseBackoff*(1<<(attempts-1)), maxBackoff)
	return d
}

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
	case UpdatePeerAddrs:
		return s.updatePeerAddrs(now, e)
	case PeerDisconnected:
		return s.disconnectPeer(now, e)
	}
	return nil
}

func (s *Store) discoverPeer(now time.Time, e DiscoverPeer) []Output {
	p, exists := s.m[e.PeerKey]
	if !exists {
		p = &Peer{
			id:           e.PeerKey,
			state:        PeerStateDiscovered,
			stage:        ConnectStageDirect,
			addrs:        e.Addrs,
			nextActionAt: now, // eligible for connection immediately
		}
		s.m[e.PeerKey] = p
		return nil
	}

	// Update addresses if peer already exists but isn't connected
	if p.state != PeerStateConnected {
		p.addrs = e.Addrs
	}
	return nil
}

func (s *Store) tick(now time.Time) []Output {
	var outputs []Output
	for _, p := range s.m {
		switch p.state {
		case PeerStateConnected, PeerStateConnecting:
			continue
		case PeerStateUnreachable:
			// Retry unreachable peers after backoff expires
			if now.Before(p.nextActionAt) {
				continue
			}
			p.state = PeerStateDiscovered
			p.stage = ConnectStageDirect
			p.stageAttempts = 0
			// Fall through to connection logic
		}

		if len(p.addrs) == 0 || now.Before(p.nextActionAt) {
			continue
		}

		var out Output
		switch p.stage {
		case ConnectStageDirect, ConnectStageUnspecified:
			out = AttemptConnect{PeerKey: p.id, Addrs: p.addrs}
		case ConnectStagePunch:
			out = RequestPunchCoordination{PeerKey: p.id, Addrs: p.addrs}
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
	p.identityPub = e.IdentityPub
	p.connectedAt = now
	p.stage = ConnectStageDirect // reset for next time
	p.stageAttempts = 0

	return []Output{PeerConnected{PeerKey: e.PeerKey}}
}

func (s *Store) connectFailed(now time.Time, e ConnectFailed) []Output {
	p, exists := s.m[e.PeerKey]
	if !exists || p.state == PeerStateConnected {
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
			// All connection methods exhausted, retry after long backoff
			p.state = PeerStateUnreachable
			p.nextActionAt = now.Add(unreachableRetryInterval)
			return nil
		}
	case ConnectStageRelay:
		// TODO(saml) implement relay stage
	}

	p.nextActionAt = now.Add(backoff(p.stageAttempts))
	p.state = PeerStateDiscovered // eligible for retry after backoff
	return nil
}

func (s *Store) disconnectPeer(now time.Time, e PeerDisconnected) []Output {
	p, exists := s.m[e.PeerKey]
	if !exists || p.state != PeerStateConnected {
		return nil
	}

	// Transition back to Discovered for immediate reconnection attempt
	p.state = PeerStateDiscovered
	p.stage = ConnectStageDirect
	p.stageAttempts = 0
	p.nextActionAt = now
	return nil
}

func (s *Store) updatePeerAddrs(now time.Time, e UpdatePeerAddrs) []Output {
	p, exists := s.m[e.PeerKey]
	if !exists {
		// Treat as discovery
		return s.discoverPeer(now, DiscoverPeer(e))
	}

	if p.state == PeerStateConnected {
		// Still update addresses for future reconnection
		p.addrs = e.Addrs
		return nil
	}

	// Update addresses and reset to direct stage for immediate retry
	p.addrs = e.Addrs
	p.nextActionAt = now
	p.stage = ConnectStageDirect
	p.stageAttempts = 0
	p.state = PeerStateDiscovered
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
