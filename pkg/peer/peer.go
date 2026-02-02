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
	ConnectStagePunchBirthday
	ConnectStageRelay
)

type PunchMode int

const (
	PunchModeUnspecified PunchMode = iota
	PunchModeDirect
	PunchModeBirthday
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

type Config struct {
	FirstBackoff                  time.Duration
	BaseBackoff                   time.Duration
	MaxBackoff                    time.Duration
	DirectAttemptThreshold        int
	PunchAttemptThreshold         int
	PunchBirthdayAttemptThreshold int
	UnreachableRetryInterval      time.Duration
	DisconnectedRetryInterval     time.Duration
}

type Store struct {
	m   map[types.PeerKey]*Peer
	cfg Config
}

func NewStore() *Store {
	return NewStoreWithConfig(DefaultConfig())
}

func NewStoreWithConfig(cfg Config) *Store {
	cfg = cfg.withDefaults()
	return &Store{
		m:   make(map[types.PeerKey]*Peer),
		cfg: cfg,
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
	Addr    string
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
	Mode    PunchMode
}

func (RequestPunchCoordination) isOutput() {}

// Configuration.
const (
	defaultBaseBackoff  = 1 * time.Second
	defaultMaxBackoff   = 60 * time.Second
	defaultFirstBackoff = 500 * time.Millisecond

	defaultDirectAttemptThreshold        = 1
	defaultPunchAttemptThreshold         = 1
	defaultPunchBirthdayAttemptThreshold = 1

	defaultUnreachableRetryInterval  = 60 * time.Second
	defaultDisconnectedRetryInterval = 60 * time.Second
)

func DefaultConfig() Config {
	return Config{
		FirstBackoff:                  defaultFirstBackoff,
		BaseBackoff:                   defaultBaseBackoff,
		MaxBackoff:                    defaultMaxBackoff,
		DirectAttemptThreshold:        defaultDirectAttemptThreshold,
		PunchAttemptThreshold:         defaultPunchAttemptThreshold,
		PunchBirthdayAttemptThreshold: defaultPunchBirthdayAttemptThreshold,
		UnreachableRetryInterval:      defaultUnreachableRetryInterval,
		DisconnectedRetryInterval:     defaultDisconnectedRetryInterval,
	}
}

func (c Config) withDefaults() Config {
	def := DefaultConfig()
	if c.FirstBackoff <= 0 {
		c.FirstBackoff = def.FirstBackoff
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = def.BaseBackoff
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = def.MaxBackoff
	}
	if c.DirectAttemptThreshold <= 0 {
		c.DirectAttemptThreshold = def.DirectAttemptThreshold
	}
	if c.PunchAttemptThreshold <= 0 {
		c.PunchAttemptThreshold = def.PunchAttemptThreshold
	}
	if c.PunchBirthdayAttemptThreshold <= 0 {
		c.PunchBirthdayAttemptThreshold = def.PunchBirthdayAttemptThreshold
	}
	if c.UnreachableRetryInterval <= 0 {
		c.UnreachableRetryInterval = def.UnreachableRetryInterval
	}
	if c.DisconnectedRetryInterval <= 0 {
		c.DisconnectedRetryInterval = def.DisconnectedRetryInterval
	}
	return c
}

func (s *Store) backoff(attempts int) time.Duration {
	if attempts == 0 {
		return s.cfg.FirstBackoff
	}
	// exponential: 1s, 2s, 4s, 8s, ...
	d := min(s.cfg.BaseBackoff*(1<<(attempts-1)), s.cfg.MaxBackoff)
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
	var outputs []Output //nolint:prealloc
	for _, p := range s.m {
		if now.Before(p.nextActionAt) {
			continue
		}

		switch p.state { //nolint:exhaustive
		case PeerStateConnected:
			continue
		case PeerStateConnecting:
			return s.connectFailed(now, ConnectFailed{p.id})
		case PeerStateUnreachable:
			// Retry unreachable peers after backoff expires
			p.state = PeerStateDiscovered
			p.stage = ConnectStageDirect
			p.stageAttempts = 0
			// Fall through to connection logic
		}

		if len(p.addrs) == 0 {
			continue
		}

		var out Output
		switch p.stage {
		case ConnectStageUnspecified, ConnectStageDirect:
			out = AttemptConnect{PeerKey: p.id, Addrs: p.addrs}
		case ConnectStagePunch:
			out = RequestPunchCoordination{PeerKey: p.id, Addrs: p.addrs, Mode: PunchModeDirect}
		case ConnectStagePunchBirthday:
			out = RequestPunchCoordination{PeerKey: p.id, Addrs: p.addrs, Mode: PunchModeBirthday}
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
	p.addrs = []string{e.Addr}
	p.connectedAt = now
	p.stage = ConnectStageDirect // reset for next time
	p.stageAttempts = 0

	return []Output{PeerConnected{PeerKey: e.PeerKey, Addr: e.Addr}}
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
		if p.stageAttempts >= s.cfg.DirectAttemptThreshold {
			p.stage = ConnectStagePunch
			p.stageAttempts = 0
		}
	case ConnectStagePunch:
		if p.stageAttempts >= s.cfg.PunchAttemptThreshold {
			p.stage = ConnectStagePunchBirthday
			p.stageAttempts = 0
		}
	case ConnectStagePunchBirthday:
		if p.stageAttempts >= s.cfg.PunchBirthdayAttemptThreshold {
			// All connection methods exhausted, retry after long backoff
			p.state = PeerStateUnreachable
			p.nextActionAt = now.Add(s.cfg.UnreachableRetryInterval)
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
	p.nextActionAt = now.Add(s.cfg.DisconnectedRetryInterval)
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
