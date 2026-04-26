// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"crypto/ed25519"
	"net"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/peercache"
	"github.com/sambigeara/pollen/pkg/types"
	"google.golang.org/protobuf/types/known/structpb"
)

// AuthzOptions configures the authorisation router supervisor builds
// internally. A zero value selects allow-all so unconfigured supervisors
// are usable. Supervisor wires the seed-backed PDP factory to its own
// placement service, so callers don't plumb a Caller themselves. Field
// shapes mirror the on-disk evaluator config so daemon can pass them
// through without translation.
type AuthzOptions struct {
	// Default is the evaluator spec applied to gates not listed in
	// Gates. Empty selects "allow_all".
	Default string
	// Gates binds gate names to evaluator specs ("allow_all",
	// "attribute_matcher", "seed/<name>"). Unknown names fail at
	// supervisor.New.
	Gates map[string]string
	// MatcherRules is the path to the YAML rule file consumed by the
	// attribute_matcher built-in. Required if any gate (or Default)
	// resolves to "attribute_matcher". Reload via Supervisor.ReloadAuthzMatcher.
	MatcherRules string
}

// Options holds runtime parameters for constructing a Supervisor.
type Options struct {
	PacketConn         net.PacketConn
	ShutdownFunc       func()
	RuntimeState       *statev1.RuntimeState
	PeerCache          *peercache.Store
	Authz              AuthzOptions
	SocketPath         string
	PollenDir          string
	NodeName           string
	HTTPAddr           string
	StaticAddr         string
	ControlAddr        string
	ControlToken       string
	InitialServices    []ServiceEntry
	InitialConnections []ConnectionEntry
	AdvertisedIPs      []string
	SigningKey         ed25519.PrivateKey
	IdleInstanceTTL    time.Duration
	PeerTickInterval   time.Duration
	MaxConnectionAge   time.Duration
	GossipInterval     time.Duration
	GossipJitter       float64
	ListenPort         int
	MemBudgetPercent   uint32
	CPUBudgetPercent   uint32
	MetricsEnabled     bool
	BootstrapPublic    bool
	DisableNATPunch    bool
	// RelayOnly disables workload hosting; the node still gossips and forwards
	// routed streams. Pair with an empty StaticAddr to also skip static hosting.
	RelayOnly bool
}

// ConnectionEntry describes a desired tunnel connection for initial state.
type ConnectionEntry struct {
	PeerKey    types.PeerKey
	RemotePort uint32
	LocalPort  uint32
	Protocol   statev1.ServiceProtocol
}

// ServiceEntry describes a service to register in state at startup.
type ServiceEntry struct {
	Properties *structpb.Struct
	Name       string
	Port       uint32
	Protocol   statev1.ServiceProtocol
}
