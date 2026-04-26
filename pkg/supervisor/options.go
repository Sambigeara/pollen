// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"crypto/ed25519"
	"net"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/evaluator"
	"github.com/sambigeara/pollen/pkg/peercache"
	"github.com/sambigeara/pollen/pkg/types"
	"google.golang.org/protobuf/types/known/structpb"
)

// Options holds runtime parameters for constructing a Supervisor.
type Options struct {
	PacketConn         net.PacketConn
	ShutdownFunc       func()
	RuntimeState       *statev1.RuntimeState
	PeerCache          *peercache.Store
	AuthzRouter        *evaluator.Router
	SocketPath         string
	PollenDir          string
	NodeName           string
	HTTPAddr           string
	StaticAddr         string
	ControlAddr        string
	ControlToken       string
	SigningKey         ed25519.PrivateKey
	AdvertisedIPs      []string
	InitialConnections []ConnectionEntry
	InitialServices    []ServiceEntry
	GossipInterval     time.Duration
	PeerTickInterval   time.Duration
	MaxConnectionAge   time.Duration
	IdleInstanceTTL    time.Duration
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
