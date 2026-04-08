package supervisor

import (
	"crypto/ed25519"
	"net"
	"time"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

// Options holds runtime parameters for constructing a Supervisor.
type Options struct {
	PacketConn          net.PacketConn
	ShutdownFunc        func()
	SocketPath          string
	PollenDir           string
	NodeName            string
	RuntimeState        *statev1.RuntimeState
	SigningKey          ed25519.PrivateKey
	AdvertisedIPs       []string
	BootstrapPeers      []BootstrapTarget
	InitialConnections  []ConnectionEntry
	InitialServices     []ServiceEntry
	GossipInterval      time.Duration
	PeerTickInterval    time.Duration
	MaxConnectionAge    time.Duration
	GossipJitter        float64
	ListenPort          int
	BootstrapPublic     bool
	MetricsEnabled      bool
	CPUBudgetPercent    uint32
	MemBudgetPercent    uint32
	DisableGossipJitter bool
	DisableNATPunch     bool
}

// BootstrapTarget is a resolved bootstrap peer for runtime use.
type BootstrapTarget struct {
	Addrs   []string
	PeerKey types.PeerKey
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
	Name     string
	Port     uint32
	Protocol statev1.ServiceProtocol
}
