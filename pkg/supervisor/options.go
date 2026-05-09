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

type Options struct {
	PacketConn         net.PacketConn
	ShutdownFunc       func()
	RuntimeState       *statev1.RuntimeState
	PeerCache          *peercache.Store
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
	MetricsEnabled     bool
	BootstrapPublic    bool
	DisableNATPunch    bool
	RelayOnly          bool
}

type ConnectionEntry struct {
	PeerKey    types.PeerKey
	RemotePort uint32
	LocalPort  uint32
	Protocol   statev1.ServiceProtocol
}

type ServiceEntry struct {
	Properties *structpb.Struct
	Name       string
	Port       uint32
	Protocol   statev1.ServiceProtocol
}
