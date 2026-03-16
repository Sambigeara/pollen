package store

import (
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/topology"
	"github.com/sambigeara/pollen/pkg/types"
)

// Snapshot is an immutable, point-in-time view of cluster state.
// It is produced under a single lock acquisition and can be read freely
// without further synchronization.
type Snapshot struct {
	LocalID     types.PeerKey
	Nodes       map[types.PeerKey]NodeView
	PeerKeys    []types.PeerKey
	Specs       map[string]WorkloadSpecView
	Claims      map[string]map[types.PeerKey]struct{}
	Placements  map[types.PeerKey]NodePlacementState
	Heatmaps    map[types.PeerKey]map[types.PeerKey]TrafficSnapshot
	Connections []Connection
}

// NodeView is an immutable view of a single node's state, mirroring the
// internal nodeRecord with exported fields.
type NodeView struct {
	TrafficRates       map[types.PeerKey]TrafficSnapshot
	Services           map[string]*statev1.Service
	WorkloadSpecs      map[string]*statev1.WorkloadSpecChange
	WorkloadClaims     map[string]struct{}
	VivaldiCoord       *topology.Coord
	Reachable          map[types.PeerKey]struct{}
	LastAddr           string
	ObservedExternalIP string
	IPs                []string
	IdentityPub        []byte
	MemTotalBytes      uint64
	NatType            nat.Type
	CertExpiry         int64
	LocalPort          uint32
	ExternalPort       uint32
	CPUPercent         uint32
	MemPercent         uint32
	NumCPU             uint32
	PubliclyAccessible bool
}

// StoreEvent is a sealed sum type representing changes emitted by the store.
type StoreEvent interface {
	storeEvent()
}

// GossipApplied is emitted after applying remote gossip events.
type GossipApplied struct {
	Rebroadcast []*statev1.GossipEvent
}

// LocalMutationApplied is emitted after a local state change.
type LocalMutationApplied struct {
	Events []*statev1.GossipEvent
}

// DenyApplied is emitted when a deny event is processed.
type DenyApplied struct {
	PeerKey types.PeerKey
}

// RouteInvalidated is emitted when reachability or coordinate changes
// affect routing decisions.
type RouteInvalidated struct{}

// WorkloadChanged is emitted when workload specs or claims change.
type WorkloadChanged struct{}

// TrafficChanged is emitted when the traffic heatmap changes.
type TrafficChanged struct{}

func (GossipApplied) storeEvent()        {}
func (LocalMutationApplied) storeEvent() {}
func (DenyApplied) storeEvent()          {}
func (RouteInvalidated) storeEvent()     {}
func (WorkloadChanged) storeEvent()      {}
func (TrafficChanged) storeEvent()       {}
