package state

import (
	"github.com/sambigeara/pollen/pkg/types"
)

type Event interface {
	stateEvent()
}

type (
	PeerJoined     struct{ Key types.PeerKey }
	PeerLeft       struct{ Key types.PeerKey }
	PeerDenied     struct{ Key types.PeerKey }
	ServiceChanged struct {
		Name string
		Peer types.PeerKey
	}
)

type (
	WorkloadChanged struct{ Hash string }
	TopologyChanged struct{ Peer types.PeerKey }
	GossipApplied   struct{}
)

func (PeerJoined) stateEvent()      {}
func (PeerLeft) stateEvent()        {}
func (PeerDenied) stateEvent()      {}
func (ServiceChanged) stateEvent()  {}
func (WorkloadChanged) stateEvent() {}
func (TopologyChanged) stateEvent() {}
func (GossipApplied) stateEvent()   {}
