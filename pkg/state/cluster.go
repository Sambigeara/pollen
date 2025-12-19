package state

import (
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
)

type Cluster struct {
	LocalID types.PeerKey
	Nodes   *Map[*statev1.Node]
}

func NewCluster(localNodeID types.PeerKey) *Cluster {
	return &Cluster{
		LocalID: localNodeID,
		Nodes:   NewMap[*statev1.Node](localNodeID),
	}
}
