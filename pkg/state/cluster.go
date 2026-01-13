package state

import (
	"github.com/sambigeara/pollen/pkg/types"
)

type Cluster struct {
	Nodes   *NodeMap
	LocalID types.PeerKey
}

func NewCluster(localNodeID types.PeerKey) *Cluster {
	return &Cluster{
		LocalID: localNodeID,
		Nodes:   NewNodeMap(localNodeID),
	}
}
