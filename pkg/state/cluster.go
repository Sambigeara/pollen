package state

import statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"

type Cluster struct {
	LocalID string
	Nodes   *Map[*statev1.Node]
}

func NewCluster(localNodeID string) *Cluster {
	return &Cluster{
		LocalID: localNodeID,
		Nodes:   NewMap[*statev1.Node](localNodeID),
	}
}
