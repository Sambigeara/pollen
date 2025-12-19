package state

import (
	"bytes"

	"github.com/sambigeara/pollen/pkg/types"
)

type Timestamp struct {
	Counter int64         `json:"c"`
	NodeID  types.PeerKey `json:"id"` // tie-breaker
}

func (t Timestamp) Less(other Timestamp) bool {
	if t.Counter < other.Counter {
		return true
	}
	if t.Counter > other.Counter {
		return false
	}
	return bytes.Compare(t.NodeID[:], other.NodeID[:]) < 0
}
