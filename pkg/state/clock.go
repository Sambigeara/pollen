package state

import "strings"

type Timestamp struct {
	Counter int64  `json:"c"`
	NodeID  string `json:"id"` // tie-breaker
}

func (t Timestamp) Less(other Timestamp) bool {
	if t.Counter < other.Counter {
		return true
	}
	if t.Counter > other.Counter {
		return false
	}
	return strings.Compare(t.NodeID, other.NodeID) < 0
}
