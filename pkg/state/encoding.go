package state

import (
	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
)

func ToNodeDelta(m *Map[*statev1.Node]) map[string]*statev1.NodeRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string]*statev1.NodeRecord, len(m.Data))

	for key, record := range m.Data {
		out[key] = &statev1.NodeRecord{
			Value:     record.Value,
			Tombstone: record.Tombstone,
			Ts: &statev1.Timestamp{
				Counter: record.Timestamp.Counter,
				NodeId:  record.Timestamp.NodeID,
			},
		}
	}

	return out
}
