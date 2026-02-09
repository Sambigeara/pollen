package state

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	statev1 "github.com/sambigeara/pollen/api/genpb/pollen/state/v1"
	"github.com/sambigeara/pollen/pkg/types"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	stateDir  = "state"
	stateFile = "cluster_state.bin"

	keyFilePerm = 0o600
	keyDirPerm  = 0o700
)

// Persistence handles the disk I/O for the cluster state.
type Persistence struct {
	Cluster  *Cluster
	filePath string
	mu       sync.Mutex
}

func Load(pollenDir string, localNodeID types.PeerKey) (*Persistence, error) {
	dir := filepath.Join(pollenDir, stateDir)

	if err := os.MkdirAll(dir, keyDirPerm); err != nil {
		return nil, fmt.Errorf("failed to create state dir: %w", err)
	}

	path := filepath.Join(dir, stateFile)

	p := &Persistence{
		Cluster:  NewCluster(localNodeID),
		filePath: path,
	}

	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, keyFilePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to open state file: %w", err)
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read state file: %w", err)
	}

	if len(b) == 0 {
		return p, nil
	}

	delta := &statev1.DeltaState{}
	if err := delta.UnmarshalVT(b); err != nil {
		// TODO(saml) if the file is corrupt, we might want to return an error or just start fresh.
		// For v1, let's return error to be safe.
		return nil, fmt.Errorf("corrupt state file: %w", err)
	}

	p.Hydrate(delta)

	return p, nil
}

func (p *Persistence) Save() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	deltaState := &statev1.DeltaState{
		Nodes: ToNodeDelta(p.Cluster.Nodes),
	}

	b, err := deltaState.MarshalVT()
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(p.filePath, b, keyFilePerm); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	return nil
}

// Hydrate populates the in-memory maps from the protobuf definitions.
// It essentially performs a "Merge" operation from Disk -> Memory.
func (p *Persistence) Hydrate(delta *statev1.DeltaState) {
	// We can manually reconstruct the Record structs to feed into the generic Merge.
	// This ensures our internal Logic Clock respects the persisted state.
	nodeRecords := make(map[types.PeerKey]Record, len(delta.Nodes))
	for k, v := range delta.Nodes {
		// if v.Value.Id[:8] == "9de92c4d" {
		// if v.Value.Id[:8] == "4c55621d" {
		// 	continue
		// }
		keyBytes, err := hex.DecodeString(k)
		if err != nil {
			continue
		}
		nodeID := types.PeerKeyFromBytes(keyBytes)
		if nodeID == p.Cluster.LocalID {
			continue
		}

		tsID := nodeID
		if v.Ts == nil {
			continue
		}
		if tsBytes, err := hex.DecodeString(v.Ts.NodeId); err == nil {
			tsID = types.PeerKeyFromBytes(tsBytes)
		}

		nodeRecords[nodeID] = Record{
			Node:      v.Value,
			Tombstone: v.Tombstone,
			Timestamp: Timestamp{
				Counter: v.Ts.Counter,
				NodeID:  tsID,
			},
		}
	}
	p.Cluster.Nodes.Merge(nodeRecords)
}

func (p *Persistence) ConnectNode(key types.PeerKey) {
	_ = p.Cluster.Nodes.Update(p.Cluster.LocalID, func(n *statev1.Node) {
		if n.Connected == nil {
			n.Connected = make(map[string]*emptypb.Empty)
		}
		n.Connected[key.Short()] = &emptypb.Empty{}
	})
	_ = p.Cluster.Nodes.Update(key, func(n *statev1.Node) {
		if n.Connected == nil {
			n.Connected = make(map[string]*emptypb.Empty)
		}
		n.Connected[p.Cluster.LocalID.Short()] = &emptypb.Empty{}
	})
}

// TODO(saml) not currently used. Should be.
func (p *Persistence) DisconnectNode(key types.PeerKey) {
	_ = p.Cluster.Nodes.Update(key, func(n *statev1.Node) {
		if n.Connected != nil {
			delete(n.Connected, key.String())
		}
	})
}
