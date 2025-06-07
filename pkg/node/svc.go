package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

type NodeService struct {
	controlv1.UnimplementedControlServiceServer
	node *Node
}

func NewNodeService(addr, peers string) *NodeService {
	n := New(addr)

	if peers != "" {
		for peer := range strings.SplitSeq(peers, ",") {
			n.AddPeer(peer)
		}
	}

	go n.StartGossip()
	go n.Listen()

	return &NodeService{node: n}
}

func (s *NodeService) Seed(ctx context.Context, request *controlv1.SeedRequest) (*controlv1.SeedResponse, error) {
	// data, err := os.ReadFile(request.WasmPath)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to read WASM file: %w", err)
	// }
	data := []byte(request.WasmPath)

	hash := sha256.Sum256(data)
	h := hex.EncodeToString(hash[:])

	s.node.AddFunction(request.WasmPath, h)
	return &controlv1.SeedResponse{}, nil
}

func (s *NodeService) Run(ctx context.Context, request *controlv1.RunRequest) (*controlv1.RunResponse, error) {
	return &controlv1.RunResponse{
		Result: []byte{},
	}, nil
}
