package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	// "google.golang.org/grpc/status"
)

type NodeService struct {
	controlv1.UnimplementedControlServiceServer
	node *Node
}

func NewNodeService(n *Node) *NodeService {
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

	s.node.addFunction(request.WasmPath, h)
	return &controlv1.SeedResponse{}, nil
}

func (s *NodeService) Run(ctx context.Context, request *controlv1.RunRequest) (*controlv1.RunResponse, error) {
	return &controlv1.RunResponse{
		Result: []byte{},
	}, nil
}
