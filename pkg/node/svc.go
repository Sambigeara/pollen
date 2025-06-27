package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bytecodealliance/wasmtime-go/v33"
	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ controlv1.ControlServiceServer = (*NodeService)(nil)

type NodeService struct {
	controlv1.UnimplementedControlServiceServer
	node *Node
}

func NewNodeService(n *Node) *NodeService {
	return &NodeService{node: n}
}

func (s *NodeService) Seed(ctx context.Context, req *controlv1.SeedRequest) (*controlv1.SeedResponse, error) {
	blob, err := os.ReadFile(req.WasmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read WASM file: %w", err)
	}

	hash := sha256.Sum256(blob)
	h := hex.EncodeToString(hash[:])

	fn := strings.TrimSuffix(filepath.Base(req.WasmPath), ".wasm")

	s.node.addFunction(fn, h, blob)
	return &controlv1.SeedResponse{Hash: h}, nil
}

func (s *NodeService) Run(ctx context.Context, req *controlv1.RunRequest) (*controlv1.RunResponse, error) {
	fn := s.node.getFunction(req.Name)
	if fn == nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("function not found: %s", req.Name))
	}

	engine := wasmtime.NewEngine()
	module, err := wasmtime.NewModule(engine, fn.Blob)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create module: %w", err))
	}

	store := wasmtime.NewStore(engine)
	instance, err := wasmtime.NewInstance(store, module, []wasmtime.AsExtern{})
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create instance: %w", err))
	}

	// TODO(saml) hardcoded function name, figure this out
	runFn := instance.GetFunc(store, "run")
	if runFn == nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("function '%s' not found in WASM module", req.Name))
	}

	res, err := runFn.Call(store)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to execute function: %w", err))
	}

	return &controlv1.RunResponse{
		Result: []byte(fmt.Sprintf("Result: %v", res)),
	}, nil
}

func (s *NodeService) ListFunctions(ctx context.Context, _ *controlv1.ListFunctionsRequest) (*controlv1.ListFunctionsResponse, error) {
	return &controlv1.ListFunctionsResponse{
		Functions: s.node.listFunctions(),
	}, nil
}
