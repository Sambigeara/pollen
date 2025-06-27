package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sourcegraph/conc/pool"
	"google.golang.org/grpc"

	"github.com/sambigeara/pollen/pkg/node"
)

const (
	pollenRootDir = ".pollen"
	socketName    = "pollen.sock"
)

type GrpcServer struct{}

func NewGRPCServer() *GrpcServer {
	return &GrpcServer{}
}

func (s *GrpcServer) TryStart(ctx context.Context, nodeServ *node.NodeService, path string) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	server := grpc.NewServer()
	controlv1.RegisterControlServiceServer(server, nodeServ)

	if _, err := os.Stat(path); err == nil {
		return nil
	}

	l, err := net.Listen("unix", path)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return fmt.Errorf("failed to listen: %v", err)
	}

	p := pool.New().WithContext(ctx).WithCancelOnError().WithFirstError()
	p.Go(func(_ context.Context) error {
		if err := server.Serve(l); err != nil {
			return err
		}

		return nil
	})

	p.Go(func(ctx context.Context) error {
		<-ctx.Done()
		server.GracefulStop()
		return nil
	})

	if err := p.Wait(); err != nil {
		return err
	}

	return nil
}
