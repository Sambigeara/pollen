package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sourcegraph/conc/pool"
	"google.golang.org/grpc"

	"github.com/sambigeara/pollen/pkg/node"
)

type GrpcServer struct{}

func NewGRPCServer() *GrpcServer {
	return &GrpcServer{}
}

func (s *GrpcServer) Start(ctx context.Context, nodeServ *node.NodeService, path string) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	server := grpc.NewServer()
	controlv1.RegisterControlServiceServer(server, nodeServ)

	if _, err := os.Stat(path); err == nil {
		dialCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		dialer := &net.Dialer{Timeout: time.Second}
		conn, dialErr := dialer.DialContext(dialCtx, "unix", path)
		if dialErr == nil {
			_ = conn.Close()
			return nil
		}
		_ = os.Remove(path)
	}

	l, err := (&net.ListenConfig{}).Listen(ctx, "unix", path)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return fmt.Errorf("failed to listen: %w", err)
	}
	defer os.Remove(path)

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
