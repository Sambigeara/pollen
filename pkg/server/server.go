package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/sambigeara/pollen/pkg/perm"
	"github.com/sourcegraph/conc/pool"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	log *zap.SugaredLogger
}

func NewGRPCServer() *GrpcServer {
	return &GrpcServer{log: zap.S().Named("grpc")}
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

	if err := perm.SetGroupSocket(path); err != nil {
		s.log.Warnw("socket group permissions", "err", err)
	}

	p := pool.New().WithContext(ctx).WithCancelOnError().WithFirstError()
	p.Go(func(_ context.Context) error { return server.Serve(l) })

	p.Go(func(ctx context.Context) error {
		<-ctx.Done()
		server.GracefulStop()
		return nil
	})

	return p.Wait()
}
