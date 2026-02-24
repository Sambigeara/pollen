package node_test

import (
	"context"
	"testing"
	"time"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
	"github.com/sambigeara/pollen/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestNodeServiceShutdownInvokesCallback(t *testing.T) {
	done := make(chan struct{})
	svc := node.NewNodeService(nil, func() {
		close(done)
	}, nil, nil)

	_, err := svc.Shutdown(context.Background(), &controlv1.ShutdownRequest{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}
