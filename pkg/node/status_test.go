package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sambigeara/pollen/internal/testutil/memtransport"
	"github.com/stretchr/testify/require"

	controlv1 "github.com/sambigeara/pollen/api/genpb/pollen/control/v1"
)

func TestStatusServicesClusterWide(t *testing.T) {
	ctx := t.Context()
	network := memtransport.NewNetwork()

	dirA := t.TempDir()
	nodeA, portA := newNode(t, dirA, 0, network, nil)

	token, err := NewInvite([]string{"127.0.0.1"}, fmt.Sprintf("%d", portA))
	require.NoError(t, err)

	nodeA.AdmissionStore.AddInvite(token)

	initCtx, cancelFn := context.WithCancel(ctx)
	t.Cleanup(cancelFn)

	stopA := make(chan error, 1)
	stopB := make(chan error, 1)
	go func() { stopA <- nodeA.Start(initCtx, nil) }()

	dirB := t.TempDir()
	nodeB, _ := newNode(t, dirB, 0, network, nil)
	go func() { stopB <- nodeB.Start(initCtx, token) }()

	svcA := NewNodeService(nodeA)
	_, err = svcA.RegisterService(ctx, &controlv1.RegisterServiceRequest{
		Port: 8080,
		Name: ptrString("api"),
	})
	require.NoError(t, err)

	svcB := NewNodeService(nodeB)
	require.Eventually(t, func() bool {
		resp, err := svcB.GetStatus(ctx, &controlv1.GetStatusRequest{})
		if err != nil {
			return false
		}
		found := false
		for _, svc := range resp.GetServices() {
			if svc.GetName() == "api" && svc.GetPort() == 8080 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
		for _, n := range resp.GetNodes() {
			if string(n.GetNode().GetPeerId()) == string(nodeA.Store.Cluster.LocalID.Bytes()) {
				return n.GetStatus() == controlv1.NodeStatus_NODE_STATUS_ONLINE
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)

	cancelFn()
	<-stopA
	<-stopB
}

func ptrString(v string) *string {
	return &v
}
