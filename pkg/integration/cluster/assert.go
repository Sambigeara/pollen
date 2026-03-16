//go:build integration

package cluster

import (
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/stretchr/testify/require"
)

const (
	assertPoll    = 25 * time.Millisecond
	assertTimeout = 10 * time.Second
	// eagerTimeout is shorter than one gossip tick (1s). Assertions using this
	// timeout only pass if state was broadcast eagerly — clock-sync-based pull
	// cannot fire in time.
	eagerTimeout = 500 * time.Millisecond
)

// RequireConverged asserts that every node knows about all other nodes via gossip.
func (c *Cluster) RequireConverged(t testing.TB) { //nolint:thelper
	t.Helper()
	expected := len(c.ordered) - 1

	require.Eventually(t, func() bool {
		for _, n := range c.ordered {
			if len(n.Store().KnownPeers()) < expected {
				return false
			}
		}
		return true
	}, assertTimeout, assertPoll, "cluster did not converge: not all nodes see %d peers", expected)
}

// RequireHealthy asserts that every node has at least one connected peer.
func (c *Cluster) RequireHealthy(t testing.TB) { //nolint:thelper
	t.Helper()

	require.Eventually(t, func() bool {
		for _, n := range c.ordered {
			if len(n.ConnectedPeers()) == 0 {
				return false
			}
		}
		return true
	}, assertTimeout, assertPoll, "cluster not healthy: some nodes have no connections")
}

// RequirePeerVisible asserts that all other nodes can see the named peer via gossip.
func (c *Cluster) RequirePeerVisible(t testing.TB, name string) { //nolint:thelper
	t.Helper()
	target := c.PeerKeyByName(name)

	require.Eventually(t, func() bool {
		for _, n := range c.ordered {
			if n.Name() == name {
				continue
			}
			found := false
			for _, kp := range n.Store().KnownPeers() {
				if kp.PeerID == target {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}, assertTimeout, assertPoll, "peer %q not visible to all nodes", name)
}

// RequireConnectedPeers asserts that the named node has at least n connected peers.
func (c *Cluster) RequireConnectedPeers(t testing.TB, name string, n int) { //nolint:thelper
	t.Helper()
	node := c.mustNode(name)

	require.Eventually(t, func() bool {
		return len(node.ConnectedPeers()) >= n
	}, assertTimeout, assertPoll, "node %q has fewer than %d connected peers", name, n)
}

// RequireEventually polls pred until it returns true.
func (c *Cluster) RequireEventually(t testing.TB, pred func() bool, timeout time.Duration, msgAndArgs ...interface{}) { //nolint:thelper
	t.Helper()
	require.Eventually(t, pred, timeout, assertPoll, msgAndArgs...)
}

// RequireNever asserts that pred stays false for duration.
func (c *Cluster) RequireNever(t testing.TB, pred func() bool, duration time.Duration, msgAndArgs ...interface{}) { //nolint:thelper
	t.Helper()
	require.Never(t, pred, duration, assertPoll, msgAndArgs...)
}

// RequireWorkloadReplicas asserts that at least count nodes have claimed the given workload hash.
func (c *Cluster) RequireWorkloadReplicas(t testing.TB, hash string, count int) { //nolint:thelper
	t.Helper()
	require.Eventually(t, func() bool {
		total := 0
		for _, n := range c.ordered {
			claims := n.Store().AllWorkloadClaims()
			if claimants, ok := claims[hash]; ok {
				if _, claimed := claimants[n.PeerKey()]; claimed {
					total++
				}
			}
		}
		return total >= count
	}, assertTimeout, assertPoll, "expected %d nodes to claim workload %s", count, hash)
}

// RequireWorkloadClaimedBy asserts that the named nodes have all claimed the given workload hash.
func (c *Cluster) RequireWorkloadClaimedBy(t testing.TB, hash string, nodeNames []string) { //nolint:thelper
	t.Helper()
	expected := make(map[types.PeerKey]struct{}, len(nodeNames))
	for _, name := range nodeNames {
		expected[c.PeerKeyByName(name)] = struct{}{}
	}
	require.Eventually(t, func() bool {
		for _, n := range c.ordered {
			claims := n.Store().AllWorkloadClaims()
			claimants, ok := claims[hash]
			if !ok {
				continue
			}
			allFound := true
			for pk := range expected {
				if _, found := claimants[pk]; !found {
					allFound = false
					break
				}
			}
			if allFound {
				return true
			}
		}
		return false
	}, assertTimeout, assertPoll, "expected nodes %v to claim workload %s", nodeNames, hash)
}

// RequireServiceVisible asserts that all nodes (except the publisher) can see the service on the given port.
func (c *Cluster) RequireServiceVisible(t testing.TB, publisherName string, port uint32) { //nolint:thelper
	t.Helper()
	publisherKey := c.PeerKeyByName(publisherName)
	require.Eventually(t, func() bool {
		for _, n := range c.ordered {
			if n.PeerKey() == publisherKey {
				continue
			}
			if !n.Store().HasServicePort(publisherKey, port) {
				return false
			}
		}
		return true
	}, assertTimeout, assertPoll,
		"expected all nodes to see service on port %d from %s", port, publisherName)
}
