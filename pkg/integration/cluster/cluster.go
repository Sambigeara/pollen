//go:build integration

package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
)

const (
	defaultPort      = 60611
	introduceTimeout = 5 * time.Second
)

// pendingNode records a node definition before the cluster is started.
type pendingNode struct {
	name           string
	role           NodeRole
	enableNATPunch bool
}

// latencySetting records a directed latency override between two named nodes.
type latencySetting struct {
	from, to string
	latency  time.Duration
}

// introduction records a pair of nodes to connect after startup.
type introduction struct {
	from, to string
}

// Builder accumulates cluster configuration before Start() materializes it.
type Builder struct {
	t              testing.TB
	pending        []pendingNode
	latencies      []latencySetting
	introductions  []introduction
	defaultLatency time.Duration
	defaultJitter  float64
}

// New returns a Builder that will construct a Cluster when Start() is called.
func New(t testing.TB) *Builder { //nolint:thelper
	return &Builder{
		t:              t,
		defaultLatency: 5 * time.Millisecond, //nolint:mnd
	}
}

// AddNode registers a named node with the given role.
func (b *Builder) AddNode(name string, role NodeRole) *Builder {
	b.pending = append(b.pending, pendingNode{name: name, role: role})
	return b
}

// SetLatency sets per-link latency between two named nodes (both directions).
func (b *Builder) SetLatency(from, to string, d time.Duration) *Builder {
	b.latencies = append(b.latencies, latencySetting{from: from, to: to, latency: d})
	return b
}

// SetDefaultLatency overrides the switch-level default latency.
func (b *Builder) SetDefaultLatency(d time.Duration) *Builder {
	b.defaultLatency = d
	return b
}

// SetDefaultJitter overrides the switch-level default jitter ratio.
func (b *Builder) SetDefaultJitter(j float64) *Builder {
	b.defaultJitter = j
	return b
}

// EnableNATPunch enables NAT punch on the named node. Must be called after
// AddNode for that name.
func (b *Builder) EnableNATPunch(name string) *Builder {
	for i := range b.pending {
		if b.pending[i].name == name {
			b.pending[i].enableNATPunch = true
			return b
		}
	}
	panic(fmt.Sprintf("EnableNATPunch: unknown node %q", name))
}

// Introduce records that fromNode should connect to toNode after startup.
func (b *Builder) Introduce(from, to string) *Builder {
	b.introductions = append(b.introductions, introduction{from: from, to: to})
	return b
}

// Start materializes the cluster: creates the switch, nodes, waits for readiness,
// executes introductions, and registers cleanup.
func (b *Builder) Start(ctx context.Context) *Cluster {
	t := b.t
	t.Helper()

	sw := NewVirtualSwitch(SwitchConfig{
		DefaultLatency: b.defaultLatency,
		DefaultJitter:  b.defaultJitter,
	})

	auth := NewClusterAuth(t)

	c := &Cluster{
		t:       t,
		sw:      sw,
		auth:    auth,
		byName:  make(map[string]*TestNode),
		ctx:     ctx,
		nextIdx: len(b.pending),
	}

	for i, pn := range b.pending {
		addr := indexToAddr(i)
		tn := NewTestNode(t, TestNodeConfig{
			Switch:         sw,
			Auth:           auth,
			Addr:           addr,
			Role:           pn.role,
			Context:        ctx,
			Name:           pn.name,
			EnableNATPunch: pn.enableNATPunch,
		})
		c.ordered = append(c.ordered, tn)
		c.byName[pn.name] = tn
	}

	for _, ls := range b.latencies {
		fromNode := c.mustNode(ls.from)
		toNode := c.mustNode(ls.to)
		fromAddr := fromNode.VirtualAddr().String()
		toAddr := toNode.VirtualAddr().String()
		sw.SetLinkLatency(fromAddr, toAddr, ls.latency)
		sw.SetLinkLatency(toAddr, fromAddr, ls.latency)
	}

	for _, intro := range b.introductions {
		c.introduce(ctx, intro.from, intro.to)
	}

	return c
}

// Cluster is the top-level test harness owning a virtual switch, auth, and nodes.
type Cluster struct {
	t       testing.TB
	ctx     context.Context
	sw      *VirtualSwitch
	auth    *ClusterAuth
	byName  map[string]*TestNode
	ordered []*TestNode
	nextIdx int
}

// Node returns the named TestNode, or nil if not found.
func (c *Cluster) Node(name string) *TestNode {
	return c.byName[name]
}

// Nodes returns all nodes in creation order.
func (c *Cluster) Nodes() []*TestNode {
	return c.ordered
}

// NodesByRole returns nodes matching the given role, in creation order.
func (c *Cluster) NodesByRole(role NodeRole) []*TestNode {
	var result []*TestNode
	for _, n := range c.ordered {
		if n.Role() == role {
			result = append(result, n)
		}
	}
	return result
}

// Switch returns the underlying VirtualSwitch.
func (c *Cluster) Switch() *VirtualSwitch {
	return c.sw
}

// PeerKeyByName returns the PeerKey for the named node.
func (c *Cluster) PeerKeyByName(name string) types.PeerKey {
	return c.mustNode(name).PeerKey()
}

// Partition blocks traffic between two groups of named nodes.
func (c *Cluster) Partition(groupA, groupB []string) {
	c.sw.Partition(c.resolveAddrs(groupA), c.resolveAddrs(groupB))
}

// Heal removes a partition between two groups of named nodes.
func (c *Cluster) Heal(groupA, groupB []string) {
	c.sw.Heal(c.resolveAddrs(groupA), c.resolveAddrs(groupB))
}

// AddNodeAndStart creates and starts a new node mid-test.
func (c *Cluster) AddNodeAndStart(t testing.TB, name string, role NodeRole, ctx context.Context) *TestNode { //nolint:thelper
	t.Helper()

	addr := indexToAddr(c.nextIdx)
	c.nextIdx++

	tn := NewTestNode(t, TestNodeConfig{
		Switch:  c.sw,
		Auth:    c.auth,
		Addr:    addr,
		Role:    role,
		Context: ctx,
		Name:    name,
	})

	c.ordered = append(c.ordered, tn)
	c.byName[name] = tn

	return tn
}

func (c *Cluster) introduce(ctx context.Context, fromName, toName string) {
	from := c.mustNode(fromName)
	to := c.mustNode(toName)

	introCtx, cancel := context.WithTimeout(ctx, introduceTimeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			c.t.Logf("introduce %s -> %s: recovered panic: %v", fromName, toName, r)
		}
	}()

	err := from.Node().ConnectPeer(introCtx, to.PeerKey(), []*net.UDPAddr{to.VirtualAddr()})
	if err != nil {
		c.t.Logf("introduce %s -> %s: %v", fromName, toName, err)
	}
}

func (c *Cluster) mustNode(name string) *TestNode {
	n := c.byName[name]
	if n == nil {
		c.t.Fatalf("unknown node %q", name)
	}
	return n
}

func (c *Cluster) resolveAddrs(names []string) []string {
	addrs := make([]string, len(names))
	for i, name := range names {
		addrs[i] = c.mustNode(name).VirtualAddr().String()
	}
	return addrs
}

// indexToAddr converts a zero-based index to a 10.0.x.y:60611 address.
func indexToAddr(i int) *net.UDPAddr {
	return &net.UDPAddr{
		IP:   net.IPv4(10, 0, byte(i/256), byte(i%256+1)), //nolint:mnd
		Port: defaultPort,
	}
}

// ---------------------------------------------------------------------------
// Preset factories
// ---------------------------------------------------------------------------

// PublicMesh creates n Public nodes in a full mesh.
func PublicMesh(t testing.TB, n int, ctx context.Context) *Cluster { //nolint:thelper
	t.Helper()

	b := New(t).
		SetDefaultLatency(5 * time.Millisecond). //nolint:mnd
		SetDefaultJitter(0.15)                   //nolint:mnd

	for i := range n {
		b.AddNode(fmt.Sprintf("node-%d", i), Public)
	}

	for i := range n {
		for j := i + 1; j < n; j++ {
			b.Introduce(fmt.Sprintf("node-%d", i), fmt.Sprintf("node-%d", j))
		}
	}

	return b.Start(ctx)
}

// RelayRegions creates a multi-region cluster with relays and private nodes.
func RelayRegions(t testing.TB, regions, nodesPerRegion int, ctx context.Context) *Cluster { //nolint:thelper
	t.Helper()

	b := New(t).
		SetDefaultLatency(2 * time.Millisecond). //nolint:mnd
		SetDefaultJitter(0.15)                   //nolint:mnd

	for r := range regions {
		b.AddNode(fmt.Sprintf("relay-%d", r), Public)
	}

	for r := range regions {
		relayName := fmt.Sprintf("relay-%d", r)
		for n := range nodesPerRegion {
			nodeName := fmt.Sprintf("region-%d-node-%d", r, n)
			b.AddNode(nodeName, Private)
			b.Introduce(nodeName, relayName)
		}
	}

	for r := range regions {
		for s := r + 1; s < regions; s++ {
			fromRelay := fmt.Sprintf("relay-%d", r)
			toRelay := fmt.Sprintf("relay-%d", s)
			b.SetLatency(fromRelay, toRelay, 50*time.Millisecond) //nolint:mnd
			b.Introduce(fromRelay, toRelay)
		}
	}

	return b.Start(ctx)
}
