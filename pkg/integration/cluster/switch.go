//go:build integration

package cluster

import (
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"time"
)

// NodeRole distinguishes public (relay/bootstrap) nodes from private (NATed) nodes.
type NodeRole int

const (
	Public  NodeRole = iota
	Private NodeRole = iota
)

// SwitchConfig holds default network characteristics for the virtual switch.
type SwitchConfig struct {
	DefaultLatency time.Duration
	DefaultJitter  float64 // 0 means zero jitter
}

// LinkConfig overrides network characteristics for a specific directed link.
type LinkConfig struct {
	Latency    time.Duration
	Jitter     float64
	PacketLoss float64
	Blocked    bool
}

// linkKey identifies a directed link between two addresses.
type linkKey struct {
	from, to string
}

// pendingPunch records a packet from a private node that hasn't been matched yet.
type pendingPunch struct {
	destConn   *VirtualPacketConn
	senderAddr string
	data       []byte
}

// VirtualSwitch routes UDP datagrams between in-process VirtualPacketConns with
// configurable latency, jitter, packet loss, partitions, and NAT gating.
type VirtualSwitch struct {
	conns        map[string]*VirtualPacketConn
	roles        map[string]NodeRole
	links        map[linkKey]*LinkConfig
	partitions   map[linkKey]bool
	punchOpen    map[linkKey]bool
	punchPending map[linkKey]*pendingPunch
	natForward   map[string]*net.UDPAddr // privateAddr → publicNATAddr
	natReverse   map[string]*net.UDPAddr // publicNATAddr → privateAddr
	config       SwitchConfig
	mu           sync.Mutex
}

// NewVirtualSwitch creates a switch with the given default config.
func NewVirtualSwitch(cfg SwitchConfig) *VirtualSwitch {
	return &VirtualSwitch{
		config:       cfg,
		conns:        make(map[string]*VirtualPacketConn),
		roles:        make(map[string]NodeRole),
		links:        make(map[linkKey]*LinkConfig),
		partitions:   make(map[linkKey]bool),
		punchOpen:    make(map[linkKey]bool),
		punchPending: make(map[linkKey]*pendingPunch),
		natForward:   make(map[string]*net.UDPAddr),
		natReverse:   make(map[string]*net.UDPAddr),
	}
}

// Bind registers a new virtual connection on the switch.
func (vs *VirtualSwitch) Bind(addr *net.UDPAddr, role NodeRole) *VirtualPacketConn {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	conn := &VirtualPacketConn{
		addr:           addr,
		sw:             vs,
		inCh:           make(chan inboundPacket, 256), //nolint:mnd
		closedCh:       make(chan struct{}),
		deadlineNotify: make(chan struct{}, 1),
	}
	key := addr.String()
	vs.conns[key] = conn
	vs.roles[key] = role
	return conn
}

// unregister removes a connection from the switch (called by VirtualPacketConn.Close).
func (vs *VirtualSwitch) unregister(addr *net.UDPAddr) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	delete(vs.conns, addr.String())
	delete(vs.roles, addr.String())
}

// deliver routes a packet from src to dst through the switch, applying NAT
// address rewriting, NAT gating, partitions, link config, packet loss,
// latency, and jitter.
func (vs *VirtualSwitch) deliver(from, to *net.UDPAddr, data []byte) {
	vs.mu.Lock()

	fromKey := from.String()
	toKey := to.String()

	// NAT: resolve destination through reverse mapping for routing.
	if realAddr, ok := vs.natReverse[toKey]; ok {
		to = realAddr
		toKey = realAddr.String()
	}

	destConn, ok := vs.conns[toKey]
	if !ok {
		vs.mu.Unlock()
		return
	}

	// NAT: rewrite source through forward mapping.
	actualFrom := from
	if mapped, ok := vs.natForward[fromKey]; ok {
		actualFrom = mapped
	}

	fromRole := vs.roles[fromKey]
	toRole := vs.roles[toKey]

	// NAT gating: private→private blocked unless punch opened.
	if fromRole == Private && toRole == Private {
		pairFwd := linkKey{fromKey, toKey}
		pairRev := linkKey{toKey, fromKey}

		if !vs.punchOpen[pairFwd] && !vs.punchOpen[pairRev] {
			// Check for pending punch from the opposite direction.
			if pending, exists := vs.punchPending[pairRev]; exists && pending.senderAddr == toKey {
				// Simultaneous open: toKey previously sent to fromKey, and now
				// fromKey is sending to toKey. Open the path in both directions.
				vs.punchOpen[pairFwd] = true
				vs.punchOpen[pairRev] = true
				delete(vs.punchPending, pairRev)
				vs.mu.Unlock()

				// Deliver the buffered packet (toKey→fromKey) to its original dest.
				pending.destConn.enqueue(inboundPacket{data: pending.data, addr: to})
				// Deliver the current packet (fromKey→toKey).
				destConn.enqueue(inboundPacket{data: copyBytes(data), addr: actualFrom})
				return
			}

			// No opposite-direction pending — store this as pending punch.
			dataCopy := copyBytes(data)
			vs.punchPending[pairFwd] = &pendingPunch{
				senderAddr: fromKey,
				data:       dataCopy,
				destConn:   destConn,
			}
			vs.mu.Unlock()
			return
		}
	}

	// Check partition.
	if vs.isPartitioned(fromKey, toKey) {
		vs.mu.Unlock()
		return
	}

	// Get link config.
	lk := linkKey{fromKey, toKey}
	lc := vs.links[lk]

	latency := vs.config.DefaultLatency
	jitter := vs.config.DefaultJitter
	var packetLoss float64

	if lc != nil {
		if lc.Blocked {
			vs.mu.Unlock()
			return
		}
		latency = lc.Latency
		jitter = lc.Jitter
		packetLoss = lc.PacketLoss
	}

	vs.mu.Unlock()

	// Apply packet loss.
	if packetLoss > 0 && rand.Float64() < packetLoss { //nolint:gosec
		return
	}

	// Compute delay.
	delay := computeDelay(latency, jitter)

	pkt := inboundPacket{data: copyBytes(data), addr: actualFrom}

	if delay == 0 {
		destConn.enqueue(pkt)
		return
	}

	time.AfterFunc(delay, func() {
		destConn.enqueue(pkt)
	})
}

func (vs *VirtualSwitch) isPartitioned(a, b string) bool {
	return vs.partitions[linkKey{a, b}] || vs.partitions[linkKey{b, a}]
}

// Partition blocks traffic between two groups of addresses.
func (vs *VirtualSwitch) Partition(groupA, groupB []string) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	for _, a := range groupA {
		for _, b := range groupB {
			vs.partitions[linkKey{a, b}] = true
			vs.partitions[linkKey{b, a}] = true
		}
	}
}

// Heal removes a partition between two groups of addresses.
func (vs *VirtualSwitch) Heal(groupA, groupB []string) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	for _, a := range groupA {
		for _, b := range groupB {
			delete(vs.partitions, linkKey{a, b})
			delete(vs.partitions, linkKey{b, a})
		}
	}
}

// SetLinkLatency configures latency for a directed link.
func (vs *VirtualSwitch) SetLinkLatency(from, to string, d time.Duration) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	lc := vs.getOrCreateLink(from, to)
	lc.Latency = d
}

// SetLinkLoss configures packet loss for a directed link.
func (vs *VirtualSwitch) SetLinkLoss(from, to string, loss float64) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	lc := vs.getOrCreateLink(from, to)
	lc.PacketLoss = loss
}

// SetLinkJitter configures jitter for a directed link.
func (vs *VirtualSwitch) SetLinkJitter(from, to string, j float64) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	lc := vs.getOrCreateLink(from, to)
	lc.Jitter = j
}

func (vs *VirtualSwitch) getOrCreateLink(from, to string) *LinkConfig {
	lk := linkKey{from, to}
	lc := vs.links[lk]
	if lc == nil {
		lc = &LinkConfig{Latency: vs.config.DefaultLatency, Jitter: vs.config.DefaultJitter}
		vs.links[lk] = lc
	}
	return lc
}

// SetNATMapping configures address rewriting for a private node. When the node
// sends to a public node, the source address is rewritten to publicAddr. When a
// public node sends to publicAddr, the packet is routed to the real private conn.
func (vs *VirtualSwitch) SetNATMapping(privateAddr, publicAddr *net.UDPAddr) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.natForward[privateAddr.String()] = publicAddr
	vs.natReverse[publicAddr.String()] = privateAddr
}

// OpenPunch explicitly opens a bidirectional path between two private nodes.
func (vs *VirtualSwitch) OpenPunch(a, b string) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.punchOpen[linkKey{a, b}] = true
	vs.punchOpen[linkKey{b, a}] = true
}

func computeDelay(base time.Duration, jitter float64) time.Duration {
	if base == 0 {
		return 0
	}
	if jitter == 0 {
		return base
	}
	// Delay = base ± (jitter * base * random)
	offset := float64(base) * jitter * (2*rand.Float64() - 1) //nolint:gosec,mnd
	d := max(time.Duration(float64(base)+offset), 0)
	return d
}

func copyBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// inboundPacket is a datagram received by a VirtualPacketConn.
type inboundPacket struct {
	addr *net.UDPAddr
	data []byte
}

// VirtualPacketConn implements net.PacketConn for in-process virtual networking.
type VirtualPacketConn struct {
	deadline       time.Time
	addr           *net.UDPAddr
	sw             *VirtualSwitch
	inCh           chan inboundPacket
	closedCh       chan struct{}
	deadlineNotify chan struct{}
	closeOnce      sync.Once
	deadlineMu     sync.Mutex
}

func (c *VirtualPacketConn) enqueue(pkt inboundPacket) {
	select {
	case c.inCh <- pkt:
	case <-c.closedCh:
	}
}

// ReadFrom blocks until a packet arrives, the deadline expires, or the conn is closed.
func (c *VirtualPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	for {
		// Check if closed.
		select {
		case <-c.closedCh:
			return 0, nil, net.ErrClosed
		default:
		}

		// Evaluate deadline.
		c.deadlineMu.Lock()
		dl := c.deadline
		c.deadlineMu.Unlock()

		var timer *time.Timer
		var timerCh <-chan time.Time

		if !dl.IsZero() {
			remaining := time.Until(dl)
			if remaining <= 0 {
				return 0, nil, os.ErrDeadlineExceeded
			}
			timer = time.NewTimer(remaining)
			timerCh = timer.C
		}

		select {
		case pkt := <-c.inCh:
			if timer != nil {
				timer.Stop()
			}
			n := copy(b, pkt.data)
			return n, pkt.addr, nil

		case <-c.closedCh:
			if timer != nil {
				timer.Stop()
			}
			return 0, nil, net.ErrClosed

		case <-timerCh:
			return 0, nil, os.ErrDeadlineExceeded

		case <-c.deadlineNotify:
			if timer != nil {
				timer.Stop()
			}
			// Re-evaluate deadline at top of loop.
		}
	}
}

// WriteTo sends a packet through the virtual switch.
func (c *VirtualPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	select {
	case <-c.closedCh:
		return 0, net.ErrClosed
	default:
	}

	udpAddr := addr.(*net.UDPAddr) //nolint:forcetypeassert
	c.sw.deliver(c.addr, udpAddr, b)
	return len(b), nil
}

// Close shuts down the connection and unregisters it from the switch.
func (c *VirtualPacketConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closedCh)
		c.sw.unregister(c.addr)
	})
	return nil
}

// LocalAddr returns the virtual UDP address of this connection.
func (c *VirtualPacketConn) LocalAddr() net.Addr {
	return c.addr
}

// SetDeadline sets both read and write deadlines.
func (c *VirtualPacketConn) SetDeadline(t time.Time) error {
	return c.SetReadDeadline(t)
}

// SetReadDeadline stores the deadline and signals any blocked ReadFrom to re-evaluate.
func (c *VirtualPacketConn) SetReadDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.deadline = t
	c.deadlineMu.Unlock()

	// Signal deadlineNotify to wake any blocked ReadFrom.
	select {
	case c.deadlineNotify <- struct{}{}:
	default:
	}
	return nil
}

// SetWriteDeadline is a no-op (writes are non-blocking).
func (c *VirtualPacketConn) SetWriteDeadline(time.Time) error {
	return nil
}
