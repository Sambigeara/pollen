package tunneling

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
)

const (
	proxyReadBuf      = 1500
	proxyIdleTimeout  = 30 * time.Second
	proxyReadDeadline = time.Second
)

// udpServiceProxy forwards tunnel datagrams to a local UDP service and routes
// responses back to the originating peer. One net.Conn per peer ensures the
// local service sees a distinct source address per tunnel consumer, so
// responses are correctly attributed. Idle peer connections are evicted by
// the response-reading goroutine after proxyIdleTimeout of inactivity.
type udpServiceProxy struct {
	datagrams DatagramTransport
	peers     map[types.PeerKey]*peerConn
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
	port      uint32
}

type peerConn struct {
	conn   net.Conn
	cancel context.CancelFunc
}

func newUDPServiceProxy(ctx context.Context, port uint32, datagrams DatagramTransport) *udpServiceProxy {
	ctx, cancel := context.WithCancel(ctx)
	return &udpServiceProxy{
		port:      port,
		datagrams: datagrams,
		peers:     make(map[types.PeerKey]*peerConn),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (p *udpServiceProxy) Forward(peer types.PeerKey, payload []byte) {
	p.mu.Lock()
	pc, ok := p.peers[peer]
	if ok {
		p.mu.Unlock()
		_, _ = pc.conn.Write(payload)
		return
	}

	conn, err := (&net.Dialer{}).DialContext(p.ctx, "udp", fmt.Sprintf("localhost:%d", p.port))
	if err != nil {
		p.mu.Unlock()
		return
	}
	peerCtx, peerCancel := context.WithCancel(p.ctx)
	pc = &peerConn{conn: conn, cancel: peerCancel}
	p.peers[peer] = pc
	p.mu.Unlock()

	_, _ = conn.Write(payload)
	p.wg.Go(func() { p.readResponses(peerCtx, conn, peer) })
}

func (p *udpServiceProxy) readResponses(ctx context.Context, conn net.Conn, peer types.PeerKey) {
	defer p.removePeer(peer)
	buf := make([]byte, proxyReadBuf)
	lastActivity := time.Now()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(proxyReadDeadline))
		n, err := conn.Read(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if time.Since(lastActivity) > proxyIdleTimeout {
				return
			}
			continue
		}
		lastActivity = time.Now()
		if n > maxTunnelPayload {
			continue
		}
		frame := make([]byte, 2+n) //nolint:mnd
		binary.BigEndian.PutUint16(frame[:2], uint16(p.port))
		copy(frame[2:], buf[:n])
		_ = p.datagrams.SendTunnelDatagram(ctx, peer, frame)
	}
}

func (p *udpServiceProxy) removePeer(peer types.PeerKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pc, ok := p.peers[peer]; ok {
		pc.cancel()
		_ = pc.conn.Close()
		delete(p.peers, peer)
	}
}

func (p *udpServiceProxy) Close() {
	p.cancel()
	p.mu.Lock()
	for _, pc := range p.peers {
		pc.cancel()
		_ = pc.conn.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}
