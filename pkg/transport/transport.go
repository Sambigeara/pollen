package transport

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
)

var _ Transport = (*megaSock)(nil)

const (
	// Protocol: 4 bytes Magic + 8 bytes Nonce = 12 bytes
	headerSize = 12
	// Magic: "PING" (0x50494E47) and "PONG" (0x504F4E47)
	magicPing = 0x50494E47
	magicPong = 0x504F4E47

	udpReadBufferSize = 64 * 1024

	// Resource limits
	ephemeralSocketCount = 256
	searchTickerInterval = 10 * time.Millisecond
	searchTimeout        = 3 * time.Second
)

type Transport interface {
	Recv() (src string, b []byte, err error)
	// TODO(saml) can we make `withPunch` implicit?
	Send(dst string, b []byte, withPunch bool) error
	Close() error
}

type packet struct {
	src  string
	data []byte
	err  error
}

type route struct {
	conn       *net.UDPConn
	remoteAddr *net.UDPAddr
}

type pendingProbe struct {
	expectedIP net.IP
	result     chan route
}

type megaSock struct {
	log *zap.SugaredLogger

	primary *net.UDPConn

	// address -> working route
	sessions map[string]route
	sessMu   sync.RWMutex

	// nonce -> probe context
	pending map[uint64]pendingProbe
	pendMu  sync.Mutex

	// remote IP -> active search
	// this differs from `sessions` which key to the `addr:port`
	inflight map[string]struct{}
	inflMu   sync.Mutex

	recvChan    chan packet
	closeCtx    context.Context
	closeCancel context.CancelFunc
}

func NewTransport(port int) (Transport, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		return nil, fmt.Errorf("failed to listen UDP: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ms := &megaSock{
		log:         zap.S().Named("magicsock"),
		primary:     conn,
		sessions:    make(map[string]route),
		pending:     make(map[uint64]pendingProbe),
		inflight:    make(map[string]struct{}),
		recvChan:    make(chan packet, 1024),
		closeCtx:    ctx,
		closeCancel: cancel,
	}

	go ms.readLoop(conn)

	return ms, nil
}

func (m *megaSock) Recv() (string, []byte, error) {
	select {
	case p := <-m.recvChan:
		return p.src, p.data, p.err
	case <-m.closeCtx.Done():
		return "", nil, fmt.Errorf("transport closed")
	}
}

func (m *megaSock) Send(dst string, b []byte, withPunch bool) error {
	addr, err := net.ResolveUDPAddr("udp", dst)
	if err != nil {
		return err
	}

	directKey := addr.String()

	m.sessMu.RLock()
	r, ok := m.sessions[directKey]
	m.sessMu.RUnlock()
	if ok {
		_, err := r.conn.WriteToUDP(b, r.remoteAddr)
		return err
	}

	// Try send on primary (same network, or port-preserving remote)
	// TODO(saml) can probably make smart decisions based on locality (if a commonly known local IP address)
	if _, err := m.primary.WriteToUDP(b, addr); err != nil {
		return err
	}

	if !withPunch {
		return nil
	}

	searchKey := addr.IP.String()
	m.inflMu.Lock()
	if _, active := m.inflight[searchKey]; active {
		m.inflMu.Unlock()
		return nil
	}
	m.inflight[searchKey] = struct{}{}
	m.inflMu.Unlock()

	winner, ok, err := m.fanOut(addr, searchKey)
	if err != nil {
		return err
	}
	if ok {
		_, err := winner.conn.WriteToUDP(b, winner.remoteAddr)
		return err
	}
	return nil
}

func (m *megaSock) Close() error {
	m.closeCancel()

	err := m.primary.Close()

	m.sessMu.Lock()
	defer m.sessMu.Unlock()
	for _, r := range m.sessions {
		if r.conn != m.primary {
			r.conn.Close()
		}
	}
	// Clear maps to aid GC
	m.sessions = make(map[string]route)

	return err
}

func (m *megaSock) readLoop(conn *net.UDPConn) {
	buf := make([]byte, udpReadBufferSize)
	for {
		select {
		case <-m.closeCtx.Done():
			return
		default:
		}

		if err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			return
		}
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}

		// Protocol Check (Magic + Nonce)
		if n == headerSize {
			magic := binary.BigEndian.Uint32(buf[:4])
			nonce := binary.BigEndian.Uint64(buf[4:])

			if magic == magicPing {
				// Peer searching for us: Echo PONG back to exact source
				reply := make([]byte, headerSize)
				binary.BigEndian.PutUint32(reply[:4], magicPong)
				binary.BigEndian.PutUint64(reply[4:], nonce)
				if _, err := conn.WriteToUDP(reply, addr); err != nil {
					select {
					case <-m.closeCtx.Done():
						return
					default:
					}
				}
				continue
			}

			if magic == magicPong {
				// We found peer: Match nonce
				m.pendMu.Lock()
				probe, ok := m.pending[nonce]
				m.pendMu.Unlock()
				if ok && probe.expectedIP.Equal(addr.IP) {
					probe.result <- route{conn: conn, remoteAddr: addr}
				}
				continue
			}
		}

		// Data
		data := make([]byte, n)
		copy(data, buf[:n])

		// TODO(saml) there's a chance that this causes problems if a peer previously resided in the same network and is now offline.
		// When we randomly open sockets for future punches, then other nodes outside of the network might land on those sockets.
		// I think here in particular is problematic, as we're establishing connections for any inbound message
		r := route{conn: conn, remoteAddr: addr}
		m.sessMu.Lock()
		if _, ok := m.sessions[addr.String()]; !ok {
			m.sessions[addr.String()] = r
		}
		m.sessMu.Unlock()

		select {
		case m.recvChan <- packet{src: addr.String(), data: data}:
		case <-m.closeCtx.Done():
			return
		}
	}
}

func (m *megaSock) fanOut(target *net.UDPAddr, inflightKey string) (route, bool, error) {
	defer func() {
		m.inflMu.Lock()
		delete(m.inflight, inflightKey)
		m.inflMu.Unlock()
	}()

	var nonce uint64
	if err := binary.Read(rand.Reader, binary.BigEndian, &nonce); err != nil {
		return route{}, false, err
	}

	winChan := make(chan route, 1)
	m.pendMu.Lock()
	m.pending[nonce] = pendingProbe{expectedIP: target.IP, result: winChan}
	m.pendMu.Unlock()

	defer func() {
		m.pendMu.Lock()
		delete(m.pending, nonce)
		m.pendMu.Unlock()
	}()

	sockets := make([]*net.UDPConn, 0, ephemeralSocketCount)
	for range ephemeralSocketCount {
		c, err := net.ListenUDP("udp", &net.UDPAddr{IP: nil, Port: 0})
		if err == nil {
			sockets = append(sockets, c)
			go m.readLoop(c)
		}
	}

	ctx, cancel := context.WithTimeout(m.closeCtx, searchTimeout)
	defer cancel()

	ping := make([]byte, headerSize)
	binary.BigEndian.PutUint32(ping[:4], magicPing)
	binary.BigEndian.PutUint64(ping[4:], nonce)

	ticker := time.NewTicker(searchTickerInterval)
	defer ticker.Stop()

	var winner route
	success := false
	tickCount := 0

search:
	for {
		select {
		case w := <-winChan:
			winner = w
			success = true
			break search
		case <-ctx.Done():
			break search
		case <-ticker.C:
			// Easy-side behaviour: keep source port fixed (primary socket) and
			// probe random destination ports on the peer to discover an open mapping.
			{
				var b [2]byte
				if _, err := rand.Read(b[:]); err == nil {
					port := 1024 + int(binary.BigEndian.Uint16(b[:])%64511)
					dst := &net.UDPAddr{IP: target.IP, Port: port}
					m.primary.WriteToUDP(ping, dst)
				}
			}

			// Hard-side behaviour: vary source port by cycling ephemeral sockets
			// while sending to a fixed destination (the peer’s known ip:port) to
			// create many NAT mappings on our side.
			if target.Port > 0 && len(sockets) > 0 {
				sock := sockets[tickCount%len(sockets)]
				sock.WriteToUDP(ping, target)
			}

			tickCount++
		}
	}

	for _, c := range sockets {
		if success && c == winner.conn {
			continue
		}
		c.Close()
	}

	if success {
		m.sessMu.Lock()
		m.sessions[target.String()] = winner
		if winner.remoteAddr != nil {
			m.sessions[winner.remoteAddr.String()] = winner
		}
		m.sessMu.Unlock()
	}

	return winner, success, nil
}
