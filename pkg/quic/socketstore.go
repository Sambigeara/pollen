package quic

import (
	"context"
	"crypto/rand"
	"net"
	"sync"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const (
	probeTimeout   = 1 * time.Second
	probeNonceSize = 16
	probeReqByte   = 0x01
	probeRespByte  = 0x02
	punchReqByte   = 0x03
	punchTrigByte  = 0x04

	punchTriggerWait = 3 * time.Second
)

// GetOrCreateOpts configures optional behaviour for GetOrCreate.
type GetOrCreateOpts struct {
	Coordinators []*net.UDPAddr
}

// Socket is a thin wrapper around a UDP socket with a proven remote address.
type Socket struct {
	store *SocketStore
	conn  *net.UDPConn
	addr  *net.UDPAddr
	id    uint64 // 0 = main socket
}

func (s *Socket) UDPConn() *net.UDPConn   { return s.conn }
func (s *Socket) RemoteAddr() *net.UDPAddr { return s.addr }
func (s *Socket) IsMain() bool            { return s.id == 0 }
func (s *Socket) Close()                  { s.store.release(s.id) }

// SocketStore owns all UDP sockets, proves raw connectivity via probes, and
// coordinates hole punching. QUIC only gets involved once a reachable socket
// exists.
type SocketStore struct {
	mainConn *net.UDPConn
	demux    *demuxConn
	localID  types.PeerKey
	log      *zap.SugaredLogger

	// Probe response channels (main socket probes only).
	probeMu sync.Mutex
	probes  map[[probeNonceSize]byte]chan *net.UDPAddr

	// Ephemeral sockets with refcounts.
	ephMu      sync.Mutex
	ephSockets map[uint64]*ephSocket
	nextEphID  uint64

	// Observed addresses (populated by NoteAddress).
	addrMu   sync.RWMutex
	observed map[types.PeerKey]*net.UDPAddr

	// Punch trigger delivery.
	triggerMu sync.Mutex
	triggers  map[types.PeerKey]chan *net.UDPAddr
}

type ephSocket struct {
	conn     *net.UDPConn
	refcount int32
}

// NewSocketStore binds the main UDP socket and returns a new store.
func NewSocketStore(port int, localID types.PeerKey) (*SocketStore, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		return nil, err
	}

	ss := &SocketStore{
		mainConn:   conn,
		localID:    localID,
		log:        zap.S().Named("socketstore"),
		probes:     make(map[[probeNonceSize]byte]chan *net.UDPAddr),
		ephSockets: make(map[uint64]*ephSocket),
		observed:   make(map[types.PeerKey]*net.UDPAddr),
		triggers:   make(map[types.PeerKey]chan *net.UDPAddr),
	}

	ss.demux = newDemuxConn(conn, ss.handlePacket)
	return ss, nil
}

// MainPacketConn returns a net.PacketConn that filters out socket-store
// protocol traffic. Pass this to the QUIC transport for listening.
func (ss *SocketStore) MainPacketConn() net.PacketConn { return ss.demux }

// NoteAddress records an observed address for a peer. Called by upper layers
// when a QUIC connection is established.
func (ss *SocketStore) NoteAddress(peerID types.PeerKey, addr *net.UDPAddr) {
	if addr == nil {
		return
	}
	ss.addrMu.Lock()
	ss.observed[peerID] = addr
	ss.addrMu.Unlock()
}

// GetObservedAddr returns the last observed address for a peer.
func (ss *SocketStore) GetObservedAddr(peerID types.PeerKey) (*net.UDPAddr, bool) {
	ss.addrMu.RLock()
	addr, ok := ss.observed[peerID]
	ss.addrMu.RUnlock()
	return addr, ok
}

// GetOrCreate returns a Socket that can reach one of the given addresses.
// Tries direct probes first; if coordinators are provided and direct probes
// fail, attempts punch coordination.
func (ss *SocketStore) GetOrCreate(ctx context.Context, peerID types.PeerKey, addrs []*net.UDPAddr, opts *GetOrCreateOpts) (*Socket, error) {
	if len(addrs) > 0 {
		sock, err := ss.probeAddrs(ctx, addrs)
		if err == nil {
			return sock, nil
		}
	}

	if opts == nil || len(opts.Coordinators) == 0 {
		return nil, ErrUnreachable
	}

	return ss.punchViaCoordinators(ctx, peerID, opts.Coordinators)
}

// Close shuts down all sockets.
func (ss *SocketStore) Close() error {
	ss.ephMu.Lock()
	for id, eph := range ss.ephSockets {
		_ = eph.conn.Close()
		delete(ss.ephSockets, id)
	}
	ss.ephMu.Unlock()
	return ss.mainConn.Close()
}

// --- probing ---

func (ss *SocketStore) probeAddrs(ctx context.Context, addrs []*net.UDPAddr) (*Socket, error) {
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	resultCh := make(chan *Socket, 1)
	var wg sync.WaitGroup

	for _, addr := range addrs {
		if addr == nil {
			continue
		}
		wg.Add(1)
		go func(a *net.UDPAddr) {
			defer wg.Done()
			if ss.probeMain(probeCtx, a) {
				select {
				case resultCh <- &Socket{store: ss, conn: ss.mainConn, addr: a, id: 0}:
					cancel()
				default:
				}
			}
		}(addr)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	sock, ok := <-resultCh
	if !ok {
		return nil, ErrUnreachable
	}
	return sock, nil
}

func (ss *SocketStore) probeMain(ctx context.Context, addr *net.UDPAddr) bool {
	var nonce [probeNonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return false
	}

	ch := make(chan *net.UDPAddr, 1)
	ss.probeMu.Lock()
	ss.probes[nonce] = ch
	ss.probeMu.Unlock()

	defer func() {
		ss.probeMu.Lock()
		delete(ss.probes, nonce)
		ss.probeMu.Unlock()
	}()

	pkt := make([]byte, 1+probeNonceSize)
	pkt[0] = probeReqByte
	copy(pkt[1:], nonce[:])
	if _, err := ss.mainConn.WriteToUDP(pkt, addr); err != nil {
		return false
	}

	select {
	case <-ctx.Done():
		return false
	case <-ch:
		return true
	}
}

func (ss *SocketStore) probeEphemeral(ctx context.Context, addr *net.UDPAddr) (*Socket, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return nil, err
	}

	var nonce [probeNonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		_ = conn.Close()
		return nil, err
	}

	pkt := make([]byte, 1+probeNonceSize)
	pkt[0] = probeReqByte
	copy(pkt[1:], nonce[:])
	if _, err := conn.WriteToUDP(pkt, addr); err != nil {
		_ = conn.Close()
		return nil, err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(probeTimeout)
	}
	_ = conn.SetReadDeadline(deadline)

	buf := make([]byte, 1+probeNonceSize)
	for {
		n, from, readErr := conn.ReadFromUDP(buf)
		if readErr != nil {
			_ = conn.Close()
			return nil, readErr
		}
		if n == 1+probeNonceSize && buf[0] == probeRespByte {
			var resp [probeNonceSize]byte
			copy(resp[:], buf[1:])
			if resp == nonce {
				_ = conn.SetReadDeadline(time.Time{})
				id := ss.registerEphemeral(conn)
				return &Socket{store: ss, conn: conn, addr: from, id: id}, nil
			}
		}
	}
}

func (ss *SocketStore) registerEphemeral(conn *net.UDPConn) uint64 {
	ss.ephMu.Lock()
	defer ss.ephMu.Unlock()
	ss.nextEphID++
	id := ss.nextEphID
	ss.ephSockets[id] = &ephSocket{conn: conn, refcount: 1}
	return id
}

func (ss *SocketStore) release(id uint64) {
	if id == 0 {
		return
	}
	ss.ephMu.Lock()
	defer ss.ephMu.Unlock()
	eph, ok := ss.ephSockets[id]
	if !ok {
		return
	}
	eph.refcount--
	if eph.refcount <= 0 {
		_ = eph.conn.Close()
		delete(ss.ephSockets, id)
	}
}

// --- packet handling (called from demux) ---

func (ss *SocketStore) handlePacket(data []byte, addr *net.UDPAddr) {
	if len(data) < 1 {
		return
	}
	switch data[0] {
	case probeReqByte:
		ss.handleProbeRequest(data, addr)
	case probeRespByte:
		ss.handleProbeResponse(data, addr)
	case punchReqByte:
		ss.handlePunchRequest(data, addr)
	case punchTrigByte:
		ss.handlePunchTrigger(data)
	}
}

func (ss *SocketStore) handleProbeRequest(data []byte, addr *net.UDPAddr) {
	if len(data) != 1+probeNonceSize {
		return
	}
	resp := make([]byte, 1+probeNonceSize)
	resp[0] = probeRespByte
	copy(resp[1:], data[1:])
	_, _ = ss.mainConn.WriteToUDP(resp, addr)
}

func (ss *SocketStore) handleProbeResponse(data []byte, addr *net.UDPAddr) {
	if len(data) != 1+probeNonceSize {
		return
	}
	var nonce [probeNonceSize]byte
	copy(nonce[:], data[1:])

	ss.probeMu.Lock()
	ch, ok := ss.probes[nonce]
	ss.probeMu.Unlock()

	if ok {
		select {
		case ch <- addr:
		default:
		}
	}
}

func (ss *SocketStore) handlePunchRequest(data []byte, requesterAddr *net.UDPAddr) {
	// [0x03][32-byte requester peer ID][32-byte target peer ID]
	if len(data) != 1+32+32 {
		return
	}

	targetID := types.PeerKeyFromBytes(data[33:65])

	ss.addrMu.RLock()
	targetAddr, ok := ss.observed[targetID]
	ss.addrMu.RUnlock()

	if !ok {
		return
	}

	// Tell requester about target.
	ss.sendPunchTrigger(requesterAddr, targetID, targetAddr)
	// Tell target about requester.
	ss.sendPunchTrigger(targetAddr, types.PeerKeyFromBytes(data[1:33]), requesterAddr)
}

func (ss *SocketStore) sendPunchTrigger(to *net.UDPAddr, peerID types.PeerKey, peerAddr *net.UDPAddr) {
	ab := encodeUDPAddr(peerAddr)
	pkt := make([]byte, 1+32+len(ab))
	pkt[0] = punchTrigByte
	copy(pkt[1:33], peerID[:])
	copy(pkt[33:], ab)
	_, _ = ss.mainConn.WriteToUDP(pkt, to)
}

func (ss *SocketStore) handlePunchTrigger(data []byte) {
	// [0x04][32-byte peer ID][addr bytes]
	if len(data) < 1+32+6 {
		return
	}

	peerID := types.PeerKeyFromBytes(data[1:33])
	peerAddr := decodeUDPAddr(data[33:])
	if peerAddr == nil {
		return
	}

	ss.triggerMu.Lock()
	ch, ok := ss.triggers[peerID]
	ss.triggerMu.Unlock()

	if ok {
		select {
		case ch <- peerAddr:
		default:
		}
	}
}

// --- punch coordination ---

func (ss *SocketStore) punchViaCoordinators(ctx context.Context, peerID types.PeerKey, coordinators []*net.UDPAddr) (*Socket, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	trigCh := make(chan *net.UDPAddr, len(coordinators)*2)
	ss.triggerMu.Lock()
	ss.triggers[peerID] = trigCh
	ss.triggerMu.Unlock()
	defer func() {
		ss.triggerMu.Lock()
		delete(ss.triggers, peerID)
		ss.triggerMu.Unlock()
	}()

	// Send punch requests to coordinators.
	pkt := make([]byte, 1+32+32)
	pkt[0] = punchReqByte
	copy(pkt[1:33], ss.localID[:])
	copy(pkt[33:65], peerID[:])
	for _, coord := range coordinators {
		_, _ = ss.mainConn.WriteToUDP(pkt, coord)
	}

	resultCh := make(chan *Socket, 1)
	var wg sync.WaitGroup

	// Process triggers as they arrive.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case targetAddr, ok := <-trigCh:
				if !ok {
					return
				}
				wg.Add(1)
				go func(target *net.UDPAddr) {
					defer wg.Done()
					sock, err := ss.fanoutPunch(ctx, target)
					if err != nil {
						return
					}
					select {
					case resultCh <- sock:
						cancel()
					default:
						sock.Close()
					}
				}(targetAddr)
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	sock, ok := <-resultCh
	if !ok {
		return nil, ErrUnreachable
	}
	return sock, nil
}

func (ss *SocketStore) fanoutPunch(ctx context.Context, targetAddr *net.UDPAddr) (*Socket, error) {
	candidates := buildPunchCandidates(targetAddr, punchRemoteRandomPortAttempts)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan *Socket, 1)
	var wg sync.WaitGroup

	// Probe from main socket to random ports.
	for _, addr := range candidates {
		wg.Add(1)
		go func(a *net.UDPAddr) {
			defer wg.Done()
			if ss.probeMain(ctx, a) {
				select {
				case resultCh <- &Socket{store: ss, conn: ss.mainConn, addr: a, id: 0}:
					cancel()
				default:
				}
			}
		}(addr)
	}

	// Probe from ephemeral sockets to the known target addr.
	for range punchLocalSocketAttempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sock, err := ss.probeEphemeral(ctx, targetAddr)
			if err != nil {
				return
			}
			select {
			case resultCh <- sock:
				cancel()
			default:
				sock.Close()
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	sock, ok := <-resultCh
	if !ok {
		return nil, ErrUnreachable
	}
	return sock, nil
}

// --- addr encoding ---

func encodeUDPAddr(addr *net.UDPAddr) []byte {
	ip := addr.IP.To4()
	if ip == nil {
		ip = addr.IP.To16()
	}
	buf := make([]byte, len(ip)+2)
	copy(buf, ip)
	buf[len(ip)] = byte(addr.Port >> 8)
	buf[len(ip)+1] = byte(addr.Port)
	return buf
}

func decodeUDPAddr(data []byte) *net.UDPAddr {
	switch len(data) {
	case 6: // IPv4
		return &net.UDPAddr{
			IP:   net.IP(append([]byte(nil), data[:4]...)),
			Port: int(data[4])<<8 | int(data[5]),
		}
	case 18: // IPv6
		return &net.UDPAddr{
			IP:   net.IP(append([]byte(nil), data[:16]...)),
			Port: int(data[16])<<8 | int(data[17]),
		}
	default:
		return nil
	}
}
