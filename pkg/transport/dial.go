package transport

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/sambigeara/pollen/pkg/nat"
	"github.com/sambigeara/pollen/pkg/types"

	"github.com/quic-go/quic-go"
)

type directDialResult struct {
	session *peerSession
	err     error
}

func (m *impl) dialDirect(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey) (*peerSession, error) {
	tlsCfg := newExpectedPeerTLSConfig(&m.meshCert, expectedPeer, m.rootPub, m.reconnectWindow)
	qCfg := quicConfig()

	qc, err := m.mainQT.Dial(ctx, addr, tlsCfg, qCfg)
	if err != nil {
		return nil, fmt.Errorf("quic dial %s: %w", addr, err)
	}

	return newPeerSession(qc, m.mainQT, true), nil
}

func (m *impl) dialPunch(ctx context.Context, addr *net.UDPAddr, expectedPeer types.PeerKey, localNAT nat.Type) (*peerSession, error) {
	tlsCfg := newExpectedPeerTLSConfig(&m.meshCert, expectedPeer, m.rootPub, m.reconnectWindow)
	qCfg := quicConfig()

	conn, err := m.socks.Punch(ctx, addr, localNAT)
	if err != nil {
		return nil, fmt.Errorf("quic dial %s: sock store: %w", addr, err)
	}

	dialAddr := addr
	if peerAddr := conn.Peer(); peerAddr != nil {
		dialAddr = peerAddr
	}

	// Easy-side punch winners reuse the main transport and don't carry a UDPConn.
	if conn.UDPConn == nil {
		qc, err := m.mainQT.Dial(ctx, dialAddr, tlsCfg, qCfg)
		if err != nil {
			return nil, fmt.Errorf("quic dial %s: %w", dialAddr, err)
		}
		return newPeerSession(qc, m.mainQT, true), nil
	}

	qt := &quic.Transport{Conn: conn.UDPConn}

	qc, err := qt.Dial(ctx, dialAddr, tlsCfg, qCfg)
	if err != nil {
		_ = qt.Close()
		conn.Close()
		return nil, fmt.Errorf("quic dial %s: %w", dialAddr, err)
	}

	s := newPeerSession(qc, qt, true)
	s.sockConn = conn
	return s, nil
}

func (m *impl) raceDirectDial(ctx context.Context, peerKey types.PeerKey, addrs []netip.AddrPort) (*peerSession, error) {
	dialCtx, cancelDial := context.WithCancel(ctx)
	defer cancelDial()

	ch := make(chan directDialResult, len(addrs))
	for _, ap := range addrs {
		go func() {
			s, err := m.dialDirect(dialCtx, net.UDPAddrFromAddrPort(ap), peerKey)
			ch <- directDialResult{session: s, err: err}
		}()
	}

	var lastErr error
	remaining := len(addrs)
	for remaining > 0 {
		r := <-ch
		remaining--
		if r.err != nil {
			lastErr = r.err
			continue
		}

		cancelDial()
		go func(remaining int) {
			for range remaining {
				r := <-ch
				if r.err == nil {
					m.closeSession(r.session, "replaced")
				}
			}
		}(remaining)
		return r.session, nil
	}

	return nil, lastErr
}

func (m *impl) Connect(ctx context.Context, peerKey types.PeerKey, addrs []netip.AddrPort) error {
	if len(addrs) == 0 {
		return fmt.Errorf("connect to %s: no addresses", peerKey.Short())
	}

	winner, err := m.raceDirectDial(ctx, peerKey, addrs)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", peerKey.Short(), err)
	}

	m.addPeer(ctx, winner, peerKey)
	return nil
}

func (m *impl) Punch(ctx context.Context, peerKey types.PeerKey, addr *net.UDPAddr, localNAT nat.Type) error {
	if m.disableNATPunch {
		return fmt.Errorf("NAT punch disabled")
	}
	s, err := m.dialPunch(ctx, addr, peerKey, localNAT)
	if err != nil {
		return err
	}
	m.addPeer(ctx, s, peerKey)
	return nil
}

func (m *impl) tryDial(pk types.PeerKey, addrs []netip.AddrPort) {
	dialCtx, cancel := context.WithTimeout(context.Background(), internalConnectTimeout)
	defer cancel()

	winner, err := m.raceDirectDial(dialCtx, pk, addrs)
	if err != nil {
		m.peers.step(time.Now(), connectFailed{PeerKey: pk})
		return
	}
	m.addPeer(dialCtx, winner, pk)
}

func (m *impl) internalConnect(pk types.PeerKey, ips []net.IP, port int) {
	addrs := make([]netip.AddrPort, 0, len(ips))
	for _, ip := range ips {
		addr, ok := netip.AddrFromSlice(ip)
		if !ok {
			continue
		}
		addrs = append(addrs, netip.AddrPortFrom(addr, uint16(port))) //nolint:gosec
	}
	if len(addrs) == 0 {
		m.peers.step(time.Now(), connectFailed{PeerKey: pk})
		return
	}
	m.tryDial(pk, addrs)
}

func (m *impl) internalEagerConnect(pk types.PeerKey, addr *net.UDPAddr) {
	m.tryDial(pk, []netip.AddrPort{addr.AddrPort()})
}
