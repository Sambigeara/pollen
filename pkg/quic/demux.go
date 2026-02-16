package quic

import (
	"net"
	"time"
)

const quicFirstByteMin = 0x40

// demuxConn wraps a *net.UDPConn and intercepts socket-store packets (first
// byte < 0x40), passing only QUIC packets through to QUIC. It implements
// net.PacketConn.
//
// It intentionally does NOT implement quic-go's OOBCapablePacketConn, because
// that interface triggers a code path in quic-go that requires the full
// net.Conn interface (Read, Write, RemoteAddr) which doesn't apply to a
// demuxing wrapper. QUIC falls back to the basic ReadFrom/WriteTo path, which
// is correct here.
type demuxConn struct {
	conn    *net.UDPConn
	handler func(data []byte, addr *net.UDPAddr)
}

func newDemuxConn(conn *net.UDPConn, handler func([]byte, *net.UDPAddr)) *demuxConn {
	return &demuxConn{conn: conn, handler: handler}
}

func (d *demuxConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		n, addr, err := d.conn.ReadFromUDP(p)
		if err != nil {
			return 0, nil, err
		}
		if n > 0 && p[0] < quicFirstByteMin {
			buf := make([]byte, n)
			copy(buf, p[:n])
			d.handler(buf, addr)
			continue
		}
		return n, addr, nil
	}
}

func (d *demuxConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return d.conn.WriteTo(p, addr)
	}
	return d.conn.WriteToUDP(p, udpAddr)
}

func (d *demuxConn) Close() error                      { return d.conn.Close() }
func (d *demuxConn) LocalAddr() net.Addr               { return d.conn.LocalAddr() }
func (d *demuxConn) SetDeadline(t time.Time) error     { return d.conn.SetDeadline(t) }
func (d *demuxConn) SetReadDeadline(t time.Time) error  { return d.conn.SetReadDeadline(t) }
func (d *demuxConn) SetWriteDeadline(t time.Time) error { return d.conn.SetWriteDeadline(t) }
