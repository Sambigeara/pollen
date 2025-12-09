package mesh

import (
	"context"
	"math/rand/v2"
	"net"
	"time"

	"go.uber.org/zap"
)

const (
	sessionRefreshInterval       = time.Second * 120 // new IK handshake every 2 mins
	sessionRefreshIntervalJitter = 0.1
	pingInterval                 = time.Second * 25
	pingJitter                   = 0.05
)

type UDPConn struct {
	*net.UDPConn
	ctx      context.Context
	pingMgrs map[string]*pingMgr
}

func newUDPConn(ctx context.Context, port int) (*UDPConn, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: port})
	if err != nil {
		return nil, err
	}

	return &UDPConn{
		UDPConn:  conn,
		ctx:      ctx,
		pingMgrs: make(map[string]*pingMgr),
	}, nil
}

func (c *UDPConn) ReadFromUDP(b []byte) (int, *net.UDPAddr, error) {
	n, addr, err := c.UDPConn.ReadFromUDP(b)
	if err == nil {
		c.bumpPinger(addr)
	}
	return n, addr, err
}

func (c *UDPConn) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	n, err := c.UDPConn.WriteToUDP(b, addr)
	if err == nil {
		c.bumpPinger(addr)
	}
	return n, err
}

func (c *UDPConn) bumpPinger(addr *net.UDPAddr) {
	mgr, ok := c.pingMgrs[addr.String()]
	if ok {
		select {
		case mgr.bumpCh <- struct{}{}:
		default:
		}
		return
	}

	zap.S().Infof("creating new pinger: %s", addr.String())
	c.pingMgrs[addr.String()] = c.newPingMgr(addr)
}

type pingMgr struct {
	bumpCh chan struct{}
}

func (c *UDPConn) newPingMgr(peerAddr *net.UDPAddr) *pingMgr {
	logger := zap.S().Named("mesh")
	bumpCh := make(chan struct{}, 1)
	go func() {
		timer := time.NewTimer(jitteredPingInterval())
		for {
			select {
			case <-c.ctx.Done():
				logger.Debugf("closing pinger: %s", peerAddr.String())
				return
			case <-timer.C:
				logger.Debugf("pinging peer: %s", peerAddr.String())
				write(c, peerAddr, MessageTypePing, 0, 0, []byte{})
				timer.Reset(jitteredPingInterval())
			case <-bumpCh:
				timer.Reset(jitteredPingInterval())
			}
		}
	}()

	return &pingMgr{
		bumpCh: bumpCh,
	}
}

func jitteredPingInterval() time.Duration {
	return jitter(pingInterval, pingJitter)
}

func jitteredSessionRefreshInterval() time.Duration {
	return jitter(sessionRefreshInterval, sessionRefreshIntervalJitter)
}

// jitter returns d randomized by ±percent (0–1).
// e.g. jitter(30*time.Second, 0.1) → 27s–33s.
func jitter(d time.Duration, percent float64) time.Duration {
	delta := time.Duration(float64(d) * percent) // max deviation
	// Sample in [0, 2*delta] then shift to [-delta, +delta].
	n := int64(delta)*2 + 1
	offset := time.Duration(rand.N(n)) - delta
	return d + offset
}
