package mesh

import (
	"context"
	"math/rand/v2"
	"time"

	"go.uber.org/zap"
)

type pingMgr struct {
	bumpCh chan struct{}
}

func (m *Mesh) newPingMgr(ctx context.Context, peerAddr string) *pingMgr {
	logger := zap.S().Named("ping")
	bumpCh := make(chan struct{}, 1)
	go func() {
		timer := time.NewTimer(jitteredPingInterval())
		for {
			select {
			case <-ctx.Done():
				logger.Debugf("closing pinger: %s", peerAddr)
				return
			case <-timer.C:
				m.write(ctx, peerAddr, MessageTypePing, 0, 0, []byte{})
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

func (m *Mesh) bumpPinger(ctx context.Context, addr string) {
	m.pingMu.Lock()
	defer m.pingMu.Unlock()

	mgr, ok := m.pingMgrs[addr]
	if ok {
		select {
		case mgr.bumpCh <- struct{}{}:
		default:
		}
		return
	}

	zap.S().Infof("creating new pinger: %s", addr)
	m.pingMgrs[addr] = m.newPingMgr(ctx, addr)
}

func jitteredPingInterval() time.Duration {
	return jitter(pingInterval, pingJitter)
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
