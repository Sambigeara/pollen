package link

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"go.uber.org/zap"
)

const (
	pingInterval = time.Second * 25
	pingJitter   = 0.05
	jitterScale  = 2
)

type pingMgr struct {
	bumpCh chan struct{}
}

func (i *impl) newPingMgr(ctx context.Context, peerAddr string) *pingMgr {
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
				if err := i.transport.Send(peerAddr, encodeFrame(&frame{typ: types.MsgTypePing})); err != nil {
					logger.Debugw("ping send failed", "addr", peerAddr, "err", err)
				}
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

func (i *impl) bumpPinger(ctx context.Context, addr string) {
	i.pingMu.Lock()
	defer i.pingMu.Unlock()

	mgr, ok := i.pingMgrs[addr]
	if ok {
		select {
		case mgr.bumpCh <- struct{}{}:
		default:
		}
		return
	}

	zap.S().Infof("creating new pinger: %s", addr)
	i.pingMgrs[addr] = i.newPingMgr(ctx, addr)
}

func jitteredPingInterval() time.Duration {
	return jitter(pingInterval, pingJitter)
}

// jitter returns d randomized by ±percent (0–1).
// e.g. jitter(30*time.Second, 0.1) → 27s–33s.
func jitter(d time.Duration, percent float64) time.Duration {
	delta := time.Duration(float64(d) * percent) // max deviation
	// Sample in [0, 2*delta] then shift to [-delta, +delta].
	n := int64(delta)*jitterScale + 1
	//nolint:gosec
	offset := time.Duration(rand.N(n)) - delta
	return d + offset
}
