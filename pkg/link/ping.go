package link

import (
	"context"
	"time"

	"github.com/sambigeara/pollen/pkg/types"
	"github.com/sambigeara/pollen/pkg/util"
	"go.uber.org/zap"
)

const (
	pingInterval = time.Second * 25
	pingJitter   = 0.05
)

type pingMgr struct {
	bumpCh chan struct{}
}

func (i *impl) newPingMgr(ctx context.Context, peerAddr string) *pingMgr {
	logger := zap.S().Named("ping")
	bumpCh := make(chan struct{}, 1)
	go func() {
		ticker := util.NewJitterTicker(ctx, pingInterval, pingJitter)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				logger.Debugf("closing pinger: %s", peerAddr)
				return
			case <-ticker.C:
				if err := i.transport.Send(peerAddr, encodeFrame(&frame{typ: types.MsgTypePing})); err != nil {
					logger.Debugw("ping send failed", "addr", peerAddr, "err", err)
				}
			case <-bumpCh:
				ticker.Bump()
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
