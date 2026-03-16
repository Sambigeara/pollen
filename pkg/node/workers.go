package node

import (
	"context"
	"sync"
)

type workerPool struct {
	sem  chan struct{}
	work chan func()
	wg   sync.WaitGroup
}

func newWorkerPool(maxConcurrency int) *workerPool {
	return &workerPool{
		sem:  make(chan struct{}, maxConcurrency),
		work: make(chan func(), 64), //nolint:mnd
	}
}

func (p *workerPool) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case fn := <-p.work:
					p.exec(fn)
				default:
					return
				}
			}
		case fn := <-p.work:
			p.exec(fn)
		}
	}
}

func (p *workerPool) exec(fn func()) {
	p.sem <- struct{}{}
	go func() {
		defer func() {
			<-p.sem
			p.wg.Done()
		}()
		fn()
	}()
}

func (p *workerPool) submit(ctx context.Context, fn func()) {
	p.wg.Add(1)
	select {
	case p.work <- fn:
	case <-ctx.Done():
		p.wg.Done()
	}
}

func (p *workerPool) wait() {
	p.wg.Wait()
}
