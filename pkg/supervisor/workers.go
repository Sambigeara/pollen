package supervisor

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

type workerPool struct {
	work chan func()
	eg   *errgroup.Group
	wg   sync.WaitGroup
}

func newWorkerPool(maxConcurrency int) *workerPool {
	eg := &errgroup.Group{}
	eg.SetLimit(maxConcurrency)
	return &workerPool{
		work: make(chan func(), 64), //nolint:mnd
		eg:   eg,
	}
}

func (p *workerPool) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case fn := <-p.work:
					p.eg.Go(func() error { fn(); p.wg.Done(); return nil })
				default:
					return
				}
			}
		case fn := <-p.work:
			p.eg.Go(func() error { fn(); p.wg.Done(); return nil })
		}
	}
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
