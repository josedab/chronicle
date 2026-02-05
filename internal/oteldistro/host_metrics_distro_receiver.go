package oteldistro

import (
	"context"
	"time"
)

func (r *HostMetricsDistroReceiver) Start(ctx context.Context, host Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return nil
	}

	interval := 10 * time.Second
	r.ticker = time.NewTicker(interval)
	go r.collectLoop(ctx)

	r.running = true
	return nil
}

func (r *HostMetricsDistroReceiver) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ticker != nil {
		r.ticker.Stop()
	}

	r.running = false
	return nil
}

func (r *HostMetricsDistroReceiver) collectLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.ticker.C:
			r.collectMetrics()
		}
	}
}

func (r *HostMetricsDistroReceiver) collectMetrics() {

}
