package oteldistro

import (
	"context"
	"sync"
	"time"
)

// batchState holds per-instance state for BatchDistroProcessor.
type batchState struct {
	mu            sync.Mutex
	batch         []*Metrics
	timer         *time.Timer
	nextProcessor Processor
}

func (p *BatchDistroProcessor) Start(ctx context.Context, host Host) error {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()

	timeout := p.config.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	p.state.timer = time.AfterFunc(timeout, func() {
		p.flushBatch()
	})
	return nil
}

func (p *BatchDistroProcessor) Shutdown(ctx context.Context) error {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()

	if p.state.timer != nil {
		p.state.timer.Stop()
	}
	p.flushBatchLocked(&p.state)
	return nil
}

func (p *BatchDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()

	p.state.batch = append(p.state.batch, metrics)

	if p.config.SendBatchSize > 0 && len(p.state.batch) >= p.config.SendBatchSize {
		merged := p.mergeBatchLocked(&p.state)
		p.state.batch = nil
		if p.state.timer != nil {
			p.state.timer.Reset(p.config.Timeout)
		}
		if p.state.nextProcessor != nil {
			return p.state.nextProcessor.ProcessMetrics(ctx, merged)
		}
		return merged, nil
	}

	return metrics, nil
}

func (p *BatchDistroProcessor) flushBatch() {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	p.flushBatchLocked(&p.state)
	if p.state.timer != nil {
		p.state.timer.Reset(p.config.Timeout)
	}
}

func (p *BatchDistroProcessor) flushBatchLocked(s *batchState) {
	if len(s.batch) == 0 {
		return
	}
	merged := p.mergeBatchLocked(s)
	s.batch = nil
	if s.nextProcessor != nil {
		s.nextProcessor.ProcessMetrics(context.Background(), merged)
	}
}

func (p *BatchDistroProcessor) mergeBatchLocked(s *batchState) *Metrics {
	merged := &Metrics{}
	for _, m := range s.batch {
		if m != nil {
			merged.ResourceMetrics = append(merged.ResourceMetrics, m.ResourceMetrics...)
		}
	}
	return merged
}
