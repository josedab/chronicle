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

var batchStates sync.Map // *BatchDistroProcessor → *batchState

func getBatchState(p *BatchDistroProcessor) *batchState {
	if v, ok := batchStates.Load(p); ok {
		return v.(*batchState)
	}
	s := &batchState{}
	actual, _ := batchStates.LoadOrStore(p, s)
	return actual.(*batchState)
}

func (p *BatchDistroProcessor) Start(ctx context.Context, host Host) error {
	s := getBatchState(p)
	s.mu.Lock()
	defer s.mu.Unlock()

	timeout := p.config.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	s.timer = time.AfterFunc(timeout, func() {
		p.flushBatch()
	})
	return nil
}

func (p *BatchDistroProcessor) Shutdown(ctx context.Context) error {
	s := getBatchState(p)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.timer != nil {
		s.timer.Stop()
	}
	p.flushBatchLocked(s)
	batchStates.Delete(p)
	return nil
}

func (p *BatchDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {
	s := getBatchState(p)
	s.mu.Lock()
	defer s.mu.Unlock()

	s.batch = append(s.batch, metrics)

	if p.config.SendBatchSize > 0 && len(s.batch) >= p.config.SendBatchSize {
		merged := p.mergeBatchLocked(s)
		s.batch = nil
		if s.timer != nil {
			s.timer.Reset(p.config.Timeout)
		}
		if s.nextProcessor != nil {
			return s.nextProcessor.ProcessMetrics(ctx, merged)
		}
		return merged, nil
	}

	return metrics, nil
}

func (p *BatchDistroProcessor) flushBatch() {
	s := getBatchState(p)
	s.mu.Lock()
	defer s.mu.Unlock()
	p.flushBatchLocked(s)
	if s.timer != nil {
		s.timer.Reset(p.config.Timeout)
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
