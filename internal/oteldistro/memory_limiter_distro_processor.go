package oteldistro

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// memLimiterState holds per-instance state for MemoryLimiterDistroProcessor.
type memLimiterState struct {
	mu        sync.Mutex
	memStats  runtime.MemStats
	lastCheck time.Time
	dropped   int64
}

var memLimiterStates sync.Map // *MemoryLimiterDistroProcessor → *memLimiterState

func getMemLimiterState(p *MemoryLimiterDistroProcessor) *memLimiterState {
	if v, ok := memLimiterStates.Load(p); ok {
		return v.(*memLimiterState)
	}
	s := &memLimiterState{}
	actual, _ := memLimiterStates.LoadOrStore(p, s)
	return actual.(*memLimiterState)
}

func (p *MemoryLimiterDistroProcessor) Start(ctx context.Context, host Host) error {
	s := getMemLimiterState(p)
	s.mu.Lock()
	defer s.mu.Unlock()
	runtime.ReadMemStats(&s.memStats)
	s.lastCheck = time.Now()
	return nil
}

func (p *MemoryLimiterDistroProcessor) Shutdown(ctx context.Context) error {
	memLimiterStates.Delete(p)
	return nil
}

func (p *MemoryLimiterDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {
	s := getMemLimiterState(p)
	s.mu.Lock()
	defer s.mu.Unlock()

	checkInterval := p.config.CheckInterval
	if checkInterval <= 0 {
		checkInterval = time.Second
	}

	if time.Since(s.lastCheck) >= checkInterval {
		runtime.ReadMemStats(&s.memStats)
		s.lastCheck = time.Now()
	}

	if p.config.LimitMiB > 0 {
		usedMiB := s.memStats.Alloc / (1024 * 1024)
		if usedMiB >= uint64(p.config.LimitMiB) {
			atomic.AddInt64(&s.dropped, 1)
			return nil, fmt.Errorf("memory limit exceeded: %d MiB used, limit %d MiB", usedMiB, p.config.LimitMiB)
		}
	}

	return metrics, nil
}
