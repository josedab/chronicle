package oteldistro

import "context"

func (p *MemoryLimiterDistroProcessor) Start(ctx context.Context, host Host) error { return nil }

func (p *MemoryLimiterDistroProcessor) Shutdown(ctx context.Context) error { return nil }

func (p *MemoryLimiterDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {

	return metrics, nil
}
