package oteldistro

import "context"

func (p *FilterDistroProcessor) Start(ctx context.Context, host Host) error { return nil }

func (p *FilterDistroProcessor) Shutdown(ctx context.Context) error { return nil }

func (p *FilterDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {

	return metrics, nil
}
