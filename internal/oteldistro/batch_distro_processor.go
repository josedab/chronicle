package oteldistro

import "context"

func (p *BatchDistroProcessor) Start(ctx context.Context, host Host) error { return nil }

func (p *BatchDistroProcessor) Shutdown(ctx context.Context) error { return nil }

func (p *BatchDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {

	return metrics, nil
}
