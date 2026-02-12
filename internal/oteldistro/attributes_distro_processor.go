package oteldistro

import "context"

func (p *AttributesDistroProcessor) Start(ctx context.Context, host Host) error { return nil }

func (p *AttributesDistroProcessor) Shutdown(ctx context.Context) error { return nil }

func (p *AttributesDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {
	for _, action := range p.config.Actions {
		for i := range metrics.ResourceMetrics {
			switch action.Action {
			case "insert":
				metrics.ResourceMetrics[i].Resource.Attributes[action.Key] = action.Value
			case "delete":
				delete(metrics.ResourceMetrics[i].Resource.Attributes, action.Key)
			}
		}
	}
	return metrics, nil
}
