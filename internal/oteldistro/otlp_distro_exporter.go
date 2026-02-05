package oteldistro

import "context"

func (e *OTLPDistroExporter) Start(ctx context.Context, host Host) error { return nil }

func (e *OTLPDistroExporter) Shutdown(ctx context.Context) error { return nil }

func (e *OTLPDistroExporter) ExportMetrics(ctx context.Context, metrics *Metrics) error {

	return nil
}
