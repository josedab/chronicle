package oteldistro

import "context"

func (e *DebugDistroExporter) Start(ctx context.Context, host Host) error { return nil }

func (e *DebugDistroExporter) Shutdown(ctx context.Context) error { return nil }

func (e *DebugDistroExporter) ExportMetrics(ctx context.Context, metrics *Metrics) error {

	return nil
}
