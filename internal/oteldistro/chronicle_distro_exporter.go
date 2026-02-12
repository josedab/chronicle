package oteldistro

import (
	"context"
	"fmt"
	"time"
)

func (e *ChronicleDistroExporter) Start(ctx context.Context, host Host) error { return nil }

func (e *ChronicleDistroExporter) Shutdown(ctx context.Context) error { return nil }

func (e *ChronicleDistroExporter) ExportMetrics(ctx context.Context, metrics *Metrics) error {
	if e.pw == nil {
		return nil
	}

	for _, rm := range metrics.ResourceMetrics {
		tags := make(map[string]string)
		for k, v := range rm.Resource.Attributes {
			tags[k] = fmt.Sprintf("%v", v)
		}

		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {

				point := Point{
					Metric:    m.Name,
					Timestamp: time.Now().UnixNano(),
					Tags:      tags,
				}
				e.pw.Write(point)
			}
		}
	}

	return nil
}
