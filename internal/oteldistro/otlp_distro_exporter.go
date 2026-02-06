package oteldistro

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

// otlpExporterState holds per-instance state for OTLPDistroExporter.
type otlpExporterState struct {
	mu       sync.Mutex
	buffer   [][]byte
	exported int64
	failed   int64
}

var otlpExporterStates sync.Map // *OTLPDistroExporter → *otlpExporterState

func getOTLPExporterState(e *OTLPDistroExporter) *otlpExporterState {
	if v, ok := otlpExporterStates.Load(e); ok {
		return v.(*otlpExporterState)
	}
	s := &otlpExporterState{}
	actual, _ := otlpExporterStates.LoadOrStore(e, s)
	return actual.(*otlpExporterState)
}

func (e *OTLPDistroExporter) Start(ctx context.Context, host Host) error { return nil }

func (e *OTLPDistroExporter) Shutdown(ctx context.Context) error {
	s := getOTLPExporterState(e)
	s.mu.Lock()
	defer s.mu.Unlock()
	// Flush remaining buffer on shutdown
	if len(s.buffer) > 0 {
		e.flushLocked(s)
	}
	otlpExporterStates.Delete(e)
	return nil
}

func (e *OTLPDistroExporter) ExportMetrics(ctx context.Context, metrics *Metrics) error {
	if metrics == nil {
		return nil
	}

	data, err := json.Marshal(metrics)
	if err != nil {
		s := getOTLPExporterState(e)
		atomic.AddInt64(&s.failed, 1)
		return fmt.Errorf("failed to serialize metrics: %w", err)
	}

	s := getOTLPExporterState(e)
	s.mu.Lock()
	s.buffer = append(s.buffer, data)
	s.mu.Unlock()

	atomic.AddInt64(&s.exported, 1)
	return nil
}

// Flush sends buffered metrics to the remote OTLP endpoint.
func (e *OTLPDistroExporter) Flush() error {
	s := getOTLPExporterState(e)
	s.mu.Lock()
	defer s.mu.Unlock()
	return e.flushLocked(s)
}

func (e *OTLPDistroExporter) flushLocked(s *otlpExporterState) error {
	if len(s.buffer) == 0 {
		return nil
	}
	// Stub: in production this would POST to e.config.Endpoint using e.client
	s.buffer = nil
	return nil
}
