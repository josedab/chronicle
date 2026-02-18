package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// TraceStoreConfig configures the trace store.
type TraceStoreConfig struct {
	Enabled           bool          `json:"enabled"`
	MaxSpansPerTrace  int           `json:"max_spans_per_trace"`
	RetentionDuration time.Duration `json:"retention_duration"`
}

// DefaultTraceStoreConfig returns sensible defaults.
func DefaultTraceStoreConfig() TraceStoreConfig {
	return TraceStoreConfig{
		Enabled:           true,
		MaxSpansPerTrace:  10000,
		RetentionDuration: 7 * 24 * time.Hour,
	}
}

// TraceStore stores distributed trace spans alongside metrics.
type TraceStore struct {
	config    TraceStoreConfig
	spans     map[string][]Span    // traceID -> spans
	byService map[string][]string  // service -> traceIDs
	mu        sync.RWMutex
}

// NewTraceStore creates a new trace store.
func NewTraceStore(config TraceStoreConfig) *TraceStore {
	return &TraceStore{
		config:    config,
		spans:     make(map[string][]Span),
		byService: make(map[string][]string),
	}
}

// WriteSpan stores a span.
func (ts *TraceStore) WriteSpan(span Span) error {
	if span.TraceID == "" {
		return fmt.Errorf("trace_id is required")
	}
	if span.SpanID == "" {
		return fmt.Errorf("span_id is required")
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	spans := ts.spans[span.TraceID]
	if len(spans) >= ts.config.MaxSpansPerTrace {
		return fmt.Errorf("max spans per trace exceeded (%d)", ts.config.MaxSpansPerTrace)
	}

	ts.spans[span.TraceID] = append(spans, span)

	if span.Service != "" {
		ts.byService[span.Service] = traceStoreAppendUnique(ts.byService[span.Service], span.TraceID)
	}

	return nil
}

// GetTrace returns all spans for a trace.
func (ts *TraceStore) GetTrace(traceID string) ([]Span, bool) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	spans, ok := ts.spans[traceID]
	if !ok {
		return nil, false
	}
	result := make([]Span, len(spans))
	copy(result, spans)
	return result, true
}

// QueryTraces queries traces by service and time range.
func (ts *TraceStore) QueryTraces(service string, startNano, endNano int64, limit int) [][]Span {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	var traces [][]Span
	traceIDs := ts.byService[service]

	for _, tid := range traceIDs {
		spans := ts.spans[tid]
		if len(spans) == 0 {
			continue
		}

		spanStartNano := spans[0].StartTime.UnixNano()
		if startNano > 0 && spanStartNano < startNano {
			continue
		}
		if endNano > 0 && spanStartNano > endNano {
			continue
		}

		result := make([]Span, len(spans))
		copy(result, spans)
		traces = append(traces, result)

		if len(traces) >= limit {
			break
		}
	}

	return traces
}

// Stats returns trace store statistics.
func (ts *TraceStore) Stats() map[string]any {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	totalSpans := 0
	for _, spans := range ts.spans {
		totalSpans += len(spans)
	}

	return map[string]any{
		"trace_count":   len(ts.spans),
		"span_count":    totalSpans,
		"service_count": len(ts.byService),
	}
}

func traceStoreAppendUnique(slice []string, val string) []string {
	for _, s := range slice {
		if s == val {
			return slice
		}
	}
	return append(slice, val)
}

// RegisterTraceHTTPHandlers registers trace store HTTP endpoints.
func (ts *TraceStore) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/traces", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			var span Span
			if err := json.NewDecoder(r.Body).Decode(&span); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := ts.WriteSpan(span); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]string{"status": "created"})
		case http.MethodGet:
			traceID := r.URL.Query().Get("trace_id")
			if traceID != "" {
				spans, ok := ts.GetTrace(traceID)
				if !ok {
					http.Error(w, "trace not found", http.StatusNotFound)
					return
				}
				json.NewEncoder(w).Encode(spans)
			} else {
				json.NewEncoder(w).Encode(ts.Stats())
			}
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}
