package chronicle

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// CorrelationLink represents a correlation between two signals.
type CorrelationLink struct {
	SourceType  SignalType        `json:"source_type"`
	SourceID    string            `json:"source_id"`
	TargetType  SignalType        `json:"target_type"`
	TargetID    string            `json:"target_id"`
	LinkType    string            `json:"link_type"` // "exemplar", "trace_id", "shared_labels"
	Labels      map[string]string `json:"labels,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}

// CorrelatedResult bundles results from multiple signal types.
type CorrelatedResult struct {
	Metrics []Point     `json:"metrics,omitempty"`
	Traces  []Span      `json:"traces,omitempty"`
	Logs    []LogRecord `json:"logs,omitempty"`
	Links   []CorrelationLink `json:"links,omitempty"`
}

// SignalCorrelationEngine provides cross-signal correlation between
// metrics, traces, and logs using shared labels, trace IDs, and exemplars.
type SignalCorrelationEngine struct {
	traceStore *TraceStore
	logStore   *LogStore
	db         *DB

	// Exemplar index: metric series key -> trace IDs
	exemplars map[string][]string
	// Shared label index: label key:value -> signal references
	labelIndex map[string][]signalRef

	mu sync.RWMutex
}

type signalRef struct {
	signalType SignalType
	id         string
}

// NewSignalCorrelationEngine creates a new cross-signal correlation engine.
func NewSignalCorrelationEngine(db *DB, traceStore *TraceStore, logStore *LogStore) *SignalCorrelationEngine {
	return &SignalCorrelationEngine{
		db:         db,
		traceStore: traceStore,
		logStore:   logStore,
		exemplars:  make(map[string][]string),
		labelIndex: make(map[string][]signalRef),
	}
}

// LinkExemplar creates a correlation between a metric and a trace via an exemplar.
func (e *SignalCorrelationEngine) LinkExemplar(metric string, tags map[string]string, traceID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := makeSeriesKey(metric, tags)
	e.exemplars[key] = appendUnique(e.exemplars[key], traceID)
}

// IndexLabels indexes shared labels from any signal type.
func (e *SignalCorrelationEngine) IndexLabels(signalType SignalType, id string, labels map[string]string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for k, v := range labels {
		indexKey := k + ":" + v
		e.labelIndex[indexKey] = append(e.labelIndex[indexKey], signalRef{
			signalType: signalType,
			id:         id,
		})
	}
}

// CorrelateMetricWithTraces finds traces correlated with a metric via exemplars.
func (e *SignalCorrelationEngine) CorrelateMetricWithTraces(metric string, tags map[string]string) CorrelatedResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := CorrelatedResult{}
	key := makeSeriesKey(metric, tags)

	traceIDs := e.exemplars[key]
	for _, tid := range traceIDs {
		if spans, ok := e.traceStore.GetTrace(tid); ok {
			result.Traces = append(result.Traces, spans...)
			result.Links = append(result.Links, CorrelationLink{
				SourceType: SignalMetric,
				SourceID:   key,
				TargetType: SignalTrace,
				TargetID:   tid,
				LinkType:   "exemplar",
				CreatedAt:  time.Now(),
			})
		}
	}

	return result
}

// CorrelateTraceWithLogs finds logs correlated with a trace via trace_id.
func (e *SignalCorrelationEngine) CorrelateTraceWithLogs(traceID string) CorrelatedResult {
	result := CorrelatedResult{}

	logs := e.logStore.QueryByTraceID(traceID)
	result.Logs = logs

	for range logs {
		result.Links = append(result.Links, CorrelationLink{
			SourceType: SignalTrace,
			SourceID:   traceID,
			TargetType: SignalLog,
			TargetID:   traceID,
			LinkType:   "trace_id",
			CreatedAt:  time.Now(),
		})
	}

	return result
}

// CorrelateByLabel finds all signals sharing a specific label.
func (e *SignalCorrelationEngine) CorrelateByLabel(key, value string) CorrelatedResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := CorrelatedResult{}
	indexKey := key + ":" + value

	refs := e.labelIndex[indexKey]
	for _, ref := range refs {
		switch ref.signalType {
		case SignalTrace:
			if spans, ok := e.traceStore.GetTrace(ref.id); ok {
				result.Traces = append(result.Traces, spans...)
			}
		case SignalLog:
			logs := e.logStore.QueryByTraceID(ref.id)
			result.Logs = append(result.Logs, logs...)
		}
	}

	return result
}

// CorrelateService returns all signals for a given service name.
func (e *SignalCorrelationEngine) CorrelateService(service string) CorrelatedResult {
	result := CorrelatedResult{}

	// Get traces for service
	traces := e.traceStore.QueryTraces(service, 0, 0, 100)
	for _, spans := range traces {
		result.Traces = append(result.Traces, spans...)
	}

	// Get logs for service
	logs := e.logStore.QueryByService(service, 0, 0, 100)
	result.Logs = logs

	// Get metrics with service label
	if e.db != nil {
		qr, err := e.db.Execute(&Query{
			Metric: "",
			Tags:   map[string]string{"service": service},
		})
		if err == nil {
			result.Metrics = qr.Points
		}
	}

	return result
}

// Stats returns correlation engine statistics.
func (e *SignalCorrelationEngine) Stats() map[string]any {
	e.mu.RLock()
	defer e.mu.RUnlock()

	totalExemplars := 0
	for _, traceIDs := range e.exemplars {
		totalExemplars += len(traceIDs)
	}

	return map[string]any{
		"exemplar_links":  totalExemplars,
		"label_index_size": len(e.labelIndex),
		"trace_store":     e.traceStore.Stats(),
		"log_store":       e.logStore.Stats(),
	}
}

// RegisterHTTPHandlers registers correlation HTTP endpoints.
func (e *SignalCorrelationEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/correlate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		traceID := r.URL.Query().Get("trace_id")
		service := r.URL.Query().Get("service")
		labelKey := r.URL.Query().Get("label_key")
		labelValue := r.URL.Query().Get("label_value")

		var result CorrelatedResult

		switch {
		case traceID != "":
			result = e.CorrelateTraceWithLogs(traceID)
			if spans, ok := e.traceStore.GetTrace(traceID); ok {
				result.Traces = spans
			}
		case service != "":
			result = e.CorrelateService(service)
		case labelKey != "" && labelValue != "":
			result = e.CorrelateByLabel(labelKey, labelValue)
		default:
			json.NewEncoder(w).Encode(e.Stats())
			return
		}

		json.NewEncoder(w).Encode(result)
	})
}
