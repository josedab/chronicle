package chronicle

import (
	"encoding/json"
	"fmt"
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
	// Trace ID index: trace_id -> signal refs across all signal types
	traceIndex map[string][]signalRef

	// Cardinality limits
	maxExemplarsPerKey int
	maxLabelRefsPerKey int
	maxTraceIndex      int

	// Performance stats
	lookupCount   int64
	lookupTotalNs int64

	mu sync.RWMutex
}

type signalRef struct {
	signalType SignalType
	id         string
}

// NewSignalCorrelationEngine creates a new cross-signal correlation engine.
func NewSignalCorrelationEngine(db *DB, traceStore *TraceStore, logStore *LogStore) *SignalCorrelationEngine {
	return &SignalCorrelationEngine{
		db:                 db,
		traceStore:         traceStore,
		logStore:           logStore,
		exemplars:          make(map[string][]string),
		labelIndex:         make(map[string][]signalRef),
		traceIndex:         make(map[string][]signalRef),
		maxExemplarsPerKey: 1000,
		maxLabelRefsPerKey: 10000,
		maxTraceIndex:      1000000,
	}
}

// SetCardinalityLimits configures maximum index sizes to prevent unbounded growth.
func (e *SignalCorrelationEngine) SetCardinalityLimits(maxExemplars, maxLabelRefs, maxTraceIndex int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if maxExemplars > 0 {
		e.maxExemplarsPerKey = maxExemplars
	}
	if maxLabelRefs > 0 {
		e.maxLabelRefsPerKey = maxLabelRefs
	}
	if maxTraceIndex > 0 {
		e.maxTraceIndex = maxTraceIndex
	}
}

// LinkExemplar creates a correlation between a metric and a trace via an exemplar.
func (e *SignalCorrelationEngine) LinkExemplar(metric string, tags map[string]string, traceID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := makeSeriesKey(metric, tags)
	existing := e.exemplars[key]
	if len(existing) >= e.maxExemplarsPerKey {
		return // cardinality limit reached
	}
	e.exemplars[key] = appendUnique(existing, traceID)

	// Also index by trace_id for reverse lookup
	if len(e.traceIndex) < e.maxTraceIndex {
		e.traceIndex[traceID] = append(e.traceIndex[traceID], signalRef{
			signalType: SignalMetric,
			id:         key,
		})
	}
}

// IndexLabels indexes shared labels from any signal type.
func (e *SignalCorrelationEngine) IndexLabels(signalType SignalType, id string, labels map[string]string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for k, v := range labels {
		indexKey := k + ":" + v
		existing := e.labelIndex[indexKey]
		if len(existing) >= e.maxLabelRefsPerKey {
			continue // cardinality limit reached
		}
		e.labelIndex[indexKey] = append(existing, signalRef{
			signalType: signalType,
			id:         id,
		})
	}

	// Index trace_id if present
	if traceID, ok := labels["trace_id"]; ok && len(e.traceIndex) < e.maxTraceIndex {
		e.traceIndex[traceID] = append(e.traceIndex[traceID], signalRef{
			signalType: signalType,
			id:         id,
		})
	}
}

// CorrelateByTraceID finds all signals linked to a trace_id across all signal types.
func (e *SignalCorrelationEngine) CorrelateByTraceID(traceID string) CorrelatedResult {
	start := time.Now()
	defer func() {
		e.mu.Lock()
		e.lookupCount++
		e.lookupTotalNs += time.Since(start).Nanoseconds()
		e.mu.Unlock()
	}()

	result := CorrelatedResult{}

	// Get spans from trace store
	if spans, ok := e.traceStore.GetTrace(traceID); ok {
		result.Traces = append(result.Traces, spans...)
	}

	// Get logs for this trace
	logs := e.logStore.QueryByTraceID(traceID)
	result.Logs = logs

	// Get correlated signals from trace index
	e.mu.RLock()
	refs := e.traceIndex[traceID]
	e.mu.RUnlock()

	for _, ref := range refs {
		result.Links = append(result.Links, CorrelationLink{
			SourceType: SignalTrace,
			SourceID:   traceID,
			TargetType: ref.signalType,
			TargetID:   ref.id,
			LinkType:   "trace_id",
			CreatedAt:  time.Now(),
		})
	}

	return result
}

// CorrelateWithTimeRange correlates signals within a specific time window.
func (e *SignalCorrelationEngine) CorrelateWithTimeRange(service string, startTime, endTime int64) CorrelatedResult {
	start := time.Now()
	defer func() {
		e.mu.Lock()
		e.lookupCount++
		e.lookupTotalNs += time.Since(start).Nanoseconds()
		e.mu.Unlock()
	}()

	result := CorrelatedResult{}

	// Get traces for service within time range
	traces := e.traceStore.QueryTraces(service, startTime, endTime, 100)
	for _, spans := range traces {
		result.Traces = append(result.Traces, spans...)
	}

	// Get logs for service within time range
	logs := e.logStore.QueryByService(service, startTime, endTime, 100)
	result.Logs = logs

	// Get metrics within time range
	if e.db != nil {
		qr, err := e.db.Execute(&Query{
			Metric: "",
			Tags:   map[string]string{"service": service},
			Start:  startTime,
			End:    endTime,
		})
		if err == nil {
			result.Metrics = qr.Points
		}
	}

	return result
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

	var avgLookupMs float64
	if e.lookupCount > 0 {
		avgLookupMs = float64(e.lookupTotalNs) / float64(e.lookupCount) / 1e6
	}

	return map[string]any{
		"exemplar_links":      totalExemplars,
		"label_index_size":    len(e.labelIndex),
		"trace_index_size":    len(e.traceIndex),
		"total_lookups":       e.lookupCount,
		"avg_lookup_ms":       avgLookupMs,
		"max_exemplars_per_key": e.maxExemplarsPerKey,
		"max_label_refs_per_key": e.maxLabelRefsPerKey,
		"trace_store":         e.traceStore.Stats(),
		"log_store":           e.logStore.Stats(),
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
		startStr := r.URL.Query().Get("start")
		endStr := r.URL.Query().Get("end")

		var result CorrelatedResult

		switch {
		case traceID != "":
			result = e.CorrelateByTraceID(traceID)
		case service != "" && startStr != "" && endStr != "":
			var startTime, endTime int64
			fmt.Sscanf(startStr, "%d", &startTime)
			fmt.Sscanf(endStr, "%d", &endTime)
			result = e.CorrelateWithTimeRange(service, startTime, endTime)
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
