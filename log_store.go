package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// LogSeverity defines log severity levels.
type LogSeverity string

const (
	LogSeverityTrace LogSeverity = "TRACE"
	LogSeverityDebug LogSeverity = "DEBUG"
	LogSeverityInfo  LogSeverity = "INFO"
	LogSeverityWarn  LogSeverity = "WARN"
	LogSeverityError LogSeverity = "ERROR"
	LogSeverityFatal LogSeverity = "FATAL"
)

// LogRecord represents a structured log entry.
type LogRecord struct {
	Timestamp   int64             `json:"timestamp"` // nanoseconds
	Severity    LogSeverity       `json:"severity"`
	Body        string            `json:"body"`
	ServiceName string            `json:"service_name,omitempty"`
	TraceID     string            `json:"trace_id,omitempty"`
	SpanID      string            `json:"span_id,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// LogStoreConfig configures the log store.
type LogStoreConfig struct {
	Enabled           bool          `json:"enabled"`
	MaxLogsPerQuery   int           `json:"max_logs_per_query"`
	RetentionDuration time.Duration `json:"retention_duration"`
}

// DefaultLogStoreConfig returns sensible defaults.
func DefaultLogStoreConfig() LogStoreConfig {
	return LogStoreConfig{
		Enabled:           true,
		MaxLogsPerQuery:   10000,
		RetentionDuration: 7 * 24 * time.Hour,
	}
}

// LogStore stores structured log records alongside metrics and traces.
type LogStore struct {
	config LogStoreConfig
	logs   []LogRecord

	// Indexes for correlation
	byTraceID  map[string][]int // traceID -> log indices
	byService  map[string][]int // service -> log indices
	byLabel    map[string]map[string][]int // label key -> label value -> indices

	mu sync.RWMutex
}

// NewLogStore creates a new log store.
func NewLogStore(config LogStoreConfig) *LogStore {
	return &LogStore{
		config:    config,
		logs:      make([]LogRecord, 0),
		byTraceID: make(map[string][]int),
		byService: make(map[string][]int),
		byLabel:   make(map[string]map[string][]int),
	}
}

// WriteLog stores a log record.
func (ls *LogStore) WriteLog(record LogRecord) error {
	if record.Body == "" {
		return fmt.Errorf("log body is required")
	}
	if record.Timestamp == 0 {
		record.Timestamp = time.Now().UnixNano()
	}
	if record.Severity == "" {
		record.Severity = LogSeverityInfo
	}

	ls.mu.Lock()
	defer ls.mu.Unlock()

	idx := len(ls.logs)
	ls.logs = append(ls.logs, record)

	// Index by trace ID
	if record.TraceID != "" {
		ls.byTraceID[record.TraceID] = append(ls.byTraceID[record.TraceID], idx)
	}

	// Index by service
	if record.ServiceName != "" {
		ls.byService[record.ServiceName] = append(ls.byService[record.ServiceName], idx)
	}

	// Index by labels
	for k, v := range record.Labels {
		if _, exists := ls.byLabel[k]; !exists {
			ls.byLabel[k] = make(map[string][]int)
		}
		ls.byLabel[k][v] = append(ls.byLabel[k][v], idx)
	}

	return nil
}

// QueryByTraceID returns logs correlated with a trace.
func (ls *LogStore) QueryByTraceID(traceID string) []LogRecord {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	indices := ls.byTraceID[traceID]
	result := make([]LogRecord, 0, len(indices))
	for _, idx := range indices {
		if idx < len(ls.logs) {
			result = append(result, ls.logs[idx])
		}
	}
	return result
}

// QueryByService returns logs for a service within a time range.
func (ls *LogStore) QueryByService(service string, start, end int64, limit int) []LogRecord {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	if limit <= 0 {
		limit = ls.config.MaxLogsPerQuery
	}

	indices := ls.byService[service]
	var result []LogRecord

	for _, idx := range indices {
		if idx >= len(ls.logs) {
			continue
		}
		log := ls.logs[idx]
		if start > 0 && log.Timestamp < start {
			continue
		}
		if end > 0 && log.Timestamp > end {
			continue
		}
		result = append(result, log)
		if len(result) >= limit {
			break
		}
	}
	return result
}

// QueryByLabel returns logs matching a label key-value pair.
func (ls *LogStore) QueryByLabel(key, value string, limit int) []LogRecord {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	if limit <= 0 {
		limit = ls.config.MaxLogsPerQuery
	}

	labelVals, exists := ls.byLabel[key]
	if !exists {
		return nil
	}

	indices := labelVals[value]
	var result []LogRecord
	for _, idx := range indices {
		if idx < len(ls.logs) {
			result = append(result, ls.logs[idx])
		}
		if len(result) >= limit {
			break
		}
	}
	return result
}

// Stats returns log store statistics.
func (ls *LogStore) Stats() map[string]any {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	return map[string]any{
		"total_logs":    len(ls.logs),
		"trace_count":   len(ls.byTraceID),
		"service_count": len(ls.byService),
	}
}

// RegisterHTTPHandlers registers log store HTTP endpoints.
func (ls *LogStore) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/logs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			var record LogRecord
			if err := json.NewDecoder(r.Body).Decode(&record); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := ls.WriteLog(record); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]string{"status": "created"})
		case http.MethodGet:
			traceID := r.URL.Query().Get("trace_id")
			if traceID != "" {
				json.NewEncoder(w).Encode(ls.QueryByTraceID(traceID))
			} else {
				json.NewEncoder(w).Encode(ls.Stats())
			}
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}
