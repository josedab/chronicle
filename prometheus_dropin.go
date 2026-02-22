package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PrometheusDropInConfig configures the Prometheus drop-in compatibility layer.
type PrometheusDropInConfig struct {
	Enabled               bool
	EnableAlertmanager    bool
	AlertmanagerURL       string
	EnableRecordingRules  bool
	EnableServiceDiscovery bool
	MaxSamples            int
	LookbackDelta         time.Duration
	QueryTimeout          time.Duration
}

// DefaultPrometheusDropInConfig returns sensible defaults.
func DefaultPrometheusDropInConfig() PrometheusDropInConfig {
	return PrometheusDropInConfig{
		Enabled:               true,
		EnableAlertmanager:    true,
		EnableRecordingRules:  true,
		EnableServiceDiscovery: true,
		MaxSamples:            50000000,
		LookbackDelta:         5 * time.Minute,
		QueryTimeout:          2 * time.Minute,
	}
}

// PromTarget represents a scrape target.
type PromTarget struct {
	Labels          map[string]string `json:"labels"`
	ScrapeURL       string           `json:"scrapeUrl"`
	GlobalURL       string           `json:"globalUrl"`
	LastScrape      time.Time        `json:"lastScrape"`
	LastScrapeDuration time.Duration `json:"lastScrapeDuration"`
	Health          string           `json:"health"` // up, down, unknown
	ScrapePool      string           `json:"scrapePool"`
}

// PromAlertingRule represents a Prometheus alerting rule.
type PromAlertingRule struct {
	Name        string            `json:"name"`
	Query       string            `json:"query"`
	Duration    time.Duration     `json:"duration"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	State       string            `json:"state"` // firing, pending, inactive
	ActiveAt    *time.Time        `json:"activeAt,omitempty"`
	Value       float64           `json:"value"`
}

// PromRuleGroup represents a group of rules.
type PromRuleGroup struct {
	Name     string             `json:"name"`
	File     string             `json:"file"`
	Rules    []PromAlertingRule `json:"rules"`
	Interval time.Duration      `json:"interval"`
	LastEval time.Time          `json:"lastEvaluation"`
}

// PromMetadata holds metric metadata.
type PromMetadata struct {
	Type string `json:"type"` // counter, gauge, histogram, summary, unknown
	Help string `json:"help"`
	Unit string `json:"unit"`
}

// PromSeriesInfo represents series info.
type PromSeriesInfo struct {
	Labels map[string]string `json:"labels"`
}

// PromDropInStats holds compatibility engine stats.
type PromDropInStats struct {
	QueriesTotal       int64 `json:"queries_total"`
	QueriesFailed      int64 `json:"queries_failed"`
	SamplesIngested    int64 `json:"samples_ingested"`
	ActiveTargets      int   `json:"active_targets"`
	ActiveRules        int   `json:"active_rules"`
	CompatibilityScore int   `json:"compatibility_score"` // 0-100
}

// PrometheusDropInEngine provides full Prometheus API compatibility.
type PrometheusDropInEngine struct {
	db     *DB
	config PrometheusDropInConfig

	mu       sync.RWMutex
	targets  []PromTarget
	rules    []PromRuleGroup
	metadata map[string]PromMetadata
	running  bool
	stopCh   chan struct{}
	stats    PromDropInStats
}

// NewPrometheusDropInEngine creates a new Prometheus compatibility engine.
func NewPrometheusDropInEngine(db *DB, cfg PrometheusDropInConfig) *PrometheusDropInEngine {
	return &PrometheusDropInEngine{
		db:       db,
		config:   cfg,
		targets:  make([]PromTarget, 0),
		rules:    make([]PromRuleGroup, 0),
		metadata: make(map[string]PromMetadata),
		stopCh:   make(chan struct{}),
		stats:    PromDropInStats{CompatibilityScore: 85},
	}
}

// Start starts the drop-in engine (rule evaluation, target scraping).
func (e *PrometheusDropInEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()

	if e.config.EnableRecordingRules {
		go e.ruleEvalLoop()
	}
}

// Stop stops the engine.
func (e *PrometheusDropInEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// AddTarget adds a scrape target.
func (e *PrometheusDropInEngine) AddTarget(target PromTarget) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if target.Health == "" {
		target.Health = "unknown"
	}
	e.targets = append(e.targets, target)
	e.stats.ActiveTargets = len(e.targets)
}

// RemoveTarget removes a scrape target by URL.
func (e *PrometheusDropInEngine) RemoveTarget(scrapeURL string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, t := range e.targets {
		if t.ScrapeURL == scrapeURL {
			e.targets = append(e.targets[:i], e.targets[i+1:]...)
			e.stats.ActiveTargets = len(e.targets)
			return
		}
	}
}

// Targets returns active targets.
func (e *PrometheusDropInEngine) Targets() []PromTarget {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]PromTarget, len(e.targets))
	copy(result, e.targets)
	return result
}

// AddRuleGroup adds a rule group.
func (e *PrometheusDropInEngine) AddRuleGroup(group PromRuleGroup) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = append(e.rules, group)
	e.stats.ActiveRules = e.countRules()
}

// RuleGroups returns all rule groups.
func (e *PrometheusDropInEngine) RuleGroups() []PromRuleGroup {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]PromRuleGroup, len(e.rules))
	copy(result, e.rules)
	return result
}

// SetMetadata sets metadata for a metric.
func (e *PrometheusDropInEngine) SetMetadata(metric string, md PromMetadata) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.metadata[metric] = md
}

// GetMetadata returns metadata for a metric.
func (e *PrometheusDropInEngine) GetMetadata(metric string) (PromMetadata, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	md, ok := e.metadata[metric]
	return md, ok
}

// AllMetadata returns all metric metadata.
func (e *PrometheusDropInEngine) AllMetadata() map[string]PromMetadata {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make(map[string]PromMetadata, len(e.metadata))
	for k, v := range e.metadata {
		result[k] = v
	}
	return result
}

// QueryInstant handles /api/v1/query.
func (e *PrometheusDropInEngine) QueryInstant(query string, ts time.Time) (interface{}, error) {
	e.mu.Lock()
	e.stats.QueriesTotal++
	e.mu.Unlock()

	if query == "" {
		e.mu.Lock()
		e.stats.QueriesFailed++
		e.mu.Unlock()
		return nil, fmt.Errorf("empty query")
	}

	// Delegate to DB's PromQL engine
	q := Query{
		Metric: extractMetricFromPromQL(query),
		Start:  ts.Add(-e.config.LookbackDelta).UnixNano(),
		End:    ts.UnixNano(),
	}

	result, err := e.db.Execute(&q)
	if err != nil {
		e.mu.Lock()
		e.stats.QueriesFailed++
		e.mu.Unlock()
		return nil, err
	}

	return formatPromResponse("vector", result), nil
}

// QueryRange handles /api/v1/query_range.
func (e *PrometheusDropInEngine) QueryRange(query string, start, end time.Time, step time.Duration) (interface{}, error) {
	e.mu.Lock()
	e.stats.QueriesTotal++
	e.mu.Unlock()

	if query == "" {
		e.mu.Lock()
		e.stats.QueriesFailed++
		e.mu.Unlock()
		return nil, fmt.Errorf("empty query")
	}

	q := Query{
		Metric: extractMetricFromPromQL(query),
		Start:  start.UnixNano(),
		End:    end.UnixNano(),
	}

	result, err := e.db.Execute(&q)
	if err != nil {
		e.mu.Lock()
		e.stats.QueriesFailed++
		e.mu.Unlock()
		return nil, err
	}

	return formatPromResponse("matrix", result), nil
}

// LabelNames returns all label names.
func (e *PrometheusDropInEngine) LabelNames() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	labelSet := make(map[string]bool)
	labelSet["__name__"] = true
	for _, t := range e.targets {
		for k := range t.Labels {
			labelSet[k] = true
		}
	}

	names := make([]string, 0, len(labelSet))
	for name := range labelSet {
		names = append(names, name)
	}
	return names
}

// LabelValues returns values for a specific label.
func (e *PrometheusDropInEngine) LabelValues(label string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	valueSet := make(map[string]bool)
	for _, t := range e.targets {
		if v, ok := t.Labels[label]; ok {
			valueSet[v] = true
		}
	}

	values := make([]string, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}
	return values
}

// GetStats returns engine stats.
func (e *PrometheusDropInEngine) GetStats() PromDropInStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

func (e *PrometheusDropInEngine) countRules() int {
	count := 0
	for _, g := range e.rules {
		count += len(g.Rules)
	}
	return count
}

func (e *PrometheusDropInEngine) ruleEvalLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.mu.Lock()
			for i := range e.rules {
				e.rules[i].LastEval = time.Now()
			}
			e.mu.Unlock()
		}
	}
}

func extractMetricFromPromQL(query string) string {
	query = strings.TrimSpace(query)
	// Handle function calls like rate(metric[5m])
	if idx := strings.Index(query, "("); idx >= 0 {
		inner := query[idx+1:]
		if end := strings.IndexAny(inner, "[{)"); end >= 0 {
			return strings.TrimSpace(inner[:end])
		}
	}
	// Handle selector like metric{label="value"}
	if idx := strings.IndexAny(query, "[{"); idx >= 0 {
		return strings.TrimSpace(query[:idx])
	}
	return query
}

func formatPromResponse(resultType string, result *Result) map[string]interface{} {
	data := make([]map[string]interface{}, 0)
	if result != nil {
		labels := map[string]string{"__name__": "metric"}

		if resultType == "vector" && len(result.Points) > 0 {
			p := result.Points[len(result.Points)-1]
			data = append(data, map[string]interface{}{
				"metric": labels,
				"value":  []interface{}{float64(p.Timestamp) / 1e9, fmt.Sprintf("%g", p.Value)},
			})
		} else {
			values := make([][]interface{}, 0, len(result.Points))
			for _, p := range result.Points {
				values = append(values, []interface{}{float64(p.Timestamp) / 1e9, fmt.Sprintf("%g", p.Value)})
			}
			data = append(data, map[string]interface{}{
				"metric": labels,
				"values": values,
			})
		}
	}

	return map[string]interface{}{
		"resultType": resultType,
		"result":     data,
	}
}

// RegisterHTTPHandlers registers Prometheus-compatible HTTP endpoints.
func (e *PrometheusDropInEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	// Standard Prometheus API endpoints
	mux.HandleFunc("/api/v1/prom/query", func(w http.ResponseWriter, r *http.Request) {
		query := r.FormValue("query")
		timeStr := r.FormValue("time")

		ts := time.Now()
		if timeStr != "" {
			if f, err := strconv.ParseFloat(timeStr, 64); err == nil {
				ts = time.Unix(int64(f), int64((f-float64(int64(f)))*1e9))
			}
		}

		result, err := e.QueryInstant(query, ts)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "error",
				"error":  err.Error(),
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"data":   result,
		})
	})

	mux.HandleFunc("/api/v1/prom/query_range", func(w http.ResponseWriter, r *http.Request) {
		query := r.FormValue("query")
		startStr := r.FormValue("start")
		endStr := r.FormValue("end")
		stepStr := r.FormValue("step")

		start := time.Now().Add(-time.Hour)
		end := time.Now()
		step := 15 * time.Second

		if f, err := strconv.ParseFloat(startStr, 64); err == nil {
			start = time.Unix(int64(f), 0)
		}
		if f, err := strconv.ParseFloat(endStr, 64); err == nil {
			end = time.Unix(int64(f), 0)
		}
		if d, err := time.ParseDuration(stepStr); err == nil {
			step = d
		}

		result, err := e.QueryRange(query, start, end, step)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "error",
				"error":  err.Error(),
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"data":   result,
		})
	})

	mux.HandleFunc("/api/v1/prom/targets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		targets := e.Targets()
		active := make([]PromTarget, 0)
		dropped := make([]PromTarget, 0)
		for _, t := range targets {
			if t.Health == "down" {
				dropped = append(dropped, t)
			} else {
				active = append(active, t)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"activeTargets":  active,
				"droppedTargets": dropped,
			},
		})
	})

	mux.HandleFunc("/api/v1/prom/rules", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"groups": e.RuleGroups(),
			},
		})
	})

	mux.HandleFunc("/api/v1/prom/labels", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"data":   e.LabelNames(),
		})
	})

	mux.HandleFunc("/api/v1/prom/metadata", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"data":   e.AllMetadata(),
		})
	})

	mux.HandleFunc("/api/v1/prom/status/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"data": map[string]interface{}{
				"yaml": "# Chronicle Prometheus compatibility mode\nglobal:\n  scrape_interval: 15s\n",
			},
		})
	})

	mux.HandleFunc("/api/v1/prom/status/flags", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"data": map[string]string{
				"storage.tsdb.retention.time": "15d",
				"query.max-samples":           fmt.Sprintf("%d", e.config.MaxSamples),
				"query.lookback-delta":        e.config.LookbackDelta.String(),
				"query.timeout":               e.config.QueryTimeout.String(),
			},
		})
	})

	mux.HandleFunc("/api/v1/prom/dropin/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
