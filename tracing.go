package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// TracingConfig configures distributed tracing correlation.
type TracingConfig struct {
	// Enabled enables tracing correlation
	Enabled bool `json:"enabled"`

	// JaegerEndpoint for Jaeger trace backend
	JaegerEndpoint string `json:"jaeger_endpoint,omitempty"`

	// ZipkinEndpoint for Zipkin trace backend
	ZipkinEndpoint string `json:"zipkin_endpoint,omitempty"`

	// OTLPEndpoint for OpenTelemetry trace backend
	OTLPEndpoint string `json:"otlp_endpoint,omitempty"`

	// TraceIDTagKey is the tag key used for trace IDs
	TraceIDTagKey string `json:"trace_id_tag_key"`

	// SpanIDTagKey is the tag key used for span IDs
	SpanIDTagKey string `json:"span_id_tag_key"`

	// ServiceNameTagKey is the tag key for service name
	ServiceNameTagKey string `json:"service_name_tag_key"`

	// MaxTracesPerQuery limits traces returned per query
	MaxTracesPerQuery int `json:"max_traces_per_query"`

	// TraceRetention how long to keep trace references
	TraceRetention time.Duration `json:"trace_retention"`

	// CacheSize for trace metadata cache
	CacheSize int `json:"cache_size"`

	// HTTPClient for backend queries
	HTTPClient HTTPDoer `json:"-"`
}

// DefaultTracingConfig returns default tracing configuration.
func DefaultTracingConfig() TracingConfig {
	return TracingConfig{
		Enabled:           false,
		TraceIDTagKey:     "trace_id",
		SpanIDTagKey:      "span_id",
		ServiceNameTagKey: "service",
		MaxTracesPerQuery: 100,
		TraceRetention:    7 * 24 * time.Hour,
		CacheSize:         10000,
	}
}

// TraceCorrelator provides correlation between metrics and distributed traces.
type TraceCorrelator struct {
	db     *DB
	config TracingConfig
	client HTTPDoer
	mu     sync.RWMutex

	// Trace reference index
	traceIndex   map[string]*TraceReference // traceID -> reference
	metricTraces map[string][]string        // metric -> traceIDs

	// Cache
	traceCache *traceCache

	// Background tasks
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// TraceReference links a metric to a trace.
type TraceReference struct {
	TraceID     string            `json:"trace_id"`
	SpanID      string            `json:"span_id,omitempty"`
	ServiceName string            `json:"service_name,omitempty"`
	Metric      string            `json:"metric"`
	Tags        map[string]string `json:"tags,omitempty"`
	Value       float64           `json:"value"`
	Timestamp   int64             `json:"timestamp"`

	// Correlation metadata
	Duration   time.Duration `json:"duration,omitempty"`
	StatusCode string        `json:"status_code,omitempty"`
	ErrorType  string        `json:"error_type,omitempty"`
}

// TraceSpan represents a span from the trace backend.
type TraceSpan struct {
	TraceID       string            `json:"traceID"`
	SpanID        string            `json:"spanID"`
	ParentSpanID  string            `json:"parentSpanID,omitempty"`
	OperationName string            `json:"operationName"`
	ServiceName   string            `json:"serviceName"`
	StartTime     int64             `json:"startTime"`
	Duration      int64             `json:"duration"`
	Tags          map[string]string `json:"tags,omitempty"`
	Logs          []SpanLog         `json:"logs,omitempty"`
	Status        SpanStatus        `json:"status"`
}

// SpanLog represents a log entry within a span.
type SpanLog struct {
	Timestamp int64             `json:"timestamp"`
	Fields    map[string]string `json:"fields"`
}

// SpanStatus represents the status of a span.
type SpanStatus struct {
	Code    string `json:"code"` // OK, ERROR, UNSET
	Message string `json:"message,omitempty"`
}

// Trace represents a complete distributed trace.
type Trace struct {
	TraceID     string      `json:"traceID"`
	Spans       []TraceSpan `json:"spans"`
	ServiceName string      `json:"serviceName"`
	StartTime   int64       `json:"startTime"`
	Duration    int64       `json:"duration"`
	SpanCount   int         `json:"spanCount"`
}

// MetricTraceCorrelation represents a correlated metric and trace.
type MetricTraceCorrelation struct {
	// Metric information
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Value     float64           `json:"value"`
	Timestamp int64             `json:"timestamp"`

	// Trace information
	TraceID       string `json:"trace_id"`
	TraceURL      string `json:"trace_url,omitempty"`
	ServiceName   string `json:"service_name,omitempty"`
	OperationName string `json:"operation_name,omitempty"`
	TraceDuration int64  `json:"trace_duration_us,omitempty"`
	TraceStatus   string `json:"trace_status,omitempty"`

	// Analysis
	CorrelationScore float64 `json:"correlation_score"`
	SuggestedAction  string  `json:"suggested_action,omitempty"`
}

// traceCache caches trace metadata.
type traceCache struct {
	mu      sync.RWMutex
	traces  map[string]*Trace
	order   []string
	maxSize int
}

func newTraceCache(maxSize int) *traceCache {
	return &traceCache{
		traces:  make(map[string]*Trace),
		order:   make([]string, 0, maxSize),
		maxSize: maxSize,
	}
}

func (c *traceCache) get(traceID string) *Trace {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.traces[traceID]
}

func (c *traceCache) put(traceID string, trace *Trace) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.traces[traceID]; exists {
		return
	}

	if len(c.order) >= c.maxSize {
		// Evict oldest
		oldest := c.order[0]
		delete(c.traces, oldest)
		c.order = c.order[1:]
	}

	c.traces[traceID] = trace
	c.order = append(c.order, traceID)
}

// NewTraceCorrelator creates a new trace correlator.
func NewTraceCorrelator(db *DB, config TracingConfig) *TraceCorrelator {
	ctx, cancel := context.WithCancel(context.Background())

	tc := &TraceCorrelator{
		db:           db,
		config:       config,
		traceIndex:   make(map[string]*TraceReference),
		metricTraces: make(map[string][]string),
		traceCache:   newTraceCache(config.CacheSize),
		ctx:          ctx,
		cancel:       cancel,
	}

	if config.HTTPClient != nil {
		tc.client = config.HTTPClient
	} else {
		tc.client = &http.Client{Timeout: 30 * time.Second}
	}

	return tc
}

// Start starts background tasks.
func (tc *TraceCorrelator) Start() {
	tc.wg.Add(1)
	go tc.cleanupLoop()
}

// Stop stops the correlator.
func (tc *TraceCorrelator) Stop() {
	tc.cancel()
	tc.wg.Wait()
}

func (tc *TraceCorrelator) cleanupLoop() {
	defer tc.wg.Done()

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-tc.ctx.Done():
			return
		case <-ticker.C:
			tc.cleanup()
		}
	}
}

func (tc *TraceCorrelator) cleanup() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	cutoff := time.Now().Add(-tc.config.TraceRetention).UnixNano()

	// Clean old trace references
	for traceID, ref := range tc.traceIndex {
		if ref.Timestamp < cutoff {
			delete(tc.traceIndex, traceID)
		}
	}

	// Clean metric index
	for metric, traces := range tc.metricTraces {
		var valid []string
		for _, traceID := range traces {
			if _, ok := tc.traceIndex[traceID]; ok {
				valid = append(valid, traceID)
			}
		}
		if len(valid) > 0 {
			tc.metricTraces[metric] = valid
		} else {
			delete(tc.metricTraces, metric)
		}
	}
}

// RecordTraceReference records a link between a metric and a trace.
func (tc *TraceCorrelator) RecordTraceReference(ref *TraceReference) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.traceIndex[ref.TraceID] = ref

	// Update metric index
	traces := tc.metricTraces[ref.Metric]
	tc.metricTraces[ref.Metric] = append(traces, ref.TraceID)

	// Limit traces per metric
	if len(tc.metricTraces[ref.Metric]) > tc.config.MaxTracesPerQuery*10 {
		tc.metricTraces[ref.Metric] = tc.metricTraces[ref.Metric][len(tc.metricTraces[ref.Metric])-tc.config.MaxTracesPerQuery:]
	}
}

// RecordFromExemplar creates a trace reference from an exemplar.
func (tc *TraceCorrelator) RecordFromExemplar(metric string, tags map[string]string, value float64, timestamp int64, exemplar *Exemplar) {
	if exemplar == nil {
		return
	}

	traceID := exemplar.Labels[tc.config.TraceIDTagKey]
	if traceID == "" {
		return
	}

	ref := &TraceReference{
		TraceID:     traceID,
		SpanID:      exemplar.Labels[tc.config.SpanIDTagKey],
		ServiceName: exemplar.Labels[tc.config.ServiceNameTagKey],
		Metric:      metric,
		Tags:        tags,
		Value:       value,
		Timestamp:   timestamp,
	}

	tc.RecordTraceReference(ref)
}

// GetTracesForMetric returns traces associated with a metric.
func (tc *TraceCorrelator) GetTracesForMetric(metric string, start, end int64) []*TraceReference {
	tc.mu.RLock()
	traceIDs := tc.metricTraces[metric]
	tc.mu.RUnlock()

	var refs []*TraceReference
	tc.mu.RLock()
	for _, traceID := range traceIDs {
		if ref, ok := tc.traceIndex[traceID]; ok {
			if ref.Timestamp >= start && ref.Timestamp <= end {
				refs = append(refs, ref)
			}
		}
	}
	tc.mu.RUnlock()

	// Sort by timestamp descending
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].Timestamp > refs[j].Timestamp
	})

	// Limit results
	if len(refs) > tc.config.MaxTracesPerQuery {
		refs = refs[:tc.config.MaxTracesPerQuery]
	}

	return refs
}

// GetTracesForAnomaly finds traces correlated with an anomaly.
func (tc *TraceCorrelator) GetTracesForAnomaly(anomaly *ClassifiedAnomaly) ([]*MetricTraceCorrelation, error) {
	if anomaly == nil {
		return nil, errors.New("anomaly is nil")
	}

	// Get traces around the anomaly timestamp
	windowSize := int64(5 * time.Minute)
	start := anomaly.Timestamp - windowSize
	end := anomaly.Timestamp + windowSize

	refs := tc.GetTracesForMetric(anomaly.Metric, start, end)
	if len(refs) == 0 {
		return nil, nil
	}

	var correlations []*MetricTraceCorrelation
	for _, ref := range refs {
		correlation := &MetricTraceCorrelation{
			Metric:      ref.Metric,
			Tags:        ref.Tags,
			Value:       ref.Value,
			Timestamp:   ref.Timestamp,
			TraceID:     ref.TraceID,
			ServiceName: ref.ServiceName,
		}

		// Calculate correlation score based on temporal proximity
		timeDiff := abs64(ref.Timestamp - anomaly.Timestamp)
		maxDiff := float64(windowSize)
		correlation.CorrelationScore = 1.0 - float64(timeDiff)/maxDiff

		// Boost score for high anomaly scores
		correlation.CorrelationScore *= (1.0 + anomaly.Score)

		// Generate trace URL
		correlation.TraceURL = tc.getTraceURL(ref.TraceID)

		// Suggest action based on anomaly type
		correlation.SuggestedAction = tc.suggestAction(anomaly, ref)

		correlations = append(correlations, correlation)
	}

	// Sort by correlation score
	sort.Slice(correlations, func(i, j int) bool {
		return correlations[i].CorrelationScore > correlations[j].CorrelationScore
	})

	return correlations, nil
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func (tc *TraceCorrelator) suggestAction(anomaly *ClassifiedAnomaly, ref *TraceReference) string {
	switch anomaly.Type {
	case AnomalyTypeSpike:
		if ref.StatusCode != "" && strings.HasPrefix(ref.StatusCode, "5") {
			return "Investigate server errors in trace"
		}
		return "Check trace for high latency operations"
	case AnomalyTypeDip:
		return "Verify service health from trace spans"
	case AnomalyTypeOutlier:
		return "Review trace for unusual operations"
	default:
		return "Inspect trace for root cause"
	}
}

func (tc *TraceCorrelator) getTraceURL(traceID string) string {
	if tc.config.JaegerEndpoint != "" {
		return fmt.Sprintf("%s/trace/%s", tc.config.JaegerEndpoint, traceID)
	}
	if tc.config.ZipkinEndpoint != "" {
		return fmt.Sprintf("%s/traces/%s", tc.config.ZipkinEndpoint, traceID)
	}
	return ""
}

// FetchTrace fetches a trace from the backend.
func (tc *TraceCorrelator) FetchTrace(traceID string) (*Trace, error) {
	// Check cache
	if cached := tc.traceCache.get(traceID); cached != nil {
		return cached, nil
	}

	// Fetch from backend
	trace, err := tc.fetchFromBackend(traceID)
	if err != nil {
		return nil, err
	}

	// Cache result
	if trace != nil {
		tc.traceCache.put(traceID, trace)
	}

	return trace, nil
}

func (tc *TraceCorrelator) fetchFromBackend(traceID string) (*Trace, error) {
	var url string
	var parser func([]byte) (*Trace, error)

	if tc.config.JaegerEndpoint != "" {
		url = fmt.Sprintf("%s/api/traces/%s", tc.config.JaegerEndpoint, traceID)
		parser = parseJaegerTrace
	} else if tc.config.ZipkinEndpoint != "" {
		url = fmt.Sprintf("%s/api/v2/trace/%s", tc.config.ZipkinEndpoint, traceID)
		parser = parseZipkinTrace
	} else {
		return nil, errors.New("no trace backend configured")
	}

	ctx, cancel := context.WithTimeout(tc.ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := tc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("trace backend returned status %d", resp.StatusCode)
	}

	var body []byte
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		body = append(body, buf[:n]...)
		if err != nil {
			break
		}
	}

	return parser(body)
}

func parseJaegerTrace(data []byte) (*Trace, error) {
	var response struct {
		Data []struct {
			TraceID string `json:"traceID"`
			Spans   []struct {
				TraceID       string `json:"traceID"`
				SpanID        string `json:"spanID"`
				OperationName string `json:"operationName"`
				StartTime     int64  `json:"startTime"`
				Duration      int64  `json:"duration"`
				ProcessID     string `json:"processID"`
				Tags          []struct {
					Key   string `json:"key"`
					Value any    `json:"value"`
				} `json:"tags"`
			} `json:"spans"`
			Processes map[string]struct {
				ServiceName string `json:"serviceName"`
			} `json:"processes"`
		} `json:"data"`
	}

	if err := json.Unmarshal(data, &response); err != nil {
		return nil, err
	}

	if len(response.Data) == 0 {
		return nil, errors.New("trace not found")
	}

	traceData := response.Data[0]
	trace := &Trace{
		TraceID:   traceData.TraceID,
		SpanCount: len(traceData.Spans),
	}

	for _, span := range traceData.Spans {
		tags := make(map[string]string)
		for _, tag := range span.Tags {
			tags[tag.Key] = fmt.Sprintf("%v", tag.Value)
		}

		serviceName := ""
		if proc, ok := traceData.Processes[span.ProcessID]; ok {
			serviceName = proc.ServiceName
		}

		ts := TraceSpan{
			TraceID:       span.TraceID,
			SpanID:        span.SpanID,
			OperationName: span.OperationName,
			ServiceName:   serviceName,
			StartTime:     span.StartTime,
			Duration:      span.Duration,
			Tags:          tags,
		}
		trace.Spans = append(trace.Spans, ts)

		// Track min start time and max duration
		if trace.StartTime == 0 || span.StartTime < trace.StartTime {
			trace.StartTime = span.StartTime
			trace.ServiceName = serviceName
		}
		if span.StartTime+span.Duration > trace.StartTime+trace.Duration {
			trace.Duration = span.StartTime + span.Duration - trace.StartTime
		}
	}

	return trace, nil
}

func parseZipkinTrace(data []byte) (*Trace, error) {
	var spans []struct {
		TraceID       string `json:"traceId"`
		ID            string `json:"id"`
		ParentID      string `json:"parentId,omitempty"`
		Name          string `json:"name"`
		Timestamp     int64  `json:"timestamp"`
		Duration      int64  `json:"duration"`
		LocalEndpoint struct {
			ServiceName string `json:"serviceName"`
		} `json:"localEndpoint"`
		Tags map[string]string `json:"tags,omitempty"`
	}

	if err := json.Unmarshal(data, &spans); err != nil {
		return nil, err
	}

	if len(spans) == 0 {
		return nil, errors.New("trace not found")
	}

	trace := &Trace{
		TraceID:   spans[0].TraceID,
		SpanCount: len(spans),
	}

	for _, span := range spans {
		ts := TraceSpan{
			TraceID:       span.TraceID,
			SpanID:        span.ID,
			ParentSpanID:  span.ParentID,
			OperationName: span.Name,
			ServiceName:   span.LocalEndpoint.ServiceName,
			StartTime:     span.Timestamp,
			Duration:      span.Duration,
			Tags:          span.Tags,
		}
		trace.Spans = append(trace.Spans, ts)

		if trace.StartTime == 0 || span.Timestamp < trace.StartTime {
			trace.StartTime = span.Timestamp
			trace.ServiceName = span.LocalEndpoint.ServiceName
		}
		if span.Timestamp+span.Duration > trace.StartTime+trace.Duration {
			trace.Duration = span.Timestamp + span.Duration - trace.StartTime
		}
	}

	return trace, nil
}

// FindRootCause attempts to find the root cause span for an anomaly.
func (tc *TraceCorrelator) FindRootCause(traceID string) (*TraceSpan, error) {
	trace, err := tc.FetchTrace(traceID)
	if err != nil {
		return nil, err
	}

	if len(trace.Spans) == 0 {
		return nil, errors.New("no spans in trace")
	}

	// Look for error spans first
	for _, span := range trace.Spans {
		if span.Status.Code == "ERROR" {
			return &span, nil
		}
		if span.Tags["error"] == "true" {
			return &span, nil
		}
	}

	// Look for slowest span
	var slowest *TraceSpan
	maxDuration := int64(0)
	for i := range trace.Spans {
		if trace.Spans[i].Duration > maxDuration {
			maxDuration = trace.Spans[i].Duration
			slowest = &trace.Spans[i]
		}
	}

	return slowest, nil
}

// GetTraceStats returns statistics about traced metrics.
func (tc *TraceCorrelator) GetTraceStats() TraceStats {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	stats := TraceStats{
		TotalTraces:     len(tc.traceIndex),
		TracedMetrics:   len(tc.metricTraces),
		TracesByService: make(map[string]int),
		TracesByMetric:  make(map[string]int),
	}

	for _, ref := range tc.traceIndex {
		stats.TracesByService[ref.ServiceName]++
	}

	for metric, traces := range tc.metricTraces {
		stats.TracesByMetric[metric] = len(traces)
	}

	return stats
}

// TraceStats contains tracing statistics.
type TraceStats struct {
	TotalTraces     int            `json:"total_traces"`
	TracedMetrics   int            `json:"traced_metrics"`
	TracesByService map[string]int `json:"traces_by_service"`
	TracesByMetric  map[string]int `json:"traces_by_metric"`
}

// QueryTracesWithMetric queries metrics and returns correlated traces.
func (tc *TraceCorrelator) QueryTracesWithMetric(query *Query) (*TracedQueryResult, error) {
	if tc.db == nil {
		return nil, errors.New("database not configured")
	}

	// Execute metric query
	result, err := tc.db.Execute(query)
	if err != nil {
		return nil, err
	}

	tracedResult := &TracedQueryResult{
		Points:       result.Points,
		Correlations: make([]*MetricTraceCorrelation, 0),
	}

	// Find traces for each point
	for _, point := range result.Points {
		refs := tc.GetTracesForMetric(point.Metric, point.Timestamp-int64(time.Second), point.Timestamp+int64(time.Second))
		for _, ref := range refs {
			correlation := &MetricTraceCorrelation{
				Metric:      point.Metric,
				Tags:        point.Tags,
				Value:       point.Value,
				Timestamp:   point.Timestamp,
				TraceID:     ref.TraceID,
				ServiceName: ref.ServiceName,
				TraceURL:    tc.getTraceURL(ref.TraceID),
			}
			tracedResult.Correlations = append(tracedResult.Correlations, correlation)
		}
	}

	return tracedResult, nil
}

// TracedQueryResult contains query results with trace correlations.
type TracedQueryResult struct {
	Points       []Point                   `json:"points"`
	Correlations []*MetricTraceCorrelation `json:"correlations"`
}
