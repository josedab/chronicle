package chronicle

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// OTelCollectorConfig configures the OTel Collector integration.
type OTelCollectorConfig struct {
	// Enabled enables the collector plugin
	Enabled bool `json:"enabled"`

	// Endpoint is the Chronicle HTTP endpoint (e.g., "http://localhost:8086")
	Endpoint string `json:"endpoint"`

	// APIKey for authentication (optional)
	APIKey string `json:"api_key,omitempty"`

	// BatchSize is the number of points to batch before sending
	BatchSize int `json:"batch_size"`

	// FlushInterval is how often to flush batched points
	FlushInterval time.Duration `json:"flush_interval"`

	// Timeout for HTTP requests
	Timeout time.Duration `json:"timeout"`

	// RetryEnabled enables automatic retry on failure
	RetryEnabled bool `json:"retry_enabled"`

	// MaxRetries is the maximum number of retry attempts
	MaxRetries int `json:"max_retries"`

	// RetryBackoff is the initial backoff duration for retries
	RetryBackoff time.Duration `json:"retry_backoff"`

	// Headers are additional HTTP headers to send
	Headers map[string]string `json:"headers,omitempty"`

	// ResourceToTags maps OTel resource attributes to Chronicle tags
	ResourceToTags bool `json:"resource_to_tags"`

	// ScopeToTags includes instrumentation scope as tags
	ScopeToTags bool `json:"scope_to_tags"`

	// MetricPrefix is prepended to all metric names
	MetricPrefix string `json:"metric_prefix"`
}

// DefaultOTelCollectorConfig returns sensible defaults.
func DefaultOTelCollectorConfig() OTelCollectorConfig {
	return OTelCollectorConfig{
		Enabled:        true,
		Endpoint:       "http://localhost:8086",
		BatchSize:      1000,
		FlushInterval:  10 * time.Second,
		Timeout:        30 * time.Second,
		RetryEnabled:   true,
		MaxRetries:     3,
		RetryBackoff:   time.Second,
		ResourceToTags: true,
		ScopeToTags:    true,
	}
}

// OTelCollectorExporter exports metrics from OTel Collector to Chronicle.
// This implements the OpenTelemetry Collector exporter interface pattern.
type OTelCollectorExporter struct {
	config     OTelCollectorConfig
	client     *http.Client
	batch      []Point
	batchMu    sync.Mutex
	flushTimer *time.Timer
	stopCh     chan struct{}
	wg         sync.WaitGroup
	stats      ExporterStats
	statsMu    sync.RWMutex
}

// ExporterStats tracks exporter performance metrics.
type ExporterStats struct {
	PointsExported   int64         `json:"points_exported"`
	PointsDropped    int64         `json:"points_dropped"`
	ExportSuccesses  int64         `json:"export_successes"`
	ExportFailures   int64         `json:"export_failures"`
	RetryAttempts    int64         `json:"retry_attempts"`
	LastExportTime   time.Time     `json:"last_export_time"`
	LastError        string        `json:"last_error,omitempty"`
	AverageLatencyMs float64       `json:"average_latency_ms"`
	totalLatency     time.Duration // internal tracking
	latencyCount     int64         // internal tracking
}

// NewOTelCollectorExporter creates a new exporter for OTel Collector.
func NewOTelCollectorExporter(config OTelCollectorConfig) (*OTelCollectorExporter, error) {
	if config.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}

	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 10 * time.Second
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.RetryBackoff <= 0 {
		config.RetryBackoff = time.Second
	}

	e := &OTelCollectorExporter{
		config: config,
		client: &http.Client{
			Timeout: config.Timeout,
		},
		batch:  make([]Point, 0, config.BatchSize),
		stopCh: make(chan struct{}),
	}

	// Start flush timer
	e.flushTimer = time.AfterFunc(config.FlushInterval, e.timerFlush)

	return e, nil
}

// Start begins the exporter background processing.
func (e *OTelCollectorExporter) Start(ctx context.Context) error {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		<-ctx.Done()
		e.Stop()
	}()
	return nil
}

// Stop gracefully shuts down the exporter.
func (e *OTelCollectorExporter) Stop() error {
	close(e.stopCh)
	e.flushTimer.Stop()
	// Final flush
	e.flush(context.Background())
	return nil
}

// ExportMetrics exports a batch of OTel metrics to Chronicle.
// This is the main entry point called by OTel Collector.
func (e *OTelCollectorExporter) ExportMetrics(ctx context.Context, metrics *OTLPExportRequest) error {
	points := e.convertMetrics(metrics)
	if len(points) == 0 {
		return nil
	}

	e.batchMu.Lock()
	e.batch = append(e.batch, points...)
	shouldFlush := len(e.batch) >= e.config.BatchSize
	e.batchMu.Unlock()

	if shouldFlush {
		return e.flush(ctx)
	}
	return nil
}

// convertMetrics converts OTel metrics to Chronicle points.
func (e *OTelCollectorExporter) convertMetrics(metrics *OTLPExportRequest) []Point {
	var points []Point

	for _, rm := range metrics.ResourceMetrics {
		// Extract resource attributes as tags
		resourceTags := make(map[string]string)
		if e.config.ResourceToTags {
			for _, attr := range rm.Resource.Attributes {
				resourceTags[attr.Key] = otelAttrValueToString(attr.Value)
			}
		}

		for _, sm := range rm.ScopeMetrics {
			// Include scope as tags if configured
			scopeTags := make(map[string]string)
			if e.config.ScopeToTags && sm.Scope.Name != "" {
				scopeTags["otel.scope.name"] = sm.Scope.Name
				if sm.Scope.Version != "" {
					scopeTags["otel.scope.version"] = sm.Scope.Version
				}
			}

			for _, metric := range sm.Metrics {
				metricName := metric.Name
				if e.config.MetricPrefix != "" {
					metricName = e.config.MetricPrefix + "." + metricName
				}

				// Convert based on metric type
				pts := e.convertMetricDataPoints(metricName, metric, resourceTags, scopeTags)
				points = append(points, pts...)
			}
		}
	}

	return points
}

// convertMetricDataPoints converts a single OTel metric to Chronicle points.
func (e *OTelCollectorExporter) convertMetricDataPoints(
	name string,
	metric OTLPMetric,
	resourceTags, scopeTags map[string]string,
) []Point {
	var points []Point

	// Handle gauge metrics
	if metric.Gauge != nil {
		for _, dp := range metric.Gauge.DataPoints {
			tags := e.mergeTags(resourceTags, scopeTags, dp.Attributes)
			points = append(points, Point{
				Metric:    name,
				Value:     otlpNumberValue(dp),
				Timestamp: otlpTimestampStr(dp.TimeUnixNano),
				Tags:      tags,
			})
		}
	}

	// Handle sum metrics
	if metric.Sum != nil {
		for _, dp := range metric.Sum.DataPoints {
			tags := e.mergeTags(resourceTags, scopeTags, dp.Attributes)
			points = append(points, Point{
				Metric:    name,
				Value:     otlpNumberValue(dp),
				Timestamp: otlpTimestampStr(dp.TimeUnixNano),
				Tags:      tags,
			})
		}
	}

	// Handle histogram metrics
	if metric.Histogram != nil {
		for _, dp := range metric.Histogram.DataPoints {
			tags := e.mergeTags(resourceTags, scopeTags, dp.Attributes)
			ts := otlpTimestampStr(dp.TimeUnixNano)

			// Export histogram as multiple points
			points = append(points, Point{
				Metric:    name + ".count",
				Value:     float64(dp.Count),
				Timestamp: ts,
				Tags:      tags,
			})
			if dp.Sum != nil {
				points = append(points, Point{
					Metric:    name + ".sum",
					Value:     *dp.Sum,
					Timestamp: ts,
					Tags:      tags,
				})
			}

			// Export bucket counts
			for i, bc := range dp.BucketCounts {
				if i < len(dp.ExplicitBounds) {
					bucketTags := otelCopyTags(tags)
					bucketTags["le"] = fmt.Sprintf("%g", dp.ExplicitBounds[i])
					points = append(points, Point{
						Metric:    name + ".bucket",
						Value:     float64(bc),
						Timestamp: ts,
						Tags:      bucketTags,
					})
				}
			}
		}
	}

	// Handle summary metrics
	if metric.Summary != nil {
		for _, dp := range metric.Summary.DataPoints {
			tags := e.mergeTags(resourceTags, scopeTags, dp.Attributes)
			ts := otlpTimestampStr(dp.TimeUnixNano)

			points = append(points, Point{
				Metric:    name + ".count",
				Value:     float64(dp.Count),
				Timestamp: ts,
				Tags:      tags,
			})
			points = append(points, Point{
				Metric:    name + ".sum",
				Value:     dp.Sum,
				Timestamp: ts,
				Tags:      tags,
			})

			// Export quantiles
			for _, q := range dp.QuantileValues {
				quantileTags := otelCopyTags(tags)
				quantileTags["quantile"] = fmt.Sprintf("%g", q.Quantile)
				points = append(points, Point{
					Metric:    name + ".quantile",
					Value:     q.Value,
					Timestamp: ts,
					Tags:      quantileTags,
				})
			}
		}
	}

	return points
}

func (e *OTelCollectorExporter) mergeTags(maps ...any) map[string]string {
	result := make(map[string]string)
	for _, m := range maps {
		switch v := m.(type) {
		case map[string]string:
			for k, val := range v {
				result[k] = val
			}
		case []OTLPKeyValue:
			for _, attr := range v {
				result[attr.Key] = otelAttrValueToString(attr.Value)
			}
		}
	}
	return result
}

func otelCopyTags(tags map[string]string) map[string]string {
	result := make(map[string]string, len(tags))
	for k, v := range tags {
		result[k] = v
	}
	return result
}

func otlpTimestampStr(nanos string) int64 {
	if nanos == "" {
		return time.Now().UnixNano()
	}
	n, err := strconv.ParseInt(nanos, 10, 64)
	if err != nil {
		return time.Now().UnixNano()
	}
	return n
}

func otlpNumberValue(dp OTLPNumberDataPoint) float64 {
	if dp.AsInt != nil {
		return float64(*dp.AsInt)
	}
	if dp.AsDouble != nil {
		return *dp.AsDouble
	}
	return 0
}

func otelAttrValueToString(v OTLPAnyValue) string {
	if v.StringValue != nil {
		return *v.StringValue
	}
	if v.IntValue != nil {
		return fmt.Sprintf("%d", *v.IntValue)
	}
	if v.DoubleValue != nil {
		return fmt.Sprintf("%g", *v.DoubleValue)
	}
	if v.BoolValue != nil {
		return fmt.Sprintf("%t", *v.BoolValue)
	}
	return ""
}

func (e *OTelCollectorExporter) timerFlush() {
	select {
	case <-e.stopCh:
		return
	default:
	}
	e.flush(context.Background())
	e.flushTimer.Reset(e.config.FlushInterval)
}

func (e *OTelCollectorExporter) flush(ctx context.Context) error {
	e.batchMu.Lock()
	if len(e.batch) == 0 {
		e.batchMu.Unlock()
		return nil
	}
	points := e.batch
	e.batch = make([]Point, 0, e.config.BatchSize)
	e.batchMu.Unlock()

	return e.sendPoints(ctx, points)
}

func (e *OTelCollectorExporter) sendPoints(ctx context.Context, points []Point) error {
	start := time.Now()
	var lastErr error

	for attempt := 0; attempt <= e.config.MaxRetries; attempt++ {
		if attempt > 0 {
			e.statsMu.Lock()
			e.stats.RetryAttempts++
			e.statsMu.Unlock()

			// Exponential backoff
			backoff := e.config.RetryBackoff * time.Duration(1<<uint(attempt-1))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		err := e.doSend(ctx, points)
		if err == nil {
			e.statsMu.Lock()
			e.stats.PointsExported += int64(len(points))
			e.stats.ExportSuccesses++
			e.stats.LastExportTime = time.Now()
			e.stats.totalLatency += time.Since(start)
			e.stats.latencyCount++
			e.stats.AverageLatencyMs = float64(e.stats.totalLatency.Milliseconds()) / float64(e.stats.latencyCount)
			e.statsMu.Unlock()
			return nil
		}

		lastErr = err
		if !e.config.RetryEnabled {
			break
		}
	}

	e.statsMu.Lock()
	e.stats.PointsDropped += int64(len(points))
	e.stats.ExportFailures++
	e.stats.LastError = lastErr.Error()
	e.statsMu.Unlock()

	return lastErr
}

func (e *OTelCollectorExporter) doSend(ctx context.Context, points []Point) error {
	// Convert to Line Protocol for Chronicle ingestion
	var buf bytes.Buffer
	for _, p := range points {
		buf.WriteString(p.Metric)

		// Write tags
		for k, v := range p.Tags {
			buf.WriteString(",")
			buf.WriteString(k)
			buf.WriteString("=")
			buf.WriteString(v)
		}

		// Write value and timestamp
		buf.WriteString(" value=")
		buf.WriteString(fmt.Sprintf("%g", p.Value))
		buf.WriteString(" ")
		buf.WriteString(fmt.Sprintf("%d", p.Timestamp))
		buf.WriteString("\n")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", e.config.Endpoint+"/write", &buf)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "text/plain")
	if e.config.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+e.config.APIKey)
	}
	for k, v := range e.config.Headers {
		req.Header.Set(k, v)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("server error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Stats returns current exporter statistics.
func (e *OTelCollectorExporter) Stats() ExporterStats {
	e.statsMu.RLock()
	defer e.statsMu.RUnlock()
	return e.stats
}

// OTelCollectorReceiver implements a receiver that accepts data from OTel Collector.
// This can be embedded directly in Chronicle to receive data.
type OTelCollectorReceiver struct {
	db      *DB
	config  OTelCollectorReceiverConfig
	server  *http.Server
	stats   ReceiverStats
	statsMu sync.RWMutex
}

// OTelCollectorReceiverConfig configures the receiver.
type OTelCollectorReceiverConfig struct {
	Enabled         bool   `json:"enabled"`
	ListenAddr      string `json:"listen_addr"`
	TLSEnabled      bool   `json:"tls_enabled"`
	TLSCertFile     string `json:"tls_cert_file,omitempty"`
	TLSKeyFile      string `json:"tls_key_file,omitempty"`
	MaxBatchSize    int    `json:"max_batch_size"`
	CompressionType string `json:"compression_type"` // "none", "gzip", "zstd"
}

// DefaultOTelCollectorReceiverConfig returns default receiver config.
func DefaultOTelCollectorReceiverConfig() OTelCollectorReceiverConfig {
	return OTelCollectorReceiverConfig{
		Enabled:         true,
		ListenAddr:      ":4317",
		MaxBatchSize:    10000,
		CompressionType: "gzip",
	}
}

// ReceiverStats tracks receiver performance.
type ReceiverStats struct {
	PointsReceived  int64     `json:"points_received"`
	PointsDropped   int64     `json:"points_dropped"`
	RequestsTotal   int64     `json:"requests_total"`
	RequestsFailed  int64     `json:"requests_failed"`
	LastReceiveTime time.Time `json:"last_receive_time"`
	BytesReceived   int64     `json:"bytes_received"`
}

// NewOTelCollectorReceiver creates a new receiver.
func NewOTelCollectorReceiver(db *DB, config OTelCollectorReceiverConfig) *OTelCollectorReceiver {
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 10000
	}
	return &OTelCollectorReceiver{
		db:     db,
		config: config,
	}
}

// Start begins listening for OTel Collector data.
func (r *OTelCollectorReceiver) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/metrics", r.handleMetrics)
	mux.HandleFunc("/health", r.handleHealth)

	r.server = &http.Server{
		Addr:    r.config.ListenAddr,
		Handler: mux,
	}

	go func() {
		var err error
		if r.config.TLSEnabled {
			err = r.server.ListenAndServeTLS(r.config.TLSCertFile, r.config.TLSKeyFile)
		} else {
			err = r.server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			// Log error
		}
	}()

	return nil
}

// Stop gracefully shuts down the receiver.
func (r *OTelCollectorReceiver) Stop(ctx context.Context) error {
	if r.server != nil {
		return r.server.Shutdown(ctx)
	}
	return nil
}

func (r *OTelCollectorReceiver) handleMetrics(w http.ResponseWriter, req *http.Request) {
	r.statsMu.Lock()
	r.stats.RequestsTotal++
	r.statsMu.Unlock()

	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Handle compression
	var reader io.Reader = req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		gz, err := NewGzipReader(req.Body)
		if err != nil {
			http.Error(w, "invalid gzip", http.StatusBadRequest)
			return
		}
		defer gz.Close()
		reader = gz
	}

	// Read body
	body, err := io.ReadAll(io.LimitReader(reader, 10*1024*1024))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		r.statsMu.Lock()
		r.stats.RequestsFailed++
		r.statsMu.Unlock()
		return
	}

	r.statsMu.Lock()
	r.stats.BytesReceived += int64(len(body))
	r.statsMu.Unlock()

	// Parse OTLP metrics
	var metrics OTLPExportRequest
	if err := json.Unmarshal(body, &metrics); err != nil {
		http.Error(w, "invalid OTLP format", http.StatusBadRequest)
		r.statsMu.Lock()
		r.stats.RequestsFailed++
		r.statsMu.Unlock()
		return
	}

	// Convert and write points
	pointCount := 0
	for _, rm := range metrics.ResourceMetrics {
		resourceTags := make(map[string]string)
		for _, attr := range rm.Resource.Attributes {
			resourceTags[attr.Key] = otelAttrValueToString(attr.Value)
		}

		for _, sm := range rm.ScopeMetrics {
			for _, metric := range sm.Metrics {
				points := r.convertMetric(metric, resourceTags)
				for _, p := range points {
					if err := r.db.Write(p); err != nil {
						r.statsMu.Lock()
						r.stats.PointsDropped++
						r.statsMu.Unlock()
					} else {
						pointCount++
					}
				}
			}
		}
	}

	r.statsMu.Lock()
	r.stats.PointsReceived += int64(pointCount)
	r.stats.LastReceiveTime = time.Now()
	r.statsMu.Unlock()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"partialSuccess":{}}`))
}

func (r *OTelCollectorReceiver) convertMetric(metric OTLPMetric, resourceTags map[string]string) []Point {
	var points []Point

	if metric.Gauge != nil {
		for _, dp := range metric.Gauge.DataPoints {
			tags := make(map[string]string)
			for k, v := range resourceTags {
				tags[k] = v
			}
			for _, attr := range dp.Attributes {
				tags[attr.Key] = otelAttrValueToString(attr.Value)
			}
			points = append(points, Point{
				Metric:    metric.Name,
				Value:     otlpNumberValue(dp),
				Timestamp: otlpTimestampStr(dp.TimeUnixNano),
				Tags:      tags,
			})
		}
	}

	if metric.Sum != nil {
		for _, dp := range metric.Sum.DataPoints {
			tags := make(map[string]string)
			for k, v := range resourceTags {
				tags[k] = v
			}
			for _, attr := range dp.Attributes {
				tags[attr.Key] = otelAttrValueToString(attr.Value)
			}
			points = append(points, Point{
				Metric:    metric.Name,
				Value:     otlpNumberValue(dp),
				Timestamp: otlpTimestampStr(dp.TimeUnixNano),
				Tags:      tags,
			})
		}
	}

	return points
}

func (r *OTelCollectorReceiver) handleHealth(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy"}`))
}

// Stats returns receiver statistics.
func (r *OTelCollectorReceiver) Stats() ReceiverStats {
	r.statsMu.RLock()
	defer r.statsMu.RUnlock()
	return r.stats
}

// GzipReader wraps gzip.Reader to add Close method.
type GzipReader struct {
	*gzip.Reader
	underlying io.ReadCloser
}

// NewGzipReader creates a new GzipReader.
func NewGzipReader(r io.ReadCloser) (*GzipReader, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return &GzipReader{Reader: gr, underlying: r}, nil
}

// Close closes both the gzip reader and underlying reader.
func (g *GzipReader) Close() error {
	g.Reader.Close()
	return g.underlying.Close()
}

// OTelCollectorConfig YAML generation for easy integration.
func GenerateOTelCollectorYAML(config OTelCollectorConfig) string {
	return fmt.Sprintf(`# OpenTelemetry Collector configuration for Chronicle exporter
exporters:
  chronicle:
    endpoint: %s
    batch_size: %d
    flush_interval: %s
    timeout: %s
    retry:
      enabled: %t
      max_attempts: %d
      initial_interval: %s
    resource_to_tags: %t
    scope_to_tags: %t
%s
service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [chronicle]
`,
		config.Endpoint,
		config.BatchSize,
		config.FlushInterval.String(),
		config.Timeout.String(),
		config.RetryEnabled,
		config.MaxRetries,
		config.RetryBackoff.String(),
		config.ResourceToTags,
		config.ScopeToTags,
		formatHeaders(config),
	)
}

func formatHeaders(config OTelCollectorConfig) string {
	if len(config.Headers) == 0 && config.APIKey == "" {
		return ""
	}
	var sb bytes.Buffer
	sb.WriteString("    headers:\n")
	if config.APIKey != "" {
		sb.WriteString(fmt.Sprintf("      Authorization: Bearer %s\n", config.APIKey))
	}
	for k, v := range config.Headers {
		sb.WriteString(fmt.Sprintf("      %s: %s\n", k, v))
	}
	return sb.String()
}
