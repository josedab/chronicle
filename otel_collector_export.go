// otel_collector_export.go contains extended otel collector functionality.
package chronicle

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

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
	wg      sync.WaitGroup
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
func (r *OTelCollectorReceiver) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/otel-collector/ingest", r.handleMetrics)
	mux.HandleFunc("/api/v1/otel-collector/health", r.handleHealth)

	r.server = &http.Server{
		Addr:    r.config.ListenAddr,
		Handler: mux,
	}

	r.wg.Add(1)
	go func(ctx context.Context) {
		defer r.wg.Done()
		var err error
		if r.config.TLSEnabled {
			err = r.server.ListenAndServeTLS(r.config.TLSCertFile, r.config.TLSKeyFile)
		} else {
			err = r.server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			// Log error
		}
	}(ctx)

	// Shut down the server when the parent context is cancelled.
	go func(ctx context.Context) {
		<-ctx.Done()
		r.Stop(context.Background())
	}(ctx)

	return nil
}

// Stop gracefully shuts down the receiver.
func (r *OTelCollectorReceiver) Stop(ctx context.Context) error {
	if r.server != nil {
		err := r.server.Shutdown(ctx)
		r.wg.Wait()
		return err
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

// GenerateOTelReceiverYAML generates a complete OTel Collector config with
// Chronicle as the backend storage. This config accepts OTLP metrics and
// exports them to a Chronicle HTTP endpoint.
func GenerateOTelReceiverYAML(chronicleEndpoint string, otlpPort int) string {
	if otlpPort <= 0 {
		otlpPort = 4317
	}
	if chronicleEndpoint == "" {
		chronicleEndpoint = "http://localhost:8086"
	}

	return fmt.Sprintf(`# OpenTelemetry Collector configuration with Chronicle backend
# Generated by chronicle.GenerateOTelReceiverYAML()

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:%d"
      http:
        endpoint: "0.0.0.0:%d"

processors:
  batch:
    send_batch_size: 1000
    timeout: 10s
  memory_limiter:
    check_interval: 5s
    limit_mib: 512
    spike_limit_mib: 128
  resource:
    attributes:
      - key: collector.name
        value: chronicle-otel-collector
        action: upsert

exporters:
  otlphttp/chronicle:
    endpoint: "%s"
    tls:
      insecure: true
    headers:
      Content-Type: application/json

  logging:
    loglevel: info

service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch, resource]
      exporters: [otlphttp/chronicle, logging]
  telemetry:
    logs:
      level: info
    metrics:
      address: ":8888"
`, otlpPort, otlpPort+1, chronicleEndpoint)
}

// OTelExporterFactory creates OTel Collector exporter instances.
// This follows the OpenTelemetry Collector SDK factory pattern, allowing
// Chronicle to be integrated as a collector component.
//
// Example registration in a custom collector:
//
//	factory := chronicle.NewOTelExporterFactory()
//	exporter, _ := factory.CreateMetricsExporter(cfg)
type OTelExporterFactory struct {
	defaultConfig OTelCollectorConfig
}

// NewOTelExporterFactory creates a factory with default config.
func NewOTelExporterFactory() *OTelExporterFactory {
	return &OTelExporterFactory{
		defaultConfig: DefaultOTelCollectorConfig(),
	}
}

// Type returns the component type identifier.
func (f *OTelExporterFactory) Type() string {
	return "chronicle"
}

// CreateDefaultConfig returns the default exporter configuration.
func (f *OTelExporterFactory) CreateDefaultConfig() OTelCollectorConfig {
	return f.defaultConfig
}

// CreateMetricsExporter creates a new Chronicle metrics exporter from config.
func (f *OTelExporterFactory) CreateMetricsExporter(cfg OTelCollectorConfig) (*OTelCollectorExporter, error) {
	return NewOTelCollectorExporter(cfg)
}

// CreateMetricsReceiver creates a new Chronicle metrics receiver that accepts OTLP data.
func (f *OTelExporterFactory) CreateMetricsReceiver(db *DB, cfg OTelCollectorReceiverConfig) *OTelCollectorReceiver {
	return NewOTelCollectorReceiver(db, cfg)
}
