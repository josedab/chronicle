package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// OTelDistroConfig configures the Chronicle OpenTelemetry Distribution.
type OTelDistroConfig struct {
	// Enabled enables the distribution.
	Enabled bool

	// ServiceName is the service name for this collector.
	ServiceName string

	// Version is the distribution version.
	Version string

	// Receivers configures data receivers.
	Receivers ReceiversConfig

	// Processors configures data processors.
	Processors ProcessorsConfig

	// Exporters configures data exporters.
	Exporters ExportersConfig

	// Pipelines configures data pipelines.
	Pipelines PipelinesConfig

	// Extensions configures extensions.
	Extensions ExtensionsConfig

	// Telemetry configures internal telemetry.
	Telemetry TelemetryConfig
}

// ReceiversConfig configures all receivers.
type ReceiversConfig struct {
	OTLP      *OTLPReceiverConfig      `json:"otlp,omitempty"`
	Prometheus *PrometheusReceiverConfig `json:"prometheus,omitempty"`
	HostMetrics *HostMetricsReceiverConfig `json:"host_metrics,omitempty"`
	Chronicle  *ChronicleReceiverConfig  `json:"chronicle,omitempty"`
}

// OTLPReceiverConfig configures the OTLP receiver.
type OTLPReceiverConfig struct {
	Protocols OTLPProtocols `json:"protocols"`
}

// OTLPProtocols configures OTLP protocols.
type OTLPProtocols struct {
	GRPC *OTLPGRPCConfig `json:"grpc,omitempty"`
	HTTP *OTLPHTTPConfig `json:"http,omitempty"`
}

// OTLPGRPCConfig configures OTLP gRPC.
type OTLPGRPCConfig struct {
	Endpoint string `json:"endpoint"`
}

// OTLPHTTPConfig configures OTLP HTTP.
type OTLPHTTPConfig struct {
	Endpoint string `json:"endpoint"`
}

// PrometheusReceiverConfig configures the Prometheus receiver.
type PrometheusReceiverConfig struct {
	ScrapeConfigs []PrometheusScrapeConfig `json:"scrape_configs"`
}

// PrometheusScrapeConfig configures a Prometheus scrape target.
type PrometheusScrapeConfig struct {
	JobName        string   `json:"job_name"`
	ScrapeInterval string   `json:"scrape_interval"`
	StaticConfigs  []StaticConfig `json:"static_configs"`
}

// StaticConfig defines static scrape targets.
type StaticConfig struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels,omitempty"`
}

// HostMetricsReceiverConfig configures host metrics collection.
type HostMetricsReceiverConfig struct {
	CollectionInterval string `json:"collection_interval"`
	Scrapers          HostMetricScrapers `json:"scrapers"`
}

// HostMetricScrapers configures which host metrics to collect.
type HostMetricScrapers struct {
	CPU        *struct{} `json:"cpu,omitempty"`
	Memory     *struct{} `json:"memory,omitempty"`
	Disk       *struct{} `json:"disk,omitempty"`
	Network    *struct{} `json:"network,omitempty"`
	Filesystem *struct{} `json:"filesystem,omitempty"`
	Load       *struct{} `json:"load,omitempty"`
	Processes  *struct{} `json:"processes,omitempty"`
}

// ChronicleReceiverConfig configures the Chronicle receiver (push from Chronicle).
type ChronicleReceiverConfig struct {
	Endpoint string `json:"endpoint"`
	Metrics  []string `json:"metrics,omitempty"`
}

// ProcessorsConfig configures all processors.
type ProcessorsConfig struct {
	Batch           *BatchProcessorConfig           `json:"batch,omitempty"`
	Memory          *MemoryLimiterConfig            `json:"memory_limiter,omitempty"`
	Attributes      *AttributesProcessorConfig      `json:"attributes,omitempty"`
	Filter          *FilterProcessorConfig          `json:"filter,omitempty"`
	Transform       *TransformProcessorConfig       `json:"transform,omitempty"`
	MetricsTransform *MetricsTransformProcessorConfig `json:"metrics_transform,omitempty"`
}

// BatchProcessorConfig configures batch processing.
type BatchProcessorConfig struct {
	SendBatchSize    int           `json:"send_batch_size"`
	SendBatchMaxSize int           `json:"send_batch_max_size"`
	Timeout          time.Duration `json:"timeout"`
}

// MemoryLimiterConfig configures memory limiting.
type MemoryLimiterConfig struct {
	CheckInterval       time.Duration `json:"check_interval"`
	LimitMiB            int           `json:"limit_mib"`
	SpikeLimitMiB       int           `json:"spike_limit_mib"`
	LimitPercentage     int           `json:"limit_percentage"`
}

// AttributesProcessorConfig configures attribute processing.
type AttributesProcessorConfig struct {
	Actions []AttributeAction `json:"actions"`
}

// AttributeAction defines an attribute action.
type AttributeAction struct {
	Key    string `json:"key"`
	Action string `json:"action"` // insert, update, delete, hash, extract
	Value  string `json:"value,omitempty"`
}

// FilterProcessorConfig configures filtering.
type FilterProcessorConfig struct {
	Metrics FilterConfig `json:"metrics,omitempty"`
}

// FilterConfig defines filter rules.
type FilterConfig struct {
	Include *FilterMatch `json:"include,omitempty"`
	Exclude *FilterMatch `json:"exclude,omitempty"`
}

// FilterMatch defines filter match criteria.
type FilterMatch struct {
	MatchType   string   `json:"match_type"` // strict, regexp
	MetricNames []string `json:"metric_names,omitempty"`
}

// TransformProcessorConfig configures OTTL transforms.
type TransformProcessorConfig struct {
	MetricStatements []string `json:"metric_statements,omitempty"`
}

// MetricsTransformProcessorConfig configures metrics transformation.
type MetricsTransformProcessorConfig struct {
	Transforms []MetricTransform `json:"transforms"`
}

// MetricTransform defines a metric transformation.
type MetricTransform struct {
	MetricNameMatch string              `json:"metric_name_match"`
	Action          string              `json:"action"` // update, combine, insert
	NewName         string              `json:"new_name,omitempty"`
	Operations      []MetricOperation   `json:"operations,omitempty"`
}

// MetricOperation defines a metric operation.
type MetricOperation struct {
	Action   string `json:"action"` // add_label, update_label, delete_label, aggregate_labels
	Label    string `json:"label,omitempty"`
	NewLabel string `json:"new_label,omitempty"`
	NewValue string `json:"new_value,omitempty"`
}

// ExportersConfig configures all exporters.
type ExportersConfig struct {
	Chronicle    *ChronicleExporterConfig    `json:"chronicle,omitempty"`
	OTLP         *OTLPExporterConfig         `json:"otlp,omitempty"`
	OTLPHttp     *OTLPHTTPExporterConfig     `json:"otlphttp,omitempty"`
	Prometheus   *PrometheusExporterConfig   `json:"prometheus,omitempty"`
	Debug        *DebugExporterConfig        `json:"debug,omitempty"`
}

// ChronicleExporterConfig configures Chronicle export.
type ChronicleExporterConfig struct {
	Endpoint       string            `json:"endpoint"`
	APIKey         string            `json:"api_key,omitempty"`
	Compression    string            `json:"compression"` // gzip, snappy, zstd, none
	BatchSize      int               `json:"batch_size"`
	FlushInterval  time.Duration     `json:"flush_interval"`
	RetryOnFailure bool              `json:"retry_on_failure"`
	Headers        map[string]string `json:"headers,omitempty"`
}

// OTLPExporterConfig configures OTLP export.
type OTLPExporterConfig struct {
	Endpoint    string            `json:"endpoint"`
	Compression string            `json:"compression"`
	Headers     map[string]string `json:"headers,omitempty"`
}

// OTLPHTTPExporterConfig configures OTLP HTTP export.
type OTLPHTTPExporterConfig struct {
	Endpoint    string            `json:"endpoint"`
	Compression string            `json:"compression"`
	Headers     map[string]string `json:"headers,omitempty"`
}

// PrometheusExporterConfig configures Prometheus export.
type PrometheusExporterConfig struct {
	Endpoint string `json:"endpoint"`
}

// DebugExporterConfig configures debug output.
type DebugExporterConfig struct {
	Verbosity string `json:"verbosity"` // basic, normal, detailed
}

// PipelinesConfig configures data pipelines.
type PipelinesConfig struct {
	Metrics *PipelineConfig `json:"metrics,omitempty"`
	Traces  *PipelineConfig `json:"traces,omitempty"`
	Logs    *PipelineConfig `json:"logs,omitempty"`
}

// PipelineConfig configures a single pipeline.
type PipelineConfig struct {
	Receivers  []string `json:"receivers"`
	Processors []string `json:"processors"`
	Exporters  []string `json:"exporters"`
}

// ExtensionsConfig configures extensions.
type ExtensionsConfig struct {
	Health     *HealthExtConfig     `json:"health_check,omitempty"`
	ZPages     *ZPagesConfig        `json:"zpages,omitempty"`
	PProfExt   *PProfExtConfig      `json:"pprof,omitempty"`
}

// HealthExtConfig configures health check extension.
type HealthExtConfig struct {
	Endpoint string `json:"endpoint"`
}

// ZPagesConfig configures zPages extension.
type ZPagesConfig struct {
	Endpoint string `json:"endpoint"`
}

// PProfExtConfig configures pprof extension.
type PProfExtConfig struct {
	Endpoint string `json:"endpoint"`
}

// TelemetryConfig configures internal telemetry.
type TelemetryConfig struct {
	Logs    TelemetryLogsConfig    `json:"logs"`
	Metrics TelemetryMetricsConfig `json:"metrics"`
}

// TelemetryLogsConfig configures log telemetry.
type TelemetryLogsConfig struct {
	Level            string `json:"level"` // debug, info, warn, error
	Development      bool   `json:"development"`
	Encoding         string `json:"encoding"` // json, console
	OutputPaths      []string `json:"output_paths"`
}

// TelemetryMetricsConfig configures metrics telemetry.
type TelemetryMetricsConfig struct {
	Level   string `json:"level"` // none, basic, normal, detailed
	Address string `json:"address"`
}

// DefaultOTelDistroConfig returns default configuration.
func DefaultOTelDistroConfig() OTelDistroConfig {
	return OTelDistroConfig{
		Enabled:     true,
		ServiceName: "chronicle-otel-distro",
		Version:     "1.0.0",
		Receivers: ReceiversConfig{
			OTLP: &OTLPReceiverConfig{
				Protocols: OTLPProtocols{
					GRPC: &OTLPGRPCConfig{Endpoint: "0.0.0.0:4317"},
					HTTP: &OTLPHTTPConfig{Endpoint: "0.0.0.0:4318"},
				},
			},
		},
		Processors: ProcessorsConfig{
			Batch: &BatchProcessorConfig{
				SendBatchSize:    1000,
				SendBatchMaxSize: 5000,
				Timeout:          10 * time.Second,
			},
			Memory: &MemoryLimiterConfig{
				CheckInterval:   time.Second,
				LimitMiB:        512,
				SpikeLimitMiB:   128,
			},
		},
		Exporters: ExportersConfig{
			Chronicle: &ChronicleExporterConfig{
				Endpoint:       "http://localhost:8086",
				BatchSize:      1000,
				FlushInterval:  10 * time.Second,
				RetryOnFailure: true,
				Compression:    "gzip",
			},
		},
		Pipelines: PipelinesConfig{
			Metrics: &PipelineConfig{
				Receivers:  []string{"otlp"},
				Processors: []string{"memory_limiter", "batch"},
				Exporters:  []string{"chronicle"},
			},
		},
		Extensions: ExtensionsConfig{
			Health: &HealthExtConfig{Endpoint: ":13133"},
			ZPages: &ZPagesConfig{Endpoint: ":55679"},
		},
		Telemetry: TelemetryConfig{
			Logs: TelemetryLogsConfig{
				Level:    "info",
				Encoding: "json",
			},
			Metrics: TelemetryMetricsConfig{
				Level:   "normal",
				Address: ":8888",
			},
		},
	}
}

// ChronicleOTelDistro is the Chronicle OpenTelemetry Collector Distribution.
type ChronicleOTelDistro struct {
	config     OTelDistroConfig
	db         *DB

	// Component registries
	receivers  map[string]Receiver
	processors map[string]Processor
	exporters  map[string]OTelExporter
	extensions map[string]Extension
	pipelines  map[string]*Pipeline

	// Runtime state
	running    bool
	mu         sync.RWMutex

	// Metrics
	metrics    *DistroMetrics

	// Lifecycle
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// Receiver receives telemetry data.
type Receiver interface {
	Start(ctx context.Context, host Host) error
	Shutdown(ctx context.Context) error
}

// Processor processes telemetry data.
type Processor interface {
	Start(ctx context.Context, host Host) error
	Shutdown(ctx context.Context) error
	ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error)
}

// OTelExporter exports telemetry data.
type OTelExporter interface {
	Start(ctx context.Context, host Host) error
	Shutdown(ctx context.Context) error
	ExportMetrics(ctx context.Context, metrics *Metrics) error
}

// Extension provides additional functionality.
type Extension interface {
	Start(ctx context.Context, host Host) error
	Shutdown(ctx context.Context) error
}

// Host provides access to collector services.
type Host interface {
	GetExtension(id string) Extension
	ReportFatalError(err error)
}

// Pipeline represents a data processing pipeline.
type Pipeline struct {
	Name       string
	Receivers  []Receiver
	Processors []Processor
	Exporters  []OTelExporter
	dataChan   chan *Metrics
	running    bool
	mu         sync.Mutex
}

// Metrics represents a batch of metrics.
type Metrics struct {
	ResourceMetrics []ResourceMetrics
}

// ResourceMetrics holds metrics with resource info.
type ResourceMetrics struct {
	Resource     Resource
	ScopeMetrics []ScopeMetrics
}

// Resource represents a resource.
type Resource struct {
	Attributes map[string]interface{}
}

// ScopeMetrics holds metrics with scope info.
type ScopeMetrics struct {
	Scope   InstrumentationScope
	Metrics []Metric
}

// InstrumentationScope represents the instrumentation scope.
type InstrumentationScope struct {
	Name    string
	Version string
}

// Metric represents a single metric.
type Metric struct {
	Name        string
	Description string
	Unit        string
	Data        interface{} // Gauge, Sum, Histogram, etc.
}

// DistroMetrics tracks distribution metrics.
type DistroMetrics struct {
	ReceiversStarted   int64
	ProcessorsStarted  int64
	ExportersStarted   int64
	PipelinesStarted   int64
	MetricsReceived    int64
	MetricsProcessed   int64
	MetricsExported    int64
	MetricsDropped     int64
	Errors             int64
	LastError          string
	Uptime             time.Duration
	StartTime          time.Time
	mu                 sync.RWMutex
}

// NewChronicleOTelDistro creates a new distribution instance.
func NewChronicleOTelDistro(db *DB, config OTelDistroConfig) *ChronicleOTelDistro {
	ctx, cancel := context.WithCancel(context.Background())

	return &ChronicleOTelDistro{
		config:     config,
		db:         db,
		receivers:  make(map[string]Receiver),
		processors: make(map[string]Processor),
		exporters:  make(map[string]OTelExporter),
		extensions: make(map[string]Extension),
		pipelines:  make(map[string]*Pipeline),
		metrics:    &DistroMetrics{StartTime: time.Now()},
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start starts the distribution.
func (d *ChronicleOTelDistro) Start() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.running {
		return nil
	}

	// Initialize components
	if err := d.initializeReceivers(); err != nil {
		return fmt.Errorf("failed to initialize receivers: %w", err)
	}

	if err := d.initializeProcessors(); err != nil {
		return fmt.Errorf("failed to initialize processors: %w", err)
	}

	if err := d.initializeExporters(); err != nil {
		return fmt.Errorf("failed to initialize exporters: %w", err)
	}

	if err := d.initializeExtensions(); err != nil {
		return fmt.Errorf("failed to initialize extensions: %w", err)
	}

	if err := d.initializePipelines(); err != nil {
		return fmt.Errorf("failed to initialize pipelines: %w", err)
	}

	// Start extensions
	for name, ext := range d.extensions {
		if err := ext.Start(d.ctx, d); err != nil {
			return fmt.Errorf("failed to start extension %s: %w", name, err)
		}
	}

	// Start pipelines
	for name, pipeline := range d.pipelines {
		if err := d.startPipeline(pipeline); err != nil {
			return fmt.Errorf("failed to start pipeline %s: %w", name, err)
		}
		atomic.AddInt64(&d.metrics.PipelinesStarted, 1)
	}

	d.running = true
	return nil
}

// Stop stops the distribution.
func (d *ChronicleOTelDistro) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.running {
		return nil
	}

	d.cancel()

	// Stop pipelines
	for _, pipeline := range d.pipelines {
		d.stopPipeline(pipeline)
	}

	// Stop extensions
	for _, ext := range d.extensions {
		ext.Shutdown(context.Background())
	}

	// Stop exporters
	for _, exp := range d.exporters {
		exp.Shutdown(context.Background())
	}

	// Stop processors
	for _, proc := range d.processors {
		proc.Shutdown(context.Background())
	}

	// Stop receivers
	for _, recv := range d.receivers {
		recv.Shutdown(context.Background())
	}

	d.wg.Wait()
	d.running = false
	return nil
}

// GetExtension implements Host interface.
func (d *ChronicleOTelDistro) GetExtension(id string) Extension {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.extensions[id]
}

// ReportFatalError implements Host interface.
func (d *ChronicleOTelDistro) ReportFatalError(err error) {
	d.metrics.mu.Lock()
	d.metrics.Errors++
	d.metrics.LastError = err.Error()
	d.metrics.mu.Unlock()
}

func (d *ChronicleOTelDistro) initializeReceivers() error {
	// OTLP Receiver
	if d.config.Receivers.OTLP != nil {
		recv := NewOTLPDistroReceiver(d.config.Receivers.OTLP, d)
		d.receivers["otlp"] = recv
		atomic.AddInt64(&d.metrics.ReceiversStarted, 1)
	}

	// Prometheus Receiver
	if d.config.Receivers.Prometheus != nil {
		recv := NewPrometheusDistroReceiver(d.config.Receivers.Prometheus, d)
		d.receivers["prometheus"] = recv
		atomic.AddInt64(&d.metrics.ReceiversStarted, 1)
	}

	// Host Metrics Receiver
	if d.config.Receivers.HostMetrics != nil {
		recv := NewHostMetricsDistroReceiver(d.config.Receivers.HostMetrics, d)
		d.receivers["hostmetrics"] = recv
		atomic.AddInt64(&d.metrics.ReceiversStarted, 1)
	}

	return nil
}

func (d *ChronicleOTelDistro) initializeProcessors() error {
	// Batch Processor
	if d.config.Processors.Batch != nil {
		proc := NewBatchDistroProcessor(d.config.Processors.Batch)
		d.processors["batch"] = proc
		atomic.AddInt64(&d.metrics.ProcessorsStarted, 1)
	}

	// Memory Limiter
	if d.config.Processors.Memory != nil {
		proc := NewMemoryLimiterDistroProcessor(d.config.Processors.Memory)
		d.processors["memory_limiter"] = proc
		atomic.AddInt64(&d.metrics.ProcessorsStarted, 1)
	}

	// Attributes Processor
	if d.config.Processors.Attributes != nil {
		proc := NewAttributesDistroProcessor(d.config.Processors.Attributes)
		d.processors["attributes"] = proc
		atomic.AddInt64(&d.metrics.ProcessorsStarted, 1)
	}

	// Filter Processor
	if d.config.Processors.Filter != nil {
		proc := NewFilterDistroProcessor(d.config.Processors.Filter)
		d.processors["filter"] = proc
		atomic.AddInt64(&d.metrics.ProcessorsStarted, 1)
	}

	return nil
}

func (d *ChronicleOTelDistro) initializeExporters() error {
	// Chronicle Exporter
	if d.config.Exporters.Chronicle != nil {
		exp := NewChronicleDistroExporter(d.db, d.config.Exporters.Chronicle)
		d.exporters["chronicle"] = exp
		atomic.AddInt64(&d.metrics.ExportersStarted, 1)
	}

	// OTLP Exporter
	if d.config.Exporters.OTLP != nil {
		exp := NewOTLPDistroExporter(d.config.Exporters.OTLP)
		d.exporters["otlp"] = exp
		atomic.AddInt64(&d.metrics.ExportersStarted, 1)
	}

	// Debug Exporter
	if d.config.Exporters.Debug != nil {
		exp := NewDebugDistroExporter(d.config.Exporters.Debug)
		d.exporters["debug"] = exp
		atomic.AddInt64(&d.metrics.ExportersStarted, 1)
	}

	return nil
}

func (d *ChronicleOTelDistro) initializeExtensions() error {
	// Health Check Extension
	if d.config.Extensions.Health != nil {
		ext := NewHealthCheckExtension(d.config.Extensions.Health, d)
		d.extensions["health_check"] = ext
	}

	// ZPages Extension
	if d.config.Extensions.ZPages != nil {
		ext := NewZPagesExtension(d.config.Extensions.ZPages)
		d.extensions["zpages"] = ext
	}

	return nil
}

func (d *ChronicleOTelDistro) initializePipelines() error {
	// Metrics Pipeline
	if d.config.Pipelines.Metrics != nil {
		pipeline := &Pipeline{
			Name:     "metrics",
			dataChan: make(chan *Metrics, 1000),
		}

		// Add receivers
		for _, name := range d.config.Pipelines.Metrics.Receivers {
			if recv, ok := d.receivers[name]; ok {
				pipeline.Receivers = append(pipeline.Receivers, recv)
			}
		}

		// Add processors
		for _, name := range d.config.Pipelines.Metrics.Processors {
			if proc, ok := d.processors[name]; ok {
				pipeline.Processors = append(pipeline.Processors, proc)
			}
		}

		// Add exporters
		for _, name := range d.config.Pipelines.Metrics.Exporters {
			if exp, ok := d.exporters[name]; ok {
				pipeline.Exporters = append(pipeline.Exporters, exp)
			}
		}

		d.pipelines["metrics"] = pipeline
	}

	return nil
}

func (d *ChronicleOTelDistro) startPipeline(pipeline *Pipeline) error {
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()

	if pipeline.running {
		return nil
	}

	// Start receivers
	for _, recv := range pipeline.Receivers {
		if err := recv.Start(d.ctx, d); err != nil {
			return err
		}
	}

	// Start processors
	for _, proc := range pipeline.Processors {
		if err := proc.Start(d.ctx, d); err != nil {
			return err
		}
	}

	// Start exporters
	for _, exp := range pipeline.Exporters {
		if err := exp.Start(d.ctx, d); err != nil {
			return err
		}
	}

	// Start pipeline worker
	d.wg.Add(1)
	go d.pipelineWorker(pipeline)

	pipeline.running = true
	return nil
}

func (d *ChronicleOTelDistro) stopPipeline(pipeline *Pipeline) {
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()

	if !pipeline.running {
		return
	}

	close(pipeline.dataChan)
	pipeline.running = false
}

func (d *ChronicleOTelDistro) pipelineWorker(pipeline *Pipeline) {
	defer d.wg.Done()

	for metrics := range pipeline.dataChan {
		// Process through all processors
		var err error
		for _, proc := range pipeline.Processors {
			metrics, err = proc.ProcessMetrics(d.ctx, metrics)
			if err != nil {
				atomic.AddInt64(&d.metrics.Errors, 1)
				continue
			}
		}

		atomic.AddInt64(&d.metrics.MetricsProcessed, 1)

		// Export to all exporters
		for _, exp := range pipeline.Exporters {
			if err := exp.ExportMetrics(d.ctx, metrics); err != nil {
				atomic.AddInt64(&d.metrics.Errors, 1)
				atomic.AddInt64(&d.metrics.MetricsDropped, 1)
			} else {
				atomic.AddInt64(&d.metrics.MetricsExported, 1)
			}
		}
	}
}

// PushMetrics pushes metrics to the pipeline.
func (d *ChronicleOTelDistro) PushMetrics(metrics *Metrics) {
	d.mu.RLock()
	pipeline, ok := d.pipelines["metrics"]
	d.mu.RUnlock()

	if !ok || !pipeline.running {
		return
	}

	atomic.AddInt64(&d.metrics.MetricsReceived, 1)

	select {
	case pipeline.dataChan <- metrics:
	default:
		atomic.AddInt64(&d.metrics.MetricsDropped, 1)
	}
}

// GetMetrics returns distribution metrics.
func (d *ChronicleOTelDistro) GetMetrics() DistroMetrics {
	d.metrics.mu.RLock()
	defer d.metrics.mu.RUnlock()

	m := *d.metrics
	m.Uptime = time.Since(d.metrics.StartTime)
	return m
}

// ========== Receiver Implementations ==========

// OTLPDistroReceiver receives OTLP data.
type OTLPDistroReceiver struct {
	config  *OTLPReceiverConfig
	distro  *ChronicleOTelDistro
	server  *http.Server
	running bool
	mu      sync.Mutex
}

// NewOTLPDistroReceiver creates a new OTLP receiver.
func NewOTLPDistroReceiver(config *OTLPReceiverConfig, distro *ChronicleOTelDistro) *OTLPDistroReceiver {
	return &OTLPDistroReceiver{
		config: config,
		distro: distro,
	}
}

func (r *OTLPDistroReceiver) Start(ctx context.Context, host Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/metrics", r.handleMetrics)

	endpoint := "0.0.0.0:4318"
	if r.config.Protocols.HTTP != nil {
		endpoint = r.config.Protocols.HTTP.Endpoint
	}

	r.server = &http.Server{
		Addr:    endpoint,
		Handler: mux,
	}

	go r.server.ListenAndServe()

	r.running = true
	return nil
}

func (r *OTLPDistroReceiver) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return nil
	}

	if r.server != nil {
		r.server.Shutdown(ctx)
	}

	r.running = false
	return nil
}

func (r *OTLPDistroReceiver) handleMetrics(w http.ResponseWriter, req *http.Request) {
	// Parse and push metrics
	var otlpReq OTLPExportRequest
	if err := json.NewDecoder(req.Body).Decode(&otlpReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	metrics := r.convertOTLPToMetrics(&otlpReq)
	r.distro.PushMetrics(metrics)

	w.WriteHeader(http.StatusOK)
}

func (r *OTLPDistroReceiver) convertOTLPToMetrics(otlp *OTLPExportRequest) *Metrics {
	metrics := &Metrics{}

	for _, rm := range otlp.ResourceMetrics {
		resourceMetrics := ResourceMetrics{
			Resource: Resource{Attributes: make(map[string]interface{})},
		}

		for _, attr := range rm.Resource.Attributes {
			resourceMetrics.Resource.Attributes[attr.Key] = otelAttrValueToString(attr.Value)
		}

		for _, sm := range rm.ScopeMetrics {
			scopeMetrics := ScopeMetrics{
				Scope: InstrumentationScope{
					Name:    sm.Scope.Name,
					Version: sm.Scope.Version,
				},
			}

			for _, m := range sm.Metrics {
				scopeMetrics.Metrics = append(scopeMetrics.Metrics, Metric{
					Name:        m.Name,
					Description: m.Description,
					Unit:        m.Unit,
					Data:        m,
				})
			}

			resourceMetrics.ScopeMetrics = append(resourceMetrics.ScopeMetrics, scopeMetrics)
		}

		metrics.ResourceMetrics = append(metrics.ResourceMetrics, resourceMetrics)
	}

	return metrics
}

// PrometheusDistroReceiver scrapes Prometheus targets.
type PrometheusDistroReceiver struct {
	config  *PrometheusReceiverConfig
	distro  *ChronicleOTelDistro
	ticker  *time.Ticker
	running bool
	mu      sync.Mutex
}

// NewPrometheusDistroReceiver creates a new Prometheus receiver.
func NewPrometheusDistroReceiver(config *PrometheusReceiverConfig, distro *ChronicleOTelDistro) *PrometheusDistroReceiver {
	return &PrometheusDistroReceiver{
		config: config,
		distro: distro,
	}
}

func (r *PrometheusDistroReceiver) Start(ctx context.Context, host Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return nil
	}

	// Start scraping
	r.ticker = time.NewTicker(15 * time.Second)
	go r.scrapeLoop(ctx)

	r.running = true
	return nil
}

func (r *PrometheusDistroReceiver) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ticker != nil {
		r.ticker.Stop()
	}

	r.running = false
	return nil
}

func (r *PrometheusDistroReceiver) scrapeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.ticker.C:
			r.scrapeTargets()
		}
	}
}

func (r *PrometheusDistroReceiver) scrapeTargets() {
	// Scrape all configured targets
	for _, sc := range r.config.ScrapeConfigs {
		for _, static := range sc.StaticConfigs {
			for _, target := range static.Targets {
				r.scrapeTarget(target, sc.JobName, static.Labels)
			}
		}
	}
}

func (r *PrometheusDistroReceiver) scrapeTarget(target, job string, labels map[string]string) {
	// Fetch and parse metrics
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", target))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	// Parse Prometheus format and convert to Metrics
	// Simplified implementation
}

// HostMetricsDistroReceiver collects host metrics.
type HostMetricsDistroReceiver struct {
	config  *HostMetricsReceiverConfig
	distro  *ChronicleOTelDistro
	ticker  *time.Ticker
	running bool
	mu      sync.Mutex
}

// NewHostMetricsDistroReceiver creates a host metrics receiver.
func NewHostMetricsDistroReceiver(config *HostMetricsReceiverConfig, distro *ChronicleOTelDistro) *HostMetricsDistroReceiver {
	return &HostMetricsDistroReceiver{
		config: config,
		distro: distro,
	}
}

func (r *HostMetricsDistroReceiver) Start(ctx context.Context, host Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return nil
	}

	interval := 10 * time.Second
	r.ticker = time.NewTicker(interval)
	go r.collectLoop(ctx)

	r.running = true
	return nil
}

func (r *HostMetricsDistroReceiver) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ticker != nil {
		r.ticker.Stop()
	}

	r.running = false
	return nil
}

func (r *HostMetricsDistroReceiver) collectLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.ticker.C:
			r.collectMetrics()
		}
	}
}

func (r *HostMetricsDistroReceiver) collectMetrics() {
	// Collect host metrics (CPU, memory, disk, etc.)
	// Push to distro
}

// ========== Processor Implementations ==========

// BatchDistroProcessor batches metrics.
type BatchDistroProcessor struct {
	config *BatchProcessorConfig
}

// NewBatchDistroProcessor creates a batch processor.
func NewBatchDistroProcessor(config *BatchProcessorConfig) *BatchDistroProcessor {
	return &BatchDistroProcessor{config: config}
}

func (p *BatchDistroProcessor) Start(ctx context.Context, host Host) error { return nil }
func (p *BatchDistroProcessor) Shutdown(ctx context.Context) error        { return nil }

func (p *BatchDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {
	// In a real implementation, would batch metrics
	return metrics, nil
}

// MemoryLimiterDistroProcessor limits memory usage.
type MemoryLimiterDistroProcessor struct {
	config *MemoryLimiterConfig
}

// NewMemoryLimiterDistroProcessor creates a memory limiter.
func NewMemoryLimiterDistroProcessor(config *MemoryLimiterConfig) *MemoryLimiterDistroProcessor {
	return &MemoryLimiterDistroProcessor{config: config}
}

func (p *MemoryLimiterDistroProcessor) Start(ctx context.Context, host Host) error { return nil }
func (p *MemoryLimiterDistroProcessor) Shutdown(ctx context.Context) error        { return nil }

func (p *MemoryLimiterDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {
	// Check memory and potentially drop if over limit
	return metrics, nil
}

// AttributesDistroProcessor modifies attributes.
type AttributesDistroProcessor struct {
	config *AttributesProcessorConfig
}

// NewAttributesDistroProcessor creates an attributes processor.
func NewAttributesDistroProcessor(config *AttributesProcessorConfig) *AttributesDistroProcessor {
	return &AttributesDistroProcessor{config: config}
}

func (p *AttributesDistroProcessor) Start(ctx context.Context, host Host) error { return nil }
func (p *AttributesDistroProcessor) Shutdown(ctx context.Context) error        { return nil }

func (p *AttributesDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {
	for _, action := range p.config.Actions {
		for i := range metrics.ResourceMetrics {
			switch action.Action {
			case "insert":
				metrics.ResourceMetrics[i].Resource.Attributes[action.Key] = action.Value
			case "delete":
				delete(metrics.ResourceMetrics[i].Resource.Attributes, action.Key)
			}
		}
	}
	return metrics, nil
}

// FilterDistroProcessor filters metrics.
type FilterDistroProcessor struct {
	config *FilterProcessorConfig
}

// NewFilterDistroProcessor creates a filter processor.
func NewFilterDistroProcessor(config *FilterProcessorConfig) *FilterDistroProcessor {
	return &FilterDistroProcessor{config: config}
}

func (p *FilterDistroProcessor) Start(ctx context.Context, host Host) error { return nil }
func (p *FilterDistroProcessor) Shutdown(ctx context.Context) error        { return nil }

func (p *FilterDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {
	// Apply include/exclude filters
	return metrics, nil
}

// ========== Exporter Implementations ==========

// ChronicleDistroExporter exports to Chronicle.
type ChronicleDistroExporter struct {
	db     *DB
	config *ChronicleExporterConfig
}

// NewChronicleDistroExporter creates a Chronicle exporter.
func NewChronicleDistroExporter(db *DB, config *ChronicleExporterConfig) *ChronicleDistroExporter {
	return &ChronicleDistroExporter{db: db, config: config}
}

func (e *ChronicleDistroExporter) Start(ctx context.Context, host Host) error { return nil }
func (e *ChronicleDistroExporter) Shutdown(ctx context.Context) error        { return nil }

func (e *ChronicleDistroExporter) ExportMetrics(ctx context.Context, metrics *Metrics) error {
	if e.db == nil {
		return nil
	}

	// Convert and write to Chronicle
	for _, rm := range metrics.ResourceMetrics {
		tags := make(map[string]string)
		for k, v := range rm.Resource.Attributes {
			tags[k] = fmt.Sprintf("%v", v)
		}

		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				// Simplified conversion
				point := Point{
					Metric:    m.Name,
					Timestamp: time.Now().UnixNano(),
					Tags:      tags,
				}
				e.db.Write(point)
			}
		}
	}

	return nil
}

// OTLPDistroExporter exports to OTLP endpoint.
type OTLPDistroExporter struct {
	config *OTLPExporterConfig
	client *http.Client
}

// NewOTLPDistroExporter creates an OTLP exporter.
func NewOTLPDistroExporter(config *OTLPExporterConfig) *OTLPDistroExporter {
	return &OTLPDistroExporter{
		config: config,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (e *OTLPDistroExporter) Start(ctx context.Context, host Host) error { return nil }
func (e *OTLPDistroExporter) Shutdown(ctx context.Context) error        { return nil }

func (e *OTLPDistroExporter) ExportMetrics(ctx context.Context, metrics *Metrics) error {
	// Convert and send to OTLP endpoint
	return nil
}

// DebugDistroExporter outputs debug information.
type DebugDistroExporter struct {
	config *DebugExporterConfig
}

// NewDebugDistroExporter creates a debug exporter.
func NewDebugDistroExporter(config *DebugExporterConfig) *DebugDistroExporter {
	return &DebugDistroExporter{config: config}
}

func (e *DebugDistroExporter) Start(ctx context.Context, host Host) error { return nil }
func (e *DebugDistroExporter) Shutdown(ctx context.Context) error        { return nil }

func (e *DebugDistroExporter) ExportMetrics(ctx context.Context, metrics *Metrics) error {
	// Log metrics for debugging
	return nil
}

// ========== Extension Implementations ==========

// HealthCheckExtension provides health check endpoint.
type HealthCheckExtension struct {
	config *HealthExtConfig
	distro *ChronicleOTelDistro
	server *http.Server
}

// NewHealthCheckExtension creates a health check extension.
func NewHealthCheckExtension(config *HealthExtConfig, distro *ChronicleOTelDistro) *HealthCheckExtension {
	return &HealthCheckExtension{config: config, distro: distro}
}

func (e *HealthCheckExtension) Start(ctx context.Context, host Host) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", e.handleHealth)

	e.server = &http.Server{
		Addr:    e.config.Endpoint,
		Handler: mux,
	}

	go e.server.ListenAndServe()
	return nil
}

func (e *HealthCheckExtension) Shutdown(ctx context.Context) error {
	if e.server != nil {
		return e.server.Shutdown(ctx)
	}
	return nil
}

func (e *HealthCheckExtension) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"uptime": e.distro.GetMetrics().Uptime.String(),
	})
}

// ZPagesExtension provides diagnostic pages.
type ZPagesExtension struct {
	config *ZPagesConfig
	server *http.Server
}

// NewZPagesExtension creates a zpages extension.
func NewZPagesExtension(config *ZPagesConfig) *ZPagesExtension {
	return &ZPagesExtension{config: config}
}

func (e *ZPagesExtension) Start(ctx context.Context, host Host) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/tracez", e.handleTracez)
	mux.HandleFunc("/debug/rpcz", e.handleRpcz)
	mux.HandleFunc("/debug/servicez", e.handleServicez)

	e.server = &http.Server{
		Addr:    e.config.Endpoint,
		Handler: mux,
	}

	go e.server.ListenAndServe()
	return nil
}

func (e *ZPagesExtension) Shutdown(ctx context.Context) error {
	if e.server != nil {
		return e.server.Shutdown(ctx)
	}
	return nil
}

func (e *ZPagesExtension) handleTracez(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Trace information"))
}

func (e *ZPagesExtension) handleRpcz(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("RPC information"))
}

func (e *ZPagesExtension) handleServicez(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Service information"))
}

// ========== Configuration Generation ==========

// GenerateOTelDistroYAML generates an OTel Collector config YAML.
func GenerateOTelDistroYAML(config OTelDistroConfig) string {
	var sb strings.Builder

	sb.WriteString("# Chronicle OpenTelemetry Collector Distribution Configuration\n")
	sb.WriteString("# Generated by Chronicle\n\n")

	// Receivers
	sb.WriteString("receivers:\n")
	if config.Receivers.OTLP != nil {
		sb.WriteString("  otlp:\n")
		sb.WriteString("    protocols:\n")
		if config.Receivers.OTLP.Protocols.GRPC != nil {
			sb.WriteString(fmt.Sprintf("      grpc:\n        endpoint: %s\n", config.Receivers.OTLP.Protocols.GRPC.Endpoint))
		}
		if config.Receivers.OTLP.Protocols.HTTP != nil {
			sb.WriteString(fmt.Sprintf("      http:\n        endpoint: %s\n", config.Receivers.OTLP.Protocols.HTTP.Endpoint))
		}
	}

	// Processors
	sb.WriteString("\nprocessors:\n")
	if config.Processors.Batch != nil {
		sb.WriteString("  batch:\n")
		sb.WriteString(fmt.Sprintf("    send_batch_size: %d\n", config.Processors.Batch.SendBatchSize))
		sb.WriteString(fmt.Sprintf("    timeout: %s\n", config.Processors.Batch.Timeout))
	}
	if config.Processors.Memory != nil {
		sb.WriteString("  memory_limiter:\n")
		sb.WriteString(fmt.Sprintf("    check_interval: %s\n", config.Processors.Memory.CheckInterval))
		sb.WriteString(fmt.Sprintf("    limit_mib: %d\n", config.Processors.Memory.LimitMiB))
	}

	// Exporters
	sb.WriteString("\nexporters:\n")
	if config.Exporters.Chronicle != nil {
		sb.WriteString("  chronicle:\n")
		sb.WriteString(fmt.Sprintf("    endpoint: %s\n", config.Exporters.Chronicle.Endpoint))
		sb.WriteString(fmt.Sprintf("    compression: %s\n", config.Exporters.Chronicle.Compression))
		sb.WriteString(fmt.Sprintf("    batch_size: %d\n", config.Exporters.Chronicle.BatchSize))
	}

	// Extensions
	sb.WriteString("\nextensions:\n")
	if config.Extensions.Health != nil {
		sb.WriteString(fmt.Sprintf("  health_check:\n    endpoint: %s\n", config.Extensions.Health.Endpoint))
	}
	if config.Extensions.ZPages != nil {
		sb.WriteString(fmt.Sprintf("  zpages:\n    endpoint: %s\n", config.Extensions.ZPages.Endpoint))
	}

	// Service
	sb.WriteString("\nservice:\n")
	sb.WriteString("  extensions: [")
	var exts []string
	if config.Extensions.Health != nil {
		exts = append(exts, "health_check")
	}
	if config.Extensions.ZPages != nil {
		exts = append(exts, "zpages")
	}
	sb.WriteString(strings.Join(exts, ", "))
	sb.WriteString("]\n")

	sb.WriteString("  pipelines:\n")
	if config.Pipelines.Metrics != nil {
		sb.WriteString("    metrics:\n")
		sb.WriteString(fmt.Sprintf("      receivers: [%s]\n", strings.Join(config.Pipelines.Metrics.Receivers, ", ")))
		sb.WriteString(fmt.Sprintf("      processors: [%s]\n", strings.Join(config.Pipelines.Metrics.Processors, ", ")))
		sb.WriteString(fmt.Sprintf("      exporters: [%s]\n", strings.Join(config.Pipelines.Metrics.Exporters, ", ")))
	}

	// Telemetry
	sb.WriteString("\n  telemetry:\n")
	sb.WriteString("    logs:\n")
	sb.WriteString(fmt.Sprintf("      level: %s\n", config.Telemetry.Logs.Level))
	sb.WriteString("    metrics:\n")
	sb.WriteString(fmt.Sprintf("      level: %s\n", config.Telemetry.Metrics.Level))
	sb.WriteString(fmt.Sprintf("      address: %s\n", config.Telemetry.Metrics.Address))

	return sb.String()
}

// ListComponents returns all available components.
func ListComponents() map[string][]string {
	return map[string][]string{
		"receivers": {
			"otlp", "prometheus", "hostmetrics", "chronicle",
		},
		"processors": {
			"batch", "memory_limiter", "attributes", "filter",
			"transform", "metrics_transform",
		},
		"exporters": {
			"chronicle", "otlp", "otlphttp", "prometheus", "debug",
		},
		"extensions": {
			"health_check", "zpages", "pprof",
		},
	}
}

// GetComponentInfo returns information about a component.
func GetComponentInfo(componentType, name string) map[string]interface{} {
	components := map[string]map[string]map[string]interface{}{
		"receivers": {
			"otlp": {
				"name":        "OTLP Receiver",
				"description": "Receives data via OTLP/gRPC or OTLP/HTTP",
				"stability":   "stable",
			},
			"prometheus": {
				"name":        "Prometheus Receiver",
				"description": "Scrapes Prometheus metrics endpoints",
				"stability":   "beta",
			},
		},
		"processors": {
			"batch": {
				"name":        "Batch Processor",
				"description": "Batches data before sending to exporters",
				"stability":   "stable",
			},
		},
		"exporters": {
			"chronicle": {
				"name":        "Chronicle Exporter",
				"description": "Exports metrics to Chronicle time-series database",
				"stability":   "stable",
			},
		},
	}

	if types, ok := components[componentType]; ok {
		if info, ok := types[name]; ok {
			return info
		}
	}
	return nil
}

// ValidateConfig validates the distribution configuration.
func ValidateConfig(config OTelDistroConfig) []string {
	var errors []string

	// Check receivers
	if config.Receivers.OTLP == nil && config.Receivers.Prometheus == nil && config.Receivers.HostMetrics == nil {
		errors = append(errors, "at least one receiver must be configured")
	}

	// Check exporters
	if config.Exporters.Chronicle == nil && config.Exporters.OTLP == nil {
		errors = append(errors, "at least one exporter must be configured")
	}

	// Check pipelines reference valid components
	if config.Pipelines.Metrics != nil {
		for _, recv := range config.Pipelines.Metrics.Receivers {
			if !isValidReceiver(recv, config.Receivers) {
				errors = append(errors, fmt.Sprintf("pipeline references unconfigured receiver: %s", recv))
			}
		}
	}

	return errors
}

func isValidReceiver(name string, config ReceiversConfig) bool {
	switch name {
	case "otlp":
		return config.OTLP != nil
	case "prometheus":
		return config.Prometheus != nil
	case "hostmetrics":
		return config.HostMetrics != nil
	case "chronicle":
		return config.Chronicle != nil
	default:
		return false
	}
}

// GetDefaultPipeline returns a default metrics pipeline configuration.
func GetDefaultPipeline() *PipelineConfig {
	return &PipelineConfig{
		Receivers:  []string{"otlp"},
		Processors: []string{"memory_limiter", "batch"},
		Exporters:  []string{"chronicle"},
	}
}

// GetBuiltinTransforms returns built-in transform rules.
func GetBuiltinTransforms() []MetricTransform {
	return []MetricTransform{
		{
			MetricNameMatch: "system.*",
			Action:          "update",
			Operations: []MetricOperation{
				{Action: "add_label", Label: "source", NewValue: "host"},
			},
		},
		{
			MetricNameMatch: "http.*",
			Action:          "update",
			Operations: []MetricOperation{
				{Action: "add_label", Label: "protocol", NewValue: "http"},
			},
		},
	}
}

// SupportedFormats returns supported data formats.
func SupportedFormats() []string {
	return []string{
		"otlp_proto",
		"otlp_json",
		"prometheus",
		"influx",
		"carbon",
	}
}

// GetExampleConfigs returns example configurations.
func GetExampleConfigs() map[string]OTelDistroConfig {
	configs := make(map[string]OTelDistroConfig)

	// Basic config
	configs["basic"] = DefaultOTelDistroConfig()

	// High availability config
	ha := DefaultOTelDistroConfig()
	ha.Processors.Batch.SendBatchSize = 5000
	ha.Processors.Memory.LimitMiB = 1024
	configs["high_availability"] = ha

	// Edge config
	edge := DefaultOTelDistroConfig()
	edge.Processors.Batch.SendBatchSize = 100
	edge.Processors.Memory.LimitMiB = 64
	configs["edge"] = edge

	return configs
}

// ListConfiguredComponents returns configured components summary.
func (d *ChronicleOTelDistro) ListConfiguredComponents() map[string][]string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make(map[string][]string)

	var receivers []string
	for name := range d.receivers {
		receivers = append(receivers, name)
	}
	sort.Strings(receivers)
	result["receivers"] = receivers

	var processors []string
	for name := range d.processors {
		processors = append(processors, name)
	}
	sort.Strings(processors)
	result["processors"] = processors

	var exporters []string
	for name := range d.exporters {
		exporters = append(exporters, name)
	}
	sort.Strings(exporters)
	result["exporters"] = exporters

	var extensions []string
	for name := range d.extensions {
		extensions = append(extensions, name)
	}
	sort.Strings(extensions)
	result["extensions"] = extensions

	return result
}
