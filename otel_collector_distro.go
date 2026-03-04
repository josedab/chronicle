package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// OTelReceiverType identifies a receiver type in the collector distribution.
type OTelReceiverType string

const (
	OTelReceiverOTLP       OTelReceiverType = "otlp"
	OTelReceiverPrometheus OTelReceiverType = "prometheus"
	OTelReceiverStatsD     OTelReceiverType = "statsd"
)

// OTelProcessorType identifies a processor type.
type OTelProcessorType string

const (
	OTelProcessorBatch     OTelProcessorType = "batch"
	OTelProcessorFilter    OTelProcessorType = "filter"
	OTelProcessorTransform OTelProcessorType = "transform"
	OTelProcessorMemLimit  OTelProcessorType = "memory_limiter"
)

// OTelExporterType identifies an exporter type.
type OTelExporterType string

const (
	OTelExporterChronicle OTelExporterType = "chronicle"
	OTelExporterS3        OTelExporterType = "s3"
	OTelExporterConsole   OTelExporterType = "console"
	OTelExporterOTLP      OTelExporterType = "otlp"
)

// OTelCollectorDistroConfig configures the full collector distribution.
type OTelCollectorDistroConfig struct {
	Enabled       bool                   `json:"enabled"`
	ServiceName   string                 `json:"service_name"`
	Receivers     []ReceiverConfig       `json:"receivers"`
	Processors    []ProcessorConfig      `json:"processors"`
	Exporters     []ExporterConfig       `json:"exporters"`
	Pipelines     []CollectorPipeline    `json:"pipelines"`
	HealthCheck   bool                   `json:"health_check"`
	MetricsPort   int                    `json:"metrics_port"`
	Extensions    map[string]interface{} `json:"extensions,omitempty"`
}

// ReceiverConfig configures a single receiver.
type ReceiverConfig struct {
	Name       string                 `json:"name"`
	Type       OTelReceiverType       `json:"type"`
	Endpoint   string                 `json:"endpoint,omitempty"`
	Settings   map[string]interface{} `json:"settings,omitempty"`
	Enabled    bool                   `json:"enabled"`
}

// ProcessorConfig configures a single processor.
type ProcessorConfig struct {
	Name       string                 `json:"name"`
	Type       OTelProcessorType      `json:"type"`
	Settings   map[string]interface{} `json:"settings,omitempty"`
	Enabled    bool                   `json:"enabled"`
}

// ExporterConfig configures a single exporter.
type ExporterConfig struct {
	Name       string                 `json:"name"`
	Type       OTelExporterType       `json:"type"`
	Endpoint   string                 `json:"endpoint,omitempty"`
	Settings   map[string]interface{} `json:"settings,omitempty"`
	Enabled    bool                   `json:"enabled"`
}

// CollectorPipeline defines a complete signal processing pipeline.
type CollectorPipeline struct {
	Name       string   `json:"name"`
	SignalType string   `json:"signal_type"` // metrics, traces, logs
	Receivers  []string `json:"receivers"`
	Processors []string `json:"processors"`
	Exporters  []string `json:"exporters"`
	Enabled    bool     `json:"enabled"`
}

// DefaultOTelCollectorDistroConfig returns a default collector distribution config.
func DefaultOTelCollectorDistroConfig() OTelCollectorDistroConfig {
	return OTelCollectorDistroConfig{
		Enabled:     true,
		ServiceName: "chronicle-collector",
		Receivers: []ReceiverConfig{
			{Name: "otlp", Type: OTelReceiverOTLP, Endpoint: "0.0.0.0:4317", Enabled: true},
			{Name: "prometheus", Type: OTelReceiverPrometheus, Endpoint: "0.0.0.0:9090", Enabled: false},
			{Name: "statsd", Type: OTelReceiverStatsD, Endpoint: "0.0.0.0:8125", Enabled: false},
		},
		Processors: []ProcessorConfig{
			{Name: "batch", Type: OTelProcessorBatch, Settings: map[string]interface{}{"send_batch_size": 1000, "timeout": "5s"}, Enabled: true},
			{Name: "filter", Type: OTelProcessorFilter, Enabled: false},
			{Name: "transform", Type: OTelProcessorTransform, Enabled: false},
			{Name: "memory_limiter", Type: OTelProcessorMemLimit, Settings: map[string]interface{}{"limit_mib": 512, "check_interval": "1s"}, Enabled: true},
		},
		Exporters: []ExporterConfig{
			{Name: "chronicle", Type: OTelExporterChronicle, Enabled: true},
			{Name: "console", Type: OTelExporterConsole, Enabled: false},
		},
		Pipelines: []CollectorPipeline{
			{Name: "metrics/default", SignalType: "metrics", Receivers: []string{"otlp"}, Processors: []string{"memory_limiter", "batch"}, Exporters: []string{"chronicle"}, Enabled: true},
			{Name: "traces/default", SignalType: "traces", Receivers: []string{"otlp"}, Processors: []string{"memory_limiter", "batch"}, Exporters: []string{"chronicle"}, Enabled: true},
			{Name: "logs/default", SignalType: "logs", Receivers: []string{"otlp"}, Processors: []string{"memory_limiter", "batch"}, Exporters: []string{"chronicle"}, Enabled: true},
		},
		HealthCheck: true,
		MetricsPort: 8888,
	}
}

// OTelCollectorDistro implements a full OpenTelemetry collector distribution
// with pluggable receivers, processors, and exporters using Chronicle as storage.
type OTelCollectorDistro struct {
	db     *DB
	config OTelCollectorDistroConfig
	distro *OTelDistro

	receivers  map[string]CollectorReceiver
	processors map[string]CollectorProcessor
	exporters  map[string]CollectorExporter

	mu      sync.RWMutex
	running atomic.Bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// Stats
	totalReceived  atomic.Uint64
	totalProcessed atomic.Uint64
	totalExported  atomic.Uint64
	totalDropped   atomic.Uint64
	totalErrors    atomic.Uint64
	startTime      time.Time
}

// CollectorReceiver is the interface for OTel receivers.
type CollectorReceiver interface {
	Start(ctx context.Context) error
	Stop() error
	Name() string
	Type() OTelReceiverType
}

// CollectorProcessor is the interface for OTel processors.
type CollectorProcessor interface {
	Process(points []Point) ([]Point, error)
	Name() string
	Type() OTelProcessorType
}

// CollectorExporter is the interface for OTel exporters.
type CollectorExporter interface {
	Export(ctx context.Context, points []Point) error
	Name() string
	Type() OTelExporterType
}

// NewOTelCollectorDistro creates a new collector distribution.
func NewOTelCollectorDistro(db *DB, config OTelCollectorDistroConfig) *OTelCollectorDistro {
	otelCfg := DefaultOTelDistroConfig()
	otelCfg.Enabled = config.Enabled

	cd := &OTelCollectorDistro{
		db:         db,
		config:     config,
		distro:     NewOTelDistro(db, otelCfg),
		receivers:  make(map[string]CollectorReceiver),
		processors: make(map[string]CollectorProcessor),
		exporters:  make(map[string]CollectorExporter),
	}

	// Register built-in processors
	for _, pc := range config.Processors {
		if !pc.Enabled {
			continue
		}
		switch pc.Type {
		case OTelProcessorBatch:
			cd.processors[pc.Name] = &batchProcessor{name: pc.Name, batchSize: 1000}
		case OTelProcessorFilter:
			cd.processors[pc.Name] = &filterProcessor{name: pc.Name}
		case OTelProcessorTransform:
			cd.processors[pc.Name] = &transformProcessor{name: pc.Name}
		case OTelProcessorMemLimit:
			cd.processors[pc.Name] = &memLimitProcessor{name: pc.Name, limitMiB: 512}
		}
	}

	// Register built-in exporters
	for _, ec := range config.Exporters {
		if !ec.Enabled {
			continue
		}
		switch ec.Type {
		case OTelExporterChronicle:
			cd.exporters[ec.Name] = &chronicleExporter{name: ec.Name, db: db}
		case OTelExporterConsole:
			cd.exporters[ec.Name] = &consoleExporter{name: ec.Name}
		case OTelExporterS3:
			cd.exporters[ec.Name] = &s3Exporter{name: ec.Name}
		}
	}

	return cd
}

// Start starts the collector distribution.
func (cd *OTelCollectorDistro) Start() error {
	if !cd.config.Enabled {
		return nil
	}
	if cd.running.Swap(true) {
		return fmt.Errorf("collector distro: already running")
	}

	cd.startTime = time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	cd.cancel = cancel

	// Start receivers
	for _, r := range cd.receivers {
		if err := r.Start(ctx); err != nil {
			cancel()
			return fmt.Errorf("collector distro: start receiver %s: %w", r.Name(), err)
		}
	}

	return nil
}

// Stop stops the collector distribution.
func (cd *OTelCollectorDistro) Stop() error {
	if !cd.running.Swap(false) {
		return nil
	}
	if cd.cancel != nil {
		cd.cancel()
	}
	for _, r := range cd.receivers {
		r.Stop()
	}
	cd.wg.Wait()
	return nil
}

// Ingest processes incoming data through the pipeline.
func (cd *OTelCollectorDistro) Ingest(ctx context.Context, pipelineName string, points []Point) error {
	if !cd.running.Load() {
		return fmt.Errorf("collector distro: not running")
	}
	if len(points) == 0 {
		return nil
	}

	cd.totalReceived.Add(uint64(len(points)))

	// Find pipeline
	var pipeline *CollectorPipeline
	cd.mu.RLock()
	for i := range cd.config.Pipelines {
		if cd.config.Pipelines[i].Name == pipelineName && cd.config.Pipelines[i].Enabled {
			pipeline = &cd.config.Pipelines[i]
			break
		}
	}
	cd.mu.RUnlock()

	if pipeline == nil {
		cd.totalDropped.Add(uint64(len(points)))
		return fmt.Errorf("collector distro: pipeline %q not found or disabled", pipelineName)
	}

	// Process through processors
	processed := points
	for _, procName := range pipeline.Processors {
		proc, ok := cd.processors[procName]
		if !ok {
			continue
		}
		var err error
		processed, err = proc.Process(processed)
		if err != nil {
			cd.totalErrors.Add(1)
			return fmt.Errorf("collector distro: processor %s: %w", procName, err)
		}
	}
	cd.totalProcessed.Add(uint64(len(processed)))

	// Export through exporters
	for _, expName := range pipeline.Exporters {
		exp, ok := cd.exporters[expName]
		if !ok {
			continue
		}
		if err := exp.Export(ctx, processed); err != nil {
			cd.totalErrors.Add(1)
			return fmt.Errorf("collector distro: exporter %s: %w", expName, err)
		}
	}
	cd.totalExported.Add(uint64(len(processed)))

	return nil
}

// GetStats returns collector distribution statistics.
func (cd *OTelCollectorDistro) GetStats() map[string]interface{} {
	var uptime time.Duration
	if !cd.startTime.IsZero() {
		uptime = time.Since(cd.startTime)
	}

	cd.mu.RLock()
	numPipelines := 0
	for _, p := range cd.config.Pipelines {
		if p.Enabled {
			numPipelines++
		}
	}
	cd.mu.RUnlock()

	return map[string]interface{}{
		"service_name":     cd.config.ServiceName,
		"running":          cd.running.Load(),
		"uptime":           uptime.String(),
		"total_received":   cd.totalReceived.Load(),
		"total_processed":  cd.totalProcessed.Load(),
		"total_exported":   cd.totalExported.Load(),
		"total_dropped":    cd.totalDropped.Load(),
		"total_errors":     cd.totalErrors.Load(),
		"active_pipelines": numPipelines,
		"receivers":        len(cd.receivers),
		"processors":       len(cd.processors),
		"exporters":        len(cd.exporters),
	}
}

// GetConfig returns the collector configuration as a structured map.
func (cd *OTelCollectorDistro) GetConfig() map[string]interface{} {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	rcvrs := make(map[string]interface{})
	for _, r := range cd.config.Receivers {
		rcvrs[r.Name] = map[string]interface{}{
			"type":     string(r.Type),
			"endpoint": r.Endpoint,
			"enabled":  r.Enabled,
		}
	}

	procs := make(map[string]interface{})
	for _, p := range cd.config.Processors {
		procs[p.Name] = map[string]interface{}{
			"type":     string(p.Type),
			"enabled":  p.Enabled,
			"settings": p.Settings,
		}
	}

	exps := make(map[string]interface{})
	for _, e := range cd.config.Exporters {
		exps[e.Name] = map[string]interface{}{
			"type":     string(e.Type),
			"endpoint": e.Endpoint,
			"enabled":  e.Enabled,
		}
	}

	pipes := make(map[string]interface{})
	for _, p := range cd.config.Pipelines {
		pipes[p.Name] = map[string]interface{}{
			"signal_type": p.SignalType,
			"receivers":   p.Receivers,
			"processors":  p.Processors,
			"exporters":   p.Exporters,
			"enabled":     p.Enabled,
		}
	}

	return map[string]interface{}{
		"receivers":  rcvrs,
		"processors": procs,
		"exporters":  exps,
		"pipelines":  pipes,
	}
}

// RegisterHTTPHandlers registers collector distribution HTTP endpoints.
func (cd *OTelCollectorDistro) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/collector/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cd.GetConfig())
	})
	mux.HandleFunc("/api/v1/collector/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cd.GetStats())
	})
	mux.HandleFunc("/api/v1/collector/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Pipeline string  `json:"pipeline"`
			Points   []Point `json:"points"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if req.Pipeline == "" {
			req.Pipeline = "metrics/default"
		}
		if err := cd.Ingest(r.Context(), req.Pipeline, req.Points); err != nil {
			log.Printf("otel ingest error: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
}

// Built-in processor implementations

type batchProcessor struct {
	name      string
	batchSize int
}

func (p *batchProcessor) Process(points []Point) ([]Point, error) {
	return points, nil // pass-through in-process; batching is handled at flush
}
func (p *batchProcessor) Name() string             { return p.name }
func (p *batchProcessor) Type() OTelProcessorType  { return OTelProcessorBatch }

type filterProcessor struct {
	name       string
	includes   []string
	excludes   []string
}

func (p *filterProcessor) Process(points []Point) ([]Point, error) {
	if len(p.excludes) == 0 && len(p.includes) == 0 {
		return points, nil
	}
	result := make([]Point, 0, len(points))
	for i := range points {
		if p.shouldInclude(points[i].Metric) {
			result = append(result, points[i])
		}
	}
	return result, nil
}

func (p *filterProcessor) shouldInclude(metric string) bool {
	if len(p.excludes) > 0 {
		for _, ex := range p.excludes {
			if strings.Contains(metric, ex) {
				return false
			}
		}
	}
	if len(p.includes) > 0 {
		for _, inc := range p.includes {
			if strings.Contains(metric, inc) {
				return true
			}
		}
		return false
	}
	return true
}
func (p *filterProcessor) Name() string             { return p.name }
func (p *filterProcessor) Type() OTelProcessorType  { return OTelProcessorFilter }

type transformProcessor struct {
	name   string
	prefix string
}

func (p *transformProcessor) Process(points []Point) ([]Point, error) {
	if p.prefix == "" {
		return points, nil
	}
	for i := range points {
		points[i].Metric = p.prefix + points[i].Metric
	}
	return points, nil
}
func (p *transformProcessor) Name() string             { return p.name }
func (p *transformProcessor) Type() OTelProcessorType  { return OTelProcessorTransform }

type memLimitProcessor struct {
	name     string
	limitMiB int
}

func (p *memLimitProcessor) Process(points []Point) ([]Point, error) {
	return points, nil // placeholder: actual memory tracking done at runtime level
}
func (p *memLimitProcessor) Name() string             { return p.name }
func (p *memLimitProcessor) Type() OTelProcessorType  { return OTelProcessorMemLimit }

// Built-in exporter implementations

type chronicleExporter struct {
	name string
	db   *DB
}

func (e *chronicleExporter) Export(ctx context.Context, points []Point) error {
	if len(points) == 0 {
		return nil
	}
	return e.db.WriteBatch(points)
}
func (e *chronicleExporter) Name() string           { return e.name }
func (e *chronicleExporter) Type() OTelExporterType { return OTelExporterChronicle }

type consoleExporter struct {
	name string
}

func (e *consoleExporter) Export(_ context.Context, points []Point) error {
	for _, p := range points {
		fmt.Printf("[console-export] %s %v %f @%d\n", p.Metric, p.Tags, p.Value, p.Timestamp)
	}
	return nil
}
func (e *consoleExporter) Name() string           { return e.name }
func (e *consoleExporter) Type() OTelExporterType { return OTelExporterConsole }

type s3Exporter struct {
	name     string
	bucket   string
	prefix   string
	region   string
	buffered []Point
	mu       sync.Mutex
}

func (e *s3Exporter) Export(_ context.Context, points []Point) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.buffered = append(e.buffered, points...)
	// In production: flush to S3 as Parquet/JSON when buffer reaches threshold
	return nil
}
func (e *s3Exporter) Name() string           { return e.name }
func (e *s3Exporter) Type() OTelExporterType { return OTelExporterS3 }

// AddCollectorPipeline adds a pipeline to the running collector.
func (cd *OTelCollectorDistro) AddCollectorPipeline(pipeline CollectorPipeline) {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	cd.config.Pipelines = append(cd.config.Pipelines, pipeline)
}

// RemoveCollectorPipeline removes a pipeline by name.
func (cd *OTelCollectorDistro) RemoveCollectorPipeline(name string) bool {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	for i, p := range cd.config.Pipelines {
		if p.Name == name {
			cd.config.Pipelines = append(cd.config.Pipelines[:i], cd.config.Pipelines[i+1:]...)
			return true
		}
	}
	return false
}

// AddProcessor registers a custom processor.
func (cd *OTelCollectorDistro) AddProcessor(p CollectorProcessor) {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	cd.processors[p.Name()] = p
}

// AddExporter registers a custom exporter.
func (cd *OTelCollectorDistro) AddExporter(e CollectorExporter) {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	cd.exporters[e.Name()] = e
}

// GenerateDockerfile returns a Dockerfile for the Chronicle collector distribution.
func (cd *OTelCollectorDistro) GenerateDockerfile() string {
	return `FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /chronicle-collector ./cmd/chronicle-collector

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /chronicle-collector /usr/local/bin/chronicle-collector
EXPOSE 4317 4318 8888
ENTRYPOINT ["/usr/local/bin/chronicle-collector"]
`
}

// GenerateHelmValues returns default Helm chart values.
func (cd *OTelCollectorDistro) GenerateHelmValues() map[string]interface{} {
	return map[string]interface{}{
		"replicaCount": 1,
		"image": map[string]interface{}{
			"repository": "chronicle-db/chronicle-collector",
			"tag":        "latest",
			"pullPolicy": "IfNotPresent",
		},
		"service": map[string]interface{}{
			"type": "ClusterIP",
			"ports": map[string]interface{}{
				"grpc": 4317,
				"http": 4318,
			},
		},
		"resources": map[string]interface{}{
			"limits": map[string]interface{}{
				"cpu":    "500m",
				"memory": "512Mi",
			},
			"requests": map[string]interface{}{
				"cpu":    "100m",
				"memory": "128Mi",
			},
		},
		"config": cd.GetConfig(),
	}
}

// GenerateK8sDaemonSetManifest returns a K8s DaemonSet YAML manifest.
func (cd *OTelCollectorDistro) GenerateK8sDaemonSetManifest() string {
	return `apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: chronicle-collector
  namespace: monitoring
  labels:
    app: chronicle-collector
spec:
  selector:
    matchLabels:
      app: chronicle-collector
  template:
    metadata:
      labels:
        app: chronicle-collector
    spec:
      containers:
      - name: collector
        image: chronicle-db/chronicle-collector:latest
        ports:
        - containerPort: 4317
          name: otlp-grpc
          protocol: TCP
        - containerPort: 4318
          name: otlp-http
          protocol: TCP
        - containerPort: 8888
          name: metrics
          protocol: TCP
        resources:
          limits:
            cpu: 500m
            memory: 512Mi
          requests:
            cpu: 100m
            memory: 128Mi
        livenessProbe:
          httpGet:
            path: /api/v1/collector/stats
            port: 8888
          initialDelaySeconds: 5
          periodSeconds: 10
`
}
