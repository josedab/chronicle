package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// OTelDistroConfig configures the Chronicle OpenTelemetry distribution.
type OTelDistroConfig struct {
	Enabled          bool          `json:"enabled"`
	ListenAddr       string        `json:"listen_addr"`
	GRPCPort         int           `json:"grpc_port"`
	HTTPPort         int           `json:"http_port"`
	BatchSize        int           `json:"batch_size"`
	FlushInterval    time.Duration `json:"flush_interval"`
	MaxQueueSize     int           `json:"max_queue_size"`
	EnableMetrics    bool          `json:"enable_metrics"`
	EnableTraces     bool          `json:"enable_traces"`
	EnableLogs       bool          `json:"enable_logs"`
	ResourceAttrs    map[string]string `json:"resource_attrs,omitempty"`
	DefaultAlertRules bool         `json:"default_alert_rules"`
}

// DefaultOTelDistroConfig returns sensible defaults for the OTel distribution.
func DefaultOTelDistroConfig() OTelDistroConfig {
	return OTelDistroConfig{
		Enabled:          true,
		ListenAddr:       "0.0.0.0",
		GRPCPort:         4317,
		HTTPPort:         4318,
		BatchSize:        1000,
		FlushInterval:    5 * time.Second,
		MaxQueueSize:     100000,
		EnableMetrics:    true,
		EnableTraces:     false,
		EnableLogs:       false,
		DefaultAlertRules: true,
	}
}

// OTelDistroStats tracks runtime statistics for the distribution.
type OTelDistroStats struct {
	MetricsReceived  uint64        `json:"metrics_received"`
	MetricsExported  uint64        `json:"metrics_exported"`
	MetricsDropped   uint64        `json:"metrics_dropped"`
	BatchesFlushed   uint64        `json:"batches_flushed"`
	Errors           uint64        `json:"errors"`
	Uptime           time.Duration `json:"uptime"`
	LastFlush        time.Time     `json:"last_flush"`
	QueueDepth       int           `json:"queue_depth"`
	ActivePipelines  int           `json:"active_pipelines"`
}

// OTelPipelineConfig defines a single OTel pipeline.
type OTelPipelineConfig struct {
	Name       string   `json:"name"`
	SignalType string   `json:"signal_type"`
	Receivers  []string `json:"receivers"`
	Processors []string `json:"processors"`
	Exporters  []string `json:"exporters"`
	Enabled    bool     `json:"enabled"`
}

// OTelDistro is a pre-configured OpenTelemetry collector distribution
// that uses Chronicle as its storage backend.
type OTelDistro struct {
	db     *DB
	config OTelDistroConfig

	pipelines []OTelPipelineConfig
	queue     chan *OTelMetricBatch
	startTime time.Time

	metricsReceived atomic.Uint64
	metricsExported atomic.Uint64
	metricsDropped  atomic.Uint64
	batchesFlushed  atomic.Uint64
	errors          atomic.Uint64

	running atomic.Bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex
}

// OTelMetricBatch is a batch of metric data points for processing.
type OTelMetricBatch struct {
	Points    []Point   `json:"points"`
	Received  time.Time `json:"received"`
	Source    string    `json:"source"`
}

// NewOTelDistro creates a new OpenTelemetry distribution with Chronicle backend.
func NewOTelDistro(db *DB, cfg OTelDistroConfig) *OTelDistro {
	d := &OTelDistro{
		db:     db,
		config: cfg,
		queue:  make(chan *OTelMetricBatch, cfg.MaxQueueSize),
	}
	d.pipelines = d.defaultPipelines()
	return d
}

func (d *OTelDistro) defaultPipelines() []OTelPipelineConfig {
	var pipelines []OTelPipelineConfig
	if d.config.EnableMetrics {
		pipelines = append(pipelines, OTelPipelineConfig{
			Name:       "metrics",
			SignalType: "metrics",
			Receivers:  []string{"otlp"},
			Processors: []string{"batch", "attributes"},
			Exporters:  []string{"chronicle"},
			Enabled:    true,
		})
	}
	if d.config.EnableTraces {
		pipelines = append(pipelines, OTelPipelineConfig{
			Name:       "traces",
			SignalType: "traces",
			Receivers:  []string{"otlp"},
			Processors: []string{"batch"},
			Exporters:  []string{"chronicle"},
			Enabled:    true,
		})
	}
	if d.config.EnableLogs {
		pipelines = append(pipelines, OTelPipelineConfig{
			Name:       "logs",
			SignalType: "logs",
			Receivers:  []string{"otlp"},
			Processors: []string{"batch"},
			Exporters:  []string{"chronicle"},
			Enabled:    true,
		})
	}
	return pipelines
}

// Start starts the OTel distribution background processing.
func (d *OTelDistro) Start() error {
	if !d.config.Enabled {
		return nil
	}
	if d.running.Swap(true) {
		return fmt.Errorf("otel distro: already running")
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.startTime = time.Now()

	// Start flush worker
	d.wg.Add(1)
	go d.flushWorker(ctx)

	return nil
}

// Stop stops the OTel distribution.
func (d *OTelDistro) Stop() error {
	if !d.running.Swap(false) {
		return nil
	}
	if d.cancel != nil {
		d.cancel()
	}
	d.wg.Wait()
	return nil
}

func (d *OTelDistro) flushWorker(ctx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(d.config.FlushInterval)
	defer ticker.Stop()

	var batch []Point
	for {
		select {
		case <-ctx.Done():
			d.flushBatch(batch)
			return
		case mb := <-d.queue:
			batch = append(batch, mb.Points...)
			d.metricsReceived.Add(uint64(len(mb.Points)))
			if len(batch) >= d.config.BatchSize {
				d.flushBatch(batch)
				batch = nil
			}
		case <-ticker.C:
			if len(batch) > 0 {
				d.flushBatch(batch)
				batch = nil
			}
		}
	}
}

func (d *OTelDistro) flushBatch(points []Point) {
	if len(points) == 0 {
		return
	}
	if err := d.db.WriteBatch(points); err != nil {
		d.errors.Add(1)
		d.metricsDropped.Add(uint64(len(points)))
		return
	}
	d.metricsExported.Add(uint64(len(points)))
	d.batchesFlushed.Add(1)
}

// Push adds a batch of metric points to the processing queue.
func (d *OTelDistro) Push(batch *OTelMetricBatch) error {
	if !d.running.Load() {
		return fmt.Errorf("otel distro: not running")
	}
	select {
	case d.queue <- batch:
		return nil
	default:
		d.metricsDropped.Add(uint64(len(batch.Points)))
		return fmt.Errorf("otel distro: queue full, dropped %d points", len(batch.Points))
	}
}

// AddPipeline adds a custom pipeline configuration.
func (d *OTelDistro) AddPipeline(pipeline OTelPipelineConfig) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pipelines = append(d.pipelines, pipeline)
}

// ListPipelines returns all configured pipelines.
func (d *OTelDistro) ListPipelines() []OTelPipelineConfig {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]OTelPipelineConfig, len(d.pipelines))
	copy(out, d.pipelines)
	return out
}

// Stats returns the current runtime statistics.
func (d *OTelDistro) Stats() OTelDistroStats {
	var uptime time.Duration
	if !d.startTime.IsZero() {
		uptime = time.Since(d.startTime)
	}
	d.mu.RLock()
	activePipelines := 0
	for _, p := range d.pipelines {
		if p.Enabled {
			activePipelines++
		}
	}
	d.mu.RUnlock()

	return OTelDistroStats{
		MetricsReceived: d.metricsReceived.Load(),
		MetricsExported: d.metricsExported.Load(),
		MetricsDropped:  d.metricsDropped.Load(),
		BatchesFlushed:  d.batchesFlushed.Load(),
		Errors:          d.errors.Load(),
		Uptime:          uptime,
		QueueDepth:      len(d.queue),
		ActivePipelines: activePipelines,
	}
}

// RegisterHTTPHandlers registers HTTP endpoints for the OTel distribution.
func (d *OTelDistro) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/otel/push", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var batch OTelMetricBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		batch.Received = time.Now()
		if err := d.Push(&batch); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/api/v1/otel/pipelines", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(d.ListPipelines())
	})
	mux.HandleFunc("/api/v1/otel/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(d.Stats())
	})
}

// GenerateConfig generates a YAML-like configuration for the OTel distribution.
func (d *OTelDistro) GenerateConfig() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()

	pipelinesMap := make(map[string]interface{})
	for _, p := range d.pipelines {
		pipelinesMap[p.Name] = map[string]interface{}{
			"receivers":  p.Receivers,
			"processors": p.Processors,
			"exporters":  p.Exporters,
		}
	}

	return map[string]interface{}{
		"receivers": map[string]interface{}{
			"otlp": map[string]interface{}{
				"protocols": map[string]interface{}{
					"grpc": map[string]interface{}{"endpoint": fmt.Sprintf("%s:%d", d.config.ListenAddr, d.config.GRPCPort)},
					"http": map[string]interface{}{"endpoint": fmt.Sprintf("%s:%d", d.config.ListenAddr, d.config.HTTPPort)},
				},
			},
		},
		"exporters": map[string]interface{}{
			"chronicle": map[string]interface{}{
				"batch_size":     d.config.BatchSize,
				"flush_interval": d.config.FlushInterval.String(),
			},
		},
		"service": map[string]interface{}{
			"pipelines": pipelinesMap,
		},
	}
}
