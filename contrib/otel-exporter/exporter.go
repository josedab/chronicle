// Package chronicleexporter provides an OpenTelemetry Collector exporter
// that writes metrics to a Chronicle time-series database via its HTTP API.
//
// This package follows the OTel Collector component model:
//
//	factory := chronicleexporter.NewFactory()
//	cfg := factory.CreateDefaultConfig()
//	exporter, err := factory.CreateMetrics(ctx, set, cfg)
//
// See the README for full collector integration instructions.
package chronicleexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Config holds the exporter configuration.
type Config struct {
	// Endpoint is the Chronicle HTTP API URL (e.g., "http://localhost:8086").
	Endpoint string `mapstructure:"endpoint"`

	// BatchSize is the number of points to batch before sending.
	BatchSize int `mapstructure:"batch_size"`

	// FlushInterval is how often to flush batched points.
	FlushInterval time.Duration `mapstructure:"flush_interval"`

	// Timeout for HTTP requests to Chronicle.
	Timeout time.Duration `mapstructure:"timeout"`

	// Headers are additional HTTP headers (e.g., for authentication).
	Headers map[string]string `mapstructure:"headers"`

	// RetryEnabled enables automatic retry on failure.
	RetryEnabled bool `mapstructure:"retry_enabled"`

	// MaxRetries is the maximum retry attempts.
	MaxRetries int `mapstructure:"max_retries"`
}

// DefaultConfig returns the default exporter configuration.
func DefaultConfig() *Config {
	return &Config{
		Endpoint:      "http://localhost:8086",
		BatchSize:     1000,
		FlushInterval: 10 * time.Second,
		Timeout:       30 * time.Second,
		RetryEnabled:  true,
		MaxRetries:    3,
	}
}

// Point mirrors the Chronicle Point type for JSON serialization.
type Point struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags,omitempty"`
	Value     float64           `json:"value"`
	Timestamp int64             `json:"timestamp"`
}

// Exporter sends metrics to Chronicle.
type Exporter struct {
	config *Config
	client *http.Client
	batch  []Point
	mu     sync.Mutex
	stopCh chan struct{}
}

// NewExporter creates a new Chronicle exporter.
func NewExporter(cfg *Config) (*Exporter, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("chronicle exporter: endpoint is required")
	}
	return &Exporter{
		config: cfg,
		client: &http.Client{Timeout: cfg.Timeout},
		batch:  make([]Point, 0, cfg.BatchSize),
		stopCh: make(chan struct{}),
	}, nil
}

// Start begins the background flush loop.
func (e *Exporter) Start(_ context.Context) error {
	go e.flushLoop()
	return nil
}

// Shutdown flushes remaining points and stops the exporter.
func (e *Exporter) Shutdown(ctx context.Context) error {
	close(e.stopCh)
	return e.flush(ctx)
}

// AddPoints adds points to the batch, flushing when full.
func (e *Exporter) AddPoints(ctx context.Context, points []Point) error {
	e.mu.Lock()
	e.batch = append(e.batch, points...)
	shouldFlush := len(e.batch) >= e.config.BatchSize
	e.mu.Unlock()

	if shouldFlush {
		return e.flush(ctx)
	}
	return nil
}

func (e *Exporter) flushLoop() {
	ticker := time.NewTicker(e.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = e.flush(context.Background())
		case <-e.stopCh:
			return
		}
	}
}

func (e *Exporter) flush(ctx context.Context) error {
	e.mu.Lock()
	if len(e.batch) == 0 {
		e.mu.Unlock()
		return nil
	}
	points := e.batch
	e.batch = make([]Point, 0, e.config.BatchSize)
	e.mu.Unlock()

	return e.send(ctx, points)
}

func (e *Exporter) send(ctx context.Context, points []Point) error {
	body, err := json.Marshal(points)
	if err != nil {
		return fmt.Errorf("marshal points: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.config.Endpoint+"/write", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range e.config.Headers {
		req.Header.Set(k, v)
	}

	var lastErr error
	attempts := 1
	if e.config.RetryEnabled {
		attempts = e.config.MaxRetries
	}

	for i := 0; i < attempts; i++ {
		resp, err := e.client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		lastErr = fmt.Errorf("chronicle returned status %d", resp.StatusCode)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return lastErr // Don't retry client errors
		}
	}
	return lastErr
}

// Factory creates Chronicle exporter instances following the OTel Collector pattern.
type Factory struct{}

// NewFactory creates a new exporter factory.
func NewFactory() *Factory {
	return &Factory{}
}

// Type returns the component type identifier.
func (f *Factory) Type() string {
	return "chronicle"
}

// CreateDefaultConfig returns the default exporter configuration.
func (f *Factory) CreateDefaultConfig() *Config {
	return DefaultConfig()
}

// CreateExporter creates a new Chronicle exporter from config.
func (f *Factory) CreateExporter(cfg *Config) (*Exporter, error) {
	return NewExporter(cfg)
}
