package chronicle

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// ReplicationConfig defines outbound replication settings.
type ReplicationConfig struct {
	Enabled       bool
	TargetURL     string
	MetricsAllow  []string
	BatchSize     int
	MaxBatchBytes int
	FlushInterval time.Duration
	Timeout       time.Duration

	// Retry configuration
	MaxRetries   int           // Max retry attempts (default: 3)
	RetryBackoff time.Duration // Initial backoff (default: 100ms)

	// HTTPClient allows injecting a custom HTTP client for testing.
	// If nil, a default client is created with the configured timeout.
	HTTPClient HTTPDoer
}

type replicator struct {
	cfg     *ReplicationConfig
	queue   chan Point
	stop    chan struct{}
	client  HTTPDoer
	allow   map[string]struct{}
	retryer *Retryer
	cb      *CircuitBreaker
}

func newReplicator(cfg *ReplicationConfig) *replicator {
	r := &replicator{
		cfg:   cfg,
		queue: make(chan Point, 10000),
		stop:  make(chan struct{}),
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = 512 * 1024
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 100 * time.Millisecond
	}
	if len(cfg.MetricsAllow) > 0 {
		r.allow = make(map[string]struct{}, len(cfg.MetricsAllow))
		for _, m := range cfg.MetricsAllow {
			r.allow[m] = struct{}{}
		}
	}

	if cfg.HTTPClient != nil {
		r.client = cfg.HTTPClient
	} else {
		r.client = &http.Client{Timeout: cfg.Timeout}
	}
	r.retryer = NewRetryer(RetryConfig{
		MaxAttempts:       cfg.MaxRetries,
		InitialBackoff:    cfg.RetryBackoff,
		MaxBackoff:        30 * time.Second,
		BackoffMultiplier: 2.0,
		Jitter:            0.1,
		RetryIf:           IsRetryable,
	})
	r.cb = NewCircuitBreaker(5, 30*time.Second)

	return r
}

func (r *replicator) Start() {
	go r.loop()
}

func (r *replicator) Stop() {
	close(r.stop)
}

func (r *replicator) Enqueue(points []Point) {
	for _, p := range points {
		if r.allow != nil {
			if _, ok := r.allow[p.Metric]; !ok {
				continue
			}
		}
		select {
		case r.queue <- p:
		default:
		}
	}
}

func (r *replicator) loop() {
	ticker := time.NewTicker(r.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]Point, 0, r.cfg.BatchSize)
	for {
		select {
		case <-r.stop:
			r.flush(batch)
			return
		case p := <-r.queue:
			batch = append(batch, p)
			if len(batch) >= r.cfg.BatchSize {
				r.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				r.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

func (r *replicator) flush(points []Point) {
	if len(points) == 0 || r.cfg.TargetURL == "" {
		return
	}

	payload, err := json.Marshal(writeRequest{Points: points})
	if err != nil {
		slog.Error("replication marshal error", "err", err)
		return
	}

	if len(payload) > r.cfg.MaxBatchBytes {
		mid := len(points) / 2
		if mid > 0 {
			r.flush(points[:mid])
			r.flush(points[mid:])
		}
		return
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(payload); err != nil {
		_ = gz.Close()
		slog.Error("replication compress error", "err", err)
		return
	}
	_ = gz.Close()

	compressedPayload := buf.Bytes()

	// Use circuit breaker and retry logic
	err = r.cb.Execute(func() error {
		return r.sendWithRetry(compressedPayload)
	})

	if err != nil {
		if err == ErrCircuitOpen {
			slog.Warn("replication circuit breaker open, dropping points", "count", len(points))
		}
		// Error already logged in sendWithRetry
	}
}

func (r *replicator) sendWithRetry(payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout*time.Duration(r.cfg.MaxRetries))
	defer cancel()

	result := r.retryer.Do(ctx, func() error {
		return r.send(payload)
	})

	if result.LastErr != nil {
		slog.Error("replication failed", "attempts", result.Attempts, "err", result.LastErr)
	}

	return result.LastErr
}

func (r *replicator) send(payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.TargetURL+"/write", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 500 {
		return fmt.Errorf("server error: status %d", resp.StatusCode)
	}
	if resp.StatusCode == 429 {
		return fmt.Errorf("rate limited: status 429")
	}
	if resp.StatusCode >= 400 {
		// Client errors are not retryable
		slog.Warn("replication target returned client error", "status", resp.StatusCode)
		return nil
	}

	return nil
}
