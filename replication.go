package chronicle

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
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
	cfg      *ReplicationConfig
	queue    chan Point
	stop     chan struct{}
	stopOnce sync.Once
	client   HTTPDoer
	allow    map[string]struct{}
	retryer  *Retryer
	cb       *CircuitBreaker

	// Dead-letter queue for points that failed replication (e.g., circuit breaker open).
	// Bounded to prevent OOM; oldest points are dropped when full.
	dlqMu      sync.Mutex
	deadLetter []Point
	dlqMax     int

	// Counters for observability
	droppedPoints atomic.Int64
}

func newReplicator(cfg *ReplicationConfig) *replicator {
	r := &replicator{
		cfg:    cfg,
		queue:  make(chan Point, 10000),
		stop:   make(chan struct{}),
		dlqMax: 50000, // bounded dead letter queue: ~50k points
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
	r.stopOnce.Do(func() {
		close(r.stop)
	})
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
		case <-r.stop:
			return
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
			// Drain any remaining items from the queue before final flush
			for {
				select {
				case p := <-r.queue:
					batch = append(batch, p)
				default:
					goto done
				}
			}
		done:
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
			// Retry dead-letter points when circuit may have recovered
			r.retryDLQ()
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
		if closeErr := gz.Close(); closeErr != nil {
			slog.Warn("replication: gzip close error after write failure", "err", closeErr)
		}
		slog.Error("replication compress error", "err", err)
		return
	}
	if err := gz.Close(); err != nil {
		slog.Warn("replication: gzip close error", "err", err)
	}

	compressedPayload := buf.Bytes()

	// Use circuit breaker and retry logic
	err = r.cb.Execute(func() error {
		return r.sendWithRetry(compressedPayload)
	})

	if err != nil {
		r.enqueueDLQ(points)
		if err == ErrCircuitOpen {
			slog.Warn("replication circuit breaker open, buffering points in DLQ", "count", len(points))
		}
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
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Warn("replication: failed to close response body", "err", err)
		}
	}()

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

// enqueueDLQ adds failed points to the dead-letter queue for later retry.
// If the DLQ is full, oldest points are dropped and counted.
func (r *replicator) enqueueDLQ(points []Point) {
	r.dlqMu.Lock()
	defer r.dlqMu.Unlock()

	r.deadLetter = append(r.deadLetter, points...)

	if len(r.deadLetter) > r.dlqMax {
		excess := len(r.deadLetter) - r.dlqMax
		r.deadLetter = r.deadLetter[excess:]
		r.droppedPoints.Add(int64(excess))
		slog.Warn("replication DLQ overflow, oldest points dropped", "dropped", excess)
	}
}

// retryDLQ drains the dead-letter queue and attempts to re-flush those points.
func (r *replicator) retryDLQ() {
	r.dlqMu.Lock()
	if len(r.deadLetter) == 0 {
		r.dlqMu.Unlock()
		return
	}
	points := r.deadLetter
	r.deadLetter = nil
	r.dlqMu.Unlock()

	// Re-flush in batches to avoid oversized payloads
	for len(points) > 0 {
		end := r.cfg.BatchSize
		if end > len(points) {
			end = len(points)
		}
		r.flush(points[:end])
		points = points[end:]
	}
}

// DroppedPoints returns the total number of points permanently lost due to DLQ overflow.
func (r *replicator) DroppedPoints() int64 {
	return r.droppedPoints.Load()
}

// DLQLen returns the current number of points awaiting retry in the dead-letter queue.
func (r *replicator) DLQLen() int {
	r.dlqMu.Lock()
	defer r.dlqMu.Unlock()
	return len(r.deadLetter)
}
