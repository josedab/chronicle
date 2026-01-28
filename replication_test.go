package chronicle

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestReplicator_NewReplicator(t *testing.T) {
	tests := []struct {
		name          string
		cfg           *ReplicationConfig
		wantBatchSize int
		wantTimeout   time.Duration
		wantFlush     time.Duration
	}{
		{
			name:          "default values",
			cfg:           &ReplicationConfig{Enabled: true, TargetURL: "http://localhost"},
			wantBatchSize: 1000,
			wantTimeout:   10 * time.Second,
			wantFlush:     2 * time.Second,
		},
		{
			name: "custom values",
			cfg: &ReplicationConfig{
				Enabled:       true,
				TargetURL:     "http://localhost",
				BatchSize:     500,
				Timeout:       5 * time.Second,
				FlushInterval: 1 * time.Second,
			},
			wantBatchSize: 500,
			wantTimeout:   5 * time.Second,
			wantFlush:     1 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newReplicator(tt.cfg)
			if r.cfg.BatchSize != tt.wantBatchSize {
				t.Errorf("BatchSize = %d, want %d", r.cfg.BatchSize, tt.wantBatchSize)
			}
			if r.cfg.Timeout != tt.wantTimeout {
				t.Errorf("Timeout = %v, want %v", r.cfg.Timeout, tt.wantTimeout)
			}
			if r.cfg.FlushInterval != tt.wantFlush {
				t.Errorf("FlushInterval = %v, want %v", r.cfg.FlushInterval, tt.wantFlush)
			}
		})
	}
}

func TestReplicator_MetricsAllowFilter(t *testing.T) {
	cfg := &ReplicationConfig{
		Enabled:      true,
		TargetURL:    "http://localhost",
		MetricsAllow: []string{"cpu", "memory"},
	}
	r := newReplicator(cfg)

	if r.allow == nil {
		t.Fatal("allow map should be initialized")
	}
	if _, ok := r.allow["cpu"]; !ok {
		t.Error("cpu should be in allow map")
	}
	if _, ok := r.allow["memory"]; !ok {
		t.Error("memory should be in allow map")
	}
	if _, ok := r.allow["disk"]; ok {
		t.Error("disk should not be in allow map")
	}
}

func TestReplicator_EnqueueFiltering(t *testing.T) {
	cfg := &ReplicationConfig{
		Enabled:      true,
		TargetURL:    "http://localhost",
		MetricsAllow: []string{"cpu"},
		BatchSize:    100,
	}
	r := newReplicator(cfg)

	points := []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: time.Now().UnixNano()},
		{Metric: "memory", Value: 2.0, Timestamp: time.Now().UnixNano()}, // filtered
		{Metric: "cpu", Value: 3.0, Timestamp: time.Now().UnixNano()},
	}

	r.Enqueue(points)

	// Should only have 2 points (cpu metrics)
	count := 0
	timeout := time.After(100 * time.Millisecond)
	for {
		select {
		case <-r.queue:
			count++
		case <-timeout:
			if count != 2 {
				t.Errorf("expected 2 points in queue, got %d", count)
			}
			return
		}
	}
}

func TestReplicator_EnqueueNoFilter(t *testing.T) {
	cfg := &ReplicationConfig{
		Enabled:   true,
		TargetURL: "http://localhost",
		BatchSize: 100,
	}
	r := newReplicator(cfg)

	points := []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: time.Now().UnixNano()},
		{Metric: "memory", Value: 2.0, Timestamp: time.Now().UnixNano()},
		{Metric: "disk", Value: 3.0, Timestamp: time.Now().UnixNano()},
	}

	r.Enqueue(points)

	// All 3 points should be queued
	count := 0
	timeout := time.After(100 * time.Millisecond)
	for {
		select {
		case <-r.queue:
			count++
		case <-timeout:
			if count != 3 {
				t.Errorf("expected 3 points in queue, got %d", count)
			}
			return
		}
	}
}

func TestReplicator_FlushToServer(t *testing.T) {
	var mu sync.Mutex
	var receivedPoints []Point

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/write" {
			t.Errorf("expected /write path, got %s", r.URL.Path)
		}
		if r.Header.Get("Content-Encoding") != "gzip" {
			t.Error("expected gzip encoding")
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("expected application/json content type")
		}

		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("failed to create gzip reader: %v", err)
			return
		}
		defer gz.Close()

		body, err := io.ReadAll(gz)
		if err != nil {
			t.Errorf("failed to read body: %v", err)
			return
		}

		var req writeRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("failed to unmarshal: %v", err)
			return
		}

		mu.Lock()
		receivedPoints = append(receivedPoints, req.Points...)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &ReplicationConfig{
		Enabled:       true,
		TargetURL:     server.URL,
		BatchSize:     2,
		FlushInterval: 50 * time.Millisecond,
		Timeout:       5 * time.Second,
	}
	r := newReplicator(cfg)
	r.Start()
	defer r.Stop()

	points := []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: time.Now().UnixNano()},
		{Metric: "cpu", Value: 2.0, Timestamp: time.Now().UnixNano()},
	}
	r.Enqueue(points)

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(receivedPoints) != 2 {
		t.Errorf("expected 2 points, got %d", len(receivedPoints))
	}
}

func TestReplicator_BatchSizeTrigger(t *testing.T) {
	flushCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		flushCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &ReplicationConfig{
		Enabled:       true,
		TargetURL:     server.URL,
		BatchSize:     5,
		FlushInterval: 10 * time.Second, // Long interval to ensure batch size triggers
		Timeout:       5 * time.Second,
	}
	r := newReplicator(cfg)
	r.Start()
	defer r.Stop()

	// Enqueue exactly batch size
	points := make([]Point, 5)
	for i := range points {
		points[i] = Point{Metric: "test", Value: float64(i), Timestamp: time.Now().UnixNano()}
	}
	r.Enqueue(points)

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if flushCount != 1 {
		t.Errorf("expected 1 flush, got %d", flushCount)
	}
}

func TestReplicator_StopFlushesRemaining(t *testing.T) {
	var mu sync.Mutex
	var receivedPoints []Point

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gz, _ := gzip.NewReader(r.Body)
		body, _ := io.ReadAll(gz)
		gz.Close()

		var req writeRequest
		json.Unmarshal(body, &req)

		mu.Lock()
		receivedPoints = append(receivedPoints, req.Points...)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &ReplicationConfig{
		Enabled:       true,
		TargetURL:     server.URL,
		BatchSize:     100, // Large batch size
		FlushInterval: 10 * time.Second,
		Timeout:       5 * time.Second,
	}
	r := newReplicator(cfg)
	r.Start()

	// Enqueue some points (less than batch size)
	points := []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: time.Now().UnixNano()},
		{Metric: "cpu", Value: 2.0, Timestamp: time.Now().UnixNano()},
	}
	r.Enqueue(points)

	// Give time for points to be read from queue
	time.Sleep(50 * time.Millisecond)

	// Stop should flush remaining
	r.Stop()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(receivedPoints) != 2 {
		t.Errorf("expected 2 points after stop, got %d", len(receivedPoints))
	}
}

func TestReplicator_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := &ReplicationConfig{
		Enabled:       true,
		TargetURL:     server.URL,
		BatchSize:     1,
		FlushInterval: 50 * time.Millisecond,
		Timeout:       5 * time.Second,
	}
	r := newReplicator(cfg)
	r.Start()
	defer r.Stop()

	// Should not panic on server error
	r.Enqueue([]Point{{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano()}})
	time.Sleep(100 * time.Millisecond)
}

func TestReplicator_EmptyTargetURL(t *testing.T) {
	cfg := &ReplicationConfig{
		Enabled:   true,
		TargetURL: "", // Empty URL
		BatchSize: 1,
	}
	r := newReplicator(cfg)

	// flush should not panic with empty URL
	r.flush([]Point{{Metric: "test", Value: 1.0}})
}

func TestReplicator_EmptyBatch(t *testing.T) {
	cfg := &ReplicationConfig{
		Enabled:   true,
		TargetURL: "http://localhost",
		BatchSize: 1,
	}
	r := newReplicator(cfg)

	// flush should not panic with empty batch
	r.flush([]Point{})
}
